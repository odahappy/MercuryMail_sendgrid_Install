import logging
from datetime import datetime, timedelta
from secrets import compare_digest

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.exc import SQLAlchemyError
from jose import jwt
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import bump_user_token_version, require_admin
from app.core.config import settings
from app.core.database import commit_session_with_retry, get_db
from app.core.redis import redis_client
from app.models.models import SystemConfig, User
from app.utils.security import hash_password, verify_password

router = APIRouter(prefix="/api/auth", tags=["auth"])
logger = logging.getLogger(__name__)
ADMIN_PASSWORD_HASH_KEY = "admin_password_hash"
ADMIN_PASSWORD_CUSTOMIZED_KEY = "admin_password_customized"
LOGIN_FAILURE_LIMIT = 10
LOGIN_FAILURE_WINDOW_SECONDS = 15 * 60
LOGIN_FAILURE_LOCK_SECONDS = 15 * 60


class LoginRequest(BaseModel):
    password: str = Field(..., min_length=1)


class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int


class ChangePasswordRequest(BaseModel):
    old_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=6, max_length=128)


class MessageResponse(BaseModel):
    success: bool
    message: str


def _login_rate_limit_key(client_host: str) -> str:
    safe_host = str(client_host or "unknown").replace(":", "_")[:120]
    return f"auth:login_fail:{safe_host}"


async def _ensure_login_not_locked(client_host: str) -> None:
    key = _login_rate_limit_key(client_host)
    try:
        failures = int(await redis_client.get(key) or 0)
        if failures >= LOGIN_FAILURE_LIMIT:
            ttl = await redis_client.ttl(key)
            retry_after = max(1, int(ttl if ttl and ttl > 0 else LOGIN_FAILURE_LOCK_SECONDS))
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"登录失败次数过多，请 {max(1, retry_after // 60)} 分钟后再试",
            )
    except HTTPException:
        raise
    except Exception:
        logger.warning("登录限流检查失败，已降级放行 client=%s", client_host, exc_info=True)


async def _record_login_failure(client_host: str) -> None:
    key = _login_rate_limit_key(client_host)
    try:
        failures = await redis_client.incr(key)
        if int(failures) == 1:
            await redis_client.expire(key, LOGIN_FAILURE_WINDOW_SECONDS)
        elif int(failures) >= LOGIN_FAILURE_LIMIT:
            await redis_client.expire(key, LOGIN_FAILURE_LOCK_SECONDS)
    except Exception:
        logger.warning("记录登录失败次数失败 client=%s", client_host, exc_info=True)


async def _clear_login_failures(client_host: str) -> None:
    try:
        await redis_client.delete(_login_rate_limit_key(client_host))
    except Exception:
        logger.warning("清理登录失败计数失败 client=%s", client_host, exc_info=True)


async def get_admin_password_hash(db: AsyncSession) -> str:
    row = (await db.execute(select(SystemConfig).where(SystemConfig.key == ADMIN_PASSWORD_HASH_KEY))).scalar_one_or_none()
    return (row.value or "").strip() if row is not None else ""


async def verify_legacy_admin_password(password: str, db: AsyncSession) -> bool:
    """仅在普通管理员账户尚未创建时允许 legacy 密码迁移登录。"""
    admin_username = str(settings.admin_username or "admin").strip()
    existing_admin_id = (
        await db.execute(
            select(User.id).where(
                User.username == admin_username,
                User.is_super_admin.is_(False),
            ).limit(1)
        )
    ).scalar_one_or_none()
    if existing_admin_id is not None:
        return False

    stored_hash = await get_admin_password_hash(db)
    if stored_hash:
        return verify_password(password, stored_hash)
    configured_password = str(settings.admin_password or "").strip()
    if configured_password:
        return compare_digest(password, configured_password)
    return False


async def _sync_regular_admin_legacy_password_hash(
    db: AsyncSession,
    user: User,
    password_hash: str,
) -> None:
    """普通管理员改密后同步 SystemConfig 中的 legacy 哈希，避免旧哈希旁路登录。"""
    if bool(getattr(user, "is_super_admin", False)):
        return
    admin_username = str(settings.admin_username or "admin").strip()
    if str(user.username or "").strip() != admin_username:
        return
    row = (
        await db.execute(select(SystemConfig).where(SystemConfig.key == ADMIN_PASSWORD_HASH_KEY))
    ).scalar_one_or_none()
    now = datetime.utcnow()
    if row is None:
        db.add(SystemConfig(key=ADMIN_PASSWORD_HASH_KEY, value=password_hash, created_at=now, updated_at=now))
    else:
        row.value = password_hash
        row.updated_at = now


async def set_admin_password(password: str, db: AsyncSession) -> None:
    password_hash = hash_password(password)
    row = (await db.execute(select(SystemConfig).where(SystemConfig.key == ADMIN_PASSWORD_HASH_KEY))).scalar_one_or_none()
    now = datetime.utcnow()
    if row is None:
        db.add(SystemConfig(key=ADMIN_PASSWORD_HASH_KEY, value=password_hash, created_at=now, updated_at=now))
    else:
        row.value = password_hash
        row.updated_at = now
    await db.commit()


async def mark_admin_password_customized(db: AsyncSession, user: User) -> None:
    """标记普通管理员密码已由用户修改，禁止启动时再用环境变量覆盖。"""
    if bool(getattr(user, "is_super_admin", False)):
        return
    admin_username = str(settings.admin_username or "admin").strip()
    if str(user.username or "").strip() != admin_username:
        return
    row = (
        await db.execute(select(SystemConfig).where(SystemConfig.key == ADMIN_PASSWORD_CUSTOMIZED_KEY))
    ).scalar_one_or_none()
    now = datetime.utcnow()
    if row is None:
        db.add(SystemConfig(key=ADMIN_PASSWORD_CUSTOMIZED_KEY, value="1", created_at=now, updated_at=now))
    else:
        row.value = "1"
        row.updated_at = now


async def apply_user_password_update(
    db: AsyncSession,
    user: User,
    new_password: str,
    *,
    sync_legacy: bool = True,
) -> None:
    """写入新密码 bcrypt 哈希、递增 token_version，并提交后二次校验。"""
    new_password_hash = hash_password(new_password)
    user.password_hash = new_password_hash
    bump_user_token_version(user)
    if sync_legacy:
        await _sync_regular_admin_legacy_password_hash(db, user, new_password_hash)
    await mark_admin_password_customized(db, user)
    await db.flush()
    await commit_session_with_retry(db)
    await db.refresh(user)
    stored_hash = str(user.password_hash or "").strip()
    if not stored_hash or not verify_password(new_password, stored_hash):
        logger.error(
            "密码保存后校验失败 user_id=%s username=%s hash_len=%s",
            int(user.id),
            str(user.username or ""),
            len(stored_hash),
        )
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="密码保存失败，请稍后重试")


async def authenticate_user_by_password(password: str, db: AsyncSession) -> User | None:
    """遍历全部管理员（含不可见的超级管理员），返回密码命中且可用的账户。"""
    result = await db.execute(
        select(User)
        .where(
            (User.is_admin.is_(True))
            | (User.is_super_admin.is_(True))
            | (User.username.in_([str(settings.admin_username or "admin").strip(), str(settings.super_admin_username or "superadmin").strip()]))
        )
        .order_by(User.id.asc())
    )
    users = list(result.scalars().all())
    candidates: list[User] = []
    stats = {
        "total": len(users),
        "missing_hash": 0,
        "password_mismatch": 0,
        "deleted": 0,
        "disabled": 0,
        "matched": 0,
    }
    for user in users:
        stored_hash = str(user.password_hash or "").strip()
        if not stored_hash:
            stats["missing_hash"] += 1
            logger.warning("登录排除：用户缺少密码哈希 user_id=%s username=%s", int(user.id), str(user.username or ""))
            continue
        if not verify_password(password, stored_hash):
            stats["password_mismatch"] += 1
            continue
        deleted_at = getattr(user, "deleted_at", None)
        if deleted_at is not None:
            stats["deleted"] += 1
            logger.warning("登录排除：用户已删除 user_id=%s username=%s", int(user.id), str(user.username or ""))
            continue
        if hasattr(user, "is_active") and not bool(user.is_active):
            stats["disabled"] += 1
            logger.warning("登录排除：账户已停用 user_id=%s username=%s", int(user.id), str(user.username or ""))
            continue
        stats["matched"] += 1
        candidates.append(user)
    if not candidates:
        logger.warning(
            "登录失败详情：管理员候选数=%s 缺少哈希=%s 密码不匹配=%s 已删除=%s 已停用=%s 可用匹配=%s",
            stats["total"],
            stats["missing_hash"],
            stats["password_mismatch"],
            stats["deleted"],
            stats["disabled"],
            stats["matched"],
        )
        return None
    # 多个账户同密时优先可见的普通管理员，避免改密/登录错绑超级管理员。
    for user in candidates:
        if not bool(getattr(user, "is_super_admin", False)) and bool(getattr(user, "is_visible", True)):
            return user
    for user in candidates:
        if bool(getattr(user, "is_super_admin", False)):
            return user
    return candidates[0]


def create_access_token(user: User | None = None):
    expires_delta = timedelta(minutes=settings.access_token_expire_minutes)
    expire_at = datetime.utcnow() + expires_delta
    if user is not None:
        payload = {
            "sub": str(user.id),
            "user_id": int(user.id),
            "username": str(user.username or ""),
            "role": "admin",
            "is_super_admin": bool(getattr(user, "is_super_admin", False)),
            "token_version": int(getattr(user, "token_version", 0) or 0),
            "exp": expire_at,
            "iat": datetime.utcnow(),
        }
    else:
        payload = {
            "sub": "admin",
            "role": "admin",
            "token_version": 0,
            "exp": expire_at,
            "iat": datetime.utcnow(),
        }
    token = jwt.encode(payload, settings.secret_key, algorithm="HS256")
    return token, int(expires_delta.total_seconds())


@router.post("/login", response_model=LoginResponse)
async def login(data: LoginRequest, request: Request, db: AsyncSession = Depends(get_db)):
    """统一密码登录：不区分管理员类型，失败时仅返回模糊提示。"""
    client_host = request.client.host if request.client else "unknown"
    await _ensure_login_not_locked(client_host)
    matched_user = await authenticate_user_by_password(data.password, db)
    if matched_user is None:
        legacy_ok = await verify_legacy_admin_password(data.password, db)
        if legacy_ok:
            admin_username = str(settings.admin_username or "admin").strip()
            matched_user = (
                await db.execute(
                    select(User).where(
                        User.username == admin_username,
                        User.is_super_admin.is_(False),
                    ).limit(1)
                )
            ).scalar_one_or_none()
        if matched_user is None:
            await _record_login_failure(client_host)
            logger.warning("登录失败：密码未匹配任何可用管理员账户 client=%s", client_host)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="密码错误",
                headers={"WWW-Authenticate": "Bearer"},
            )
    await _clear_login_failures(client_host)
    await db.refresh(matched_user)
    token, expires_in = create_access_token(matched_user)
    logger.info(
        "登录成功 user_id=%s username=%s is_super_admin=%s token_version=%s",
        int(matched_user.id),
        str(matched_user.username or ""),
        bool(getattr(matched_user, "is_super_admin", False)),
        int(getattr(matched_user, "token_version", 0) or 0),
    )
    return LoginResponse(access_token=token, token_type="bearer", expires_in=expires_in)


@router.put("/password", response_model=MessageResponse, dependencies=[Depends(require_admin)])
async def change_password(
    data: ChangePasswordRequest,
    db: AsyncSession = Depends(get_db),
    actor: dict = Depends(require_admin),
):
    """修改当前登录管理员密码，并立即递增 token_version 使旧 JWT 失效。"""
    user_id = int(actor.get("user_id") or 0)
    if user_id <= 0:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="登录状态已失效，请重新登录",
        )
    user = await db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户不存在或已被删除")
    deleted_at = getattr(user, "deleted_at", None)
    if deleted_at is not None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户不存在或已被删除")
    if hasattr(user, "is_active") and not bool(user.is_active):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="账户已停用")
    try:
        if not verify_password(data.old_password, str(user.password_hash or "")):
            logger.warning(
                "管理员改密失败：旧密码不正确 user_id=%s username=%s",
                int(user.id),
                str(user.username or ""),
            )
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="旧密码不正确")
        previous_token_version = int(getattr(user, "token_version", 0) or 0)
        await apply_user_password_update(db, user, data.new_password)
        logger.warning(
            "管理员密码已修改 user_id=%s username=%s is_super_admin=%s token_version=%s->%s",
            int(user.id),
            str(user.username or ""),
            bool(getattr(user, "is_super_admin", False)),
            previous_token_version,
            int(getattr(user, "token_version", 0) or 0),
        )
        return MessageResponse(success=True, message="管理员密码已修改，请使用新密码重新登录")
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        await db.rollback()
        logger.warning("管理员密码保存失败 user_id=%s error=%s", user_id, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="密码保存失败，请稍后重试") from exc
    except Exception as exc:
        await db.rollback()
        logger.warning("管理员密码修改失败 user_id=%s error=%s", user_id, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="密码修改失败，请稍后重试") from exc
