import logging
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.models.models import User
from app.services.operation_audit import mark_export_operation_request

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=True)


async def ensure_export_operation_no_audit(request: Request) -> None:
    """导出接口依赖：标记请求豁免面向用户的操作日志。"""
    mark_export_operation_request(request)


async def check_db_ready(request: Request) -> None:
    if not bool(getattr(request.app.state, "db_ready", False)):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="系统正在初始化数据库，请稍后重试",
        )


def bump_user_token_version(user: User) -> None:
    """递增令牌版本，使已签发的 JWT 在 require_admin 校验时失效。"""
    user.token_version = int(getattr(user, "token_version", 0) or 0) + 1


def _decode_admin_payload(token: str) -> dict:
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=["HS256"])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="登录状态已失效，请重新登录",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if payload.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="管理员身份校验失败，请重新登录",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


async def _load_user_from_admin_payload(payload: dict, db: AsyncSession) -> User | None:
    user_id = payload.get("user_id")
    if user_id is not None:
        return await db.get(User, int(user_id))

    sub_value = str(payload.get("sub") or "").strip()
    if sub_value.isdigit():
        return await db.get(User, int(sub_value))

    if sub_value == "admin":
        admin_username = str(settings.admin_username or "admin").strip()
        return (
            await db.execute(
                select(User).where(
                    User.username == admin_username,
                    User.is_super_admin.is_(False),
                ).limit(1)
            )
        ).scalar_one_or_none()
    return None


def _validate_admin_user_for_token(user: User | None, payload: dict) -> User:
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户不存在或已被删除",
            headers={"WWW-Authenticate": "Bearer"},
        )

    deleted_at = getattr(user, "deleted_at", None)
    if deleted_at is not None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户不存在或已被删除",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not bool(user.is_admin):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="管理员身份校验失败，请重新登录",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if hasattr(user, "is_active") and not bool(user.is_active):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="账户已停用",
        )

    if not bool(getattr(user, "is_super_admin", False)) and not bool(getattr(user, "is_visible", True)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户不存在或已被删除",
            headers={"WWW-Authenticate": "Bearer"},
        )

    jwt_token_version = int(payload.get("token_version", 0) or 0)
    db_token_version = int(getattr(user, "token_version", 0) or 0)
    if jwt_token_version != db_token_version:
        logger.warning(
            "JWT token_version 不匹配，已拒绝访问 user_id=%s jwt=%s db=%s",
            int(user.id),
            jwt_token_version,
            db_token_version,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="密码已修改，请重新登录",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


async def require_admin_token_only(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """高频只读指标接口仅验 JWT 签名/角色，避免每秒轮询占用数据库连接。"""
    payload = _decode_admin_payload(credentials.credentials)
    return {
        **payload,
        "user_id": int(payload.get("user_id") or (payload.get("sub") if str(payload.get("sub") or "").isdigit() else 0) or 0),
        "username": str(payload.get("username") or payload.get("sub") or ""),
        "is_super_admin": bool(payload.get("is_super_admin", False)),
        "token_version": int(payload.get("token_version", 0) or 0),
    }


async def require_admin(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    payload = _decode_admin_payload(credentials.credentials)
    user = await _load_user_from_admin_payload(payload, db)
    user = _validate_admin_user_for_token(user, payload)
    return {
        **payload,
        "user_id": int(user.id),
        "username": str(user.username or ""),
        "is_super_admin": bool(getattr(user, "is_super_admin", False)),
        "token_version": int(getattr(user, "token_version", 0) or 0),
    }


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> User:
    payload = _decode_admin_payload(credentials.credentials)
    user = await _load_user_from_admin_payload(payload, db)
    return _validate_admin_user_for_token(user, payload)
