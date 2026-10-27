import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.auth import apply_user_password_update
from app.api.deps import get_current_user, require_admin
from app.core.database import get_db
from app.models.models import User

router = APIRouter(prefix="/api/users", tags=["users"])
logger = logging.getLogger(__name__)


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    username: str
    email: str | None = None
    is_admin: bool
    created_at: datetime


class UserListResponse(BaseModel):
    items: list[UserOut]
    total: int


class UpdateUserRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=255)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value):
        normalized = str(value or "").strip()
        if "@" not in normalized or normalized.startswith("@") or normalized.endswith("@"):
            raise ValueError("邮箱格式不正确")
        return normalized


class ChangeUserPasswordRequest(BaseModel):
    new_password: str = Field(..., min_length=6, max_length=128)


class MessageResponse(BaseModel):
    success: bool
    message: str


async def _get_user_or_404(user_id: int, db: AsyncSession) -> User:
    user = await db.get(User, int(user_id))
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在")
    return user


def _reject_super_admin_mutation(target: User) -> None:
    if bool(getattr(target, "is_super_admin", False)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="无权操作超级管理员账户")


@router.get("", response_model=UserListResponse, dependencies=[Depends(require_admin)])
async def list_users(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(User)
        .where(User.is_visible.is_(True))
        .order_by(User.id.asc())
    )
    users = list(result.scalars().all())
    return UserListResponse(
        items=[
            UserOut(
                id=int(item.id),
                username=str(item.username or ""),
                email=str(item.email or "").strip() or None,
                is_admin=bool(item.is_admin),
                created_at=item.created_at,
            )
            for item in users
        ],
        total=len(users),
    )


@router.put("/{user_id}", response_model=UserOut, dependencies=[Depends(require_admin)])
async def update_user(
    user_id: int,
    data: UpdateUserRequest,
    db: AsyncSession = Depends(get_db),
    _actor: User = Depends(get_current_user),
):
    user = await _get_user_or_404(user_id, db)
    _reject_super_admin_mutation(user)
    user.email = str(data.email).strip()
    try:
        await db.commit()
        await db.refresh(user)
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="用户信息更新失败，请检查邮箱格式后重试") from exc
    except SQLAlchemyError as exc:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="用户信息保存失败，请稍后重试") from exc
    return UserOut(
        id=int(user.id),
        username=str(user.username or ""),
        email=str(user.email or "").strip() or None,
        is_admin=bool(user.is_admin),
        created_at=user.created_at,
    )


@router.put("/{user_id}/password", response_model=MessageResponse, dependencies=[Depends(require_admin)])
async def change_user_password(
    user_id: int,
    data: ChangeUserPasswordRequest,
    db: AsyncSession = Depends(get_db),
    _actor: User = Depends(get_current_user),
):
    user = await _get_user_or_404(user_id, db)
    _reject_super_admin_mutation(user)
    previous_token_version = int(getattr(user, "token_version", 0) or 0)
    try:
        await apply_user_password_update(db, user, data.new_password)
        logger.warning(
            "用户密码已更新 user_id=%s username=%s token_version=%s->%s",
            int(user.id),
            str(user.username or ""),
            previous_token_version,
            int(getattr(user, "token_version", 0) or 0),
        )
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        await db.rollback()
        logger.warning("用户密码保存失败 user_id=%s error=%s", user_id, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="密码保存失败，请稍后重试") from exc
    return MessageResponse(success=True, message="用户密码已更新，该用户需重新登录")


@router.delete("/{user_id}", response_model=MessageResponse, dependencies=[Depends(require_admin)])
async def delete_user(
    user_id: int,
    db: AsyncSession = Depends(get_db),
    _actor: User = Depends(get_current_user),
):
    user = await _get_user_or_404(user_id, db)
    _reject_super_admin_mutation(user)
    user.is_visible = False
    user.is_active = False
    user.deleted_at = datetime.utcnow()
    try:
        await db.commit()
    except SQLAlchemyError as exc:
        await db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="用户删除失败，请稍后重试") from exc
    return MessageResponse(success=True, message="用户已删除")