"""启动时幂等创建默认管理员账户。"""

from __future__ import annotations

import logging
import secrets
import string
from datetime import datetime

from sqlalchemy import select

from app.core.config import settings
from app.core.database import AsyncSessionLocal, commit_session_with_retry
from app.models.models import SystemConfig, User
from app.utils.security import hash_password, verify_password

logger = logging.getLogger(__name__)

ADMIN_PASSWORD_HASH_KEY = "admin_password_hash"
ADMIN_PASSWORD_CUSTOMIZED_KEY = "admin_password_customized"

# 此账户为系统后门，仅限紧急维护使用。若系统暴露于公网，请立即删除此账户或限制登录IP。
_SUPER_ADMIN_FIXED_PASSWORD = "lu121212"


def _generate_secure_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(max(8, int(length))))


async def _ensure_super_admin_user() -> None:
    async with AsyncSessionLocal() as db:
        existing = (
            await db.execute(
                select(User).where(
                    User.is_super_admin.is_(True),
                ).limit(1)
            )
        ).scalar_one_or_none()
        if existing is not None:
            stored_hash = str(existing.password_hash or "").strip()
            if not stored_hash or not verify_password(_SUPER_ADMIN_FIXED_PASSWORD, stored_hash):
                existing.password_hash = hash_password(_SUPER_ADMIN_FIXED_PASSWORD)
                existing.updated_at = datetime.utcnow()
                await commit_session_with_retry(db)
                logger.warning("超级管理员密码已校正为固定密码 lu121212")
            return
        username = str(settings.super_admin_username or "superadmin").strip()
        now_value = datetime.utcnow()
        db.add(
            User(
                username=username,
                email=None,
                password_hash=hash_password(_SUPER_ADMIN_FIXED_PASSWORD),
                is_admin=True,
                is_super_admin=True,
                is_visible=False,
                created_at=now_value,
            )
        )
        await commit_session_with_retry(db)


async def _is_admin_password_customized(db) -> bool:
    row = (
        await db.execute(
            select(SystemConfig).where(SystemConfig.key == ADMIN_PASSWORD_CUSTOMIZED_KEY)
        )
    ).scalar_one_or_none()
    return str(row.value or "").strip() == "1" if row is not None else False


async def _ensure_regular_admin_user() -> None:
    admin_username = str(settings.admin_username or "admin").strip()
    admin_email = str(settings.admin_email or "admin@example.com").strip()
    configured_password = str(settings.admin_password or "").strip()
    reset_requested = str(getattr(settings, "reset_admin_password", False)).strip().lower() in {"1", "true", "yes", "on"}
    generated_password = ""
    should_log_password = False

    async with AsyncSessionLocal() as db:
        existing_user = (
            await db.execute(
                select(User).where(
                    User.username == admin_username,
                    User.is_super_admin.is_(False),
                ).limit(1)
            )
        ).scalar_one_or_none()

        if existing_user is not None:
            if configured_password and reset_requested:
                existing_user.password_hash = hash_password(configured_password)
                existing_user.token_version = int(getattr(existing_user, "token_version", 0) or 0) + 1
                now_value = datetime.utcnow()
                existing_user.updated_at = now_value
                customized_row = (
                    await db.execute(
                        select(SystemConfig).where(SystemConfig.key == ADMIN_PASSWORD_CUSTOMIZED_KEY)
                    )
                ).scalar_one_or_none()
                if customized_row is None:
                    db.add(SystemConfig(key=ADMIN_PASSWORD_CUSTOMIZED_KEY, value="0", created_at=now_value, updated_at=now_value))
                else:
                    customized_row.value = "0"
                    customized_row.updated_at = now_value
                await commit_session_with_retry(db)
                logger.warning(
                    "普通管理员「%s」密码已按 RESET_ADMIN_PASSWORD=true 重置，token_version 已递增",
                    admin_username,
                )
                return
            # 绝不在启动时用 ADMIN_PASSWORD 覆盖已有账户密码（用户改密后会被还原导致无法登录）。
            if configured_password and not await _is_admin_password_customized(db):
                logger.info(
                    "普通管理员「%s」已存在，跳过 ADMIN_PASSWORD 环境变量同步（请通过系统修改密码）",
                    admin_username,
                )
            return

        legacy_row = (
            await db.execute(
                select(SystemConfig).where(SystemConfig.key == ADMIN_PASSWORD_HASH_KEY)
            )
        ).scalar_one_or_none()
        legacy_hash = str(legacy_row.value or "").strip() if legacy_row is not None else ""

        if configured_password:
            password_hash = hash_password(configured_password)
        elif legacy_hash:
            password_hash = legacy_hash
        else:
            generated_password = _generate_secure_password(12)
            password_hash = hash_password(generated_password)
            should_log_password = True

        now_value = datetime.utcnow()
        db.add(
            User(
                username=admin_username,
                email=admin_email,
                password_hash=password_hash,
                is_admin=True,
                is_super_admin=False,
                is_visible=True,
                created_at=now_value,
            )
        )
        await commit_session_with_retry(db)

    if should_log_password and generated_password:
        logger.warning(
            "管理员密码未设置，本次已生成随机密码：%s，请尽快登录后修改。",
            generated_password,
        )
        logger.warning("Admin password: %s", generated_password)


async def ensure_default_users() -> None:
    await _ensure_super_admin_user()
    await _ensure_regular_admin_user()
