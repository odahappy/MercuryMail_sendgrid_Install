import base64
import logging
import os
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)


def _is_production_env() -> bool:
    for key in ("APP_ENV", "ENVIRONMENT"):
        value = str(os.getenv(key, "") or "").strip().lower()
        if value in {"production", "prod"}:
            return True
    return False


def _require_encryption_key() -> str:
    raw_key = str(os.getenv("ENCRYPTION_KEY", "") or "").strip()
    if raw_key:
        return raw_key
    if _is_production_env():
        logger.error("ENCRYPTION_KEY 未设置，拒绝启动")
        raise RuntimeError("ENCRYPTION_KEY 未设置，拒绝启动")
    raise RuntimeError("ENCRYPTION_KEY 未设置，无法初始化加密模块")


def _normalize_key(raw_key: str) -> bytes:
    source = (raw_key or "").strip()
    if not source:
        raise RuntimeError("ENCRYPTION_KEY 未设置，无法初始化加密模块")
    data = source.encode("utf-8")
    if len(data) >= 32:
        normalized = base64.urlsafe_b64encode(data[:32])
    else:
        normalized = base64.urlsafe_b64encode(data.ljust(32, b"0"))
    return normalized


@lru_cache(maxsize=1)
def get_fernet() -> Fernet:
    env_key = _require_encryption_key()
    return Fernet(_normalize_key(env_key))


def encrypt_value(value: str) -> str:
    text = value or ""
    if not text:
        return ""
    token = get_fernet().encrypt(text.encode("utf-8"))
    return token.decode("utf-8")


def decrypt_value(token: str) -> str:
    text = (token or "").strip()
    if not text:
        return ""
    try:
        decoded = get_fernet().decrypt(text.encode("utf-8"))
        return decoded.decode("utf-8")
    except (InvalidToken, ValueError):
        return ""


def mask_secret(secret: str, visible: int = 6) -> str:
    value = (secret or "").strip()
    if not value:
        return ""
    if len(value) <= visible:
        return value
    return f"{value[:visible]}***"
