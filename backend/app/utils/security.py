import base64
import binascii
import hmac
import threading
import time
import bcrypt
from collections import OrderedDict
from hashlib import sha256
from urllib.parse import quote

from app.core.config import settings

TOKEN_TTL_SECONDS = 60 * 60 * 24 * 365


def _prehash_password(password: str) -> bytes:
    return sha256(password.encode("utf-8")).hexdigest().encode("utf-8")


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_prehash_password(password), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    hashed_value = (hashed or "").strip()
    if not hashed_value:
        return False
    try:
        return bool(bcrypt.checkpw(_prehash_password(password), hashed_value.encode("utf-8")))
    except (ValueError, TypeError):
        return False


def _b64_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("utf-8").rstrip("=")


def _b64_decode(value: str) -> bytes:
    padded = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def _sign(message: str) -> str:
    digest = hmac.new(settings.secret_key.encode("utf-8"), message.encode("utf-8"), sha256).digest()
    return _b64_encode(digest)


def generate_unsubscribe_token(email: str, recipient_id: int | None, task_id: int | None, expires_in: int = TOKEN_TTL_SECONDS) -> str:
    normalized_email = email.strip().lower()
    recipient_value = str(recipient_id or 0)
    task_value = str(task_id or 0)
    expires_at = int(time.time()) + max(int(expires_in), 60)
    payload = f"{normalized_email}|{recipient_value}|{task_value}|{expires_at}"
    signature = _sign(payload)
    return _b64_encode(f"{payload}|{signature}".encode("utf-8"))


def verify_unsubscribe_token(token: str) -> dict[str, int | str]:
    try:
        decoded = _b64_decode(token).decode("utf-8")
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise ValueError("退订链接无效")
    parts = decoded.split("|")
    if len(parts) != 5:
        raise ValueError("退订链接格式错误")
    email, recipient_id, task_id, expires_at, signature = parts
    payload = "|".join(parts[:4])
    expected_signature = _sign(payload)
    if not hmac.compare_digest(signature, expected_signature):
        raise ValueError("退订签名验证失败")
    try:
        expires_value = int(expires_at)
        recipient_value = int(recipient_id)
        task_value = int(task_id)
    except ValueError:
        raise ValueError("退订链接参数错误")
    if expires_value < int(time.time()):
        raise ValueError("退订链接已过期")
    normalized_email = email.strip().lower()
    if "@" not in normalized_email or normalized_email.startswith("@") or normalized_email.endswith("@"):
        raise ValueError("退订邮箱无效")
    return {
        "email": normalized_email,
        "recipient_id": recipient_value or 0,
        "task_id": task_value or 0,
        "expires_at": expires_value,
    }


def build_unsubscribe_url(base_url: str, email: str, recipient_id: int | None, task_id: int | None) -> str:
    clean_base = base_url.rstrip("/")
    token = generate_unsubscribe_token(email, recipient_id, task_id)
    return f"{clean_base}/unsubscribe?token={quote(token, safe='')}"


SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY = "-unsubscribe_url-"


def build_substitution_unsubscribe(recipient, task_id: int | None) -> dict[str, str]:
    """为 SendGrid Substitution 生成收件人独立退订变量，返回 {"-unsubscribe_url-": url}。"""
    email = str(getattr(recipient, "email", "") or "").strip()
    if not email:
        return {}
    recipient_id = getattr(recipient, "id", None)
    try:
        normalized_recipient_id = int(recipient_id) if recipient_id is not None else None
    except (TypeError, ValueError):
        normalized_recipient_id = None
    url = build_unsubscribe_url(settings.public_base_url, email, normalized_recipient_id, task_id)
    return {SENDGRID_UNSUBSCRIBE_SUBSTITUTION_KEY: url}


class UnsubscribeUrlCache:
    def __init__(self, max_size: int = 1000) -> None:
        self._max_size = max(1, int(max_size))
        self._entries: OrderedDict[str, str] = OrderedDict()
        self._lock = threading.Lock()

    @staticmethod
    def _cache_key(email: str, recipient_id: int | None, task_id: int | None) -> str:
        return f"{email}:{task_id}:{recipient_id}"

    def get_url(self, base_url: str, email: str, recipient_id: int | None, task_id: int | None) -> str:
        cache_key = self._cache_key(email, recipient_id, task_id)
        with self._lock:
            cached = self._entries.get(cache_key)
            if cached is not None:
                self._entries.move_to_end(cache_key)
                return cached
        url = build_unsubscribe_url(base_url, email, recipient_id, task_id)
        with self._lock:
            self._entries[cache_key] = url
            self._entries.move_to_end(cache_key)
            while len(self._entries) > self._max_size:
                self._entries.popitem(last=False)
        return url

    def clear_task(self, task_id: int) -> None:
        marker = f":{int(task_id)}:"
        with self._lock:
            keys_to_remove = [key for key in self._entries if marker in key]
            for key in keys_to_remove:
                self._entries.pop(key, None)
