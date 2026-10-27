from datetime import datetime
from html import escape

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select, update

from app.core.database import AsyncSessionLocal
from app.models.models import Recipient, Unsubscribe
from app.utils.security import verify_unsubscribe_token

router = APIRouter(tags=["unsubscribe"])


def render_page(title: str, message: str, success: bool) -> HTMLResponse:
    color = "#16a34a" if success else "#dc2626"
    safe_title = escape(str(title or ""), quote=True)
    safe_message = escape(str(message or ""), quote=True)
    html = f"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{safe_title}</title>
  <style>
    body {{ margin: 0; min-height: 100vh; display: grid; place-items: center; font-family: Arial, 'Microsoft YaHei', sans-serif; background: #f8fafc; color: #0f172a; }}
    main {{ width: min(92vw, 560px); background: #fff; border: 1px solid #e2e8f0; border-radius: 24px; padding: 36px; box-shadow: 0 24px 70px rgba(15, 23, 42, .12); }}
    h1 {{ margin: 0 0 14px; color: {color}; font-size: 28px; }}
    p {{ margin: 0; color: #475569; line-height: 1.8; font-size: 16px; }}
  </style>
</head>
<body><main><h1>{safe_title}</h1><p>{safe_message}</p></main></body>
</html>
"""
    return HTMLResponse(content=html, status_code=200 if success else 400)


@router.get("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe(token: str, request: Request):
    try:
        payload = verify_unsubscribe_token(token)
    except ValueError as exc:
        return render_page("退订失败", str(exc), False)

    email = str(payload["email"] or "").strip().lower()
    normalized_email = email
    recipient_id = int(payload["recipient_id"])
    task_id = int(payload["task_id"])
    user_agent = request.headers.get("user-agent", "")[:2000]
    ip_address = request.client.host if request.client else ""

    async with AsyncSessionLocal() as db:
        if recipient_id:
            await db.execute(
                update(Recipient)
                .where(Recipient.id == recipient_id)
                .values(is_unsubscribed=True)
            )
        await db.execute(
            update(Recipient)
            .where(func.lower(func.btrim(Recipient.email)) == normalized_email)
            .values(is_unsubscribed=True)
        )
        existing = await db.execute(
            select(Unsubscribe).where(
                func.coalesce(
                    func.nullif(func.btrim(Unsubscribe.normalized_email), ""),
                    func.lower(func.btrim(Unsubscribe.email)),
                ) == normalized_email
            )
        )
        record = existing.scalar_one_or_none()
        if record is None:
            db.add(
                Unsubscribe(
                    email=email,
                    normalized_email=normalized_email,
                    recipient_id=recipient_id or None,
                    task_id=task_id or None,
                    reason="一键退订",
                    ip_address=ip_address,
                    user_agent=user_agent,
                    unsubscribed_at=datetime.utcnow(),
                )
            )
        else:
            record.email = email or record.email
            record.normalized_email = normalized_email
            record.recipient_id = recipient_id or record.recipient_id
            record.task_id = task_id or record.task_id
            record.reason = "一键退订"
            record.ip_address = ip_address
            record.user_agent = user_agent
            record.unsubscribed_at = datetime.utcnow()
            record.updated_at = datetime.utcnow()
        await db.commit()

    return render_page("退订成功", "你已成功退订 MercuryMail 邮件，系统将不再向该邮箱发送后续群发邮件。", True)
