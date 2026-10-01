"""Minimal Resend sender shared by the bot (alerts) and the control panel (credentials)."""

from __future__ import annotations

import httpx

from config.logging import get_logger
from config.settings import settings

log = get_logger(__name__)


def _api_key() -> str:
    if settings.RESEND_API_KEY:
        return settings.RESEND_API_KEY
    try:
        with open(settings.RESEND_ENV_FILE) as fh:
            for line in fh:
                if line.startswith("RESEND_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"\'')
    except OSError:
        pass
    return ""


async def send_email(to: str, subject: str, text: str) -> bool:
    """Never raises. Returns True only on a 2xx from Resend. Bodies are never logged."""
    key = _api_key()
    if not to or not key:
        log.warning("emailer.not_configured", has_to=bool(to), has_key=bool(key))
        return False
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {key}"},
                json={"from": settings.NOTIFY_EMAIL_FROM, "to": [to], "subject": subject, "text": text},
            )
        if resp.status_code // 100 == 2:
            return True
        log.error("emailer.failed", status=resp.status_code)
    except Exception as exc:
        log.error("emailer.failed", error=str(exc))
    return False
