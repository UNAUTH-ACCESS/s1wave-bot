"""
workers/telegram_alerts.py
===========================
Direct Telegram alert for critical out-of-band notifications.

Bypasses the notification queue — fires immediately.
Used only for execution.sell_failed_critical where a position
is stuck open and requires immediate human intervention.

Routine trade notifications go through NotificationWorker as usual.
"""

from __future__ import annotations

import httpx
from config.logging import get_logger
from config.settings import settings

log = get_logger(__name__)

_TELEGRAM_API = "https://api.telegram.org"


async def notify_sell_failed_critical(
    mint: str,
    trade_id: str,
    symbol: str | None,
    attempts: int,
    last_error: str | None,
) -> None:
    """
    Fire an immediate Telegram alert for a critical sell failure.
    Position is stuck — manual intervention required.
    """
    bot_token = getattr(settings, "TELEGRAM_BOT_TOKEN", None)
    chat_id   = getattr(settings, "TELEGRAM_CHAT_ID", None)
    if not bot_token or not chat_id:
        log.warning("telegram.not_configured",
                    msg="TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set, skipping critical alert")
        return

    token_label = f"{symbol} ({mint[:8]}...)" if symbol else mint[:8] + "..."
    error_text  = (last_error or "unknown")[:200]

    text = (
        "🚨 <b>CRITICAL: Sell Failed — Position Stuck</b>\n\n"
        f"Token: <code>{_esc(token_label)}</code>\n"
        f"Trade ID: <code>{_esc(str(trade_id)[:16])}</code>\n"
        f"Attempts: <code>{attempts}</code>\n"
        f"Last Error: <code>{_esc(error_text)}</code>\n\n"
        "⚠️ Manual sell required — position open and unprotected"
    )

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{_TELEGRAM_API}/bot{bot_token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
            resp.raise_for_status()
            body = resp.json()
            if not body.get("ok", False):
                raise RuntimeError(f"Telegram API error: {body.get('description')}")
            log.info("telegram.critical_alert_sent", mint=mint, trade_id=trade_id)
    except Exception as exc:
        log.error("telegram.critical_alert_failed",
                  mint=mint, trade_id=trade_id, error=str(exc))


def _esc(text: str) -> str:
    """Escape HTML-significant characters for Telegram's HTML parse mode."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
