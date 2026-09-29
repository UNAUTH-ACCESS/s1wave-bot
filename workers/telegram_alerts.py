"""
workers/telegram_alerts.py
===========================
Direct Telegram alert for critical out-of-band notifications.

Bypasses the notification queue — fires immediately, at-most-once, no
retry/dead-letter (that's NotificationWorker's job for the OLD pipeline's
routine trade notifications — see its own module docstring; this file is
for the small set of events worth a push straight to a phone).

Originally (2026-09-24) a single narrow function for
execution.sell_failed_critical. Generalized 2026-09-28 into a plain
send_alert(text) plus engine/notify.py's curated event allowlist, so any
ConfluenceNotification-worthy event (halts, stuck positions, a filter-
calibration divergence, a real withdrawal) can push the same way instead
of each needing its own bespoke Telegram function. notify_sell_failed_
critical kept as a thin wrapper — unchanged call site in engine/execution.py.

No-ops cleanly (logs once, returns) when TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID
aren't set — this is expected until a real bot + chat id are configured,
not an error condition.
"""

from __future__ import annotations

import httpx
from config.logging import get_logger
from config.settings import settings

log = get_logger(__name__)

_TELEGRAM_API = "https://api.telegram.org"


async def send_alert(text: str) -> None:
    """
    Fire an immediate Telegram message (HTML parse mode). Caller is
    responsible for escaping any untrusted text with _esc() first. Never
    raises — a Telegram outage or missing config must never break the
    real trading logic that triggered the alert.
    """
    bot_token = getattr(settings, "TELEGRAM_BOT_TOKEN", None)
    chat_id   = getattr(settings, "TELEGRAM_CHAT_ID", None)
    if not bot_token or not chat_id:
        log.warning("telegram.not_configured",
                    msg="TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set, skipping alert")
        return

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
            log.info("telegram.alert_sent")
    except Exception as exc:
        log.error("telegram.alert_failed", error=str(exc))


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
    await send_alert(text)


def _esc(text: str) -> str:
    """Escape HTML-significant characters for Telegram's HTML parse mode."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
