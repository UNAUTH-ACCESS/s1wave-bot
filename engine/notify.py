"""
engine/notify.py
==================
Single shared funnel for every ConfluenceNotification (2026-09-28) — used
by workers/confluence_live_worker.py, api/app.py, and
engine/manual_actions.py, so "does this event also push to Telegram" is
decided in exactly one place instead of separately at every call site
that creates a ConfluenceNotification row.

Real problem this closes: workers/notification_worker.py is a real,
running Telegram alerting system (retry + dead-letter queue) but it's
wired to the OLD scorer pipeline removed 2026-09-23 — nothing in the
current confluence_live path ever enqueued into it, which is why both
SolanaTracker keys dying went unnoticed for 11+ hours, and a stuck
position only ever showed as a dashboard badge. This does NOT resurrect
that old queue (different event taxonomy, different retry semantics it
doesn't need for a handful of low-frequency alerts) — it reuses
workers/telegram_alerts.py's simpler at-most-once direct sender instead,
same as the existing critical-sell-failure alert already does.

Push list (2026-09-29, per the user's explicit instruction to also push
every trade entry/exit, knowing that means real per-trade volume):
  - permanently_halted / marked_unsellable / exit_failed_critical:
    system-discovered problems a human might not be watching for.
  - filter_calibration_needs_review: the self-audit found something,
    by definition happens rarely and always matters.
  - daily_loss_limit_hit: a real, if lesser, halt.
  - withdrawal_sent: real money left the wallet — always worth a receipt,
    regardless of who/what triggered it.
  - entry_filled / exit_filled: every real trade opening and closing,
    including routine LIQUIDITY_GUARD exits — deliberately NOT filtered
    down to "only the losses" or "only the big ones," the user wants to
    see every one.
Still deliberately EXCLUDED: wallet_funded, rent_swept,
daily_loss_limit_cleared, unsellable_recovered, manual_toggle,
halt_resumed (the last two are the user's OWN just-taken action on the
dashboard — they already know it happened).
"""

from __future__ import annotations

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from config.settings import settings
from models.orm import ConfluenceNotification
from workers.telegram_alerts import send_alert

log = get_logger(__name__)

TELEGRAM_PUSH_EVENTS = frozenset({
    "permanently_halted",
    "marked_unsellable",
    "exit_failed_critical",
    "filter_calibration_needs_review",
    "daily_loss_limit_hit",
    "withdrawal_sent",
    "entry_filled",
    "exit_filled",
})

_LEVEL_EMOJI = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def notify(
    session: AsyncSession, level: str, event: str, message: str, trade_id: uuid.UUID | None = None,
) -> None:
    """
    Persist the in-app ConfluenceNotification (unchanged behavior for
    every existing caller) and ALSO push to Telegram when `event` is in
    TELEGRAM_PUSH_EVENTS. send_alert() already swallows its own failures
    and is expected to never raise, but the try/except below is a second,
    deliberate line of defense: this call happens inside the same
    `async with get_session()` block as the session.add() above, and an
    uncaught exception there would roll back the WHOLE transaction —
    silently losing even the in-app notification because of an unrelated
    Telegram problem. Never let that happen.
    """
    session.add(ConfluenceNotification(level=level, event=event, message=message, trade_id=trade_id))
    if event in TELEGRAM_PUSH_EVENTS:
        try:
            emoji = _LEVEL_EMOJI.get(level, "")
            text = f"{emoji} <b>{_esc(settings.S1WAVE_ACCOUNT_NAME)} — {_esc(event.replace('_', ' ').upper())}</b>\n\n{_esc(message)}"
            await send_alert(text)
        except Exception:
            log.error("notify.telegram_push_failed", notification_event=event, exc_info=True)
