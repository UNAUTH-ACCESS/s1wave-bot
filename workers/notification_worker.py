"""
workers/notification_worker.py
===============================
Telegram notification worker — Phase 8

Dispatch rules:
  TRADE_OPEN / TRADE_CLOSE  → immediate (2s poll)
  All other events          → batched (60s poll)

Telegram events (full spec):
  System health:
    WS_CONNECTED, WS_DISCONNECTED, WS_RECONNECTING, NO_PRICE_UPDATES
  Risk + protection:
    STOP_LOSS_TRIGGERED, TAKE_PROFIT_TRIGGERED, TIME_EXIT,
    CIRCUIT_BREAKER_ACTIVATED, DAILY_HALT
  Trade lifecycle:
    TRADE_OPEN, TRADE_CLOSE
  Scoring:
    TIER2_STRONG_BUY, TIER2_REJECTED (with actual values)
  Heartbeat:
    HEARTBEAT (balance, open trades, daily PnL)

Telegram is NOT a log mirror. Only events that require human attention
or decision-making are sent. Routine scoring cycles are silent.

Each event is still built as a Discord-style "embed" dict (title, color,
fields, description, footer) — that shape is a convenient intermediate
representation, not tied to Discord — then flattened to Telegram HTML via
_embed_to_telegram_text() before being sent through the Bot API's
sendMessage endpoint.

Retry policy:
  - Up to 3 attempts per notification
  - After 3 failures: DEAD_LETTER, logged, abandoned
  - Rate limit 429: back off (using Telegram's retry_after) and retry next cycle

At-least-once delivery: PENDING → SENT only on confirmed ok:true response.
On process restart, PENDING notifications are retried automatically.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import select

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from models.orm import NotificationEvent, NotificationQueue, NotificationStatus

log = get_logger(__name__)

_TELEGRAM_API = "https://api.telegram.org"

_IMMEDIATE_EVENTS = {
    NotificationEvent.TRADE_OPEN,
    NotificationEvent.TRADE_CLOSE,
    NotificationEvent.SELL_FAILED_CRITICAL,
    NotificationEvent.VELOCITY_BREAKER,
    NotificationEvent.CB_RESUMED,
    NotificationEvent.CIRCUIT_BREAKER,
}
_MAX_ATTEMPTS    = 3
_POLL_INTERVAL   = 2     # seconds — immediate loop
_BATCH_INTERVAL  = 60    # seconds — batched loop

# Discord embed colours
_GREEN  = 0x00C851
_RED    = 0xFF4444
_ORANGE = 0xFF8800
_YELLOW = 0xFFBB33
_GREY   = 0x888888
_BLUE   = 0x33B5E5
_PURPLE = 0xAA66CC


class NotificationWorker:

    def __init__(self, shutdown_event: asyncio.Event) -> None:
        self._shutdown = shutdown_event
        self._client: httpx.AsyncClient | None = None
        self._last_batch = 0.0

    async def run(self) -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            self._client = client
            log.info("notification_worker.started")

            while not self._shutdown.is_set():
                try:
                    await self._poll_immediate()
                    await self._maybe_dispatch_batch()
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    log.error(
                        "notification_worker.loop_error",
                        exc_type=type(exc).__name__,
                        error=str(exc),
                        exc_info=True,
                    )

                try:
                    await asyncio.wait_for(
                        self._shutdown.wait(), timeout=_POLL_INTERVAL
                    )
                except asyncio.TimeoutError:
                    pass

        log.info("notification_worker.stopped")

    # ── Loops ─────────────────────────────────────────────────────────────

    async def _poll_immediate(self) -> None:
        async with get_session() as session:
            result = await session.execute(
                select(NotificationQueue).where(
                    NotificationQueue.status == NotificationStatus.PENDING,
                    NotificationQueue.event_type.in_(_IMMEDIATE_EVENTS),
                    NotificationQueue.attempts < _MAX_ATTEMPTS,
                ).order_by(NotificationQueue.created_at)
            )
            notifications = list(result.scalars().all())

        for notif in notifications:
            await self._dispatch(notif)

    async def _maybe_dispatch_batch(self) -> None:
        now = time.monotonic()
        if now - self._last_batch < _BATCH_INTERVAL:
            return
        self._last_batch = now

        async with get_session() as session:
            result = await session.execute(
                select(NotificationQueue).where(
                    NotificationQueue.status == NotificationStatus.PENDING,
                    NotificationQueue.event_type.not_in(_IMMEDIATE_EVENTS),
                    NotificationQueue.attempts < _MAX_ATTEMPTS,
                ).order_by(NotificationQueue.created_at)
            )
            notifications = list(result.scalars().all())

        for notif in notifications:
            await self._dispatch(notif)

    # ── Dispatch ──────────────────────────────────────────────────────────

    async def _dispatch(self, notif: NotificationQueue) -> None:
        if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
            # Alerts disabled — dead-letter immediately rather than retrying
            # forever every poll cycle. Everything upstream of notifications
            # (discovery, sampling, evaluations, trading) is unaffected.
            log.debug("notification_worker.telegram_not_configured",
                      notif_event=notif.event_type.value)
            await self._update_status(
                notif.id, NotificationStatus.DEAD_LETTER,
                attempts=notif.attempts + 1, error="Telegram not configured",
            )
            return

        try:
            payload = json.loads(notif.payload)
        except Exception:
            payload = {"raw": notif.payload}

        embed = self._build_embed(notif.event_type, payload)
        text  = self._embed_to_telegram_text(embed)

        try:
            resp = await self._client.post(
                f"{_TELEGRAM_API}/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": settings.TELEGRAM_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )

            if resp.status_code == 429:
                retry_after = 5.0
                try:
                    retry_after = float(resp.json().get("parameters", {}).get("retry_after", 5))
                except Exception:
                    pass
                log.warning(
                    "notification_worker.rate_limited",
                    retry_after=retry_after,
                    notif_event=notif.event_type.value,
                )
                await asyncio.sleep(retry_after)
                await self._update_status(notif.id, NotificationStatus.PENDING,
                                          attempts=notif.attempts + 1)
                return

            resp.raise_for_status()
            body = resp.json()
            if not body.get("ok", False):
                raise RuntimeError(f"Telegram API error: {body.get('description')}")

            await self._update_status(notif.id, NotificationStatus.SENT)
            log.debug(
                "notification_worker.sent",
                notif_event=notif.event_type.value,
                notif_id=str(notif.id),
            )

        except Exception as exc:
            attempts = notif.attempts + 1
            if attempts >= _MAX_ATTEMPTS:
                await self._update_status(
                    notif.id, NotificationStatus.DEAD_LETTER,
                    attempts=attempts, error=str(exc)[:200]
                )
                log.error(
                    "notification_worker.dead_letter",
                    notif_event=notif.event_type.value,
                    notif_id=str(notif.id),
                    error=str(exc),
                )
            else:
                await self._update_status(
                    notif.id, NotificationStatus.PENDING,
                    attempts=attempts, error=str(exc)[:200]
                )
                log.warning(
                    "notification_worker.retry",
                    notif_event=notif.event_type.value,
                    notif_id=str(notif.id),
                    attempts=attempts,
                    error=str(exc),
                )

    async def _update_status(
        self,
        notif_id,
        status: NotificationStatus,
        attempts: int | None = None,
        error: str | None = None,
    ) -> None:
        async with get_session() as session:
            result = await session.execute(
                select(NotificationQueue).where(NotificationQueue.id == notif_id)
            )
            notif = result.scalar_one_or_none()
            if notif is None:
                return
            notif.status = status
            if attempts is not None:
                notif.attempts = attempts
            if error is not None:
                notif.last_error = error
            if status == NotificationStatus.SENT:
                notif.sent_at = datetime.now(timezone.utc)

    # ── Embed builders ────────────────────────────────────────────────────

    def _build_embed(self, event_type: NotificationEvent, p: dict) -> dict:
        builders = {
            NotificationEvent.TRADE_OPEN:           self._embed_trade_open,
            NotificationEvent.TRADE_CLOSE:          self._embed_trade_close,
            NotificationEvent.CIRCUIT_BREAKER:      self._embed_circuit_breaker,
            NotificationEvent.DAILY_HALT:           self._embed_daily_halt,
            NotificationEvent.HEARTBEAT:            self._embed_heartbeat,
            NotificationEvent.SELL_FAILED_CRITICAL: self._embed_sell_failed_critical,
        }
        # Extended events handled by name matching
        name = event_type.value if hasattr(event_type, "value") else str(event_type)

        if event_type in builders:
            return builders[event_type](p)

        # System health events
        if "VELOCITY_BREAKER" in name:
            return self._embed_velocity_breaker(p)
        if "CB_RESUMED" in name:
            return self._embed_cb_resumed(p)
        if "WS_CONNECTED" in name:
            return self._embed_ws_connected(p)
        if "WS_DISCONNECTED" in name:
            return self._embed_ws_disconnected(p)
        if "WS_RECONNECTING" in name:
            return self._embed_ws_reconnecting(p)
        if "NO_PRICE_UPDATES" in name:
            return self._embed_no_price_updates(p)

        return self._embed_generic(name, p)

    # ── Embed → Telegram HTML ────────────────────────────────────────────────
    # Builders below produce Discord-embed-shaped dicts (title/color/fields/
    # description/footer) unchanged — this is the only place that knows how
    # to flatten that shape into a Telegram message. `color` is intentionally
    # unused; Telegram has no per-message accent color.

    def _embed_to_telegram_text(self, embed: dict) -> str:
        parts = []

        title = embed.get("title")
        if title:
            parts.append(f"<b>{self._esc(title)}</b>")

        description = embed.get("description")
        if description:
            parts.append(self._discord_md_to_html(description))

        fields = embed.get("fields")
        if fields:
            parts.append("\n".join(
                f"{self._esc(f['name'])}: {self._discord_md_to_html(f['value'])}"
                for f in fields
            ))

        footer = (embed.get("footer") or {}).get("text")
        if footer:
            parts.append(f"<i>{self._esc(footer)}</i>")

        return "\n\n".join(parts)

    @staticmethod
    def _esc(text: str) -> str:
        """Escape HTML-significant characters for Telegram's HTML parse mode."""
        return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    @classmethod
    def _discord_md_to_html(cls, text: str) -> str:
        """
        Field values and descriptions were written as Discord markdown
        (**bold**, `code`, ```json fenced blocks```) since the embed
        builders predate Telegram. Escape entities first, then translate
        the handful of markdown constructs actually used into Telegram's
        HTML tags — this keeps every _embed_* builder unchanged.
        """
        text = cls._esc(text)
        text = re.sub(r"```(?:json)?\n?(.*?)```", r"<pre>\1</pre>", text, flags=re.S)
        text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
        text = re.sub(r"`([^`]+?)`", r"<code>\1</code>", text)
        return text

    def _embed_trade_open(self, p: dict) -> dict:
        symbol = p.get("symbol") or p.get("mint", "")[:8]
        source = p.get("entry_source", "scorer")
        is_s1  = source == "s1_wave"
        is_add = source == "scorer_addon"

        if is_s1:
            title  = f"🟢 S1 ENTRY — {symbol}"
            fields = [
                {"name": "Entry",    "value": f"`{p.get('entry_price','?')}`",   "inline": True},
                {"name": "Size",     "value": f"`${p.get('position_usd','?')}`", "inline": True},
                {"name": "BP",       "value": f"`{p.get('bp','?')}`",            "inline": True},
                {"name": "5m%",      "value": f"`{p.get('price_5m','?')}%`",     "inline": True},
                {"name": "Liq",      "value": f"`${p.get('liquidity_usd','?')}`","inline": True},
                {"name": "Floor",    "value": f"`{p.get('trailing_floor','?')}`","inline": True},
                {"name": "Balance",  "value": f"`${p.get('balance_after','?')}`","inline": False},
            ]
        elif is_add:
            title  = f"➕ SCORER ADDON — {symbol}"
            fields = [
                {"name": "Add USD",  "value": f"`${p.get('add_usd','?')}`",      "inline": True},
                {"name": "Total",    "value": f"`${p.get('position_usd','?')}`", "inline": True},
                {"name": "Score",    "value": f"`{p.get('score','?')}`",         "inline": True},
                {"name": "Balance",  "value": f"`${p.get('balance_after','?')}`","inline": False},
            ]
        else:
            title  = f"🟢 SCORER ENTRY — {symbol}"
            fields = [
                {"name": "Entry",    "value": f"`{p.get('entry_price','?')}`",    "inline": True},
                {"name": "Size",     "value": f"`${p.get('position_usd','?')}`",  "inline": True},
                {"name": "Score",    "value": f"`{p.get('score','?')}`",          "inline": True},
                {"name": "Signal",   "value": f"`{p.get('signal','?')}`",         "inline": True},
                {"name": "VM Trend", "value": f"`{p.get('vm_trend','?')}`",       "inline": True},
                {"name": "Balance",  "value": f"`${p.get('balance_after','?')}`", "inline": True},
                {"name": "Mint",     "value": f"`{p.get('mint','?')}`",           "inline": False},
            ]

        return {
            "title": title,
            "color": _GREEN,
            "fields": fields,
            "footer": {"text": f"SolanaBot v3 • {p.get('timestamp','')}"},
        }

    def _embed_trade_close(self, p: dict) -> dict:
        symbol   = p.get("symbol") or p.get("mint", "")[:8]
        pnl_usd  = float(p.get("pnl_usd", 0))
        pnl_pct  = float(str(p.get("pnl_pct", "0")).replace("%",""))
        reason   = p.get("exit_reason", "?")
        source   = p.get("entry_source", "scorer")
        is_win   = pnl_usd >= 0
        sign     = "+" if is_win else ""
        colour   = _GREEN if is_win else _RED
        emoji    = "💰" if is_win else "🔴"

        reason_emoji = {
            "TAKE_PROFIT":   "🎯",
            "STOP_LOSS":     "🛑",
            "HARD_FLOOR":    "⚠️",
            "TIME_EXIT":     "⏱️",
            "TRAILING_STOP": "📉" if not is_win else "📈",
        }.get(reason, "❓")

        hold_s   = int(p.get("hold_seconds", 0))
        hold_str = self._fmt_duration(hold_s)
        hwm      = p.get("high_watermark", "")
        source_label = "S1" if source == "s1_wave" else "Scorer"

        fields = [
            {"name": f"{reason_emoji} Exit",  "value": f"`{reason}`",                                      "inline": True},
            {"name": "PnL",                   "value": f"`{sign}${abs(pnl_usd):.2f} ({sign}{pnl_pct:.2f}%)`", "inline": True},
            {"name": "Hold",                  "value": f"`{hold_str}`",                                    "inline": True},
            {"name": "Entry",                 "value": f"`{p.get('entry_price','?')}`",                    "inline": True},
            {"name": "Exit",                  "value": f"`{p.get('exit_price','?')}`",                     "inline": True},
            {"name": "Source",                "value": f"`{source_label}`",                               "inline": True},
            {"name": "Balance",               "value": f"`${p.get('balance_after','?')}`",                "inline": True},
        ]

        if hwm and source == "s1_wave":
            fields.append({"name": "Peak (HWM)", "value": f"`{hwm}`", "inline": True})

        return {
            "title": f"{emoji} TRADE CLOSE — {symbol}",
            "color": colour,
            "fields": fields,
            "footer": {"text": f"SolanaBot v3 • {p.get('timestamp','')}"},
        }

    def _embed_circuit_breaker(self, p: dict) -> dict:
        return {
            "title": "⚡ CIRCUIT BREAKER TRIPPED",
            "color": _ORANGE,
            "description": (
                f"**{p.get('consecutive_losses','?')} consecutive losses.**\n"
                f"Trading paused until `{p.get('resume_at','?')}`\n\n"
                f"Last loss: `{p.get('last_exit_reason','?')}` on `{p.get('last_symbol','?')}`"
            ),
            "footer": {"text": "SolanaBot v3"},
        }

    def _embed_daily_halt(self, p: dict) -> dict:
        return {
            "title": "🛑 DAILY LOSS LIMIT HIT",
            "color": _RED,
            "description": (
                f"Cumulative PnL today: `${p.get('cumulative_pnl','?')}`\n"
                f"Limit: `{p.get('limit_pct','?')}%` of `${p.get('day_open_balance','?')}`\n"
                "No new entries until midnight UTC."
            ),
            "footer": {"text": "SolanaBot v3"},
        }

    def _embed_heartbeat(self, p: dict) -> dict:
        return {
            "title": "💓 Heartbeat",
            "color": _GREY,
            "fields": [
                {"name": "Balance",      "value": f"`${p.get('balance','?')}`",       "inline": True},
                {"name": "Open Trades",  "value": f"`{p.get('open_trades',0)}`",      "inline": True},
                {"name": "Today PnL",    "value": f"`${p.get('daily_pnl','?')}`",     "inline": True},
                {"name": "Total Trades", "value": f"`{p.get('total_trades',0)}`",     "inline": True},
                {"name": "Win Rate",     "value": f"`{p.get('win_rate','?')}%`",      "inline": True},
                {"name": "Uptime",       "value": f"`{p.get('uptime','?')}`",         "inline": True},
            ],
            "footer": {"text": f"SolanaBot v3 • {p.get('timestamp','')}"},
        }

    def _embed_ws_connected(self, p: dict) -> dict:
        return {
            "title": "🔗 WebSocket Connected",
            "color": _GREEN,
            "description": f"Price feed active for `{p.get('mint','?')}` (`{p.get('symbol','?')}`)",
            "footer": {"text": "SolanaBot v3"},
        }

    def _embed_ws_disconnected(self, p: dict) -> dict:
        return {
            "title": "🔌 WebSocket Disconnected",
            "color": _ORANGE,
            "description": (
                f"Lost price feed for `{p.get('symbol','?')}`\n"
                f"Reason: `{p.get('reason','unknown')}`\n"
                "Falling back to 30s polling until reconnect."
            ),
            "footer": {"text": "SolanaBot v3"},
        }

    def _embed_ws_reconnecting(self, p: dict) -> dict:
        attempt = p.get("attempt", "?")
        return {
            "title": f"🔄 WebSocket Reconnecting (attempt {attempt})",
            "color": _YELLOW,
            "description": (
                f"Reconnecting to price feed for `{p.get('symbol','?')}`\n"
                f"Last error: `{p.get('error','unknown')}`"
            ),
            "footer": {"text": "SolanaBot v3"},
        }

    def _embed_no_price_updates(self, p: dict) -> dict:
        return {
            "title": "⏱️ No Price Updates",
            "color": _ORANGE,
            "description": (
                f"`{p.get('symbol','?')}` has not received a price update in "
                f"`{p.get('stale_seconds','?')}s`.\n"
                "Risk engine is running on last known price."
            ),
            "footer": {"text": "SolanaBot v3"},
        }

    def _embed_sell_failed_critical(self, p: dict) -> dict:
        symbol = p.get("symbol","?")
        return {
            "title": "🚨 CRITICAL — Sell Failed",
            "color": _RED,
            "description": (
                f"**Position stuck open — manual intervention required.**\n\n"
                f"Token: `{symbol}` (`{p.get('mint','?')[:16]}...`)\n"
                f"Trade ID: `{p.get('trade_id','?')}`\n"
                f"Attempts: `{p.get('attempts','?')}`\n"
                f"Error: `{p.get('last_error','?')[:200]}`"
            ),
            "footer": {"text": "SolanaBot v3 — REQUIRES IMMEDIATE ACTION"},
        }

    def _embed_velocity_breaker(self, p: dict) -> dict:
        symbol = p.get("symbol","?")
        drop   = p.get("drop_pct", "?")
        return {
            "title": f"⚡ RUG DETECTED — {symbol}",
            "color": _RED,
            "description": (
                f"Velocity circuit breaker triggered.\n"
                f"Price dropped **{drop}%** in a single tick.\n"
                f"Position closed immediately at `{p.get('exit_price','?')}`."
            ),
            "fields": [
                {"name": "PnL",      "value": f"`${p.get('pnl_usd','?')} ({p.get('pnl_pct','?')}%)`", "inline": True},
                {"name": "Balance",  "value": f"`${p.get('balance_after','?')}`",                     "inline": True},
            ],
            "footer": {"text": "SolanaBot v3"},
        }

    def _embed_cb_resumed(self, p: dict) -> dict:
        return {
            "title": "✅ Circuit Breaker Resumed",
            "color": _GREEN,
            "description": (
                f"Trading resumed. Consecutive loss counter reset to 0.\n"
                f"Paused for: `{p.get('paused_duration','?')}`"
            ),
            "footer": {"text": "SolanaBot v3"},
        }

    def _embed_generic(self, event_name: str, p: dict) -> dict:
        return {
            "title": f"ℹ️ {event_name}",
            "color": _BLUE,
            "description": f"```json\n{json.dumps(p, indent=2)[:1800]}\n```",
            "footer": {"text": "SolanaBot v3"},
        }

    @staticmethod
    def _fmt_duration(seconds: int) -> str:
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds // 60}m {seconds % 60}s"
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
