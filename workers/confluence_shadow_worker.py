"""
workers/confluence_shadow_worker.py
======================================
Real-time-monitored paper-trade experiment: "confluence_entry_v1".

Built 2026-09-22 directly from this session's momentum-confluence research
(analysis/pump_timing_research.py, pump_signal_quality.py,
momentum_signal_prospective.py, momentum_signal_expectancy.py). That
research found a >5.3% trailing-3-minute price move reliably precedes a
token's peak (median ~8min lead time, validated out-of-sample), and that
requiring >=2 of 4 related rules to co-fire at that same moment roughly
doubles win rate (also validated out-of-sample). The expectancy backtest
that followed could only simulate exits against WATCHING-tier
token_snapshots (30-60s cadence) — no historical 1-second data exists for
tokens that were never actually entered as real trades. This worker closes
exactly that gap: a real-time-monitored paper position, at the same 1-second
cadence a real trade gets via TradeMonitorWorker.

Never touches Trade, CapitalEngine, TokenStatus, MAX_CONCURRENT_TRADES, the
daily-loss circuit breaker, or any production decision path. A fully
separate, isolated table (confluence_shadow_positions) and worker — a
position opening or closing here changes nothing about what the real bot
does and cannot compete with real trades for capital or concurrency.

Entry
-----
Fires once per token, the moment a momentum_signal_events row is recorded
with n_rules_cofiring >= 2 for experiment_version='momentum_confluence_v1'
(see workers/momentum_signal.py — the confluence threshold this research
validated). Entry price = that row's trigger_price, entry time =
triggered_at — same "you clicked buy the instant the signal fired"
assumption the research used throughout, zero slippage modeled (matching
this project's existing paper-trading convention, engine/execution.py's
_simulate_buy()).

Exit
----
Every second, for every open shadow position: fetch current price via
DexScreener (via workers/dexscreener_client.py's shared _best_pair() and
endpoint, extracted from the old TradeMonitorWorker before that worker
was removed with the rest of the scorer/S1Wave pipeline, 2026-09-23),
record an observation, then apply the layered exit priority:
  0. Velocity breaker — single-tick drop <= -25% -> immediate HARD_FLOOR
  1. HARD_FLOOR   (settings.HARD_FLOOR_PCT,   -7%)
  2. Trailing-stop staircase (engine/trailing_stop.py, added 2026-09-23,
     replacing the old fixed STOP_LOSS_PCT/TAKE_PROFIT_PCT pair) — starts
     at the same -6% distance below entry, ratchets up in 10% steps as
     price makes new highs, never force-sells a winner just for reaching
     the old +30% mark. Exit reason is "STOP_LOSS" if the floor never
     moved off its initial -6% level, "TRAILING_STOP" otherwise.
  3. TIME_EXIT    (settings.max_hold_seconds,  6h)
HARD_FLOOR and max_hold_seconds are sourced from the same settings the
(now-removed) RiskEngine used to read, not a hardcoded copy — kept
identical to the live worker's exit logic on purpose, since this
experiment's whole point is being an ongoing benchmark of what real money
is actually doing.

Bad-tick guard (found live 2026-09-22)
-----------------------------------------
Two of the first 17 closed positions (X7, CENTS) exited on a single-tick
price that was 20-80x the price observed one second earlier and one
second later — DexScreener occasionally returns one wildly wrong print
for a pair, not a real, fillable price. Confirmed via the raw
observation history: e.g. CENTS sat flat at +9.5% for over a minute,
one tick read +8355%, and the position closed TAKE_PROFIT on that single
print. A real order could not have filled there.

Fix: an implausible single-step move (ratio vs the last ACCEPTED price
beyond IMPLAUSIBLE_TICK_RATIO, i.e. more than a ~3x move in either
direction in one second) is held as PENDING rather than acted on
immediately. It is only accepted — recorded as an observation, eligible
to trigger an exit — if the NEXT poll confirms a similar magnitude
(within CONFIRMATION_TOLERANCE). A real, sustained move (a genuine
100x pump or a real rug) persists across consecutive ticks and gets
confirmed within ~1-2 seconds; a bad single print does not. This never
delays a real HARD_FLOOR/TAKE_PROFIT by more than one poll cycle (~1s).

Logging contract
-----------------
info:
  confluence_shadow.started / stopped
  confluence_shadow.opened   — token_id, symbol, entry_price, n_rules_cofiring
  confluence_shadow.closed   — token_id, symbol, exit_reason, pnl_pct, hold_seconds
  confluence_shadow.cycle_complete — open_positions, elapsed_ms
warning:
  confluence_shadow.rate_limited
error:
  confluence_shadow.fetch_error
  confluence_shadow.cycle_error
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal

import httpx
from sqlalchemy import select

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from engine.trailing_stop import initial_floor, update_trailing_stop
from models.orm import (
    ConfluenceShadowObservation, ConfluenceShadowPosition, MomentumSignalEvent, Token,
)
from workers.http_queue import RateLimitedQueue
from workers.dexscreener_client import _DEXSCREENER_BASE, _best_pair
from workers.entry_filters import is_buy_pressure_too_low, is_liquidity_too_high, is_wash_trading_rejected

log = get_logger(__name__)

EXPERIMENT_VERSION = "confluence_entry_v1"
SOURCE_EXPERIMENT_VERSION = "momentum_confluence_v1"  # workers/momentum_signal.py
MIN_RULES_COFIRING = 2
VELOCITY_BREAKER_PCT = Decimal("-0.25")

_TIMEOUT = httpx.Timeout(timeout=10.0, connect=5.0)

# DexScreener's multi-token endpoint 400s on a large enough comma-joined
# mint list (confirmed live 2026-09-22 — a ~190-mint URL was rejected).
# TradeMonitorWorker never hit this because MAX_CONCURRENT_TRADES=3 caps it
# at a handful of mints; this worker has no such cap (open positions here
# don't compete for real trade concurrency), so it must chunk defensively.
# 30 is a conservative guess, not a documented DexScreener limit — revisit
# if a request with fewer than 30 mints still 400s.
_MAX_MINTS_PER_REQUEST = 30

# See module docstring's "Bad-tick guard" section. A ratio of 3.0 means any
# single-poll move beyond +200%/-67% is held for confirmation rather than
# acted on immediately — well above TAKE_PROFIT (+30%) and any real
# HARD_FLOOR/STOP_LOSS distance, so it never delays a genuine exit in
# normal conditions, but catches the 20-80x single-print glitches actually
# observed live. CONFIRMATION_TOLERANCE allows the confirming tick to
# differ somewhat from the first (a real fast move keeps moving) while
# still clearly being "the same event", not a second unrelated glitch.
IMPLAUSIBLE_TICK_RATIO = 3.0
CONFIRMATION_TOLERANCE = 0.5  # confirming tick must be within 50% of the first flagged tick


class ConfluenceShadowWorker:
    """
    Opens a real-time-monitored paper position whenever momentum_signal.py
    records a confluence-qualifying signal, and closes it by replaying
    RiskEngine's exact exit priority against live 1-second DexScreener
    prices. Analysis-only — see module docstring.
    """

    def __init__(self, shutdown_event: asyncio.Event, poll_interval: float = 1.0) -> None:
        self._shutdown = shutdown_event
        self._poll_interval = poll_interval
        self._http = RateLimitedQueue()
        # Only signals recorded from THIS moment on are eligible to open a
        # position — never backfill from momentum_signal_events history.
        # Found live 2026-09-22: without this guard, startup opened 193
        # positions at once, one per historical signal, each priced at
        # trigger_price from up to hours earlier — a stale price, not a
        # real "enter right now" decision, and the resulting mint list was
        # large enough to 400 the DexScreener request outright (see
        # _MAX_MINTS_PER_REQUEST). Set once, at construction, not per-cycle.
        self._started_at = datetime.now(timezone.utc)
        # Bad-tick guard state, keyed by position id — see module docstring.
        # last accepted price per position, and any not-yet-confirmed
        # implausible tick awaiting a second look next cycle.
        self._last_accepted_price: dict = {}
        self._pending_tick: dict = {}

    async def run(self) -> None:
        log.info("confluence_shadow.started", poll_interval=self._poll_interval, started_at=self._started_at.isoformat())
        self._http.start()
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                while not self._shutdown.is_set():
                    try:
                        await self._cycle(client)
                    except Exception as exc:
                        log.error("confluence_shadow.cycle_error", error=str(exc), exc_info=True)
                    try:
                        await asyncio.wait_for(self._shutdown.wait(), timeout=self._poll_interval)
                    except asyncio.TimeoutError:
                        pass
        finally:
            self._http.stop()
            log.info("confluence_shadow.stopped")

    async def _cycle(self, client: httpx.AsyncClient) -> None:
        t0 = time.monotonic()
        await self._open_new_positions()

        open_positions = await self._load_open_positions()
        if not open_positions:
            return

        mints = list(open_positions.keys())
        snapshots: dict[str, dict] = {}
        for i in range(0, len(mints), _MAX_MINTS_PER_REQUEST):
            chunk = mints[i:i + _MAX_MINTS_PER_REQUEST]
            try:
                snapshots.update(await self._fetch(client, chunk))
            except Exception as exc:
                log.error("confluence_shadow.fetch_error", error=str(exc),
                          exc_type=type(exc).__name__, chunk_size=len(chunk))

        now = datetime.now(timezone.utc)
        for mint, pos in open_positions.items():
            data = snapshots.get(mint)
            if data is None:
                continue
            price = self._accept_price(pos, data["price_usd"])
            if price is None:
                continue  # implausible tick held for confirmation — see module docstring
            await self._record_observation(pos["id"], price, now)
            await self._maybe_close(pos, price, now)

        elapsed_ms = round((time.monotonic() - t0) * 1000)
        log.info("confluence_shadow.cycle_complete", open_positions=len(mints), elapsed_ms=elapsed_ms)

    def _accept_price(self, pos: dict, new_price: Decimal) -> Decimal | None:
        """
        Bad-tick guard — see module docstring. Returns the price to actually
        use this cycle, or None if this tick is being held for confirmation
        (nothing should be recorded or exit-evaluated this cycle for this
        position).
        """
        position_id = pos["id"]
        last_price = self._last_accepted_price.get(position_id, pos["entry_price"])
        if last_price is None or last_price <= 0:
            self._last_accepted_price[position_id] = new_price
            return new_price

        ratio = new_price / last_price
        plausible = (Decimal("1") / Decimal(str(IMPLAUSIBLE_TICK_RATIO))) <= ratio <= Decimal(str(IMPLAUSIBLE_TICK_RATIO))
        pending = self._pending_tick.get(position_id)

        if plausible:
            self._pending_tick.pop(position_id, None)
            self._last_accepted_price[position_id] = new_price
            return new_price

        if pending is not None:
            confirm_ratio = new_price / pending if pending > 0 else None
            confirmed = confirm_ratio is not None and (
                (1 - CONFIRMATION_TOLERANCE) <= confirm_ratio <= (1 + CONFIRMATION_TOLERANCE)
            )
            if confirmed:
                log.warning(
                    "confluence_shadow.implausible_tick_confirmed",
                    position_id=str(position_id), price=str(new_price),
                    ratio=str(round(float(ratio), 2)),
                )
                self._pending_tick.pop(position_id, None)
                self._last_accepted_price[position_id] = new_price
                return new_price
            # this cycle's price doesn't match the pending glitch either —
            # discard the stale pending value and re-evaluate fresh below
            self._pending_tick.pop(position_id, None)

        log.warning(
            "confluence_shadow.implausible_tick_held",
            position_id=str(position_id), last_price=str(last_price),
            new_price=str(new_price), ratio=str(round(float(ratio), 2)),
        )
        self._pending_tick[position_id] = new_price
        return None

    # ── entry ────────────────────────────────────────────────────────────

    async def _open_new_positions(self) -> None:
        async with get_session() as session:
            result = await session.execute(
                select(
                    MomentumSignalEvent.token_id, MomentumSignalEvent.trigger_price,
                    MomentumSignalEvent.triggered_at, MomentumSignalEvent.n_rules_cofiring,
                    MomentumSignalEvent.buy_pressure,
                )
                .where(
                    MomentumSignalEvent.experiment_version == SOURCE_EXPERIMENT_VERSION,
                    MomentumSignalEvent.n_rules_cofiring >= MIN_RULES_COFIRING,
                    MomentumSignalEvent.triggered_at >= self._started_at,
                )
            )
            candidates = result.all()
            if not candidates:
                return

            existing = await session.execute(
                select(ConfluenceShadowPosition.token_id).where(
                    ConfluenceShadowPosition.experiment_version == EXPERIMENT_VERSION
                )
            )
            already_open = set(existing.scalars().all())

            for token_id, entry_price, entry_time, n_cofiring, buy_pressure in candidates:
                if token_id in already_open:
                    continue
                token_result = await session.execute(select(Token).where(Token.id == token_id))
                token = token_result.scalar_one_or_none()
                if token is None:
                    continue

                # Entry-quality filter (2026-09-24) — see workers/entry_filters.py
                # for the full data behind this: WASH_TRADING-rejected tokens
                # are 56% of all trades and the single worst-performing
                # population (31.3% rug rate, net-negative even capped).
                # Recorded as a permanent 'wash_skipped' row (not just
                # silently passed over) so this candidate is never
                # re-evaluated on a later cycle — already_open only ever
                # reflects rows that exist, and this makes one exist.
                if await is_wash_trading_rejected(session, token_id, entry_time):
                    session.add(ConfluenceShadowPosition(
                        token_id=token_id,
                        experiment_version=EXPERIMENT_VERSION,
                        entry_price=entry_price,
                        entry_time=entry_time,
                        n_rules_cofiring=n_cofiring,
                        status="wash_skipped",
                    ))
                    already_open.add(token_id)
                    log.info(
                        "confluence_shadow.entry_wash_skipped",
                        token_id=str(token_id), symbol=token.symbol,
                    )
                    continue

                # Liquidity-ceiling filter (2026-09-24) — see
                # workers/entry_filters.py for the full data: liquidity <
                # $30k at signal time is a much stronger, cleaner
                # predictor than WASH_TRADING alone (6.5% vs 36.7% rug
                # rate on an evenly-split n=262 dataset), and stays
                # predictive even inside the WASH_TRADING population, so
                # it's checked independently on top of it.
                if await is_liquidity_too_high(session, token_id, entry_time):
                    session.add(ConfluenceShadowPosition(
                        token_id=token_id,
                        experiment_version=EXPERIMENT_VERSION,
                        entry_price=entry_price,
                        entry_time=entry_time,
                        n_rules_cofiring=n_cofiring,
                        status="high_liq_skip",
                    ))
                    already_open.add(token_id)
                    log.info(
                        "confluence_shadow.entry_high_liq_skipped",
                        token_id=str(token_id), symbol=token.symbol,
                    )
                    continue

                # Buy-pressure floor (2026-09-24) — see workers/entry_filters.py
                # for the full data: within the liquidity+wash-trading "good"
                # bucket, buy_pressure >= 0.97 cut the HARD_FLOOR rate from
                # ~34-44% to 11.7% and lifted win rate to 86.4% — the
                # cleanest, most monotonic discriminator found in the whole
                # analysis. Pure/synchronous check, no extra query needed.
                if is_buy_pressure_too_low(buy_pressure):
                    session.add(ConfluenceShadowPosition(
                        token_id=token_id,
                        experiment_version=EXPERIMENT_VERSION,
                        entry_price=entry_price,
                        entry_time=entry_time,
                        n_rules_cofiring=n_cofiring,
                        status="low_bp_skip",
                    ))
                    already_open.add(token_id)
                    log.info(
                        "confluence_shadow.entry_low_bp_skipped",
                        token_id=str(token_id), symbol=token.symbol,
                    )
                    continue

                session.add(ConfluenceShadowPosition(
                    token_id=token_id,
                    experiment_version=EXPERIMENT_VERSION,
                    entry_price=entry_price,
                    entry_time=entry_time,
                    n_rules_cofiring=n_cofiring,
                    status="open",
                ))
                already_open.add(token_id)
                log.info(
                    "confluence_shadow.opened",
                    token_id=str(token_id), symbol=token.symbol,
                    entry_price=str(entry_price), n_rules_cofiring=n_cofiring,
                )

    async def _load_open_positions(self) -> dict[str, dict]:
        """{mint: {id, entry_price, entry_time, ...}} for all OPEN positions."""
        async with get_session() as session:
            result = await session.execute(
                select(
                    Token.mint_address, ConfluenceShadowPosition.id,
                    ConfluenceShadowPosition.entry_price, ConfluenceShadowPosition.entry_time,
                    ConfluenceShadowPosition.trailing_stop_floor, ConfluenceShadowPosition.high_watermark_price,
                )
                .join(Token, Token.id == ConfluenceShadowPosition.token_id)
                .where(ConfluenceShadowPosition.status == "open")
            )
            return {
                mint: dict(id=pid, entry_price=ep, entry_time=et,
                           trailing_stop_floor=floor, high_watermark_price=hwm)
                for mint, pid, ep, et, floor, hwm in result.all()
            }

    # ── exit — layered stop (2026-09-23) ────────────────────────────────

    def _check_exit(
        self, entry_price: Decimal, entry_time: datetime, current_price: Decimal, now: datetime,
        current_floor: Decimal, current_hwm: Decimal,
    ) -> tuple[str | None, Decimal, Decimal]:
        """Returns (exit_reason_or_None, new_floor, new_hwm). See module
        docstring's Exit section for the full rationale."""
        if entry_price is None or entry_price <= Decimal("0"):
            return None, current_floor, current_hwm

        # Velocity breaker computes its drop from entry_price too (not the
        # previous tick, despite the name) — same formula as pnl_pct below.
        pnl_pct = (current_price - entry_price) / entry_price
        if pnl_pct <= VELOCITY_BREAKER_PCT:
            return "HARD_FLOOR", current_floor, max(current_hwm, current_price)
        hard_floor = Decimal(str(settings.HARD_FLOOR_PCT))
        if pnl_pct <= hard_floor:
            return "HARD_FLOOR", current_floor, max(current_hwm, current_price)

        new_floor, new_hwm, should_close = update_trailing_stop(
            entry_price, current_price, current_floor, current_hwm,
        )
        if should_close:
            reason = "STOP_LOSS" if new_floor <= initial_floor(entry_price) else "TRAILING_STOP"
            return reason, new_floor, new_hwm

        hold_seconds = (now - entry_time).total_seconds()
        if hold_seconds >= settings.max_hold_seconds:
            return "TIME_EXIT", new_floor, new_hwm
        return None, new_floor, new_hwm

    async def _maybe_close(self, pos: dict, current_price: Decimal, now: datetime) -> None:
        current_floor = pos.get("trailing_stop_floor") or initial_floor(pos["entry_price"])
        current_hwm = pos.get("high_watermark_price") or pos["entry_price"]
        reason, new_floor, new_hwm = self._check_exit(
            pos["entry_price"], pos["entry_time"], current_price, now, current_floor, current_hwm,
        )

        async with get_session() as session:
            result = await session.execute(
                select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.id == pos["id"])
            )
            row = result.scalar_one_or_none()
            if row is None or row.status != "open":
                return  # already closed by a concurrent cycle
            row.trailing_stop_floor = new_floor
            row.high_watermark_price = new_hwm
            if reason is None:
                return
            pnl_pct = (current_price - pos["entry_price"]) / pos["entry_price"]
            row.status = "closed"
            row.exit_price = current_price
            row.exit_time = now
            row.exit_reason = reason
            row.pnl_pct = pnl_pct
        if reason is None:
            return
        self._last_accepted_price.pop(pos["id"], None)
        self._pending_tick.pop(pos["id"], None)
        log.info(
            "confluence_shadow.closed",
            position_id=str(pos["id"]), exit_reason=reason,
            pnl_pct=str(round(float((current_price - pos["entry_price"]) / pos["entry_price"]) * 100, 2)),
            hold_seconds=round((now - pos["entry_time"]).total_seconds()),
        )

    async def _record_observation(self, position_id, price: Decimal, now: datetime) -> None:
        async with get_session() as session:
            session.add(ConfluenceShadowObservation(
                position_id=position_id, observed_at=now, price_usd=price,
            ))

    # ── DexScreener fetch — same source/parsing as TradeMonitorWorker ─────

    async def _fetch(self, client: httpx.AsyncClient, mints: list[str]) -> dict[str, dict]:
        resp = await self._http.submit(
            client.get, f"{_DEXSCREENER_BASE}/{','.join(mints)}",
        )
        if resp.status_code == 429:
            log.warning("confluence_shadow.rate_limited")
            return {}
        resp.raise_for_status()
        data = resp.json()

        pairs_by_mint: dict[str, list[dict]] = {}
        for pair in data.get("pairs") or []:
            mint = (pair.get("baseToken") or {}).get("address")
            if mint in mints:
                pairs_by_mint.setdefault(mint, []).append(pair)

        result: dict[str, dict] = {}
        for mint, pairs in pairs_by_mint.items():
            pair = _best_pair(pairs)
            if not pair:
                continue
            price = pair.get("priceUsd")
            if price is None:
                continue
            result[mint] = {"price_usd": Decimal(str(price))}
        return result
