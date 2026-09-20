"""
workers/s1_wave_worker.py
==========================
S1 Wave Entry Layer — upstream of the 3-window scorer.

Fires on snapshot 1 data alone, before scoring has enough windows to
evaluate anything. Positions the bot at the head of a token's move
rather than waiting for burst confirmation.

Entry logic — v2 filters (55-trade statistical analysis, 2026-05-17)
----------------------------------------------------------------------
Enter if at snapshot 1:
  - buy_pressure >= 0.80    (raised from 0.72 — 12% WR below 0.80)
  - price_change_5m < 80%   (move has NOT already run)
  - price_change_1m == 0    (move hasn't started yet)
  - liquidity >= $20k       (rug zone below $20k, 0-18% WR)
  - liquidity < $35k        (token too established above)
  - wash_multiplier >= 6    (16% WR below 6, 43% WR above)

Deliberately conservative — bp ceiling and liq upper not added (thin data)
Next review at 50 trades with these filters

How it hooks in
---------------
The sampling_worker publishes a SnapshotEvent onto s1_queue after
writing EVERY snapshot. This worker drains that queue and acts only
on snapshot_number == 1.

This keeps the sampling worker as a pure data writer. Entry logic
lives here, not in the sampling pipeline.

Position sizing
---------------
Same as scorer trades: TRADE_ALLOCATION_PCT x available_balance.
All risk gates apply (circuit breaker, daily halt, concurrent limit).

Trailing stop
-------------
Initialised at entry: floor = entry_price * 0.94 (-6%)
high_watermark = entry_price
trailing_stop_active = True

The TradeRiskWorker drives the staircase on every MarketEvent tick.

Logging contract
----------------
info:
  s1_wave.started / stopped
  s1_wave.entry_signal      — mint, symbol, bp, price_change_5m
  s1_wave.skip_extended     — bp > 0.72 but 5m >= 80%, already extended
  s1_wave.skip_no_signal    — bp <= 0.72
  s1_wave.trade_opened      — mint, symbol, entry_price, entry_source=s1_wave
  s1_wave.entry_blocked     — risk gate reason
debug:
  s1_wave.not_snapshot_1    — snapshot_number > 1, skip
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, func

from config.logging import get_logger
from engine.execution import ExecutionEngine
from config.settings import settings
from database.engine import get_session
from engine.trailing_stop import initial_floor
from models.orm import (
    BalanceHistory,
    CircuitBreakerState,
    DailyLossState,
    LiquidityFlag,
    NotificationEvent,
    NotificationQueue,
    SessionState,
    Token,
    TokenEvaluation,
    TokenStatus,
    Trade,
    TradeStatus,
    TrendDirection,
    VMZone,
    LiquidityTrend,
)
from workers.events import SnapshotEvent
from workers.http_queue import get_http_queue

log = get_logger(__name__)

# S1 entry thresholds — v2 filters (your exact specification)
# Analysis date: 2026-05-17
# Previous: bp>=0.72, liq<$35k, no wash filter
# New:      bp>=0.80, wash>=6, liq>=$20k
#
# Evidence:
#   bp 0.72-0.80: 12% WR (17 trades) — danger zone, eliminated
#   wash 0-6:     16% WR (36 trades) — eliminated
#   wash >= 6:    43% WR (30 trades) — strong signal
#   liq $18k-$22k: 0-18% WR — eliminated (rug had liq=$18,136)
#
# Deliberately NOT added (insufficient data):
#   bp ceiling 0.92 — only 1 rug, not enough evidence
#   liq upper $28k  — only 1 trade in that bucket
#
_BP_THRESHOLD      = Decimal("0.80")   # raised from 0.72 — eliminates 12% WR zone
_PRICE5M_EXTENDED  = 80.0              # skip if 5m change >= this (already extended)
_PRICE1M_COLD      = 0.0               # skip if 1m change != 0 (move already started)
_MIN_LIQUIDITY_USD = Decimal("20000")  # new — liq<$20k zone: 0-18% WR, rug zone
_MAX_LIQUIDITY_USD = Decimal("35000")  # restored to original — $28k had 1 trade only
_MIN_WASH_MULT     = 6.0               # new — wash<6: 16% WR vs wash>=6: 43% WR

# Staleness check: if price dropped more than this % since snapshot, skip entry.
# GhFuVwnn dropped 14.4% and EDtoyRq dropped 17.8% between snapshot and entry.
# Both exited instantly because floor was set on stale snapshot price.
_MAX_ENTRY_SLIPPAGE_PCT = 5.0


class S1WaveWorker:
    """
    Consumes SnapshotEvents, fires on snapshot 1 signals.

    Parameters
    ----------
    s1_queue       : asyncio.Queue[SnapshotEvent] — filled by sampling_worker
    shutdown_event : asyncio.Event
    """

    def __init__(
        self,
        s1_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._queue     = s1_queue
        self._shutdown  = shutdown_event
        self._execution = ExecutionEngine()

    async def run(self) -> None:
        log.info("s1_wave.started")

        while not self._shutdown.is_set():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                await self._evaluate(event)
            except Exception as exc:
                log.error("s1_wave.error", mint=event.mint,
                          error=str(exc), exc_info=True)

        log.info("s1_wave.stopped")

    async def _evaluate(self, event: SnapshotEvent) -> None:
        # Only act on the very first snapshot
        if event.snapshot_number != 1:
            log.debug("s1_wave.not_snapshot_1",
                      mint=event.mint, n=event.snapshot_number)
            return

        bp   = event.buy_pressure
        pc1  = event.price_change_1m
        pc5  = event.price_change_5m
        liq  = event.liquidity_usd

        # Every check's raw inputs, gathered once — the evaluation row (pass
        # or fail) always carries the full picture the gate looked at, not
        # just whichever field tripped the skip.
        inputs = {
            "buy_pressure":     str(bp),
            "price_change_1m":  pc1,
            "price_change_5m":  pc5,
            "liquidity_usd":    str(liq),
            "wash_multiplier":  None,  # filled in once the DB lookup runs
        }

        # Skip: bp below threshold — danger zone 0.72-0.80 has 12% WR
        if bp <= _BP_THRESHOLD:
            log.info("s1_wave.skip_no_signal",
                     mint=event.mint, symbol=event.symbol,
                     bp=str(bp), price_change_1m=pc1, price_change_5m=pc5,
                     liquidity_usd=str(liq))
            await self._write_evaluation(event.mint, passed=False,
                                          reason_code="skip_no_signal", inputs=inputs)
            return

        # Skip: 5m move already extended — buying the top
        if pc5 >= _PRICE5M_EXTENDED:
            log.info("s1_wave.skip_extended",
                     mint=event.mint, symbol=event.symbol,
                     bp=str(bp), price_change_1m=pc1, price_change_5m=pc5,
                     liquidity_usd=str(liq))
            await self._write_evaluation(event.mint, passed=False,
                                          reason_code="skip_extended", inputs=inputs)
            return

        # Skip: 1m price already moving — move started before S1
        if pc1 != _PRICE1M_COLD:
            log.info("s1_wave.skip_price_moving",
                     mint=event.mint, symbol=event.symbol,
                     bp=str(bp), price_change_1m=pc1, price_change_5m=pc5,
                     liquidity_usd=str(liq))
            await self._write_evaluation(event.mint, passed=False,
                                          reason_code="skip_price_moving", inputs=inputs)
            return

        # Skip: liquidity too low — liq<$20k zone has 0-18% WR, rug zone
        if liq < _MIN_LIQUIDITY_USD:
            log.info("s1_wave.skip_low_liquidity",
                     mint=event.mint, symbol=event.symbol,
                     bp=str(bp), liquidity_usd=str(liq))
            await self._write_evaluation(event.mint, passed=False,
                                          reason_code="skip_low_liquidity", inputs=inputs)
            return

        # Skip: liquidity too high — token too established
        if liq >= _MAX_LIQUIDITY_USD:
            log.info("s1_wave.skip_high_liquidity",
                     mint=event.mint, symbol=event.symbol,
                     bp=str(bp), liquidity_usd=str(liq))
            await self._write_evaluation(event.mint, passed=False,
                                          reason_code="skip_high_liquidity", inputs=inputs)
            return

        # Skip: wash multiplier too low — wash<6 has 16% WR vs wash>=6 at 43% WR
        # Requires DB lookup for token wash_multiplier recorded at discovery
        wash = await self._get_wash_multiplier(event.mint)
        inputs["wash_multiplier"] = wash
        if wash is not None and wash < _MIN_WASH_MULT:
            log.info("s1_wave.skip_low_wash",
                     mint=event.mint, symbol=event.symbol,
                     bp=str(bp), wash_multiplier=wash,
                     liquidity_usd=str(liq))
            await self._write_evaluation(event.mint, passed=False,
                                          reason_code="skip_low_wash", inputs=inputs)
            return

        # All filters passed — signal confirmed
        log.info("s1_wave.entry_signal",
                 mint=event.mint, symbol=event.symbol,
                 bp=str(bp), price_change_1m=pc1, price_change_5m=pc5,
                 liquidity_usd=str(liq))
        await self._write_evaluation(event.mint, passed=True,
                                      reason_code="entry_signal", inputs=inputs)

        await self._open_trade(event)

    async def _write_evaluation(
        self, mint: str, *, passed: bool, reason_code: str, inputs: dict,
    ) -> None:
        async with get_session() as session:
            result = await session.execute(
                select(Token.id).where(Token.mint_address == mint)
            )
            token_id = result.scalar_one_or_none()
            if token_id is None:
                log.warning("s1_wave.evaluation_token_not_found", mint=mint)
                return
            session.add(TokenEvaluation(
                token_id=token_id,
                gate="S1_WAVE",
                passed=passed,
                reason_code=reason_code,
                inputs_json=inputs,
            ))

    async def _open_trade(self, event: SnapshotEvent) -> None:
        """Open an S1 wave position if all risk gates pass."""

        # ── Risk gate ─────────────────────────────────────────────────────
        gate = await self._risk_gate(event.mint)
        if gate is not None:
            log.info("s1_wave.entry_blocked", mint=event.mint,
                     symbol=event.symbol, reason=gate)
            return

        # ── Position sizing ───────────────────────────────────────────────
        position_usd, balance_before = await self._size_position()
        if position_usd is None:
            log.info("s1_wave.insufficient_balance", mint=event.mint)
            return

        # ── Staleness check — re-fetch current price before entry ─────────
        # Problem: snapshot price may be stale by 200-800ms by the time
        # we reach here. On volatile memecoins, price can drop 10-20% in
        # that window. If we set the floor based on snapshot price and the
        # token has already fallen through it, we exit instantly at -14-18%.
        #
        # Fix: re-fetch the current price from ST. If it has dropped more
        # than _MAX_ENTRY_SLIPPAGE_PCT from the snapshot price, skip entry.
        # The moment has passed — the setup is no longer valid.
        current_price = await self._fetch_current_price(event.mint)
        if current_price is not None:
            snapshot_price = event.price_usd
            if snapshot_price > 0:
                slippage = float((current_price - snapshot_price) / snapshot_price * 100)
                if slippage <= -_MAX_ENTRY_SLIPPAGE_PCT:
                    log.info(
                        "s1_wave.skip_stale_price",
                        mint=event.mint,
                        symbol=event.symbol,
                        snapshot_price=str(snapshot_price),
                        current_price=str(current_price),
                        slippage_pct=round(slippage, 2),
                        threshold_pct=-_MAX_ENTRY_SLIPPAGE_PCT,
                    )
                    return
                # Use fresh price for entry and floor calculation
                event = SnapshotEvent(
                    mint=event.mint,
                    symbol=event.symbol,
                    price_usd=current_price,
                    liquidity_usd=event.liquidity_usd,
                    buy_pressure=event.buy_pressure,
                    price_change_1m=event.price_change_1m,
                    price_change_5m=event.price_change_5m,
                    snapshot_number=event.snapshot_number,
                )
                log.info(
                    "s1_wave.price_refreshed",
                    mint=event.mint,
                    symbol=event.symbol,
                    snapshot_price=str(snapshot_price),
                    current_price=str(current_price),
                    slippage_pct=round(slippage, 2),
                )

        # ── Execute buy (paper or live) ───────────────────────────────────
        now = datetime.now(timezone.utc)
        entry_price = event.price_usd
        floor = initial_floor(entry_price)

        from config.settings import settings as _s
        sol_price = Decimal(str(_s.SOL_PRICE_USD)) if _s.SOL_PRICE_USD else Decimal("150")
        exec_result = await self._execution.buy(
            mint=event.mint,
            position_usd=position_usd,
            sol_price_usd=sol_price,
        )
        if not exec_result.success:
            log.error(
                "s1_wave.buy_execution_failed",
                mint=event.mint,
                symbol=event.symbol,
                error_type=exec_result.error_type,
                error=exec_result.error_detail,
            )
            return

        # Use actual fill price if live execution
        if exec_result.actual_price:
            entry_price = exec_result.actual_price
            floor = initial_floor(entry_price)

        async with get_session() as session:
            # Re-verify token is WATCHING under lock
            token_result = await session.execute(
                select(Token)
                .where(Token.mint_address == event.mint)
                .with_for_update()
            )
            token = token_result.scalar_one_or_none()

            if token is None:
                log.warning("s1_wave.token_not_found", mint=event.mint)
                return

            if token.status != TokenStatus.WATCHING:
                # The gate said yes, but by the time we got here (risk gate,
                # position sizing, staleness re-fetch, Jupiter round-trip)
                # the token was no longer WATCHING — most likely it aged out
                # or another path already moved it. This is a distinct
                # outcome from every skip_* above: the signal was correct,
                # execution just lost the race. Give it its own label so
                # the derived-outcome analysis doesn't fold it into either
                # bucket.
                session.add(TokenEvaluation(
                    token_id=token.id,
                    gate="S1_WAVE",
                    passed=True,
                    reason_code="signal_passed_not_tradeable",
                    inputs_json={"status_at_execution": token.status.value},
                ))
                log.debug("s1_wave.token_not_watching",
                          mint=event.mint, status=token.status.value)
                return

            # Deduct position under lock
            state_result = await session.execute(
                select(SessionState).where(SessionState.id == 1).with_for_update()
            )
            state = state_result.scalar_one()

            if state.available_balance < position_usd:
                log.warning("s1_wave.balance_changed_under_lock",
                            mint=event.mint,
                            balance=str(state.available_balance),
                            position=str(position_usd))
                return

            balance_after = state.available_balance - position_usd
            state.available_balance = balance_after

            trade = Trade(
                token_id=token.id,
                token_mint=token.mint_address,
                entry_price=entry_price,
                entry_time=now,
                # S1 trades have no composite score — use 0.0
                entry_composite_score=Decimal("0.00"),
                # S1 trades have no trend analysis — use FLAT as neutral
                entry_bp_trend=TrendDirection.FLAT,
                entry_vm_trend=TrendDirection.FLAT,
                entry_vm_zone=VMZone.EARLY,
                entry_liquidity_flag=LiquidityFlag.NORMAL,
                entry_liquidity_trend=LiquidityTrend.STABLE,
                position_size_usd=position_usd,
                status=TradeStatus.OPEN,
                # S1 wave fields
                entry_source="s1_wave",
                trailing_stop_active=True,
                trailing_stop_floor=floor,
                high_watermark_price=entry_price,
                # Execution fields
                token_amount_bought=exec_result.actual_amount,
                tx_signature_entry=exec_result.tx_signature,
            )
            session.add(trade)
            await session.flush()

            token.status = TokenStatus.ENTERED

            import orjson
            notif = NotificationQueue(
                event_type=NotificationEvent.TRADE_OPEN,
                payload=orjson.dumps({
                    "trade_id":     str(trade.id),
                    "mint":         token.mint_address,
                    "symbol":       token.symbol,
                    "entry_price":  str(entry_price),
                    "position_usd": str(position_usd),
                    "balance_after": str(balance_after),
                    "entry_source": "s1_wave",
                    "bp":           str(event.buy_pressure),
                    "price_5m":     event.price_change_5m,
                    "trailing_floor": str(floor),
                    "timestamp":    now.isoformat(),
                }).decode(),
                trade_id=trade.id,
            )
            session.add(notif)

        log.info(
            "s1_wave.trade_opened",
            mint=event.mint,
            symbol=event.symbol,
            entry_source="s1_wave",
            entry_price=str(entry_price),
            position_usd=str(position_usd),
            bp=str(event.buy_pressure),
            price_change_5m=event.price_change_5m,
            trailing_floor=str(floor),
            balance_after=str(balance_after),
        )

    async def _get_wash_multiplier(self, mint: str) -> float | None:
        """Fetch wash_multiplier from tokens table recorded at discovery."""
        try:
            async with get_session() as session:
                result = await session.execute(
                    select(Token.wash_multiplier).where(Token.mint_address == mint)
                )
                row = result.scalar_one_or_none()
                return float(row) if row is not None else None
        except Exception as exc:
            log.warning("s1_wave.wash_lookup_failed", mint=mint, error=str(exc))
            return None

    async def _fetch_current_price(self, mint: str) -> Decimal | None:
        """Re-fetch current price from ST before entry to detect stale snapshots."""
        try:
            http_queue = get_http_queue()
            resp = await http_queue.post(
                url=f"https://data.solanatracker.io/tokens/multi",
                json=[mint],
                api_key=settings.SOLANA_TRACKER_API_KEY,
            )
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            token_data = data.get(mint) or data.get("tokens", {}).get(mint)
            if not token_data:
                return None
            pool = next(
                (p for p in token_data.get("pools", []) if p.get("market") == "pumpfun-amm"),
                None,
            )
            if not pool:
                return None
            price = pool.get("price", {}).get("usd", 0)
            if price and float(price) > 0:
                return Decimal(str(price))
        except Exception as exc:
            log.warning("s1_wave.price_refresh_failed", mint=mint, error=str(exc))
        return None

    async def _risk_gate(self, mint: str) -> str | None:
        """Same gates as CapitalEngine. Returns None if entry allowed."""
        async with get_session() as session:
            cb_result = await session.execute(
                select(CircuitBreakerState).where(CircuitBreakerState.id == 1)
            )
            cb = cb_result.scalar_one()
            if cb.is_paused:
                now = datetime.now(timezone.utc)
                if cb.resume_at and now < cb.resume_at:
                    return f"CIRCUIT_BREAKER_PAUSED:resume_at={cb.resume_at.isoformat()}"
                else:
                    cb.is_paused = False
                    cb.resume_at = None
                    cb.consecutive_losses = 0  # clean slate after pause
                    import orjson as _orjson
                    from models.orm import NotificationEvent, NotificationQueue
                    session.add(NotificationQueue(
                        event_type=NotificationEvent.CB_RESUMED,
                        payload=_orjson.dumps({
                            "paused_duration": "~1 hour",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }).decode(),
                    ))

            dl_result = await session.execute(
                select(DailyLossState).where(DailyLossState.id == 1)
            )
            dl = dl_result.scalar_one()
            if dl.is_halted:
                return "DAILY_LOSS_HALT"

            open_count_result = await session.execute(
                select(func.count(Trade.id)).where(Trade.status == TradeStatus.OPEN)
            )
            open_count = open_count_result.scalar_one()
            if open_count >= settings.MAX_CONCURRENT_TRADES:
                return f"MAX_CONCURRENT_TRADES:{open_count}/{settings.MAX_CONCURRENT_TRADES}"

        return None

    async def _size_position(self) -> tuple[Decimal, Decimal] | tuple[None, None]:
        async with get_session() as session:
            result = await session.execute(
                select(SessionState).where(SessionState.id == 1)
            )
            state = result.scalar_one()
            balance = state.available_balance

        allocation   = Decimal(str(settings.TRADE_ALLOCATION_PCT))
        min_position = Decimal(str(settings.MIN_POSITION_USD))
        position_usd = (balance * allocation).quantize(Decimal("0.000001"))

        if position_usd < min_position:
            return None, None

        return position_usd, balance
