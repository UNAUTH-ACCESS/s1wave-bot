"""
workers/trade_risk_worker.py
=============================
Trade risk worker — joins market events to open trades, drives exit decisions.

Routes each MarketEvent to the correct exit handler based on entry_source:

  s1_wave trades  → trailing stop staircase (engine/trailing_stop.py)
  scorer trades   → risk engine fixed rules (engine/risk.py)

The join (MarketEvent → Trade) happens here, once per drain pass.
Neither the trailing stop engine nor the risk engine touches the DB
during evaluation — only on close.

Logging contract
----------------
debug:
  trade_risk.evaluated         — mint, price_usd, entry_source
info:
  trade_risk.started / stopped
  trade_risk.trailing_stop_hit — mint, price, floor, hwm, locked_pct
  trade_risk.floor_advanced    — mint, old_floor, new_floor, hwm
error:
  trade_risk.evaluate_error
  trade_risk.cycle_error
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import orjson
from sqlalchemy import select

from config.logging import get_logger
from engine.execution import ExecutionEngine
from database.engine import get_session
from engine.risk import RiskEngine
from engine.trailing_stop import update_trailing_stop
from models.orm import (
    BalanceHistory,
    CircuitBreakerState,
    DailyLossState,
    ExitReason,
    NotificationEvent,
    NotificationQueue,
    SessionState,
    Token,
    TokenStatus,
    Trade,
    TradeStatus,
)
from workers.events import MarketEvent

log = get_logger(__name__)

_IDLE_SLEEP = 0.1


class TradeRiskWorker:
    """
    Bridges TradeMonitorWorker -> exit handlers.

    Parameters
    ----------
    monitor_queue  : asyncio.Queue[MarketEvent]
    risk_engine    : RiskEngine  (handles scorer trades)
    shutdown_event : asyncio.Event
    """

    def __init__(
        self,
        monitor_queue: asyncio.Queue,
        risk_engine: RiskEngine,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._queue     = monitor_queue
        self._risk      = risk_engine
        self._shutdown  = shutdown_event
        self._execution = ExecutionEngine()

    async def run(self) -> None:
        log.info("trade_risk.started")

        while not self._shutdown.is_set():
            try:
                await self._drain()
            except Exception as exc:
                log.error("trade_risk.cycle_error",
                          error=str(exc), exc_info=True)

            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=_IDLE_SLEEP
                )
            except asyncio.TimeoutError:
                pass

        log.info("trade_risk.stopped")

    async def _drain(self) -> None:
        events: list[MarketEvent] = []
        while True:
            try:
                event = self._queue.get_nowait()
                events.append(event)
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break

        if not events:
            return

        # Load open trades once for this drain pass — keyed by mint
        open_trades = await self._load_open_trades()
        if not open_trades:
            return

        for event in events:
            trade = open_trades.get(event.mint)
            if trade is None:
                continue

            try:
                await self._route(trade, event)
                log.debug(
                    "trade_risk.evaluated",
                    mint=event.mint,
                    price=str(event.price_usd),
                    entry_source=trade.entry_source,
                )
            except Exception as exc:
                log.error(
                    "trade_risk.evaluate_error",
                    mint=event.mint,
                    error=str(exc),
                    exc_info=True,
                )

    async def _route(self, trade: Trade, event: MarketEvent) -> None:
        """Route to the correct exit handler based on entry_source."""

        # ── Velocity circuit breaker ──────────────────────────────────────
        # If price drops more than 25% in a single 1-second tick, close
        # immediately — this is a rug pull or flash crash, not a normal
        # reversal. Caps catastrophic loss before trailing stop evaluates.
        # HIGHER dropped 96% in 6 seconds — this would have limited it to ~25%.
        if trade.entry_price and trade.entry_price > Decimal("0"):
            tick_drop = (event.price_usd - trade.entry_price) / trade.entry_price
            if tick_drop <= Decimal("-0.25"):
                log.warning(
                    "trade_risk.velocity_breaker_triggered",
                    mint=trade.token_mint,
                    entry_price=str(trade.entry_price),
                    current_price=str(event.price_usd),
                    drop_pct=round(float(tick_drop) * 100, 2),
                )
                # Close at current price — don't wait for floor evaluation
                from engine.risk import ExitReason as _ExitReason
                await self._risk.close_trade(trade, event.price_usd, _ExitReason.HARD_FLOOR)
                return

        if trade.entry_source == "s1_wave" and trade.trailing_stop_active:
            await self._evaluate_trailing_stop(trade, event)
        else:
            # scorer or scorer_addon — fixed rules
            await self._risk.evaluate(trade, event)

    async def _evaluate_trailing_stop(
        self, trade: Trade, event: MarketEvent
    ) -> None:
        """
        Run the trailing stop staircase for one S1 wave position.
        Updates DB if floor advanced or position should close.
        """
        current_price = event.price_usd
        entry_price   = trade.entry_price
        current_floor = trade.trailing_stop_floor or (entry_price * Decimal("0.94"))
        current_hwm   = trade.high_watermark_price or entry_price

        new_floor, new_hwm, should_close = update_trailing_stop(
            entry_price=entry_price,
            current_price=current_price,
            current_floor=current_floor,
            current_hwm=current_hwm,
        )

        if should_close:
            pct = float((current_price - entry_price) / entry_price * 100)
            locked_pct = float((new_floor - entry_price) / entry_price * 100)
            log.info(
                "trade_risk.trailing_stop_hit",
                mint=trade.token_mint,
                price=str(current_price),
                floor=str(new_floor),
                hwm=str(new_hwm),
                pnl_pct=round(pct, 2),
                locked_pct=round(locked_pct, 2),
            )
            await self._close_trailing_stop(trade, current_price, new_floor, new_hwm)
            return

        # Update DB if floor or hwm changed
        floor_advanced = new_floor > current_floor
        hwm_updated    = new_hwm > current_hwm

        if floor_advanced or hwm_updated:
            if floor_advanced:
                log.info(
                    "trade_risk.floor_advanced",
                    mint=trade.token_mint,
                    old_floor=str(current_floor),
                    new_floor=str(new_floor),
                    hwm=str(new_hwm),
                )
            async with get_session() as session:
                result = await session.execute(
                    select(Trade).where(Trade.id == trade.id).with_for_update()
                )
                db_trade = result.scalar_one_or_none()
                if db_trade and db_trade.status == TradeStatus.OPEN:
                    db_trade.trailing_stop_floor  = new_floor
                    db_trade.high_watermark_price = new_hwm

    async def _close_trailing_stop(
        self,
        trade: Trade,
        exit_price: Decimal,
        final_floor: Decimal,
        final_hwm: Decimal,
    ) -> None:
        """Close an S1 wave position that hit its trailing stop floor."""
        # Execute sell before writing to DB
        token_amount = trade.token_amount_bought or Decimal("0")
        exec_result = await self._execution.sell(
            mint=trade.token_mint,
            token_amount=token_amount,
            trade_id=str(trade.id),
        )
        if not exec_result.success:
            # sell_failed_critical already logged and Telegram alerted
            # Leave position open — do not write exit to DB
            return

        # Use actual fill price from live execution if available
        if exec_result.actual_price:
            exit_price = exec_result.actual_price

        now = datetime.now(timezone.utc)
        pnl_pct  = (exit_price - trade.entry_price) / trade.entry_price
        pnl_usd  = (trade.position_size_usd * pnl_pct).quantize(Decimal("0.000001"))
        proceeds = trade.position_size_usd + pnl_usd
        hold_s   = int((now - trade.entry_time).total_seconds())

        async with get_session() as session:
            trade_result = await session.execute(
                select(Trade).where(Trade.id == trade.id).with_for_update()
            )
            db_trade = trade_result.scalar_one_or_none()
            if db_trade is None or db_trade.status != TradeStatus.OPEN:
                return

            db_trade.exit_price           = exit_price
            db_trade.exit_time            = now
            db_trade.exit_reason          = ExitReason.TRAILING_STOP
            db_trade.tx_signature_exit    = exec_result.tx_signature
            db_trade.pnl_usd              = pnl_usd
            db_trade.pnl_pct              = pnl_pct.quantize(Decimal("0.000001"))
            db_trade.hold_duration_seconds = hold_s
            db_trade.status               = TradeStatus.CLOSED
            db_trade.trailing_stop_floor  = final_floor
            db_trade.high_watermark_price = final_hwm

            # Token status
            token_result = await session.execute(
                select(Token).where(Token.mint_address == trade.token_mint)
            )
            token = token_result.scalar_one_or_none()
            if token:
                token.status = TokenStatus.CLOSED

            # Return proceeds to balance
            state_result = await session.execute(
                select(SessionState).where(SessionState.id == 1).with_for_update()
            )
            state = state_result.scalar_one()
            balance_before = state.available_balance
            balance_after  = balance_before + proceeds
            state.available_balance = balance_after

            session.add(BalanceHistory(
                trade_id=db_trade.id,
                balance_before=balance_before,
                balance_after=balance_after,
                snapshot_at=now,
            ))

            # Circuit breaker
            cb_result = await session.execute(
                select(CircuitBreakerState).where(CircuitBreakerState.id == 1).with_for_update()
            )
            cb = cb_result.scalar_one()
            if pnl_usd < Decimal("0"):
                from datetime import timedelta
                from config.settings import settings
                cb.consecutive_losses += 1
                if cb.consecutive_losses >= settings.CB_LOSS_COUNT:
                    cb.is_paused = True
                    cb.pause_started_at = now
                    cb.resume_at = now + timedelta(seconds=settings.cb_pause_seconds)
            else:
                cb.consecutive_losses = 0

            # Daily loss
            dl_result = await session.execute(
                select(DailyLossState).where(DailyLossState.id == 1).with_for_update()
            )
            dl = dl_result.scalar_one()
            dl.cumulative_pnl_usd += pnl_usd
            from config.settings import settings as _s
            max_daily_loss = dl.day_open_balance * Decimal(str(_s.MAX_DAILY_LOSS_PCT))
            if not dl.is_halted and dl.cumulative_pnl_usd <= -max_daily_loss:
                dl.is_halted = True
                dl.halted_at = now

            session.add(NotificationQueue(
                event_type=NotificationEvent.TRADE_CLOSE,
                payload=orjson.dumps({
                    "trade_id":     str(db_trade.id),
                    "mint":         db_trade.token_mint,
                    "symbol":       token.symbol if token else None,
                    "exit_price":   str(exit_price),
                    "entry_price":  str(trade.entry_price),
                    "pnl_usd":      str(pnl_usd),
                    "pnl_pct":      str(round(float(pnl_pct) * 100, 2)),
                    "exit_reason":  "TRAILING_STOP",
                    "entry_source": "s1_wave",
                    "high_watermark": str(final_hwm),
                    "hold_seconds": hold_s,
                    "balance_after": str(balance_after),
                    "timestamp":    now.isoformat(),
                }).decode(),
                trade_id=db_trade.id,
            ))

        log.info(
            "risk_engine.trade_closed",
            mint=trade.token_mint,
            symbol=token.symbol if token else None,
            exit_reason="TRAILING_STOP",
            entry_source="s1_wave",
            pnl_usd=str(pnl_usd),
            pnl_pct=str(round(float(pnl_pct) * 100, 2)) + "%",
            high_watermark=str(final_hwm),
            hold_seconds=hold_s,
            balance_after=str(balance_after),
        )

    async def _load_open_trades(self) -> dict[str, Trade]:
        async with get_session() as session:
            result = await session.execute(
                select(Trade).where(Trade.status == TradeStatus.OPEN)
            )
            trades = result.scalars().all()
        return {t.token_mint: t for t in trades}
