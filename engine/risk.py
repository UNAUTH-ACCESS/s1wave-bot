"""
engine/risk.py
==============
Risk engine — pure exit decision logic.

Receives (Trade, MarketEvent) from TradeRiskWorker and evaluates exit
conditions. No DB access, no vendor data, no I/O in evaluate().
Only close_trade() touches the DB — and only when an exit is triggered.

Exit rule priority (evaluated in strict order):
  1. HARD_FLOOR   — price dropped >= HARD_FLOOR_PCT  (-7%)
  2. STOP_LOSS    — price dropped >= STOP_LOSS_PCT    (-6%)
  3. TAKE_PROFIT  — price rose    >= TAKE_PROFIT_PCT  (+30%)
  4. TIME_EXIT    — held for      >= MAX_HOLD_HOURS    (6h)

Circuit breaker:
  - consecutive_losses increments on every losing close
  - resets to 0 on any winning close
  - when consecutive_losses >= CB_LOSS_COUNT: pause for CB_PAUSE_MINUTES

Daily loss limit:
  - cumulative_pnl_usd tracked per UTC day
  - when loss >= MAX_DAILY_LOSS_PCT x day_open_balance: halt all new entries
  - resets at midnight UTC (handled by heartbeat worker)

Public API:
  RiskEngine.evaluate(trade, event)  -- called by TradeRiskWorker
  RiskEngine.close_trade(...)        -- called when exit condition is met
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import select

from config.logging import get_logger
from engine.execution import ExecutionEngine
from workers.events import MarketEvent
from config.settings import settings
from database.engine import get_session
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

log = get_logger(__name__)


class RiskEngine:
    """
    Evaluates open positions and closes them when exit conditions are met.

    Called by:
      - sampling_worker after each cycle (Phase 5 price source)
      - price_tick_worker on each WebSocket tick (Phase 7 price source)

    Parameters
    ----------
    shutdown_event: asyncio.Event
    """

    def __init__(self, shutdown_event: asyncio.Event) -> None:
        self._shutdown = shutdown_event
        self._execution = ExecutionEngine()

    # ── Public API ────────────────────────────────────────────────────────────

    async def evaluate(self, trade: Trade, event: MarketEvent) -> None:
        """
        Evaluate a single trade against a MarketEvent.
        Called by TradeRiskWorker after joining trade to market data.

        No DB access. No I/O. Pure decision logic.
        close_trade() is the only method that writes to DB.
        """
        # Velocity circuit breaker — catches rug pulls before exit rules
        # A 25%+ single-tick drop is a rug/flash crash, not a normal reversal
        if trade.entry_price and trade.entry_price > Decimal("0"):
            tick_drop = (event.price_usd - trade.entry_price) / trade.entry_price
            if tick_drop <= Decimal("-0.25"):
                log.warning(
                    "risk_engine.velocity_breaker_triggered",
                    mint=trade.token_mint,
                    drop_pct=round(float(tick_drop) * 100, 2),
                )
                await self.close_trade(trade, event.price_usd, ExitReason.HARD_FLOOR)
                return

        exit_reason = self._check_exit_conditions(trade, event.price_usd)
        if exit_reason:
            await self.close_trade(trade, event.price_usd, exit_reason)

    # ── Exit evaluation ───────────────────────────────────────────────────────

    def _check_exit_conditions(
        self, trade: Trade, current_price: Decimal
    ) -> ExitReason | None:
        """
        Evaluate exit rules in strict priority order.

        Priority 1: HARD_FLOOR  (price gap protection)
        Priority 2: STOP_LOSS
        Priority 3: TAKE_PROFIT
        Priority 4: TIME_EXIT

        Returns the first triggered ExitReason, or None if no exit.
        """
        if trade.entry_price <= Decimal("0"):
            return None

        pnl_pct = (current_price - trade.entry_price) / trade.entry_price
        now = datetime.now(timezone.utc)

        # ── Priority 1: Hard floor ────────────────────────────────────────
        hard_floor = Decimal(str(settings.HARD_FLOOR_PCT))
        if pnl_pct <= hard_floor:
            log.debug(
                "risk_engine.hard_floor_triggered",
                mint=trade.token_mint,
                pnl_pct=str(round(pnl_pct * 100, 2)),
                threshold=str(settings.HARD_FLOOR_PCT * 100),
            )
            return ExitReason.HARD_FLOOR

        # ── Priority 2: Stop loss ─────────────────────────────────────────
        stop_loss = Decimal(str(settings.STOP_LOSS_PCT))
        if pnl_pct <= stop_loss:
            log.debug(
                "risk_engine.stop_loss_triggered",
                mint=trade.token_mint,
                pnl_pct=str(round(pnl_pct * 100, 2)),
            )
            return ExitReason.STOP_LOSS

        # ── Priority 3: Take profit ───────────────────────────────────────
        take_profit = Decimal(str(settings.TAKE_PROFIT_PCT))
        if pnl_pct >= take_profit:
            log.debug(
                "risk_engine.take_profit_triggered",
                mint=trade.token_mint,
                pnl_pct=str(round(pnl_pct * 100, 2)),
            )
            return ExitReason.TAKE_PROFIT

        # ── Priority 4: Time exit ─────────────────────────────────────────
        hold_seconds = (now - trade.entry_time).total_seconds()
        if hold_seconds >= settings.max_hold_seconds:
            log.debug(
                "risk_engine.time_exit_triggered",
                mint=trade.token_mint,
                hold_hours=round(hold_seconds / 3600, 2),
            )
            return ExitReason.TIME_EXIT

        return None

    # ── Trade close ───────────────────────────────────────────────────────────

    async def close_trade(
        self,
        trade: Trade,
        exit_price: Decimal,
        exit_reason: ExitReason,
    ) -> None:
        # Execute sell (paper or live) before writing to DB
        token_amount = trade.token_amount_bought or Decimal("0")
        exec_result = await self._execution.sell(
            mint=trade.token_mint,
            token_amount=token_amount,
            trade_id=str(trade.id),
            symbol=getattr(trade, "symbol", None),
        )
        if not exec_result.success:
            # sell_failed_critical already logged and Telegram alerted
            # Leave position open — do not write exit to DB
            return

        # Use actual fill price from live execution if available
        if exec_result.actual_price:
            exit_price = exec_result.actual_price
        """
        Close a trade atomically:
          1. Compute PnL
          2. Update trade row (exit fields, status → CLOSED)
          3. Return position + PnL to available_balance
          4. Write balance_history row
          5. Update circuit breaker and daily loss state
          6. Queue TRADE_CLOSE notification (immediate)
        """
        now = datetime.now(timezone.utc)

        pnl_pct = (exit_price - trade.entry_price) / trade.entry_price
        pnl_usd = (trade.position_size_usd * pnl_pct).quantize(Decimal("0.000001"))
        proceeds = trade.position_size_usd + pnl_usd
        hold_seconds = int((now - trade.entry_time).total_seconds())

        async with get_session() as session:
            # Lock the trade row
            trade_result = await session.execute(
                select(Trade)
                .where(Trade.id == trade.id)
                .with_for_update()
            )
            db_trade = trade_result.scalar_one_or_none()

            if db_trade is None or db_trade.status != TradeStatus.OPEN:
                log.debug("risk_engine.trade_already_closed", trade_id=str(trade.id))
                return

            # ── Close trade ────────────────────────────────────────────────
            db_trade.exit_price = exit_price
            db_trade.exit_time = now
            db_trade.exit_reason = exit_reason
            db_trade.pnl_usd = pnl_usd
            db_trade.pnl_pct = pnl_pct.quantize(Decimal("0.000001"))
            db_trade.hold_duration_seconds = hold_seconds
            db_trade.status = TradeStatus.CLOSED

            # ── Update token status ────────────────────────────────────────
            token_result = await session.execute(
                select(Token).where(Token.mint_address == trade.token_mint)
            )
            token = token_result.scalar_one_or_none()
            if token:
                token.status = TokenStatus.CLOSED

            # ── Return proceeds to balance ─────────────────────────────────
            state_result = await session.execute(
                select(SessionState)
                .where(SessionState.id == 1)
                .with_for_update()
            )
            state = state_result.scalar_one()
            balance_before = state.available_balance
            balance_after = balance_before + proceeds
            state.available_balance = balance_after

            # ── Balance history ────────────────────────────────────────────
            bal_history = BalanceHistory(
                trade_id=db_trade.id,
                balance_before=balance_before,
                balance_after=balance_after,
                snapshot_at=now,
            )
            session.add(bal_history)

            # ── Circuit breaker ────────────────────────────────────────────
            cb_result = await session.execute(
                select(CircuitBreakerState)
                .where(CircuitBreakerState.id == 1)
                .with_for_update()
            )
            cb = cb_result.scalar_one()

            is_loss = pnl_usd < Decimal("0")
            if is_loss:
                cb.consecutive_losses += 1
                if cb.consecutive_losses >= settings.CB_LOSS_COUNT:
                    cb.is_paused = True
                    cb.pause_started_at = now
                    cb.resume_at = now + timedelta(seconds=settings.cb_pause_seconds)
                    log.warning(
                        "risk_engine.circuit_breaker_tripped",
                        consecutive_losses=cb.consecutive_losses,
                        resume_at=cb.resume_at.isoformat(),
                    )
            else:
                cb.consecutive_losses = 0  # reset on any win

            # ── Daily loss tracking ────────────────────────────────────────
            dl_result = await session.execute(
                select(DailyLossState)
                .where(DailyLossState.id == 1)
                .with_for_update()
            )
            dl = dl_result.scalar_one()
            dl.cumulative_pnl_usd += pnl_usd

            max_daily_loss = dl.day_open_balance * Decimal(str(settings.MAX_DAILY_LOSS_PCT))
            if not dl.is_halted and dl.cumulative_pnl_usd <= -max_daily_loss:
                dl.is_halted = True
                dl.halted_at = now
                log.warning(
                    "risk_engine.daily_loss_halt",
                    cumulative_pnl=str(dl.cumulative_pnl_usd),
                    threshold=str(-max_daily_loss),
                )

            # ── TRADE_CLOSE notification ───────────────────────────────────
            import orjson
            notif = NotificationQueue(
                event_type=NotificationEvent.TRADE_CLOSE,
                payload=orjson.dumps({
                    "trade_id": str(db_trade.id),
                    "mint": db_trade.token_mint,
                    "symbol": token.symbol if token else None,
                    "exit_price": str(exit_price),
                    "entry_price": str(trade.entry_price),
                    "pnl_usd": str(pnl_usd),
                    "pnl_pct": str(round(float(pnl_pct) * 100, 2)),
                    "exit_reason": exit_reason.value,
                    "hold_seconds": hold_seconds,
                    "balance_after": str(balance_after),
                    "timestamp": now.isoformat(),
                }).decode(),
                trade_id=db_trade.id,
            )
            session.add(notif)

        log.info(
            "risk_engine.trade_closed",
            mint=trade.token_mint,
            symbol=token.symbol if token else None,
            exit_reason=exit_reason.value,
            pnl_usd=str(pnl_usd),
            pnl_pct=str(round(float(pnl_pct) * 100, 2)) + "%",
            hold_seconds=hold_seconds,
            balance_after=str(balance_after),
        )
