"""
engine/capital.py
=================
Capital engine — Phase 5

Consumes mint addresses from the entry_queue (pushed by scoring_worker
when composite score >= TIER2_STRONG_BUY_THRESHOLD).

For each entry candidate:
  1. Risk gate checks (circuit breaker, daily halt, max concurrent trades)
  2. Position sizing (TRADE_ALLOCATION_PCT × available_balance, floored at
     MIN_POSITION_USD)
  3. Liquidity flag computation (classify_liquidity_flag from rolling_window)
  4. Trade row inserted atomically with balance deduction
  5. Token status → ENTERED
  6. Notification queued (TRADE_OPEN — dispatched immediately by Phase 8)

This is SIMULATION ONLY in v3.  No real swaps are executed.
Phase 12 wires the Jupiter execution layer.

Position sizing (PRD §7)
------------------------
position_usd = available_balance × TRADE_ALLOCATION_PCT

With default 5% allocation and $1,000 starting balance:
  Trade 1: $50.00  (5% of $1,000)
  Trade 2: $47.50  (5% of $950)   ← compounding
  ...

The position is deducted from available_balance atomically on open and
returned (with PnL) on close by the risk engine (Phase 6).

Concurrent trade limit
----------------------
If MAX_CONCURRENT_TRADES open positions already exist, the entry is
skipped — the mint address is dropped, not re-queued.  The scoring
worker will re-evaluate the token next cycle if it is still WATCHING.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select, func

from config.logging import get_logger
from engine.execution import ExecutionEngine, ExecutionResult
from config.settings import settings
from database.engine import get_session
from engine.rolling_window import (
    LiquidityTrend,
    classify_liquidity_flag,
    classify_liquidity_trend,
)
from models.orm import (
    BalanceHistory,
    CircuitBreakerState,
    DailyLossState,
    LiquidityFlag,
    NotificationEvent,
    NotificationQueue,
    SessionState,
    Token,
    TokenSnapshot,
    TokenStatus,
    Trade,
    TradeStatus,
    TrendDirection,
    VMZone,
)

log = get_logger(__name__)


class CapitalEngine:
    """
    Consumes the entry_queue and opens simulated positions.

    Parameters
    ----------
    entry_queue   : asyncio.Queue[str] — mint addresses from scoring_worker
    shutdown_event: asyncio.Event
    """

    def __init__(
        self,
        entry_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._queue = entry_queue
        self._shutdown = shutdown_event
        self._execution = ExecutionEngine()

    async def run(self) -> None:
        log.info("capital_engine.started")

        while not self._shutdown.is_set():
            try:
                mint = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                await self._process_entry(mint)
            except Exception as exc:
                log.error("capital_engine.error", mint=mint, error=str(exc), exc_info=True)
            finally:
                self._queue.task_done()

        log.info("capital_engine.stopped")

    # ── Entry processing ──────────────────────────────────────────────────────

    async def _process_entry(self, mint: str) -> None:
        """Full entry pipeline for one candidate token."""

        # ── 1. Load token and latest scoring context ──────────────────────
        async with get_session() as session:
            token_result = await session.execute(
                select(Token).where(Token.mint_address == mint)
            )
            token = token_result.scalar_one_or_none()

        if token is None:
            log.warning("capital_engine.token_not_found", mint=mint)
            return

        if token.status != TokenStatus.WATCHING:
            log.info("capital_engine.not_watching", mint=mint, status=token.status.value,
                     note="token may have been claimed by s1_wave before scorer processed queue")
            return

        # ── 2. Load the 3 most recent snapshots for scoring context ───────
        scoring_ctx = await self._load_scoring_context(token)
        if scoring_ctx is None:
            log.warning("capital_engine.no_scoring_context", mint=mint)
            return

        score, signal, bp_trend, vm_trend, vm_zone, p3_snap = scoring_ctx

        # ── 3. Risk gate (circuit breaker, daily halt, concurrent limit) ──
        gate_result = await self._risk_gate(mint)
        if gate_result is not None:
            log.info("capital_engine.entry_blocked", mint=mint, reason=gate_result)
            return

        # ── 4. Position sizing ─────────────────────────────────────────────
        position_usd, balance_before = await self._size_position()
        if position_usd is None:
            log.info("capital_engine.insufficient_balance", mint=mint)
            return

        # ── 5. Liquidity classification (passive — never gates entry) ─────
        liq_flag = classify_liquidity_flag(
            position_size_usd=position_usd,
            liquidity_usd=p3_snap.liquidity_usd or Decimal("0"),
        )
        # Infer liquidity trend from the 3 snapshots
        liq_trend = await self._classify_liq_trend(token, p3_snap)

        # ── 6. Execute buy (paper or live) ───────────────────────────────
        sol_price = Decimal(str(settings.SOL_PRICE_USD)) if settings.SOL_PRICE_USD else Decimal("150")
        exec_result = await self._execution.buy(
            mint=token.mint_address,
            position_usd=position_usd,
            sol_price_usd=sol_price,
        )
        if not exec_result.success:
            log.error(
                "capital_engine.buy_execution_failed",
                mint=token.mint_address,
                symbol=token.symbol,
                error_type=exec_result.error_type,
                error=exec_result.error_detail,
            )
            return

        # Use actual fill price if live
        entry_price = exec_result.actual_price or p3_snap.price_usd or Decimal("0")

        # ── 7. Open the trade atomically ──────────────────────────────────
        trade = await self._open_trade(
            token=token,
            entry_price=entry_price,
            position_usd=position_usd,
            balance_before=balance_before,
            composite_score=score,
            bp_trend=bp_trend,
            vm_trend=vm_trend,
            vm_zone=vm_zone,
            liq_flag=liq_flag,
            liq_trend=liq_trend,
        )

        if trade is None:
            return

        # Store execution details on the trade
        if exec_result.tx_signature or exec_result.actual_amount:
            from sqlalchemy import update as sa_update
            async with get_session() as session:
                await session.execute(
                    sa_update(Trade)
                    .where(Trade.id == trade.id)
                    .values(
                        token_amount_bought=exec_result.actual_amount,
                        tx_signature_entry=exec_result.tx_signature,
                    )
                )

        log.info(
            "capital_engine.trade_opened",
            mint=mint,
            symbol=token.symbol,
            signal=signal,
            position_usd=str(position_usd),
            entry_price=str(p3_snap.price_usd),
            score=str(score),
            bp_trend=bp_trend.value,
            vm_trend=vm_trend.value,
            vm_zone=vm_zone.value,
            liq_flag=liq_flag.value,
            balance_after=str(balance_before - position_usd),
        )



    # ── Scoring context ───────────────────────────────────────────────────────

    async def _load_scoring_context(
        self, token: Token
    ) -> tuple[Decimal, TrendDirection, TrendDirection, VMZone, TokenSnapshot] | None:
        """
        Load the most recent 3 snapshots and re-run the scorer to get
        the entry-time composite score, trends, and VMZone.

        Returns (score, bp_trend, vm_trend, vm_zone, p3_snapshot) or None.
        """
        from engine.rolling_window import SnapshotWindow, score_window

        async with get_session() as session:
            result = await session.execute(
                select(TokenSnapshot)
                .where(TokenSnapshot.token_id == token.id)
                .order_by(TokenSnapshot.sampled_at.desc())
                .limit(3)
            )
            snaps = list(result.scalars().all())

        if len(snaps) < 3:
            return None

        p3, p2, p1 = snaps[0], snaps[1], snaps[2]

        def vlr(s: TokenSnapshot) -> Decimal:
            if not s.liquidity_usd or s.liquidity_usd == Decimal("0"):
                return Decimal("0")
            return (s.volume_usd / s.liquidity_usd).quantize(Decimal("0.000001"))

        window = SnapshotWindow(
            bp_p1=p1.buy_pressure, bp_p2=p2.buy_pressure, bp_p3=p3.buy_pressure,
            vm_p1=p1.volume_mult,  vm_p2=p2.volume_mult,  vm_p3=p3.volume_mult,
            vlr_p1=vlr(p1),        vlr_p2=vlr(p2),        vlr_p3=vlr(p3),
            age_minutes=self._token_age_minutes(token),
            prev_composite_score=None,
        )

        result = score_window(window)

        # VMZone defaults to STRONG if not set (non-STRONG_BUY path)
        vm_zone = result.vm_zone or VMZone.STRONG

        return result.composite, result.signal, result.bp_trend, result.vm_trend, vm_zone, p3

    # ── Risk gate ─────────────────────────────────────────────────────────────

    async def _risk_gate(self, mint: str) -> str | None:
        """
        Check all risk controls before opening a position.

        Returns None if entry is allowed.
        Returns a reason string if blocked.

        Checks (in order):
          1. Circuit breaker paused
          2. Daily loss halt
          3. Max concurrent trades reached
        """
        async with get_session() as session:
            # Circuit breaker
            cb_result = await session.execute(
                select(CircuitBreakerState).where(CircuitBreakerState.id == 1)
            )
            cb = cb_result.scalar_one()

            if cb.is_paused:
                now = datetime.now(timezone.utc)
                if cb.resume_at and now < cb.resume_at:
                    return f"CIRCUIT_BREAKER_PAUSED:resume_at={cb.resume_at.isoformat()}"
                else:
                    # Auto-resume: pause period has elapsed
                    cb.is_paused = False
                    cb.resume_at = None
                    cb.consecutive_losses = 0  # clean slate after pause
                    log.info("capital_engine.circuit_breaker_resumed")
                    import orjson as _orjson
                    from models.orm import NotificationEvent, NotificationQueue
                    session.add(NotificationQueue(
                        event_type=NotificationEvent.CB_RESUMED,
                        payload=_orjson.dumps({
                            "paused_duration": "~1 hour",
                            "timestamp": now.isoformat(),
                        }).decode(),
                    ))

            # Daily loss halt
            dl_result = await session.execute(
                select(DailyLossState).where(DailyLossState.id == 1)
            )
            dl = dl_result.scalar_one()

            if dl.is_halted:
                return "DAILY_LOSS_HALT"

            # Max concurrent trades
            open_count_result = await session.execute(
                select(func.count(Trade.id)).where(Trade.status == TradeStatus.OPEN)
            )
            open_count = open_count_result.scalar_one()

            if open_count >= settings.MAX_CONCURRENT_TRADES:
                return f"MAX_CONCURRENT_TRADES:{open_count}/{settings.MAX_CONCURRENT_TRADES}"

        return None

    # ── Position sizing ───────────────────────────────────────────────────────

    async def _size_position(self) -> tuple[Decimal, Decimal] | tuple[None, None]:
        """
        Compute position size from current available balance.

        Returns (position_usd, balance_before) or (None, None) if
        balance is too low to place a minimum-sized trade.

        Position size = available_balance × TRADE_ALLOCATION_PCT
        Compounding: each trade uses a percentage of the CURRENT balance,
        so wins grow the next position and losses shrink it.
        """
        async with get_session() as session:
            result = await session.execute(
                select(SessionState).where(SessionState.id == 1)
            )
            state = result.scalar_one()
            balance = state.available_balance

        allocation = Decimal(str(settings.TRADE_ALLOCATION_PCT))
        min_position = Decimal(str(settings.MIN_POSITION_USD))

        position_usd = (balance * allocation).quantize(Decimal("0.000001"))

        if position_usd < min_position:
            return None, None

        return position_usd, balance

    # ── Trade open ────────────────────────────────────────────────────────────

    async def _open_trade(
        self,
        token: Token,
        entry_price: Decimal,
        position_usd: Decimal,
        balance_before: Decimal,
        composite_score: Decimal,
        bp_trend: TrendDirection,
        vm_trend: TrendDirection,
        vm_zone: VMZone,
        liq_flag: LiquidityFlag,
        liq_trend: LiquidityTrend,
    ) -> Trade | None:
        """
        Write trade row, deduct balance, update token status, queue
        notification — all in a single atomic transaction.
        """
        now = datetime.now(timezone.utc)

        async with get_session() as session:
            # Re-verify token is still WATCHING under lock
            token_result = await session.execute(
                select(Token)
                .where(Token.mint_address == token.mint_address)
                .with_for_update()
            )
            db_token = token_result.scalar_one_or_none()

            if db_token is None or db_token.status != TokenStatus.WATCHING:
                log.debug(
                    "capital_engine.token_no_longer_watching",
                    mint=token.mint_address,
                )
                return None

            # Deduct position from balance under lock
            session_result = await session.execute(
                select(SessionState)
                .where(SessionState.id == 1)
                .with_for_update()
            )
            state = session_result.scalar_one()

            # Re-check balance under lock (may have changed since sizing)
            if state.available_balance < position_usd:
                log.warning(
                    "capital_engine.balance_changed_under_lock",
                    mint=token.mint_address,
                    balance=str(state.available_balance),
                    position=str(position_usd),
                )
                return None

            balance_after = state.available_balance - position_usd
            state.available_balance = balance_after

            # Open trade
            trade = Trade(
                token_id=db_token.id,
                token_mint=db_token.mint_address,
                entry_price=entry_price,
                entry_time=now,
                entry_composite_score=composite_score,
                entry_bp_trend=bp_trend,
                entry_vm_trend=vm_trend,
                entry_vm_zone=vm_zone,
                entry_liquidity_flag=liq_flag,
                entry_liquidity_trend=liq_trend,
                position_size_usd=position_usd,
                status=TradeStatus.OPEN,
                # Scorer trades: execution happens in _process_entry before
                # this call. token_amount_bought and tx_signature_entry are
                # set after the trade row is created, via update below.
                token_amount_bought=None,
                tx_signature_entry=None,
            )
            session.add(trade)
            await session.flush()  # get trade.id

            # Advance token status
            db_token.status = TokenStatus.ENTERED

            # Queue TRADE_OPEN notification (immediate dispatch in Phase 8)
            import orjson
            notif = NotificationQueue(
                event_type=NotificationEvent.TRADE_OPEN,
                payload=orjson.dumps({
                    "trade_id": str(trade.id),
                    "mint": db_token.mint_address,
                    "symbol": db_token.symbol,
                    "entry_price": str(entry_price),
                    "position_usd": str(position_usd),
                    "balance_after": str(balance_after),
                    "composite_score": str(composite_score),
                    "bp_trend": bp_trend.value,
                    "vm_trend": vm_trend.value,
                    "vm_zone": vm_zone.value,
                    "liq_flag": liq_flag.value,
                    "timestamp": now.isoformat(),
                }).decode(),
                trade_id=trade.id,
            )
            session.add(notif)

        return trade

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _classify_liq_trend(
        self, token: Token, p3: TokenSnapshot
    ) -> LiquidityTrend:
        """
        Classify liquidity trend from the 3 most recent snapshots.
        Uses the rolling_window helper which needs a full SnapshotWindow.
        Falls back to STABLE if not enough data.
        """
        from engine.rolling_window import SnapshotWindow, classify_liquidity_trend

        async with get_session() as session:
            result = await session.execute(
                select(TokenSnapshot)
                .where(TokenSnapshot.token_id == token.id)
                .order_by(TokenSnapshot.sampled_at.desc())
                .limit(3)
            )
            snaps = list(result.scalars().all())

        if len(snaps) < 3:
            return LiquidityTrend.STABLE

        p3s, p2s, p1s = snaps[0], snaps[1], snaps[2]

        def vlr(s: TokenSnapshot) -> Decimal:
            if not s.liquidity_usd or s.liquidity_usd == Decimal("0"):
                return Decimal("0")
            return (s.volume_usd / s.liquidity_usd).quantize(Decimal("0.000001"))

        window = SnapshotWindow(
            bp_p1=p1s.buy_pressure, bp_p2=p2s.buy_pressure, bp_p3=p3s.buy_pressure,
            vm_p1=p1s.volume_mult,  vm_p2=p2s.volume_mult,  vm_p3=p3s.volume_mult,
            vlr_p1=vlr(p1s),        vlr_p2=vlr(p2s),        vlr_p3=vlr(p3s),
            age_minutes=self._token_age_minutes(token),
        )
        return classify_liquidity_trend(window)

    def _token_age_minutes(self, token: Token) -> float:
        if not token.watch_started_at:
            return 0.0
        now = datetime.now(timezone.utc)
        return (now - token.watch_started_at).total_seconds() / 60
