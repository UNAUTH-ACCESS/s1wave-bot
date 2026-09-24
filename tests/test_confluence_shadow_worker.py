"""
tests/test_confluence_shadow_worker.py
=========================================
Real-time-monitored paper-trade experiment (2026-09-22).

Coverage:
  1. A confluence-qualifying momentum_signal_events row (n_rules_cofiring
     >= 2) opens exactly one confluence_shadow_positions row.
  2. A NON-qualifying row (n_rules_cofiring < 2) never opens a position.
  3. A token already opened is never opened twice (idempotent).
  4. Exit priority matches engine/risk.py exactly: HARD_FLOOR beats
     STOP_LOSS beats TAKE_PROFIT beats TIME_EXIT, and the velocity
     breaker fires immediately on a >=25% single-step drop.
  5. A closed position is never re-closed and never keeps recording
     observations (mirrors TradeMonitorWorker's OPEN-only load pattern).
  6. This experiment never touches the real `trades` table or `Token`
     status in any way.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from config.settings import settings
from models.orm import (
    ConfluenceShadowObservation, ConfluenceShadowPosition, MomentumSignalEvent,
    Token, TokenEvaluation, TokenStatus, Trade,
)
from workers.confluence_shadow_worker import ConfluenceShadowWorker


def make_token(**overrides) -> Token:
    now = datetime.now(timezone.utc)
    defaults = dict(
        mint_address=f"Mint{uuid.uuid4().hex[:36]}",
        symbol="TEST",
        status=TokenStatus.WATCHING,
        discovered_at=now - timedelta(minutes=10),
    )
    defaults.update(overrides)
    return Token(**defaults)


def make_signal(token: Token, n_rules_cofiring=2, **overrides) -> MomentumSignalEvent:
    # Default triggered_at is "now" (not offset into the past) so a signal
    # created after a worker is constructed naturally clears that worker's
    # _started_at cutoff — tests that want a signal from BEFORE a worker
    # existed (the backfill-guard regression test) pass triggered_at
    # explicitly instead.
    defaults = dict(
        token_id=token.id,
        experiment_version="momentum_confluence_v1",
        triggered_at=datetime.now(timezone.utc),
        trigger_price=Decimal("0.001"),
        trailing_return_3min=Decimal("0.06"),
        n_rules_cofiring=n_rules_cofiring,
    )
    defaults.update(overrides)
    return MomentumSignalEvent(**defaults)


@pytest.mark.asyncio
async def test_qualifying_signal_opens_one_position(session):
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())  # started_at set here, before the signal below
    session.add(make_signal(token, n_rules_cofiring=2))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    positions = result.scalars().all()
    assert len(positions) == 1
    assert positions[0].status == "open"
    assert positions[0].entry_price == Decimal("0.001")


@pytest.mark.asyncio
async def test_non_qualifying_signal_never_opens(session):
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    session.add(make_signal(token, n_rules_cofiring=1))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    assert result.scalars().all() == []


@pytest.mark.asyncio
async def test_historical_signals_before_worker_start_are_never_opened(session):
    """Regression test for the 2026-09-22 live bug: on startup, a worker
    must NEVER backfill positions from signals recorded before it started
    (each would be priced at a stale, hours-old trigger_price, and a large
    enough backlog 400s the DexScreener request outright)."""
    token = make_token()
    session.add(token)
    await session.flush()
    old_signal_time = datetime.now(timezone.utc) - timedelta(hours=3)
    session.add(make_signal(token, n_rules_cofiring=2, triggered_at=old_signal_time))
    await session.flush()

    worker = ConfluenceShadowWorker(asyncio.Event())  # _started_at = now, well after old_signal_time
    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    assert result.scalars().all() == []


@pytest.mark.asyncio
async def test_idempotent_never_opens_twice(session):
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    session.add(make_signal(token, n_rules_cofiring=3))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()
        await worker._open_new_positions()  # second pass, same signal still there

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    assert len(result.scalars().all()) == 1


@pytest.mark.asyncio
async def test_wash_trading_rejected_token_is_skipped_not_opened(session):
    """Entry-quality filter (2026-09-24, workers/entry_filters.py) — a
    token whose most recent TIER1 evaluation before the signal was a
    WASH_TRADING rejection must not open a real (status='open') position,
    and must be recorded so it's never reconsidered on a later cycle."""
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    signal = make_signal(token, n_rules_cofiring=2)
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="WASH_TRADING",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={},
    ))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()
        await worker._open_new_positions()  # must stay skipped, not retried

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    positions = result.scalars().all()
    assert len(positions) == 1
    assert positions[0].status == "wash_skipped"


@pytest.mark.asyncio
async def test_lp_not_burned_rejected_token_still_opens(session):
    """LP_NOT_BURNED is a different rejection reason, confirmed safe by
    real data — must NOT be caught by the wash-trading filter."""
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    signal = make_signal(token, n_rules_cofiring=2)
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="LP_NOT_BURNED",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={},
    ))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    positions = result.scalars().all()
    assert len(positions) == 1
    assert positions[0].status == "open"


@pytest.mark.asyncio
async def test_high_liquidity_token_is_skipped_not_opened(session):
    """Liquidity-ceiling filter (2026-09-24, workers/entry_filters.py) —
    a token whose most recent TIER1 evaluation reports liquidity >= $30k
    must not open a real position, and must be recorded so it's never
    reconsidered on a later cycle."""
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    signal = make_signal(token, n_rules_cofiring=2)
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=True, reason_code=None,
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={"liquidity_usd": "75000"},
    ))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()
        await worker._open_new_positions()  # must stay skipped, not retried

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    positions = result.scalars().all()
    assert len(positions) == 1
    assert positions[0].status == "high_liq_skip"


@pytest.mark.asyncio
async def test_low_liquidity_token_still_opens(session):
    """A token below the liquidity ceiling, and not WASH_TRADING-rejected,
    must open normally — both filters are independent skip conditions,
    not a blanket 'anything TIER1 touched' gate."""
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    signal = make_signal(token, n_rules_cofiring=2)
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="LP_NOT_BURNED",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={"liquidity_usd": "12000"},
    ))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    positions = result.scalars().all()
    assert len(positions) == 1
    assert positions[0].status == "open"


@pytest.mark.asyncio
async def test_low_buy_pressure_token_is_skipped_not_opened(session):
    """Buy-pressure floor (2026-09-24, workers/entry_filters.py) — a
    signal with buy_pressure below 0.97 must not open a real position,
    even when it clears both other filters, and must be recorded so it's
    never reconsidered on a later cycle."""
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    signal = make_signal(token, n_rules_cofiring=2, buy_pressure=Decimal("0.85"))
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="LP_NOT_BURNED",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={"liquidity_usd": "12000"},
    ))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()
        await worker._open_new_positions()  # must stay skipped, not retried

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    positions = result.scalars().all()
    assert len(positions) == 1
    assert positions[0].status == "low_bp_skip"


@pytest.mark.asyncio
async def test_high_buy_pressure_token_still_opens(session):
    """A signal at or above the buy-pressure floor, clearing both other
    filters, must open normally."""
    token = make_token()
    session.add(token)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    signal = make_signal(token, n_rules_cofiring=2, buy_pressure=Decimal("0.99"))
    session.add(signal)
    session.add(TokenEvaluation(
        token_id=token.id, gate="TIER1", passed=False, reason_code="LP_NOT_BURNED",
        evaluated_at=signal.triggered_at - timedelta(seconds=5), inputs_json={"liquidity_usd": "12000"},
    ))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.token_id == token.id)
    )
    positions = result.scalars().all()
    assert len(positions) == 1
    assert positions[0].status == "open"


class TestLayeredExitPriority:
    """Velocity breaker -> HARD_FLOOR -> trailing-stop staircase -> TIME_EXIT
    (2026-09-23, replacing the old fixed STOP_LOSS_PCT/TAKE_PROFIT_PCT pair
    with engine/trailing_stop.py — see tests/test_trailing_stop.py for the
    staircase math itself; this class covers the priority wiring around it)."""

    def setup_method(self):
        self.worker = ConfluenceShadowWorker(asyncio.Event())
        self.entry_price = Decimal("1.00")
        self.entry_time = datetime.now(timezone.utc) - timedelta(minutes=1)
        self.now = datetime.now(timezone.utc)
        self.initial_floor = self.entry_price * Decimal("0.88")  # -12%, widened 2026-09-24

    def test_hard_floor_triggers_at_threshold(self):
        floor = Decimal(str(settings.HARD_FLOOR_PCT))
        price = self.entry_price * (1 + floor)
        reason, _, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "HARD_FLOOR"

    def test_initial_stop_loss_triggers_as_stop_loss_before_any_new_high(self):
        # Price drops straight through the initial -12% floor without ever
        # having made a new high -- the floor never moved off its initial
        # stage, so this is labeled STOP_LOSS, not TRAILING_STOP.
        price = self.entry_price * Decimal("0.87")  # below -12% initial floor, above -15% hard floor
        reason, new_floor, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "STOP_LOSS"

    def test_does_not_force_sell_at_old_take_profit_threshold(self):
        # The old fixed rule would have force-sold at +30%. The staircase
        # must NOT close here -- it should still be well above its own
        # trailing floor, since the high watermark just reached this price.
        tp = Decimal(str(settings.TAKE_PROFIT_PCT))
        price = self.entry_price * (1 + tp)
        reason, new_floor, new_hwm = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason is None
        assert new_hwm == price
        assert new_floor > self.initial_floor  # ratcheted up, locking in some of the gain

    def test_trailing_stop_closes_after_a_higher_high_then_pullback(self):
        # First tick: runs to +40%, locking the floor at +30% (per
        # engine/trailing_stop.py's own worked example).
        reason1, floor1, hwm1 = self.worker._check_exit(
            self.entry_price, self.entry_time, self.entry_price * Decimal("1.40"),
            self.now, self.initial_floor, self.entry_price,
        )
        assert reason1 is None
        assert floor1 == Decimal("1.300000000000")
        # Second tick: pulls back to +25%, below the locked +30% floor.
        reason2, floor2, hwm2 = self.worker._check_exit(
            self.entry_price, self.entry_time, self.entry_price * Decimal("1.25"),
            self.now, floor1, hwm1,
        )
        assert reason2 == "TRAILING_STOP"

    def test_no_exit_when_flat(self):
        reason, _, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, self.entry_price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason is None

    def test_time_exit_after_max_hold(self):
        old_entry = self.now - timedelta(seconds=settings.max_hold_seconds + 60)
        reason, _, _ = self.worker._check_exit(
            self.entry_price, old_entry, self.entry_price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "TIME_EXIT"

    def test_velocity_breaker_fires_as_hard_floor(self):
        price = self.entry_price * Decimal("0.70")  # -30%, past both -25% breaker and -7% floor
        reason, _, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "HARD_FLOOR"

    def test_hard_floor_beats_trailing_stop_when_somehow_both_could_apply(self):
        floor = Decimal(str(settings.HARD_FLOOR_PCT))
        price = self.entry_price * (1 + floor)
        reason, _, _ = self.worker._check_exit(
            self.entry_price, self.entry_time, price, self.now, self.initial_floor, self.entry_price,
        )
        assert reason == "HARD_FLOOR"


@pytest.mark.asyncio
async def test_maybe_close_persists_exit_fields(session):
    token = make_token()
    session.add(token)
    await session.flush()
    entry_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    position = ConfluenceShadowPosition(
        token_id=token.id, experiment_version="confluence_entry_v1",
        entry_price=Decimal("1.00"), entry_time=entry_time,
        n_rules_cofiring=2, status="open",
    )
    session.add(position)
    await session.flush()

    worker = ConfluenceShadowWorker(asyncio.Event())
    now = datetime.now(timezone.utc)
    pos_dict = dict(id=position.id, entry_price=position.entry_price, entry_time=position.entry_time,
                     trailing_stop_floor=None, high_watermark_price=None)

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        # First tick: run to +40%, locks floor at +30%, does not close.
        await worker._maybe_close(pos_dict, Decimal("1.40"), now)

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.id == position.id)
    )
    row = result.scalar_one()
    assert row.status == "open"
    assert row.trailing_stop_floor == Decimal("1.300000000000")
    assert row.high_watermark_price == Decimal("1.40")

    pos_dict2 = dict(id=position.id, entry_price=position.entry_price, entry_time=position.entry_time,
                      trailing_stop_floor=row.trailing_stop_floor, high_watermark_price=row.high_watermark_price)
    exit_price = Decimal("1.25")  # pulls back below the locked +30% floor

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._maybe_close(pos_dict2, exit_price, now)

    result = await session.execute(
        select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.id == position.id)
    )
    row = result.scalar_one()
    assert row.status == "closed"
    assert row.exit_reason == "TRAILING_STOP"
    assert row.exit_price == exit_price
    assert row.pnl_pct == pytest.approx(Decimal("0.25"))


@pytest.mark.asyncio
async def test_closed_position_is_not_loaded_for_further_monitoring(session):
    token = make_token()
    session.add(token)
    await session.flush()
    session.add(ConfluenceShadowPosition(
        token_id=token.id, experiment_version="confluence_entry_v1",
        entry_price=Decimal("1.00"), entry_time=datetime.now(timezone.utc),
        exit_price=Decimal("1.30"), exit_time=datetime.now(timezone.utc),
        exit_reason="TAKE_PROFIT", pnl_pct=Decimal("0.30"),
        n_rules_cofiring=2, status="closed",
    ))
    await session.flush()

    worker = ConfluenceShadowWorker(asyncio.Event())
    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        open_positions = await worker._load_open_positions()

    assert open_positions == {}


class TestBadTickGuard:
    """Regression coverage for the real 2026-09-22 live finding: DexScreener
    occasionally returns one wildly wrong print (X7 and CENTS both exited
    on a single tick 20-80x the price seen a second before and after).
    A single implausible tick must be held, not acted on; two consecutive
    similar implausible ticks must be accepted as a real, fast move."""

    def setup_method(self):
        self.worker = ConfluenceShadowWorker(asyncio.Event())
        self.position_id = uuid.uuid4()
        self.entry_price = Decimal("1.00")
        self.pos = dict(id=self.position_id, entry_price=self.entry_price,
                         entry_time=datetime.now(timezone.utc))

    def test_normal_tick_is_accepted_immediately(self):
        price = self.worker._accept_price(self.pos, Decimal("1.10"))
        assert price == Decimal("1.10")

    def test_single_implausible_spike_is_held_not_accepted(self):
        # First a normal tick to establish a baseline...
        self.worker._accept_price(self.pos, Decimal("1.05"))
        # ...then the real CENTS-style glitch: ~80x in one step.
        price = self.worker._accept_price(self.pos, Decimal("84.00"))
        assert price is None

    def test_glitch_that_reverts_next_tick_is_never_accepted(self):
        self.worker._accept_price(self.pos, Decimal("1.05"))
        held = self.worker._accept_price(self.pos, Decimal("84.00"))
        assert held is None
        # next tick is back to normal -- the glitch must be discarded, not
        # retroactively accepted, and this normal tick proceeds fine
        recovered = self.worker._accept_price(self.pos, Decimal("1.06"))
        assert recovered == Decimal("1.06")

    def test_sustained_large_move_is_confirmed_on_second_tick(self):
        self.worker._accept_price(self.pos, Decimal("1.05"))
        held = self.worker._accept_price(self.pos, Decimal("84.00"))
        assert held is None
        # a REAL 80x pump would still show ~84 (or close) one second later
        confirmed = self.worker._accept_price(self.pos, Decimal("85.00"))
        assert confirmed == Decimal("85.00")

    def test_first_normal_price_after_entry_is_accepted(self):
        # nothing in _last_accepted_price yet -> falls back to entry_price
        # as the baseline; a normal first poll close to entry proceeds fine
        price = self.worker._accept_price(self.pos, Decimal("1.02"))
        assert price == Decimal("1.02")

    def test_wild_first_price_vs_entry_is_also_held(self):
        # The guard protects the entry-to-first-poll transition the same
        # way as any other tick -- entry_price came from the signal at
        # trigger time, and the very first live check is just as capable
        # of hitting a bad print as any later one.
        price = self.worker._accept_price(self.pos, Decimal("50.00"))
        assert price is None


@pytest.mark.asyncio
async def test_never_touches_real_trades_table(session):
    """The only side effects are ConfluenceShadowPosition /
    ConfluenceShadowObservation rows and reads of Token/MomentumSignalEvent
    — never a real Trade row, never a TokenStatus change."""
    token = make_token()
    session.add(token)
    await session.flush()
    original_status = token.status
    worker = ConfluenceShadowWorker(asyncio.Event())
    session.add(make_signal(token, n_rules_cofiring=2))
    await session.flush()

    with patch("workers.confluence_shadow_worker.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await worker._open_new_positions()

    result = await session.execute(select(Trade).where(Trade.token_id == token.id))
    assert result.scalars().all() == []
    assert token.status == original_status
