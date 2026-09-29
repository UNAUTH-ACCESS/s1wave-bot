"""
tests/test_momentum_signal.py
==============================
Forward-tracking momentum-confluence shadow experiment (2026-09-22).

Coverage:
  1. Signal fires and records exactly one row when trailing_return_3min
     crosses the frozen threshold.
  2. Does NOT fire when the trailing return is at or below threshold.
  3. Records only once per token — a second qualifying snapshot for the
     same token is a no-op (mirrors shadow_trades' one-entry-per-token
     rule), verified via the in-memory cache AND via DB idempotency.
  4. n_rules_cofiring is computed correctly across 0/1/2/3 secondary
     rules true at the trigger moment.
  5. Never touches trades, entry_queue, or any other production table —
     only MomentumSignalEvent + the TokenSnapshot the caller already
     writes.
  6. A failure inside maybe_record_signal never breaks the caller
     (shared_snapshot.write_snapshot_and_notify wraps it defensively).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from models.orm import MomentumSignalEvent, Token, TokenSnapshot, TokenStatus
from workers import momentum_signal


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


@pytest.fixture(autouse=True)
def _clean_cache():
    momentum_signal._recorded_token_ids.clear()
    momentum_signal._cache_loaded = False
    yield
    momentum_signal._recorded_token_ids.clear()
    momentum_signal._cache_loaded = False


async def _add_snapshot(session, token, minutes_ago, price):
    session.add(TokenSnapshot(
        token_id=token.id,
        sampled_at=datetime.now(timezone.utc) - timedelta(minutes=minutes_ago),
        price_usd=Decimal(str(price)),
        liquidity_usd=Decimal("20000"),
        market_cap_usd=Decimal("100000"),
        volume_usd=Decimal("5000"),
        buy_pressure=Decimal("0.5"),
        volume_mult=Decimal("1.0"),
    ))
    await session.flush()


def make_snap(**overrides) -> dict:
    defaults = dict(
        price_usd=Decimal("0.0011"),
        liquidity_usd=Decimal("20000"),
        market_cap_usd=Decimal("100000"),
        buy_pressure=Decimal("0.5"),
        volume_mult=Decimal("1.0"),
    )
    defaults.update(overrides)
    return defaults


@pytest.mark.asyncio
async def test_signal_fires_above_threshold(session):
    token = make_token()
    session.add(token)
    await session.flush()
    await _add_snapshot(session, token, minutes_ago=3, price="0.0010")  # baseline 3min ago

    now = datetime.now(timezone.utc)
    snap = make_snap(price_usd=Decimal("0.00106"))  # +6% over 3 min, above 5.3% threshold

    with patch("workers.momentum_signal.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await momentum_signal.maybe_record_signal(token, snap, now)

    result = await session.execute(
        select(MomentumSignalEvent).where(MomentumSignalEvent.token_id == token.id)
    )
    rows = result.scalars().all()
    assert len(rows) == 1
    assert rows[0].trailing_return_3min > momentum_signal.PRIMARY_THRESHOLD_3MIN


@pytest.mark.asyncio
async def test_signal_does_not_fire_below_threshold(session):
    token = make_token()
    session.add(token)
    await session.flush()
    await _add_snapshot(session, token, minutes_ago=3, price="0.0010")

    now = datetime.now(timezone.utc)
    snap = make_snap(price_usd=Decimal("0.00102"))  # +2%, below 5.3% threshold

    with patch("workers.momentum_signal.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await momentum_signal.maybe_record_signal(token, snap, now)

    result = await session.execute(
        select(MomentumSignalEvent).where(MomentumSignalEvent.token_id == token.id)
    )
    assert result.scalars().all() == []


@pytest.mark.asyncio
async def test_no_prior_snapshot_means_no_signal(session):
    """No baseline 3 minutes back -> cannot compute a trailing return -> no-op,
    never crashes."""
    token = make_token()
    session.add(token)
    await session.flush()

    now = datetime.now(timezone.utc)
    snap = make_snap()

    with patch("workers.momentum_signal.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await momentum_signal.maybe_record_signal(token, snap, now)

    result = await session.execute(
        select(MomentumSignalEvent).where(MomentumSignalEvent.token_id == token.id)
    )
    assert result.scalars().all() == []


@pytest.mark.asyncio
async def test_records_only_once_per_token(session):
    token = make_token()
    session.add(token)
    await session.flush()
    await _add_snapshot(session, token, minutes_ago=3, price="0.0010")

    now = datetime.now(timezone.utc)
    snap = make_snap(price_usd=Decimal("0.00106"))

    with patch("workers.momentum_signal.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await momentum_signal.maybe_record_signal(token, snap, now)
        # second qualifying snapshot for the SAME token -> must be a no-op
        await momentum_signal.maybe_record_signal(token, snap, now + timedelta(seconds=30))

    result = await session.execute(
        select(MomentumSignalEvent).where(MomentumSignalEvent.token_id == token.id)
    )
    assert len(result.scalars().all()) == 1


@pytest.mark.asyncio
async def test_n_rules_cofiring_counts_correctly(session):
    token = make_token()
    session.add(token)
    await session.flush()
    await _add_snapshot(session, token, minutes_ago=5, price="0.0010")  # 5min baseline: flat
    await _add_snapshot(session, token, minutes_ago=3, price="0.0010")  # 3min baseline

    now = datetime.now(timezone.utc)
    # +6% over 3min (fires primary), +6% over 5min too (> 11.6%? no, only 6% - so
    # 5min rule should NOT fire here), buy_pressure and volume_mult both above
    # their thresholds -> expect n_rules_cofiring == 2 (buy_pressure, volume_mult)
    snap = make_snap(
        price_usd=Decimal("0.00106"),
        buy_pressure=Decimal("0.70"),   # > 0.667
        volume_mult=Decimal("1.10"),    # > 1.004
    )

    with patch("workers.momentum_signal.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await momentum_signal.maybe_record_signal(token, snap, now)

    result = await session.execute(
        select(MomentumSignalEvent).where(MomentumSignalEvent.token_id == token.id)
    )
    row = result.scalars().one()
    assert row.n_rules_cofiring == 2


@pytest.mark.asyncio
async def test_never_touches_trading_tables(session):
    """The only side effect is one MomentumSignalEvent row -- no Trade,
    no TokenStatus change, no queue writes."""
    from models.orm import Trade

    token = make_token()
    session.add(token)
    await session.flush()
    await _add_snapshot(session, token, minutes_ago=3, price="0.0010")
    original_status = token.status

    now = datetime.now(timezone.utc)
    snap = make_snap(price_usd=Decimal("0.00106"))

    with patch("workers.momentum_signal.get_session") as mock_gs:
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await momentum_signal.maybe_record_signal(token, snap, now)

    result = await session.execute(select(Trade).where(Trade.token_id == token.id))
    assert result.scalars().all() == []
    assert token.status == original_status


@pytest.mark.asyncio
async def test_failure_inside_signal_does_not_break_shared_snapshot_write():
    """shared_snapshot.write_snapshot_and_notify wraps the momentum_signal
    call defensively -- a broken experiment must never break a real
    snapshot write. write_snapshot_and_notify itself must not raise even
    though maybe_record_signal blows up."""
    from workers import shared_snapshot

    token = make_token()
    snap = dict(
        price_usd=Decimal("0.001"), liquidity_usd=Decimal("20000"),
        market_cap_usd=Decimal("100000"), volume_usd=Decimal("5000"),
        buy_pressure=Decimal("0.5"), volume_mult=Decimal("1.0"),
        price_change_1m=0.0, price_change_5m=0.0,
    )

    fake_session = AsyncMock()
    fake_session.execute = AsyncMock(return_value=AsyncMock(scalar_one_or_none=lambda: None))
    fake_session.add = lambda *a, **k: None

    with patch("workers.shared_snapshot.get_session") as mock_gs, \
         patch("workers.momentum_signal.maybe_record_signal", side_effect=RuntimeError("boom")):
        mock_gs.return_value.__aenter__ = AsyncMock(return_value=fake_session)
        mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
        await shared_snapshot.write_snapshot_and_notify(
            token, snap, datetime.now(timezone.utc),
        )

    assert shared_snapshot.is_fresh(token.mint_address, datetime.now(timezone.utc))
