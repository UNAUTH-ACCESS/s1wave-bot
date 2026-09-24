"""
tests/test_entry_filters.py
=============================
workers/entry_filters.py — the WASH_TRADING entry-quality filter added
2026-09-24 for both confluence_shadow_worker.py and
confluence_live_worker.py. See that module's docstring for the full data
behind the filter (56% of all confluence_entry_v1 trades went into
WASH_TRADING-rejected tokens, the single worst-performing population;
LP_NOT_BURNED-rejected tokens were, counter-intuitively, the safest and
most profitable, so are deliberately left unfiltered).

Coverage:
  1. Flags a token whose most recent TIER1 evaluation was a WASH_TRADING
     rejection.
  2. Does NOT flag LP_NOT_BURNED — the other common rejection reason,
     confirmed by real data to be safe.
  3. Does NOT flag a TIER1 pass.
  4. Does NOT flag a token with no TIER1 row at all.
  5. Only ever looks at or before the signal's trigger time — a
     WASH_TRADING evaluation recorded AFTER the signal fired must not
     retroactively flag it (no future-information leak).
  6. Uses the most recent qualifying evaluation, not just any row, when
     a token has been evaluated more than once.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from decimal import Decimal

from models.orm import TokenEvaluation
from workers.entry_filters import is_buy_pressure_too_low, is_liquidity_too_high, is_wash_trading_rejected


def make_evaluation(token_id, evaluated_at, passed, reason_code=None, inputs_json=None) -> TokenEvaluation:
    return TokenEvaluation(
        token_id=token_id, gate="TIER1", passed=passed,
        reason_code=reason_code, evaluated_at=evaluated_at, inputs_json=inputs_json or {},
    )


@pytest.mark.asyncio
async def test_flags_a_wash_trading_rejection(session):
    token_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, now - timedelta(seconds=5), passed=False, reason_code="WASH_TRADING"))
    await session.flush()

    assert await is_wash_trading_rejected(session, token_id, now) is True


@pytest.mark.asyncio
async def test_does_not_flag_lp_not_burned(session):
    """Confirmed by real data to be the safest rejection reason —
    deliberately left unfiltered."""
    token_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, now - timedelta(seconds=5), passed=False, reason_code="LP_NOT_BURNED"))
    await session.flush()

    assert await is_wash_trading_rejected(session, token_id, now) is False


@pytest.mark.asyncio
async def test_does_not_flag_a_tier1_pass(session):
    token_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, now - timedelta(seconds=5), passed=True, reason_code=None))
    await session.flush()

    assert await is_wash_trading_rejected(session, token_id, now) is False


@pytest.mark.asyncio
async def test_does_not_flag_a_token_with_no_tier1_row(session):
    assert await is_wash_trading_rejected(session, uuid.uuid4(), datetime.now(timezone.utc)) is False


@pytest.mark.asyncio
async def test_ignores_a_wash_trading_evaluation_recorded_after_the_signal(session):
    """A token that passed TIER1 cleanly, got a momentum signal, and only
    LATER got re-evaluated as WASH_TRADING must not be retroactively
    flagged — the entry decision can only ever see what existed at the
    time the signal actually fired."""
    token_id = uuid.uuid4()
    signal_time = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, signal_time - timedelta(minutes=5), passed=True, reason_code=None))
    session.add(make_evaluation(token_id, signal_time + timedelta(minutes=5), passed=False, reason_code="WASH_TRADING"))
    await session.flush()

    assert await is_wash_trading_rejected(session, token_id, signal_time) is False


@pytest.mark.asyncio
async def test_uses_the_most_recent_qualifying_evaluation(session):
    """An earlier LP_NOT_BURNED rejection followed by a later (but still
    pre-signal) WASH_TRADING rejection must resolve to the more recent,
    worse verdict."""
    token_id = uuid.uuid4()
    signal_time = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, signal_time - timedelta(minutes=10), passed=False, reason_code="LP_NOT_BURNED"))
    session.add(make_evaluation(token_id, signal_time - timedelta(minutes=1), passed=False, reason_code="WASH_TRADING"))
    await session.flush()

    assert await is_wash_trading_rejected(session, token_id, signal_time) is True


# ── is_liquidity_too_high() — liquidity-ceiling filter (2026-09-24) ─────────
#
# See workers/entry_filters.py's docstring for the full data: liquidity <
# $30k at signal time is a much stronger, cleaner predictor than the
# WASH_TRADING reason code alone (6.5% vs 36.7% rug rate on an evenly
# split n=262 dataset), and stays predictive even inside the WASH_TRADING
# population itself — so it's checked independently, not as a restatement.

@pytest.mark.asyncio
async def test_flags_liquidity_at_or_above_the_ceiling(session):
    token_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, now - timedelta(seconds=5), passed=True, inputs_json={"liquidity_usd": "45000"}))
    await session.flush()

    assert await is_liquidity_too_high(session, token_id, now) is True


@pytest.mark.asyncio
async def test_does_not_flag_liquidity_below_the_ceiling(session):
    token_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, now - timedelta(seconds=5), passed=True, inputs_json={"liquidity_usd": "12000"}))
    await session.flush()

    assert await is_liquidity_too_high(session, token_id, now) is False


@pytest.mark.asyncio
async def test_flags_exactly_at_the_ceiling(session):
    """>= the ceiling, not just >, per the module's own threshold definition."""
    token_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, now - timedelta(seconds=5), passed=True, inputs_json={"liquidity_usd": "30000"}))
    await session.flush()

    assert await is_liquidity_too_high(session, token_id, now) is True


@pytest.mark.asyncio
async def test_does_not_flag_a_token_with_no_tier1_row_liquidity(session):
    assert await is_liquidity_too_high(session, uuid.uuid4(), datetime.now(timezone.utc)) is False


@pytest.mark.asyncio
async def test_does_not_flag_when_liquidity_missing_from_inputs(session):
    """Permissive-when-unknown — same policy as is_wash_trading_rejected():
    a TIER1 row that exists but never recorded liquidity_usd must not
    block an entry."""
    token_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, now - timedelta(seconds=5), passed=False, reason_code="WASH_TRADING", inputs_json={}))
    await session.flush()

    assert await is_liquidity_too_high(session, token_id, now) is False


@pytest.mark.asyncio
async def test_liquidity_check_ignores_evaluations_after_the_signal(session):
    token_id = uuid.uuid4()
    signal_time = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, signal_time - timedelta(minutes=5), passed=True, inputs_json={"liquidity_usd": "5000"}))
    session.add(make_evaluation(token_id, signal_time + timedelta(minutes=5), passed=True, inputs_json={"liquidity_usd": "999999"}))
    await session.flush()

    assert await is_liquidity_too_high(session, token_id, signal_time) is False


@pytest.mark.asyncio
async def test_independent_of_wash_trading_reason(session):
    """The liquidity check fires on its own even when the token was NOT
    rejected for WASH_TRADING — it's a separate, additive condition."""
    token_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.add(make_evaluation(token_id, now - timedelta(seconds=5), passed=False, reason_code="LP_NOT_BURNED",
                                 inputs_json={"liquidity_usd": "50000"}))
    await session.flush()

    assert await is_wash_trading_rejected(session, token_id, now) is False
    assert await is_liquidity_too_high(session, token_id, now) is True


# ── is_buy_pressure_too_low() — buy-pressure floor (2026-09-24) ─────────────
#
# See workers/entry_filters.py's docstring for the data: within the
# liquidity+wash-trading "good" bucket, buy_pressure >= 0.97 cut the
# HARD_FLOOR rate from ~34-44% to 11.7% and lifted win rate to 86.4% —
# the cleanest, most monotonic discriminator found in the whole analysis.
# Synchronous and pure, unlike the other two — no DB access at all.

def test_flags_buy_pressure_below_the_floor():
    assert is_buy_pressure_too_low(Decimal("0.85")) is True


def test_does_not_flag_buy_pressure_at_or_above_the_floor():
    assert is_buy_pressure_too_low(Decimal("0.97")) is False
    assert is_buy_pressure_too_low(Decimal("0.995")) is False


def test_does_not_flag_missing_buy_pressure():
    """Permissive-when-unknown — same policy as the other two filters."""
    assert is_buy_pressure_too_low(None) is False


def test_respects_a_custom_floor():
    assert is_buy_pressure_too_low(Decimal("0.92"), floor=Decimal("0.90")) is False
    assert is_buy_pressure_too_low(Decimal("0.88"), floor=Decimal("0.90")) is True
