"""
Tests for engine/halt_override.py (2026-09-28) — the resume-after-halt
baseline reset. See models.orm.ConfluenceLiveHaltOverride's docstring for
the design: acknowledging a halt must NOT disable the safety check, it
resets its zero point, so the same CONFLUENCE_LIVE_MAX_LOSS_PCT cap keeps
protecting every dollar from the acknowledgment moment forward.
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from engine.halt_override import acknowledge_halt, apply_override, get_halt_override
from engine.live_equity import is_permanently_halted


def patched_session(session):
    ctx = patch("engine.halt_override.get_session")
    mock_gs = ctx.start()
    mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
    mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
    return ctx


@pytest.mark.asyncio
async def test_no_override_returns_none(session):
    ctx = patched_session(session)
    try:
        assert await get_halt_override() is None
    finally:
        ctx.stop()


@pytest.mark.asyncio
async def test_acknowledge_then_read_back_round_trips(session):
    ctx = patched_session(session)
    try:
        await acknowledge_halt(Decimal("4.50"), Decimal("-6.71"))
        override = await get_halt_override()
        assert override is not None
        assert override.equity_baseline_usd == Decimal("4.50")
        assert override.pnl_baseline_usd == Decimal("-6.71")
    finally:
        ctx.stop()


@pytest.mark.asyncio
async def test_second_acknowledgment_overwrites_the_first_not_additive(session):
    ctx = patched_session(session)
    try:
        await acknowledge_halt(Decimal("4.50"), Decimal("-6.71"))
        await acknowledge_halt(Decimal("2.00"), Decimal("-9.00"))
        override = await get_halt_override()
        assert override.equity_baseline_usd == Decimal("2.00")
        assert override.pnl_baseline_usd == Decimal("-9.00")
    finally:
        ctx.stop()


def test_apply_override_is_identity_when_none():
    deposit, pnl = apply_override(None, Decimal("10.65"), Decimal("-6.71"))
    assert deposit == Decimal("10.65")
    assert pnl == Decimal("-6.71")


class _FakeOverride:
    def __init__(self, equity_baseline_usd, pnl_baseline_usd):
        self.equity_baseline_usd = equity_baseline_usd
        self.pnl_baseline_usd = pnl_baseline_usd


def test_apply_override_resets_deposit_and_rebases_pnl():
    override = _FakeOverride(Decimal("4.50"), Decimal("-6.71"))
    deposit, pnl = apply_override(override, Decimal("10.65"), Decimal("-6.71"))
    # Resuming right at the halt moment: baseline == current numbers, so
    # the rebased pnl is exactly zero — a fresh start, not a discount.
    assert deposit == Decimal("4.50")
    assert pnl == Decimal("0")


def test_apply_override_reflects_further_pnl_since_acknowledgment():
    override = _FakeOverride(Decimal("4.50"), Decimal("-6.71"))
    # $2 more realized loss happens after the resume.
    deposit, pnl = apply_override(override, Decimal("10.65"), Decimal("-8.71"))
    assert deposit == Decimal("4.50")
    assert pnl == Decimal("-2.00")


def test_same_max_loss_cap_still_trips_after_a_resume():
    """
    The whole point of the baseline reset: it must not be possible to
    resume once and then silently ride the account all the way to zero.
    Simulate a resume at $4.50 equity, then a further $3.01 real loss —
    the $3 cap must trip again, exactly as it would have from the
    original deposit.
    """
    MAX_LOSS = Decimal("3.00")
    override = _FakeOverride(Decimal("4.50"), Decimal("-6.71"))
    equity_after_further_loss = Decimal("1.49")  # 4.50 - 3.01
    all_time_pnl_after_further_loss = Decimal("-9.72")  # -6.71 - 3.01
    deposit, pnl = apply_override(
        override, Decimal("10.65"), all_time_pnl_after_further_loss,
    )
    assert is_permanently_halted(
        equity_after_further_loss, pnl, deposit_usd=deposit,
    ) is True
