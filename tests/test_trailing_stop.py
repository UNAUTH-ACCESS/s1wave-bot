"""
tests/test_trailing_stop.py
==============================
Unit coverage for engine/trailing_stop.py's staircase functions.

This module had no dedicated test file before 2026-09-23 despite being
real logic (originally written for S1 wave positions, data-validated:
"5% steps outperform 10% on all 11 actual trades" per its own docstring —
note the module's _STEP is actually 0.10, i.e. 10% steps, not 5%; that
docstring line predates the current constant and is left alone here,
out of scope for this pass). It is now load-bearing for real money via
ConfluenceLiveWorker's layered exit (2026-09-23), so it gets full
coverage before being trusted with that.
"""

from __future__ import annotations

from decimal import Decimal

from engine.trailing_stop import compute_floor, initial_floor, update_trailing_stop


def D(s: str) -> Decimal:
    return Decimal(s)


def test_initial_floor_is_twelve_percent_below_entry():
    """Widened 6% -> 12% (2026-09-24) — see _INITIAL_STOP_PCT's comment in
    engine/trailing_stop.py for why: two real live trades both exited
    within ~1 second via HARD_FLOOR on ordinary first-second volatility."""
    assert initial_floor(D("1.00")) == D("0.880000000000")


def test_compute_floor_before_first_step_is_initial_floor():
    # +5% gain — hasn't cleared the first 10% step yet.
    assert compute_floor(D("1.00"), D("1.05")) == initial_floor(D("1.00"))


def test_compute_floor_at_exactly_ten_percent_locks_breakeven():
    assert compute_floor(D("1.00"), D("1.10")) == D("1.000000000000")


def test_compute_floor_mid_stage_locks_previous_stage_not_current():
    # +15% is still within the "+10% cleared" stage — floor stays at breakeven.
    assert compute_floor(D("1.00"), D("1.15")) == D("1.000000000000")


def test_compute_floor_at_twenty_percent_locks_ten_percent():
    assert compute_floor(D("1.00"), D("1.20")) == D("1.100000000000")


def test_compute_floor_at_forty_percent_locks_thirty_percent():
    # This is the case that matters most: the old fixed take-profit exited
    # at +30% outright. The staircase instead locks +30% as a floor and
    # keeps riding — proof it doesn't cap upside at the old TAKE_PROFIT_PCT.
    assert compute_floor(D("1.00"), D("1.40")) == D("1.300000000000")


def test_update_trailing_stop_high_watermark_never_goes_down():
    new_floor, new_hwm, should_close = update_trailing_stop(
        entry_price=D("1.00"), current_price=D("1.05"),
        current_floor=D("1.10"), current_hwm=D("1.20"),
    )
    assert new_hwm == D("1.20")  # a pullback tick never lowers the watermark


def test_update_trailing_stop_floor_never_goes_down():
    # Even if the naive recompute from a (hypothetically stale) watermark
    # would suggest a lower floor, the stored floor must never retreat.
    new_floor, new_hwm, should_close = update_trailing_stop(
        entry_price=D("1.00"), current_price=D("1.00"),
        current_floor=D("1.10"), current_hwm=D("1.00"),
    )
    assert new_floor == D("1.10")


def test_update_trailing_stop_closes_when_price_breaches_floor():
    new_floor, new_hwm, should_close = update_trailing_stop(
        entry_price=D("1.00"), current_price=D("1.09"),
        current_floor=D("1.10"), current_hwm=D("1.20"),
    )
    assert should_close is True


def test_update_trailing_stop_does_not_close_above_floor():
    new_floor, new_hwm, should_close = update_trailing_stop(
        entry_price=D("1.00"), current_price=D("1.15"),
        current_floor=D("1.10"), current_hwm=D("1.20"),
    )
    assert should_close is False


def test_update_trailing_stop_ratchets_floor_up_as_price_makes_new_highs():
    # Simulate a run from entry to +40%, one 10%-step tick at a time.
    floor, hwm = initial_floor(D("1.00")), D("1.00")
    for price in (D("1.05"), D("1.10"), D("1.20"), D("1.30"), D("1.40")):
        floor, hwm, should_close = update_trailing_stop(D("1.00"), price, floor, hwm)
        assert should_close is False
    assert floor == D("1.300000000000")  # +30% locked, exactly where the old fixed take-profit would have force-sold
    assert hwm == D("1.40")


def test_update_trailing_stop_locks_gain_then_closes_on_pullback():
    floor, hwm = initial_floor(D("1.00")), D("1.00")
    for price in (D("1.20"), D("1.05")):  # run to +20%, then pull back to +5%
        floor, hwm, should_close = update_trailing_stop(D("1.00"), price, floor, hwm)
    assert floor == D("1.100000000000")  # +10% locked once hwm reached +20%
    assert should_close is True  # +5% is below the locked +10% floor
