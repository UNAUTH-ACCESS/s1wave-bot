"""
tests/test_filter_calibration.py
===================================
engine/filter_calibration.py's evaluate() — the pure comparison logic
behind the periodic self-audit (2026-09-28) that checks whether the 3
production entry filters still perform like the 2026-09-28 baseline,
without ever adjusting a threshold itself (see that module's docstring
for why: this session's own entry-cost-ceiling saga showed even a
well-reasoned, data-backed threshold change needs live validation no
script can provide alone — detect-and-flag, human decides).
"""

from __future__ import annotations

from engine.filter_calibration import (
    _REVIEW_CAPPED_MEAN_FLOOR,
    _REVIEW_RUG_RATE_CEILING,
    _REVIEW_WIN_RATE_FLOOR,
    evaluate,
)


def _make_pnls(n_wins: int, n_losses_small: int, n_rugs: int, win_pct=0.30, loss_pct=-0.10, rug_pct=-0.60):
    return [win_pct] * n_wins + [loss_pct] * n_losses_small + [rug_pct] * n_rugs


def test_below_minimum_sample_never_flags_for_review():
    """14 trades — just under the 15-trade minimum — must not be judged
    at all, however bad they look, to avoid crying wolf on noise."""
    pnls = _make_pnls(n_wins=0, n_losses_small=0, n_rugs=14)
    result = evaluate(pnls)
    assert result.needs_review is False
    assert result.win_rate is None
    assert "below the 15-trade minimum" in result.reasons[0]


def test_baseline_like_performance_does_not_need_review():
    """Matches the real 2026-09-28 baseline shape (mostly wins, a
    controlled rug rate, strongly positive capped mean) — must read as
    fine."""
    pnls = _make_pnls(n_wins=17, n_losses_small=2, n_rugs=1)  # 20 trades, 85% win, 5% rug
    result = evaluate(pnls)
    assert result.needs_review is False
    assert result.win_rate == 0.85
    assert result.reasons == []


def test_win_rate_crash_triggers_review():
    pnls = _make_pnls(n_wins=5, n_losses_small=10, n_rugs=5)  # 20 trades, 25% win
    result = evaluate(pnls)
    assert result.needs_review is True
    assert any("win rate" in r for r in result.reasons)


def test_win_rate_right_at_the_floor_does_not_trigger():
    """Boundary check: exactly at the floor must not itself be flagged —
    only strictly below it."""
    n = 20
    n_wins = round(_REVIEW_WIN_RATE_FLOOR * n)  # exactly at the floor
    pnls = _make_pnls(n_wins=n_wins, n_losses_small=n - n_wins, n_rugs=0, loss_pct=-0.05)
    result = evaluate(pnls)
    assert result.win_rate == n_wins / n
    assert not any("win rate" in r for r in result.reasons)


def test_rug_rate_spike_triggers_review():
    pnls = _make_pnls(n_wins=12, n_losses_small=0, n_rugs=8)  # 20 trades, 40% rug
    result = evaluate(pnls)
    assert result.needs_review is True
    assert any("rug rate" in r for r in result.reasons)


def test_negative_capped_mean_triggers_review_even_with_ok_win_rate():
    """A population can have a technically-passing win rate while still
    being net-negative if losses are severe enough — the capped-mean
    check exists specifically to catch that a win-rate-only view would
    miss."""
    pnls = _make_pnls(n_wins=14, n_losses_small=0, n_rugs=6, win_pct=0.02, rug_pct=-0.90)  # 20 trades, 70% win
    result = evaluate(pnls)
    assert result.win_rate == 0.70  # would look "fine" on win rate alone
    assert result.needs_review is True
    assert any("capped mean" in r for r in result.reasons)


def test_multiple_simultaneous_problems_are_all_reported():
    pnls = _make_pnls(n_wins=3, n_losses_small=2, n_rugs=15)  # 20 trades, 15% win, 75% rug
    result = evaluate(pnls)
    assert result.needs_review is True
    assert len(result.reasons) == 3  # win rate, rug rate, AND capped mean all fail here
