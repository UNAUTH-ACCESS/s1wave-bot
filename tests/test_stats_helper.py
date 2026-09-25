"""
tests/test_stats_helper.py
===========================
api/app.py's _compute_pct_stats() — shared win-rate/rug-rate/capped-mean
computation used by both /confluence/shadow/stats and /confluence/live/stats
(2026-09-25), so the two can never quietly compute these differently.

Also covers entry_filters.CURRENT_FILTER_REGIME_SINCE, the single source
of truth both stats endpoints use to split "since current filters" from
"all_time" — added after a live win rate that looked like 36% turned out
to be 66% (matching the shadow benchmark) once trades from before all 3
current entry filters existed were excluded from the comparison.

pnl_pct values here are fractions (0.10 = +10%), matching
ConfluenceShadowPosition.pnl_pct's real stored convention.
"""

from __future__ import annotations

from datetime import datetime, timezone

from api.app import _compute_pct_stats
from workers.entry_filters import CURRENT_FILTER_REGIME_SINCE


def test_empty_input_returns_all_none_not_a_crash():
    result = _compute_pct_stats([])
    assert result["closed"] == 0
    assert result["win_rate"] is None
    assert result["median_pnl_pct"] is None
    assert result["mean_pnl_pct_raw"] is None
    assert result["mean_pnl_pct_capped"] is None
    assert result["rug_rate"] is None
    assert result["rug_avg_pnl_pct"] is None
    assert result["exit_reasons"] == {}


def test_win_rate_counts_only_strictly_positive_pnl():
    rows = [(0.10, "TAKE_PROFIT"), (0.0, "TIME_EXIT"), (-0.05, "STOP_LOSS")]
    result = _compute_pct_stats(rows)
    assert result["closed"] == 3
    assert result["win_rate"] == round(1 / 3, 4)  # only the +0.10 counts as a win


def test_rug_rate_counts_pnl_at_or_below_negative_40_pct():
    rows = [(-0.40, "HARD_FLOOR"), (-0.39, "HARD_FLOOR"), (0.20, "TAKE_PROFIT")]
    result = _compute_pct_stats(rows)
    assert result["rug_rate"] == round(1 / 3, 4)  # only the -0.40 counts (-0.39 is just short)
    assert result["rug_avg_pnl_pct"] == -0.40


def test_capped_mean_caps_a_huge_outlier_at_100_pct():
    """Real incident this guards: a couple of historical trades show an
    unrealistic $0-recorded-liquidity 'peak' price no real sell could fill
    at, producing a raw mean wildly higher than reality."""
    rows = [(5.0, "TAKE_PROFIT"), (0.10, "TAKE_PROFIT")]  # 5.0 = +500%, an outlier
    result = _compute_pct_stats(rows)
    assert result["mean_pnl_pct_raw"] == round((5.0 + 0.10) / 2, 4)
    assert result["mean_pnl_pct_capped"] == round((1.0 + 0.10) / 2, 4)  # 5.0 capped to 1.0


def test_none_pnl_is_excluded_from_averages_but_counted_in_closed():
    """win_rate is (preserved, pre-existing behavior) wins / ALL closed
    rows including ones with no pnl recorded — a None-pnl row can never
    count as a win, but it still counts toward the denominator."""
    rows = [(0.10, "TAKE_PROFIT"), (None, "buy_failed"), (-0.10, "STOP_LOSS")]
    result = _compute_pct_stats(rows)
    assert result["closed"] == 3
    assert result["win_rate"] == round(1 / 3, 4)
    assert result["median_pnl_pct"] is not None  # computed only over the 2 real pnls


def test_exit_reasons_tally_and_handle_missing_reason():
    rows = [(0.1, "TAKE_PROFIT"), (0.1, "TAKE_PROFIT"), (-0.1, None)]
    result = _compute_pct_stats(rows)
    assert result["exit_reasons"] == {"TAKE_PROFIT": 2, "unknown": 1}


def test_current_filter_regime_since_is_timezone_aware():
    """A naive datetime here would break every comparison against real
    entry_time columns (which are always timezone-aware)."""
    assert CURRENT_FILTER_REGIME_SINCE.tzinfo is not None
    assert CURRENT_FILTER_REGIME_SINCE.tzinfo == timezone.utc
