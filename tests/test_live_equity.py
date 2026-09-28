"""
tests/test_live_equity.py
===========================
engine/live_equity.py's is_permanently_halted() — the real-loss check
added 2026-09-28 after a critical live finding: the max-loss cap (then a
fixed $3) had ALREADY been breached in reality (real loss ~$6.71 against
a $10.65 deposit) while this function kept reporting "not halted",
because the only loss signal it saw (all_time_pnl_usd, a sum of per-trade
pnl_usd) was itself built from INTENDED swap amounts and understated real
losses by 6.36x in aggregate. Trading kept running past its own stated
stop-loss the whole time.

Same day, later: converted from a fixed dollar amount
(CONFLUENCE_LIVE_MAX_LOSS_USD) to a PERCENTAGE of the baseline
(CONFLUENCE_LIVE_MAX_LOSS_PCT) per the user's explicit instruction ("so
it'll correspond with different cc balance") — a flat $3 cap stopped
making sense once the account could be topped up or drawn down; the same
dollar loss should read very differently against a $10 balance than
against a $600 one.

deposit_usd is deliberately a parameter, not a hardcoded import of the
real DEPOSIT_USD constant — that's real, production-specific data, and
baking it in here would make this shared function's behavior depend on
unrelated real-world state inside any test that passes its own arbitrary
equity numbers. See engine/live_equity.py's own docstring.
"""

from __future__ import annotations

from decimal import Decimal

from config.settings import settings
from engine.live_equity import is_permanently_halted


def test_halts_on_real_deposit_gap_even_when_recorded_pnl_looks_fine(monkeypatch):
    """The core regression: recorded pnl_usd can understate real losses —
    this must still halt using the ground-truth deposit-vs-equity gap."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
    # Real MetaMask-incident shape: recorded pnl looks barely negative,
    # but the real gap against the deposit (-$6.33) is already past 30%
    # of it (-$3.195).
    assert is_permanently_halted(
        equity_usd=Decimal("4.32"), all_time_pnl_usd=Decimal("-1.21"), deposit_usd=Decimal("10.65"),
    ) is True


def test_does_not_halt_on_deposit_gap_within_the_limit(monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
    assert is_permanently_halted(
        equity_usd=Decimal("9.00"), all_time_pnl_usd=Decimal("-0.10"), deposit_usd=Decimal("10.65"),
    ) is False


def test_threshold_scales_with_the_baseline_not_a_fixed_dollar_amount(monkeypatch):
    """The whole point of the 2026-09-28 change: the SAME $3 real loss
    must halt against a small baseline but not against a much bigger
    one — a fixed dollar cap couldn't do this."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
    assert is_permanently_halted(equity_usd=Decimal("7.00"), deposit_usd=Decimal("10.00")) is True  # -$3 = 30% of $10
    assert is_permanently_halted(equity_usd=Decimal("97.00"), deposit_usd=Decimal("100.00")) is False  # -$3 = only 3% of $100


def test_omitting_deposit_usd_skips_the_real_loss_check():
    """Backward compatible: a caller that doesn't know the deposit (or a
    test using dummy equity numbers unrelated to any real deposit) must
    not have this check silently applied."""
    assert is_permanently_halted(
        equity_usd=Decimal("4.0"), all_time_pnl_usd=Decimal("0"), deposit_usd=None,
    ) is False


def test_recorded_pnl_signal_still_halts_when_a_baseline_is_known(monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
    assert is_permanently_halted(
        equity_usd=Decimal("20.0"), all_time_pnl_usd=Decimal("-5.0"), deposit_usd=Decimal("10.0"),
    ) is True  # -$5 <= -($10 * 30%) = -$3


def test_recorded_pnl_alone_no_longer_halts_without_a_baseline(monkeypatch):
    """2026-09-28: since the threshold is now a PERCENTAGE of deposit_usd,
    there is nothing to compute a percentage of without it — both signals
    are skipped, not just the deposit-gap one, matching the existing
    permissive-when-unknown convention."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
    assert is_permanently_halted(
        equity_usd=Decimal("20.0"), all_time_pnl_usd=Decimal("-999"), deposit_usd=None,
    ) is False


def test_still_halts_on_the_dust_floor_alone():
    assert is_permanently_halted(equity_usd=Decimal("0.0001")) is True


def test_unknown_equity_never_halts_the_equity_dependent_checks(monkeypatch):
    """A transient RPC failure (equity=None) must not masquerade as a halt
    via either equity-dependent check (dust floor or the real-deposit-gap
    check) — only a pnl-based signal (with a known baseline) can still
    halt on its own, since it doesn't need equity."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_PCT", 0.30)
    assert is_permanently_halted(equity_usd=None, all_time_pnl_usd=None, deposit_usd=Decimal("10")) is False
