"""
tests/test_live_equity.py
===========================
engine/live_equity.py's is_permanently_halted() — the real-loss check
added 2026-09-28 after a critical live finding: CONFLUENCE_LIVE_MAX_LOSS_USD
(set to $3.00) had ALREADY been breached in reality (real loss ~$6.71
against a $10.65 deposit) while this function kept reporting "not halted",
because the only loss signal it saw (all_time_pnl_usd, a sum of per-trade
pnl_usd) was itself built from INTENDED swap amounts and understated real
losses by 6.36x in aggregate. Trading kept running past its own stated
stop-loss the whole time.

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
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_USD", 3.0)
    # Real MetaMask-incident shape: recorded pnl looks barely negative,
    # but the real gap against the deposit is already past the $3 cap.
    assert is_permanently_halted(
        equity_usd=Decimal("4.32"), all_time_pnl_usd=Decimal("-1.21"), deposit_usd=Decimal("10.65"),
    ) is True


def test_does_not_halt_on_deposit_gap_within_the_limit(monkeypatch):
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_USD", 3.0)
    assert is_permanently_halted(
        equity_usd=Decimal("9.00"), all_time_pnl_usd=Decimal("-0.10"), deposit_usd=Decimal("10.65"),
    ) is False


def test_omitting_deposit_usd_skips_the_real_loss_check():
    """Backward compatible: a caller that doesn't know the deposit (or a
    test using dummy equity numbers unrelated to any real deposit) must
    not have this check silently applied."""
    assert is_permanently_halted(
        equity_usd=Decimal("4.0"), all_time_pnl_usd=Decimal("0"), deposit_usd=None,
    ) is False


def test_still_halts_on_the_recorded_pnl_signal_alone():
    """The original check must keep working when deposit_usd isn't passed."""
    from config.settings import settings as real_settings
    assert is_permanently_halted(
        equity_usd=Decimal("20.0"), all_time_pnl_usd=-Decimal(str(real_settings.CONFLUENCE_LIVE_MAX_LOSS_USD)) - 1,
    ) is True


def test_still_halts_on_the_dust_floor_alone():
    assert is_permanently_halted(equity_usd=Decimal("0.0001")) is True


def test_unknown_equity_never_halts_the_equity_dependent_checks(monkeypatch):
    """A transient RPC failure (equity=None) must not masquerade as a halt
    via either equity-dependent check (dust floor or the real-deposit-gap
    check) — only a pnl-based signal, which doesn't need equity, can still
    halt on its own."""
    monkeypatch.setattr(settings, "CONFLUENCE_LIVE_MAX_LOSS_USD", 3.0)
    assert is_permanently_halted(equity_usd=None, all_time_pnl_usd=None, deposit_usd=Decimal("10")) is False
