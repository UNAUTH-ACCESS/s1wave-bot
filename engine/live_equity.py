"""
engine/live_equity.py
======================
Single source of truth for confluence-live equity math and its two
portfolio-level safety-gate thresholds — imported by BOTH
workers/confluence_live_worker.py (real enforcement) and api/app.py
(dashboard display), so the two formulas can never quietly drift apart
the way api/app.py's wallet_address field once did (a hardcoded string
instead of a live derivation, found and fixed in the 2026-09-24 audit).
Each caller fetches the wallet balance through its own RPC path (the
worker caches it briefly since it polls every ~1s; the API fetches fresh
per request, no cache needed) — only the math below is shared.

Equity model (2026-09-24, replacing the old fixed-stake-plus-realized-pnl
formula, per the user's explicit instruction: "$7 deposited -> trade with
$7 available equity, $15 deposited -> trade with $15 ... size trades with
available wallet capital"):

    equity_usd = live on-chain SOL balance, right now, * SOL_PRICE_USD

Deliberately NOT (CONFLUENCE_LIVE_MAX_TOTAL_CAPITAL_USD + all-time realized
pnl_usd) anymore — that old formula never noticed a manual deposit or
withdrawal and would silently drift from what's actually in the wallet.
Reading the real, current, on-chain balance means a deposit/withdrawal is
picked up automatically with no config change, and realized P&L is
*already* reflected in it for free — a winning trade returns more SOL to
the wallet than it spent, a losing one returns less. No separate
bookkeeping needed for either.

Position sizing (CONFLUENCE_LIVE_EXPOSURE_PCT / CONFLUENCE_LIVE_MAX_CONCURRENT
/ CONFLUENCE_LIVE_MAX_POSITION_USD) is UNCHANGED — same percentages, same
concurrency limit, same outer ceiling, same trailing-stop/hard-floor exit
logic elsewhere. Only the equity figure they're computed against changed
from a fixed config number to this live one.

Safety gates, redefined against live equity instead of a fixed stake:
  - permanently halted: EITHER (a) live equity has fallen to (near) zero —
    CONFLUENCE_LIVE_MIN_TRADEABLE_USD is a dust floor (default $1), not a
    loss-limit percentage: below it, swap fees and slippage would dominate
    any trade anyway — OR (b) real equity has drawn down
    CONFLUENCE_LIVE_MAX_LOSS_PCT (2026-09-28: a PERCENTAGE, 30% default —
    converted from a fixed $3 the same day the user added more capital,
    specifically so the threshold scales with whatever the current balance
    actually is instead of staying pinned to the original $10 test size)
    from `deposit_usd`, the caller's current baseline. (b) is independent
    of (a): a wallet can still hold plenty of equity (e.g. topped up again)
    while having drawn down 30% from its baseline, and that must still halt.
  - daily halted: today's realized P&L has erased CONFLUENCE_LIVE_
    DAILY_LOSS_LIMIT_PCT of *today's starting* equity. Today's starting
    equity is back-derived as (current equity - today's realized pnl)
    rather than tracked separately, since today's realized pnl is exactly
    the change since start-of-day from trading — this assumes no manual
    deposit/withdrawal happened today; a deposit mid-day makes that one
    day's limit slightly stale, self-correcting the next UTC day. This is
    a SEPARATE, faster-tripping early warning — the lifetime cap above is
    the hard backstop for the whole test run, this one just pauses for the
    rest of the UTC day on a smaller drawdown.
"""

from __future__ import annotations

from decimal import Decimal

from config.settings import settings

# The wallet's one and only real deposit (2026-09-24, confirmed via direct
# RPC trace of every transaction ever made by this wallet: preBalance=0 on
# this one, the only inbound transfer in its entire history). Ground truth
# for the account-level reconciliation added 2026-09-28 after a full audit
# found the displayed all_time_realized_pnl_usd (-$1.21 at the time)
# massively understated the real loss — the honest number is
# current_wallet_usd - DEPOSIT_USD, not a sum of per-trade pnl_usd fields.
# Update this if the wallet is ever topped up again.
DEPOSIT_LAMPORTS = 92_774_730
DEPOSIT_SOL_PRICE_USD_AT_DEPOSIT = Decimal("114.75")  # nearest real heartbeat.ok reading to the deposit's block time
DEPOSIT_USD = (Decimal(DEPOSIT_LAMPORTS) / Decimal("1e9")) * DEPOSIT_SOL_PRICE_USD_AT_DEPOSIT


def compute_equity_usd(sol_balance: Decimal | None, sol_price_usd: Decimal) -> Decimal | None:
    """None if the balance is unknown (RPC error, paper mode) or price isn't
    available yet — callers must NOT treat that as zero equity."""
    if sol_balance is None or sol_price_usd is None or sol_price_usd <= 0:
        return None
    return sol_balance * sol_price_usd


def compute_position_usd(equity_usd: Decimal) -> Decimal:
    """Exposure-percentage sizing against LIVE equity — formula itself is
    unchanged from the 2026-09-23 design, only its equity input is now the
    real wallet balance instead of a fixed config stake."""
    n_slots = Decimal(str(settings.CONFLUENCE_LIVE_MAX_CONCURRENT))
    total_exposure = max(Decimal("0"), equity_usd) * Decimal(str(settings.CONFLUENCE_LIVE_EXPOSURE_PCT))
    position_usd = total_exposure / n_slots
    ceiling = Decimal(str(settings.CONFLUENCE_LIVE_MAX_POSITION_USD))
    return max(Decimal("0"), min(position_usd, ceiling))


def is_permanently_halted(
    equity_usd: Decimal | None, all_time_pnl_usd: Decimal | None = None, deposit_usd: Decimal | None = None,
) -> bool:
    """Unknown equity (None) is treated as NOT halted here — a transient RPC
    failure must block new entries via the caller's own None-handling
    upstream (no equity to size against), not masquerade as a halt state.
    all_time_pnl_usd is optional so existing callers that only care about
    the dust floor don't break; omitting it just skips the lifetime-loss
    check, it never itself causes a halt.

    CRITICAL FIX, 2026-09-28: all_time_pnl_usd (a sum of per-trade pnl_usd)
    was confirmed, via a full on-chain audit, to understate real losses by
    6.36x in aggregate — built from INTENDED swap amounts, missing a
    mandatory, non-reclaimable Pump.fun protocol-fee cost on some sells.
    Real consequence found live: the max-loss cap had ALREADY been
    breached in reality (real loss ~$6.71 against a $10.65 deposit) while
    this function kept reporting "not halted", because it only ever saw
    the understated -$1.21 figure. Trading kept running past its own
    stated stop-loss the whole time.

    deposit_usd (optional) lets a caller ALSO check the ground-truth gap
    (current equity vs. the caller's current baseline — the wallet's one
    real deposit, or a later reset baseline) — this doesn't depend on any
    per-trade attribution at all, so it can't be fooled the same way.
    Deliberately a parameter, not DEPOSIT_USD baked in directly: that
    constant is real, production-specific data, and hardcoding it here
    would make this shared function's behavior depend on unrelated
    real-world state inside tests that pass their own arbitrary equity
    numbers. The real call sites (confluence_live_worker.py, api/app.py)
    pass deposit_usd=<the override-adjusted baseline> explicitly.

    2026-09-28: the loss threshold itself is now `deposit_usd *
    CONFLUENCE_LIVE_MAX_LOSS_PCT` — a PERCENTAGE of the baseline, not a
    fixed dollar figure — so it scales automatically whenever the baseline
    does (a top-up, a withdrawal, or a resume-after-halt acknowledgment),
    instead of staying pinned to whatever dollar amount made sense for the
    original test size. Both the recorded-pnl signal and the real-
    deposit-gap signal are checked against this same scaled threshold;
    since the threshold itself now depends on deposit_usd, BOTH checks are
    skipped (not just the deposit-gap one) when deposit_usd is omitted —
    there is no baseline to compute a percentage of. Halts on EITHER
    signal crossing the limit, whichever is worse.
    """
    if equity_usd is not None and equity_usd <= Decimal(str(settings.CONFLUENCE_LIVE_MIN_TRADEABLE_USD)):
        return True
    if deposit_usd is not None and deposit_usd > 0:
        max_loss_usd = deposit_usd * Decimal(str(settings.CONFLUENCE_LIVE_MAX_LOSS_PCT))
        if all_time_pnl_usd is not None and all_time_pnl_usd <= -max_loss_usd:
            return True
        if equity_usd is not None:
            real_loss = equity_usd - deposit_usd
            if real_loss <= -max_loss_usd:
                return True
    return False


def is_daily_halted(equity_usd: Decimal | None, today_pnl_usd: Decimal) -> bool:
    if equity_usd is None:
        return False
    start_of_day_equity = equity_usd - today_pnl_usd
    daily_limit = max(Decimal("0"), start_of_day_equity) * Decimal(str(settings.CONFLUENCE_LIVE_DAILY_LOSS_LIMIT_PCT))
    return today_pnl_usd <= -daily_limit
