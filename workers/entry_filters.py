"""
workers/entry_filters.py
==========================
Shared entry-quality filter for confluence_entry_v1 (both
confluence_shadow_worker.py and confluence_live_worker.py) — added
2026-09-24 after analyzing all 241 closed shadow trades against each
token's TIER1 evaluation at discovery (see NOTEBOOK.md's 2026-09-24
entry for the full breakdown):

  - 89% of ALL confluence_entry_v1 trades (214/241) went into tokens
    TIER1 had already rejected — by design, this experiment watches
    every OBSERVING token regardless of gate result, to test whether the
    momentum signal has edge independent of the other filters.
  - But splitting the rejected population by REASON showed the rug rate
    is nowhere near uniform: tokens rejected for WASH_TRADING (56% of
    ALL trades, 134/241) had the highest rug rate (31.3%) and were
    net-negative even after capping unrealistic upside (-12.0% capped
    mean) — the single largest and worst-performing segment by far.
  - Tokens rejected for LP_NOT_BURNED (33% of trades) were, counter-
    intuitively, the SAFEST and most profitable population (1.2% rug
    rate, +21.3% capped mean) — almost certainly because most of these
    are still on the pump.fun bonding curve, where "LP burn" doesn't
    even apply yet, not a real rug setup. Left unfiltered on purpose.
  - Within the WASH_TRADING population, the *degree* of wash trading
    didn't predict rug risk (the low-wash half actually rugged MORE
    than the high-wash half, 37.3% vs 25.4%) — it's the category
    itself, not its severity, that carries the signal.

So the fix is narrow: skip a confluence_entry_v1 entry only when the
token's most recent TIER1 evaluation before the signal fired was
specifically a WASH_TRADING rejection. Every other case — LP_NOT_BURNED,
any other rejection reason, a TIER1 pass, or no TIER1 row at all — still
passes through exactly as before; this experiment remains deliberately
ungated on the general TIER1 pass/fail result.

Liquidity ceiling (2026-09-24, added after re-running the same analysis
on the larger 262-trade dataset across every available metric — age,
liquidity, market cap, LP burn, mint/freeze authority, buy pressure,
volume multiplier, signal magnitude, rule count):

  - Liquidity at signal time turned out to be a MUCH stronger, cleaner
    predictor than the WASH_TRADING reason code alone, on a much more
    evenly-populated split: liquidity < $30k -> 6.5% rug rate, +13.8%
    capped mean, 67.5% win rate (n=123); liquidity >= $30k -> 36.7% rug
    rate, -17.2% capped mean, 53.2% win rate (n=139).
  - It is NOT just a restatement of the WASH_TRADING filter — even
    *inside* the WASH_TRADING-rejected population, low liquidity redeems
    it (20% rug / -1.7% capped mean vs 35.5% rug / -15.6% for the
    high-liquidity half of that same group).
  - The two filters combine: liquidity < $30k AND not WASH_TRADING ->
    3.9% rug rate, +16.8% capped mean, 74.8% win rate, still keeping 103
    of 262 trades (39%) — the best-performing, best-populated segment
    found in the whole analysis.
  - Mechanism: low liquidity here is close to synonymous with "still on
    the pump.fun bonding curve, not yet graduated to a real AMM pool" (85
    of 89 LP_NOT_BURNED trades were also low-liquidity) — a bonding curve
    is a fixed formula nobody can drain; a graduated pool is where real
    rugs and dumps actually happen.
  - Checked and ruled out as separately actionable: mint_authority_
    renounced and freeze_authority_renounced show ZERO variance (100% of
    all 262 trades already have both renounced) — not usable as a filter,
    TIER1 already guarantees it universally in this population.

So a second, independent skip condition was added: liquidity_usd >=
`LIQUIDITY_CEILING_USD` at the same TIER1 evaluation the WASH_TRADING
check already looks at. Same permissive-when-unknown policy — a token
with no TIER1 row, or no recorded liquidity_usd, is not skipped by this
check.

Buy-pressure floor (2026-09-24, added the same day after the user asked
to re-run analysis on the shadow dataset specifically to find why the
account was still losing): inside the liquidity+wash-trading "good"
bucket (144 trades), the exit reason breakdown showed the real damage is
almost entirely concentrated in HARD_FLOOR exits — and of the 73
HARD_FLOOR exits in the full 304-trade dataset, 45% closed between -90%
and -100% (near-total wipeouts, not gentle -15% stops: these are
essentially real rug pulls, confirmed rather than filtered by the
bad-tick guard needing 2 consecutive ticks to act). Comparing HARD_FLOOR
trades against everything else in the good bucket found `buy_pressure`
(from the originating MomentumSignalEvent — buy volume / total volume at
the moment the signal fired) was the cleanest, most monotonic
discriminator found in this whole analysis:

  - buy_pressure < 0.90: 43.8% HARD_FLOOR rate, 43.8% win rate (n=32)
  - buy_pressure 0.90-0.97: 33.3% HARD_FLOOR rate, 66.7% win rate (n=9)
  - buy_pressure >= 0.97: 11.7% HARD_FLOOR rate, 86.4% win rate, +22.1%
    capped mean (n=103)

Checked for confound and ruled out: the low-buy_pressure trades are
spread across both liquidity sub-ranges and both LP_NOT_BURNED/no-TIER1-
row populations — not a restatement of either existing filter.
`trailing_return_3min` looked promising on a raw median comparison
(HARD_FLOOR trades had roughly double the trailing return of clean ones)
but did NOT hold up under proper bucketing (non-monotonic, and the 25%+
bucket actually had a good win rate) — not implemented as a filter;
buy_pressure was the real driver behind that raw comparison.

Third skip condition: buy_pressure < `BUY_PRESSURE_FLOOR` (0.97) at the
signal that triggered the candidate — permissive-when-unknown as always.
Unlike the other two, this reads straight from the MomentumSignalEvent
row the caller already has (no extra query needed), so it's a plain,
synchronous function, not async.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.orm import TokenEvaluation

WASH_TRADING_REASON = "WASH_TRADING"
LIQUIDITY_CEILING_USD = Decimal("30000")
BUY_PRESSURE_FLOOR = Decimal("0.97")

# Single source of truth for "since when has the CURRENT full filter set
# been live" (2026-09-25) — the moment the last of these three filters
# (buy-pressure) went live, found from the first-ever low_bp_skip row's
# entry_time. Real problem this fixes: a stats view mixing trades from
# before and after a filter shipped produces a misleading number (a live
# win rate that looked like 36% turned out to be 66% — matching the shadow
# benchmark — once trades from before all 3 filters existed were excluded).
# api/app.py's stats endpoints use this to report BOTH the honest all-time
# number and the "since current rules" one side by side, so nobody has to
# remember to ask for this cut by hand again.
#
# UPDATE THIS whenever a new entry filter ships (or an existing one's
# threshold changes materially) — it should always reflect the moment the
# CURRENTLY active filter combination first became fully live together.
CURRENT_FILTER_REGIME_SINCE = datetime(2026, 9, 25, 1, 29, 50, tzinfo=timezone.utc)


async def is_wash_trading_rejected(
    session: AsyncSession, token_id: uuid.UUID, at_or_before: datetime,
) -> bool:
    """
    True if this token's most recent TIER1 evaluation at or before
    `at_or_before` (the momentum signal's trigger time — never a later
    evaluation, which would be judging the entry with information that
    didn't exist yet) was a WASH_TRADING rejection. A token with no
    TIER1 row at all, or whose most recent one at that time is anything
    else (a pass, or a different rejection reason), returns False.
    """
    result = await session.execute(
        select(TokenEvaluation.passed, TokenEvaluation.reason_code)
        .where(
            TokenEvaluation.token_id == token_id,
            TokenEvaluation.gate == "TIER1",
            TokenEvaluation.evaluated_at <= at_or_before,
        )
        .order_by(TokenEvaluation.evaluated_at.desc())
        .limit(1)
    )
    row = result.first()
    if row is None:
        return False
    passed, reason_code = row
    return (not passed) and reason_code == WASH_TRADING_REASON


async def is_liquidity_too_high(
    session: AsyncSession, token_id: uuid.UUID, at_or_before: datetime,
    ceiling_usd: Decimal = LIQUIDITY_CEILING_USD,
) -> bool:
    """
    True if this token's most recent TIER1 evaluation at or before
    `at_or_before` reports liquidity_usd >= `ceiling_usd`. Independent of
    is_wash_trading_rejected() — see this module's docstring for why
    liquidity carries real signal even within the WASH_TRADING population,
    not just as a restatement of it.

    A token with no TIER1 row at all, or whose most recent one has no
    recorded liquidity_usd, returns False — same permissive-when-unknown
    policy as is_wash_trading_rejected(), never treating missing data as
    a reason to block.
    """
    result = await session.execute(
        select(TokenEvaluation.inputs_json)
        .where(
            TokenEvaluation.token_id == token_id,
            TokenEvaluation.gate == "TIER1",
            TokenEvaluation.evaluated_at <= at_or_before,
        )
        .order_by(TokenEvaluation.evaluated_at.desc())
        .limit(1)
    )
    row = result.first()
    if row is None:
        return False
    inputs = row[0] or {}
    liquidity = inputs.get("liquidity_usd")
    if liquidity is None:
        return False
    return Decimal(str(liquidity)) >= ceiling_usd


def is_buy_pressure_too_low(
    buy_pressure: Decimal | None, floor: Decimal = BUY_PRESSURE_FLOOR,
) -> bool:
    """
    True if `buy_pressure` (from the MomentumSignalEvent that triggered
    this candidate — buy volume / total volume at that moment) is below
    `floor`. See this module's docstring for the data: within the
    liquidity+wash-trading "good" bucket, buy_pressure >= 0.97 cut the
    HARD_FLOOR rate from ~34-44% down to 11.7% and lifted win rate to
    86.4%, cleanly and monotonically — the strongest single discriminator
    found in the whole analysis.

    Synchronous and pure (unlike the other two filters) — the caller
    already has this value from the same MomentumSignalEvent row it used
    to find the candidate in the first place, no extra query needed.
    None (missing data) returns False — same permissive-when-unknown
    policy as the other two filters.
    """
    if buy_pressure is None:
        return False
    return buy_pressure < floor
