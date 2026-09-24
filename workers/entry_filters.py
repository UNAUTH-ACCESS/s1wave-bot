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
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.orm import TokenEvaluation

WASH_TRADING_REASON = "WASH_TRADING"
LIQUIDITY_CEILING_USD = Decimal("30000")


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
