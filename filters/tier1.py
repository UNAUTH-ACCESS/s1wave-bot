"""
filters/tier1.py
================
Tier 1 — Hard Gate filter  (PRD §4 Event ③, §5)

All seven constraints must pass.  The gate is binary — partial passes do
not exist.  One failure rejects the token immediately with a logged reason.

Constraint evaluation order
----------------------------
Cheapest checks first to short-circuit early before touching fields that
required expensive on-chain fetches.

  1. liquidity_usd        ≥ TIER1_MIN_LIQUIDITY_USD     (DEX Screener)
  2. market_cap_usd       ≥ TIER1_MIN_MARKET_CAP_USD    (DEX Screener)
  3. token age            0.1 – 1440 minutes            (DEX Screener pairCreatedAt)
  4. mint_authority       renounced                     (Helius DAS — never assumed)
  5. freeze_authority     renounced                     (Helius DAS — never assumed)
  6. wash_multiplier      ≤ TIER1_MAX_WASH_MULTIPLIER   (Helius TX)
     Direction:  buy_count / sell_count > threshold → wash trading → REJECT
                 buy_count / sell_count ≤ threshold → organic      → PASS

Missing fields
--------------
If a required field was not populated during enrichment (None), the
constraint FAILS.  We never pass a constraint we couldn't verify.

Age calculation
---------------
Token age is derived from the pairCreatedAt field in token_snapshots when
available, or from discovered_at as a fallback.  The sampling_worker
stores pairCreatedAt in the snapshot — until then, discovered_at is used
as a conservative lower bound (actual age is older, so a very young token
might appear older than it is — it will be re-evaluated on the next cycle).

The authoritative age source is pairCreatedAt from DEX Screener.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from config.logging import get_logger
from config.settings import settings

if TYPE_CHECKING:
    from models.orm import Token

log = get_logger(__name__)


# ── Rejection reason codes ────────────────────────────────────────────────────

class RejectionReason(str, enum.Enum):
    """
    Machine-readable rejection codes stored in tokens.rejection_reason.

    These feed the GET /tokens/rejected API endpoint (Phase 9) which
    surfaces filter intelligence — which constraints are killing the most
    tokens.

    MISSING_ codes: the enrichment fetch returned None — data was unavailable.
    LOW_ / ZERO_ codes: the fetch succeeded but the value failed the threshold.

    The distinction matters for diagnostics: MISSING_ suggests an API or
    parsing issue; LOW_ / ZERO_ is a genuine quality rejection.
    """
    # Liquidity
    MISSING_LIQUIDITY_DATA      = "MISSING_LIQUIDITY_DATA"   # None from enrichment
    ZERO_LIQUIDITY              = "ZERO_LIQUIDITY"           # 0.0 — empty pool
    LOW_LIQUIDITY               = "LOW_LIQUIDITY"            # > 0 but < threshold

    # Market cap
    MISSING_MARKET_CAP_DATA     = "MISSING_MARKET_CAP_DATA"
    ZERO_MARKET_CAP             = "ZERO_MARKET_CAP"
    LOW_MARKET_CAP              = "LOW_MARKET_CAP"

    # Age
    MISSING_AGE_DATA            = "MISSING_AGE_DATA"
    TOKEN_TOO_YOUNG             = "TOKEN_TOO_YOUNG"
    TOKEN_TOO_OLD               = "TOKEN_TOO_OLD"

    # Chain-verified authority fields
    MINT_AUTHORITY_NOT_RENOUNCED   = "MINT_AUTHORITY_NOT_RENOUNCED"
    FREEZE_AUTHORITY_NOT_RENOUNCED = "FREEZE_AUTHORITY_NOT_RENOUNCED"

    # LP status
    LP_NOT_LOCKED_OR_BURNED     = "LP_NOT_LOCKED_OR_BURNED"
    MISSING_LP_DATA             = "MISSING_LP_DATA"

    # Wash trading
    WASH_TRADING                = "WASH_TRADING"
    MISSING_WASH_DATA           = "MISSING_WASH_DATA"


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Tier1Result:
    """
    Returned by run_tier1_filter() for every evaluated token.

    passed          — True only when all 7 constraints pass.
    rejection_reason — set when passed=False.  None when passed=True.
    age_minutes     — computed age at evaluation time (useful for logging).
    """
    passed: bool
    rejection_reason: RejectionReason | None
    age_minutes: float | None

    @classmethod
    def pass_(cls, age_minutes: float | None) -> "Tier1Result":
        return cls(passed=True, rejection_reason=None, age_minutes=age_minutes)

    @classmethod
    def fail(cls, reason: RejectionReason, age_minutes: float | None = None) -> "Tier1Result":
        return cls(passed=False, rejection_reason=reason, age_minutes=age_minutes)


# ── Age helper ────────────────────────────────────────────────────────────────

def compute_age_minutes(
    pair_created_at_ms: int | None,
    discovered_at: datetime,
    now: datetime | None = None,
) -> float | None:
    """
    Compute token age in minutes.

    Prefers pair_created_at_ms (DEX Screener pairCreatedAt, Unix milliseconds)
    as the authoritative source.  Falls back to discovered_at when not yet
    available.

    Returns None only if both sources are unavailable (should never happen
    in practice — discovered_at is always set).
    """
    if now is None:
        now = datetime.now(timezone.utc)

    if pair_created_at_ms is not None:
        created = datetime.fromtimestamp(pair_created_at_ms / 1000, tz=timezone.utc)
        delta = now - created
        return delta.total_seconds() / 60

    # Fallback — discovered_at is a lower bound on actual age
    if discovered_at is not None:
        delta = now - discovered_at.replace(tzinfo=timezone.utc) if discovered_at.tzinfo is None else now - discovered_at
        return delta.total_seconds() / 60

    return None


# ── Core filter function ──────────────────────────────────────────────────────

def run_tier1_filter(
    token: "Token",
    pair_created_at_ms: int | None = None,
    now: datetime | None = None,
) -> Tier1Result:
    """
    Evaluate all 7 Tier 1 constraints against a fully-enriched Token row.

    Parameters
    ----------
    token            : Token ORM instance with all enrichment fields populated.
    pair_created_at_ms : Unix millisecond timestamp from DEX Screener pairCreatedAt.
                         Pass None to fall back to token.discovered_at.
    now              : Override current time (used in tests with freezegun).

    Returns
    -------
    Tier1Result — passed=True advances to WATCHING, passed=False → REJECTED.

    Evaluation stops at the first failed constraint (short-circuit).
    Ordering: cheapest/most-common-failure first.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    age_minutes = compute_age_minutes(pair_created_at_ms, token.discovered_at, now)

    # ── Constraint 1: Liquidity ───────────────────────────────────────────────
    if token.liquidity_usd is None:
        log.debug("tier1.fail", mint=token.mint_address, reason=RejectionReason.MISSING_LIQUIDITY_DATA)
        return Tier1Result.fail(RejectionReason.MISSING_LIQUIDITY_DATA, age_minutes)

    if token.liquidity_usd == Decimal("0"):
        log.debug("tier1.fail", mint=token.mint_address, reason=RejectionReason.ZERO_LIQUIDITY)
        return Tier1Result.fail(RejectionReason.ZERO_LIQUIDITY, age_minutes)

    if token.liquidity_usd < Decimal(str(settings.TIER1_MIN_LIQUIDITY_USD)):
        log.debug(
            "tier1.fail", mint=token.mint_address,
            reason=RejectionReason.LOW_LIQUIDITY,
            liquidity=str(token.liquidity_usd),
            threshold=settings.TIER1_MIN_LIQUIDITY_USD,
        )
        return Tier1Result.fail(RejectionReason.LOW_LIQUIDITY, age_minutes)

    # ── Constraint 2: Market cap ──────────────────────────────────────────────
    if token.market_cap_usd is None:
        return Tier1Result.fail(RejectionReason.MISSING_MARKET_CAP_DATA, age_minutes)

    if token.market_cap_usd == Decimal("0"):
        return Tier1Result.fail(RejectionReason.ZERO_MARKET_CAP, age_minutes)

    if token.market_cap_usd < Decimal(str(settings.TIER1_MIN_MARKET_CAP_USD)):
        log.debug(
            "tier1.fail", mint=token.mint_address,
            reason=RejectionReason.LOW_MARKET_CAP,
            market_cap=str(token.market_cap_usd),
        )
        return Tier1Result.fail(RejectionReason.LOW_MARKET_CAP, age_minutes)

    # ── Constraint 3: Token age ───────────────────────────────────────────────
    if age_minutes is None:
        return Tier1Result.fail(RejectionReason.MISSING_AGE_DATA)

    if age_minutes < settings.TIER1_MIN_AGE_MINUTES:
        log.debug(
            "tier1.fail", mint=token.mint_address,
            reason=RejectionReason.TOKEN_TOO_YOUNG,
            age_minutes=round(age_minutes, 1),
        )
        return Tier1Result.fail(RejectionReason.TOKEN_TOO_YOUNG, age_minutes)

    if age_minutes > settings.TIER1_MAX_AGE_MINUTES:
        log.debug(
            "tier1.fail", mint=token.mint_address,
            reason=RejectionReason.TOKEN_TOO_OLD,
            age_minutes=round(age_minutes, 1),
        )
        return Tier1Result.fail(RejectionReason.TOKEN_TOO_OLD, age_minutes)

    # ── Constraint 4: Mint authority ─────────────────────────────────────────
    # NEVER assumed — always fetched from Helius DAS.
    # None means we couldn't fetch it → treat as not renounced → reject.
    if token.mint_authority_renounced is None or not token.mint_authority_renounced:
        log.debug(
            "tier1.fail", mint=token.mint_address,
            reason=RejectionReason.MINT_AUTHORITY_NOT_RENOUNCED,
        )
        return Tier1Result.fail(RejectionReason.MINT_AUTHORITY_NOT_RENOUNCED, age_minutes)

    # ── Constraint 5: Freeze authority ───────────────────────────────────────
    # NEVER assumed — always fetched from Helius DAS.
    if token.freeze_authority_renounced is None or not token.freeze_authority_renounced:
        log.debug(
            "tier1.fail", mint=token.mint_address,
            reason=RejectionReason.FREEZE_AUTHORITY_NOT_RENOUNCED,
        )
        return Tier1Result.fail(RejectionReason.FREEZE_AUTHORITY_NOT_RENOUNCED, age_minutes)

    # ── Constraint 6: Wash multiplier ────────────────────────────────────────
    # Direction: buy_count / sell_count > TIER1_MAX_WASH_MULTIPLIER → wash trading → REJECT
    #            buy_count / sell_count ≤ TIER1_MAX_WASH_MULTIPLIER → organic      → PASS
    if token.wash_multiplier is None:
        return Tier1Result.fail(RejectionReason.MISSING_WASH_DATA, age_minutes)

    if token.wash_multiplier > Decimal(str(settings.TIER1_MAX_WASH_MULTIPLIER)):
        log.debug(
            "tier1.fail", mint=token.mint_address,
            reason=RejectionReason.WASH_TRADING,
            wash_multiplier=str(token.wash_multiplier),
            threshold=settings.TIER1_MAX_WASH_MULTIPLIER,
        )
        return Tier1Result.fail(RejectionReason.WASH_TRADING, age_minutes)

    # ── All 7 constraints passed ──────────────────────────────────────────────
    log.info(
        "tier1.pass",
        mint=token.mint_address,
        symbol=token.symbol,
        liquidity=str(token.liquidity_usd),
        market_cap=str(token.market_cap_usd),
        age_minutes=round(age_minutes, 1),
        wash_mult=str(token.wash_multiplier),
    )
    return Tier1Result.pass_(age_minutes)
