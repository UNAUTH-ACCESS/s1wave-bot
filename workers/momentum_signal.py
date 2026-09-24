"""
workers/momentum_signal.py
============================
Forward-tracking shadow experiment for the pump-timing pattern found by
analysis/pump_timing_research.py and analysis/pump_signal_quality.py
(2026-09-22 research on the full collected dataset): a >5.3% price move
over a trailing 3-minute window reliably preceded a token's peak by a
median ~8 minutes across 420/438 real pumps (96% coverage), validated via
time-split (train/test) — not just in-sample. A follow-up found that
requiring >=2 of 4 related rules to fire AT THE SAME MOMENT roughly
doubled the odds the move held rather than dumping (72.9%/57.7%
train/test winner rate vs 20.4%/28.6%), also validated out-of-sample.

Never read by CapitalEngine, RiskEngine, execution.py, or any production
decision path — recording a row here changes nothing about what the real
bot does. Both prior scripts only had RETROSPECTIVE data to test on; this
module exists purely so the same signal can be validated PROSPECTIVELY,
on tokens discovered from here forward, using the exact token_snapshots
the real pipeline already writes (no extra API calls). Outcomes (forward
return, whether it held or dumped) are always derived LATER from
token_snapshots by a future analysis script, never stored here — same
convention as workers/scoring_worker.py's shadow_trades experiment.

Thresholds are frozen from the research that validated them:
  PRIMARY_THRESHOLD_3MIN            trailing_return_3min > 5.3%
  SECONDARY_THRESHOLD_5MIN          trailing_return_5min > 11.6%
  SECONDARY_THRESHOLD_BUY_PRESSURE  buy_pressure > 0.667
  SECONDARY_THRESHOLD_VOLUME_MULT   volume_mult > 1.004
Do not tune these without re-running that research — this module never
reads settings.TIER2_* and never affects them.

Integration point: called once per snapshot write from
workers.shared_snapshot.write_snapshot_and_notify() — the single write
path both DiscoveryWorker and SamplingWorker now share (see that
module's docstring for why there is only one such path). Wrapped in a
try/except at the call site so a bug here can never break a real
snapshot write.

One row per (token, experiment_version) — the FIRST time the primary
signal fires for that token, mirroring a real strategy that only enters
once. An in-memory "already recorded" cache avoids a repeat DB lookup on
every subsequent snapshot for a token that has already fired.

Logging contract
-----------------
info:  momentum_signal.recorded — token_id, symbol, trailing_return_3min, n_rules_cofiring
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config.logging import get_logger
from database.engine import get_session
from models.orm import MomentumSignalEvent, Token, TokenSnapshot

log = get_logger(__name__)

EXPERIMENT_VERSION = "momentum_confluence_v1"

PRIMARY_THRESHOLD_3MIN = Decimal("0.053")
SECONDARY_THRESHOLD_5MIN = Decimal("0.116")
SECONDARY_THRESHOLD_BUY_PRESSURE = Decimal("0.667")
SECONDARY_THRESHOLD_VOLUME_MULT = Decimal("1.004")

# In-memory "already fired" cache, keyed by token_id. Lazily seeded from
# DB on first use per process — avoids a per-snapshot DB round trip for
# every token that has already recorded its one-time signal.
_recorded_token_ids: set = set()
_cache_loaded = False


async def _ensure_cache_loaded(session) -> None:
    global _cache_loaded
    if _cache_loaded:
        return
    result = await session.execute(
        select(MomentumSignalEvent.token_id).where(
            MomentumSignalEvent.experiment_version == EXPERIMENT_VERSION
        )
    )
    _recorded_token_ids.update(result.scalars().all())
    _cache_loaded = True


async def _nearest_at_or_before(session, token_id, cutoff: datetime):
    result = await session.execute(
        select(TokenSnapshot.sampled_at, TokenSnapshot.price_usd)
        .where(TokenSnapshot.token_id == token_id, TokenSnapshot.sampled_at <= cutoff)
        .order_by(TokenSnapshot.sampled_at.desc())
        .limit(1)
    )
    return result.first()


async def maybe_record_signal(token: Token, snap: dict, now: datetime) -> None:
    """
    Called once per snapshot write. Causal only — every value used here
    is at or before `now`, nothing from the future. A no-op for every
    snapshot that isn't a first-time primary-threshold crossing.
    """
    async with get_session() as session:
        await _ensure_cache_loaded(session)
        if token.id in _recorded_token_ids:
            return

        price = snap.get("price_usd")
        if price is None or price <= 0:
            return

        back3 = await _nearest_at_or_before(session, token.id, now - timedelta(minutes=3))
        if back3 is None or back3.price_usd is None or back3.price_usd <= 0:
            return
        trailing_return_3min = price / back3.price_usd - 1

        if trailing_return_3min <= PRIMARY_THRESHOLD_3MIN:
            return

        # Primary signal fired — compute confluence among the 3 other
        # already-validated rules. Purely informational: does not gate
        # whether a row gets recorded, only what gets stored on it.
        back5 = await _nearest_at_or_before(session, token.id, now - timedelta(minutes=5))
        trailing_return_5min = (
            (price / back5.price_usd - 1)
            if back5 is not None and back5.price_usd and back5.price_usd > 0
            else None
        )
        buy_pressure = snap.get("buy_pressure")
        volume_mult = snap.get("volume_mult")

        n_cofiring = 0
        if trailing_return_5min is not None and trailing_return_5min > SECONDARY_THRESHOLD_5MIN:
            n_cofiring += 1
        if buy_pressure is not None and buy_pressure > SECONDARY_THRESHOLD_BUY_PRESSURE:
            n_cofiring += 1
        if volume_mult is not None and volume_mult > SECONDARY_THRESHOLD_VOLUME_MULT:
            n_cofiring += 1

        stmt = pg_insert(MomentumSignalEvent).values(
            token_id=token.id,
            experiment_version=EXPERIMENT_VERSION,
            triggered_at=now,
            trigger_price=price,
            trailing_return_3min=trailing_return_3min,
            trailing_return_5min=trailing_return_5min,
            buy_pressure=buy_pressure,
            volume_mult=volume_mult,
            n_rules_cofiring=n_cofiring,
            liquidity_usd=snap.get("liquidity_usd"),
            market_cap_usd=snap.get("market_cap_usd"),
        ).on_conflict_do_nothing(index_elements=["token_id", "experiment_version"])
        await session.execute(stmt)

    _recorded_token_ids.add(token.id)
    log.info(
        "momentum_signal.recorded",
        token_id=str(token.id),
        symbol=token.symbol,
        trailing_return_3min=float(trailing_return_3min),
        n_rules_cofiring=n_cofiring,
    )
