"""
workers/shared_snapshot.py
===========================
Shared snapshot-writing path used by both DiscoveryWorker and
SamplingWorker, so a WATCHING/OBSERVING token gets exactly one snapshot
sequence regardless of which poll actually supplied the data.

Why this exists
----------------
DiscoveryWorker's GET /tokens/multi/graduated response already carries
full snapshot-quality data (price, liquidity, market cap, buy/sell counts,
price_change_1m/5m) for any token still inside its 30-minute lookback
window — the same shape SamplingWorker's POST /tokens/multi response
carries. Previously, if a token discovery already knew about (self._promoted)
reappeared in a later poll, that data was just logged and discarded
(discovery_worker.known_token) — SamplingWorker would independently spend
its own API call fetching the same token 30 seconds later. This module
lets discovery's poll double as a snapshot source for young WATCHING/
OBSERVING tokens, and lets sampling skip its own fetch for anything
discovery already covered recently.

Shared state, mint-keyed: covered_at — timestamp of the last snapshot
written for a mint, regardless of source. SamplingWorker checks is_fresh()
before spending an API call: a mint discovery covered inside
FRESHNESS_WINDOW_SECONDS is skipped for that sampling cycle.

Single process, single asyncio event loop — a plain dict, no lock needed.

(A second piece of shared state used to live here too — a per-mint
snapshot counter feeding a SnapshotEvent queue for "S1WaveWorker," a
scoring engine removed on 2026-09-23 along with the rest of that
scorer/S1Wave pipeline. Nothing wired a queue into DiscoveryWorker or
SamplingWorker after that removal, so the push was silent dead code —
removed here on 2026-09-29 rather than left as unreachable scaffolding.)

The trade-off this accepts (explicitly, per user confirmation): a young
token (<30min old) that discovery covers gets its price refreshed on
discovery's 60s cadence, not sampling's faster 30s cadence, for however
many cycles discovery keeps covering it. Sampling still polls every 30s
and independently fetches anything discovery didn't just cover (including
as a fallback if discovery's poll lags or errors) — this module only
removes the DUPLICATE fetch, it never removes coverage entirely.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import select

from config.logging import get_logger
from database.engine import get_session
from models.orm import Token, TokenSnapshot
from workers import momentum_signal

log = get_logger(__name__)

# Slightly under discovery's 60s poll interval: if a mint's last coverage
# is older than this, sampling treats it as stale and fetches it itself
# rather than risk a gap if discovery's poll lagged, errored, or the token
# aged out of discovery's 30-minute window.
FRESHNESS_WINDOW_SECONDS = 55

_covered_at: dict[str, datetime] = {}


def mark_covered_batch(mints: list[str], now: datetime) -> None:
    """
    Mark every mint in this poll's known-token batch as covered, all at
    once, synchronously, BEFORE any of the actual snapshot writes happen.

    Why: DiscoveryWorker writes one snapshot per known mint in a loop with
    an `await get_session()` per iteration — a sampling tick that happens
    to run concurrently with that loop would otherwise see only whichever
    prefix of the batch had already completed by the instant it checked
    is_fresh(), and would then redundantly re-fetch the rest itself
    (confirmed live on 2026-09-21: a sampling cycle's freshness snapshot
    landed after only 1 of 9 covered mints had actually been written).
    Marking the whole batch fresh up front — a plain dict loop, no
    awaits — closes that window to effectively zero. write_snapshot_and_notify
    still updates _covered_at per-mint when the real write lands; that's a
    harmless timestamp refresh, not a second source of truth.
    """
    for mint in mints:
        _covered_at[mint] = now


def forget(mint: str) -> None:
    """Called when a token leaves WATCHING/OBSERVING (aged out, entered, rejected)
    so stale state can't linger."""
    _covered_at.pop(mint, None)


def is_fresh(mint: str, now: datetime) -> bool:
    last = _covered_at.get(mint)
    return last is not None and (now - last).total_seconds() < FRESHNESS_WINDOW_SECONDS


async def write_snapshot_and_notify(
    token: Token,
    snap: dict,
    now: datetime,
) -> None:
    """
    Write a TokenSnapshot row, mark the mint as freshly covered, and hand
    the same snapshot to momentum_signal.maybe_record_signal() — THE real
    production entry trigger (2026-09-29 correction to the comment this
    function used to carry): when >=2 of the 4 confluence rules co-fire,
    that call writes a MomentumSignalEvent row, and confluence_live_worker
    polls exactly those rows (n_rules_cofiring >= MIN_RULES_COFIRING) to
    decide real trade entries — confluence_shadow_worker's paper-trade
    benchmark reads the identical events. This is not an analysis-only
    side channel; every real trade traces back to a snapshot written here.
    """
    volume_usd = snap["volume_usd"]
    liquidity_usd = snap["liquidity_usd"]

    async with get_session() as session:
        result = await session.execute(
            select(Token).where(Token.mint_address == token.mint_address).with_for_update()
        )
        db_token = result.scalar_one_or_none()
        if db_token and (
            db_token.baseline_volume_usd is None
            or db_token.baseline_volume_usd == Decimal("0")
        ) and volume_usd > Decimal("0"):
            db_token.baseline_volume_usd = volume_usd

        session.add(TokenSnapshot(
            token_id=token.id,
            sampled_at=now,
            price_usd=snap["price_usd"],
            liquidity_usd=liquidity_usd,
            market_cap_usd=snap["market_cap_usd"],
            volume_usd=volume_usd,
            buy_pressure=snap["buy_pressure"],
            volume_mult=snap["volume_mult"],
        ))

    _covered_at[token.mint_address] = now

    # Wrapped defensively: a bug in signal recording must never break a
    # real snapshot write — the snapshot itself (already committed above)
    # is the thing every other worker depends on existing.
    try:
        await momentum_signal.maybe_record_signal(token, snap, now)
    except Exception as exc:
        log.warning("momentum_signal.record_error", mint=token.mint_address, error=str(exc))
