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

Two pieces of shared state, both mint-keyed:
  1. snapshot_counts — the per-mint sequence number SnapshotEvent carries
     for S1WaveWorker. Must be ONE shared counter: if discovery and
     sampling each kept their own counter, S1WaveWorker could see a
     repeated or wrong snapshot_number depending on which source fired
     more recently.
  2. covered_at — timestamp of the last snapshot written for a mint,
     regardless of source. SamplingWorker checks is_fresh() before
     spending an API call: a mint discovery covered inside
     FRESHNESS_WINDOW_SECONDS is skipped for that sampling cycle.

Single process, single asyncio event loop — plain dicts, no lock needed.

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
from workers.events import SnapshotEvent

log = get_logger(__name__)

# Slightly under discovery's 60s poll interval: if a mint's last coverage
# is older than this, sampling treats it as stale and fetches it itself
# rather than risk a gap if discovery's poll lagged, errored, or the token
# aged out of discovery's 30-minute window.
FRESHNESS_WINDOW_SECONDS = 55

_snapshot_counts: dict[str, int] = {}
_covered_at: dict[str, datetime] = {}


def anchor_count(mint: str, count: int) -> None:
    """Seed the shared counter from DB truth (called once at sampling startup)."""
    _snapshot_counts[mint] = count


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
    _snapshot_counts.pop(mint, None)
    _covered_at.pop(mint, None)


def is_fresh(mint: str, now: datetime) -> bool:
    last = _covered_at.get(mint)
    return last is not None and (now - last).total_seconds() < FRESHNESS_WINDOW_SECONDS


async def write_snapshot_and_notify(
    token: Token,
    snap: dict,
    now: datetime,
    is_observing: bool,
    s1_queue,
) -> int:
    """
    Write a TokenSnapshot row, bump the shared per-mint counter, mark the
    mint as freshly covered, and push a SnapshotEvent to S1Wave (unless
    OBSERVING — same rule sampling_worker always applied: OBSERVING tokens
    were already rejected upstream, so there's no entry decision left to
    make on them). Returns the new snapshot count.
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

    count = _snapshot_counts.get(token.mint_address, 0) + 1
    _snapshot_counts[token.mint_address] = count
    _covered_at[token.mint_address] = now

    # Forward-tracking research experiment (analysis-only, never read by
    # any production decision path) — see workers/momentum_signal.py.
    # Wrapped defensively: a bug in an experimental observation layer
    # must never break a real snapshot write.
    try:
        await momentum_signal.maybe_record_signal(token, snap, now)
    except Exception as exc:
        log.warning("momentum_signal.record_error", mint=token.mint_address, error=str(exc))

    if s1_queue is not None and not is_observing:
        await s1_queue.put(SnapshotEvent(
            mint=token.mint_address,
            symbol=token.symbol,
            snapshot_number=count,
            price_usd=snap["price_usd"],
            buy_pressure=snap["buy_pressure"],
            price_change_1m=snap["price_change_1m"],
            price_change_5m=snap["price_change_5m"],
            liquidity_usd=snap["liquidity_usd"],
            sampled_at=now,
        ))

    return count
