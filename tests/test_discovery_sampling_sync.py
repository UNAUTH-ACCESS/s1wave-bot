"""
tests/test_discovery_sampling_sync.py
======================================
Discovery/sampling request sync (2026-09-21).

Coverage:
  1. workers.shared_snapshot.is_fresh() — the time-window logic sampling
     relies on to decide whether to skip its own API call for a mint.
  2. write_snapshot_and_notify() bumps the ONE shared counter and marks a
     mint fresh, and correctly withholds the SnapshotEvent for OBSERVING
     tokens (same rule sampling_worker always applied).
  3. DiscoveryWorker only writes a snapshot for an already-promoted mint
     that reappears in its poll if that mint is currently WATCHING or
     OBSERVING — never for REJECTED/ENTERED/CLOSED.
  4. SamplingWorker skips fetching (and never calls the API for) a mint
     shared_snapshot says is fresh, while still running age-out for it.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from models.orm import Token, TokenSnapshot, TokenStatus
from workers import shared_snapshot


def make_token(**overrides) -> Token:
    now = datetime.now(timezone.utc)
    defaults = dict(
        mint_address=f"Mint{uuid.uuid4().hex[:36]}",
        symbol="TEST",
        status=TokenStatus.WATCHING,
        discovered_at=now - timedelta(minutes=5),
        watch_started_at=now - timedelta(minutes=5),
    )
    defaults.update(overrides)
    return Token(**defaults)


def make_snap(**overrides) -> dict:
    defaults = dict(
        price_usd=Decimal("0.001"),
        liquidity_usd=Decimal("20000"),
        market_cap_usd=Decimal("100000"),
        volume_usd=Decimal("5000"),
        buy_pressure=Decimal("0.6"),
        volume_mult=Decimal("1.1"),
        price_change_1m=1.0,
        price_change_5m=5.0,
    )
    defaults.update(overrides)
    return defaults


@pytest.fixture(autouse=True)
def _clean_shared_state():
    """shared_snapshot's dicts are module-level — isolate each test."""
    shared_snapshot._snapshot_counts.clear()
    shared_snapshot._covered_at.clear()
    yield
    shared_snapshot._snapshot_counts.clear()
    shared_snapshot._covered_at.clear()


# ── 1: freshness window ────────────────────────────────────────────────────

class TestIsFresh:

    def test_never_covered_is_not_fresh(self):
        assert shared_snapshot.is_fresh("MintNever", datetime.now(timezone.utc)) is False

    def test_covered_seconds_ago_is_fresh(self):
        mint = "MintA"
        t0 = datetime.now(timezone.utc)
        shared_snapshot._covered_at[mint] = t0
        assert shared_snapshot.is_fresh(mint, t0 + timedelta(seconds=30)) is True

    def test_covered_past_window_is_stale(self):
        mint = "MintB"
        t0 = datetime.now(timezone.utc)
        shared_snapshot._covered_at[mint] = t0
        now = t0 + timedelta(seconds=shared_snapshot.FRESHNESS_WINDOW_SECONDS + 1)
        assert shared_snapshot.is_fresh(mint, now) is False

    def test_forget_clears_freshness_and_count(self):
        mint = "MintC"
        shared_snapshot._covered_at[mint] = datetime.now(timezone.utc)
        shared_snapshot._snapshot_counts[mint] = 3
        shared_snapshot.forget(mint)
        assert mint not in shared_snapshot._covered_at
        assert mint not in shared_snapshot._snapshot_counts

    def test_mark_covered_batch_marks_all_mints_fresh_atomically(self):
        """The race-condition fix (2026-09-21): a batch of mints must all
        become fresh in one synchronous call, not incrementally — so a
        concurrently-running sampling cycle can never observe only a
        partial prefix of the batch as covered."""
        mints = ["MintX", "MintY", "MintZ"]
        now = datetime.now(timezone.utc)
        shared_snapshot.mark_covered_batch(mints, now)
        assert all(shared_snapshot.is_fresh(m, now) for m in mints)


# ── 2: write_snapshot_and_notify ────────────────────────────────────────────

class TestWriteSnapshotAndNotify:

    @pytest.mark.asyncio
    async def test_increments_shared_counter_and_marks_fresh(self, session):
        token = make_token()
        session.add(token)
        await session.flush()

        s1_queue = asyncio.Queue()
        now = datetime.now(timezone.utc)

        with patch("workers.shared_snapshot.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            count = await shared_snapshot.write_snapshot_and_notify(
                token, make_snap(), now, is_observing=False, s1_queue=s1_queue,
            )

        assert count == 1
        assert shared_snapshot.is_fresh(token.mint_address, now) is True
        assert s1_queue.qsize() == 1

        result = await session.execute(
            select(TokenSnapshot).where(TokenSnapshot.token_id == token.id)
        )
        assert len(result.scalars().all()) == 1

    @pytest.mark.asyncio
    async def test_observing_token_gets_no_s1_event(self, session):
        token = make_token(status=TokenStatus.OBSERVING)
        session.add(token)
        await session.flush()

        s1_queue = asyncio.Queue()
        with patch("workers.shared_snapshot.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            await shared_snapshot.write_snapshot_and_notify(
                token, make_snap(), datetime.now(timezone.utc),
                is_observing=True, s1_queue=s1_queue,
            )

        assert s1_queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_counter_continues_across_calls_regardless_of_caller(self, session):
        """The whole point of a SHARED counter: two calls for the same mint
        (as if one came from discovery, one from sampling) must produce
        sequential numbers, not both starting at 1."""
        token = make_token()
        session.add(token)
        await session.flush()

        with patch("workers.shared_snapshot.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            c1 = await shared_snapshot.write_snapshot_and_notify(
                token, make_snap(), datetime.now(timezone.utc), False, None,
            )
            c2 = await shared_snapshot.write_snapshot_and_notify(
                token, make_snap(), datetime.now(timezone.utc), False, None,
            )

        assert (c1, c2) == (1, 2)


# ── 3: DiscoveryWorker only covers WATCHING/OBSERVING known mints ─────────

class TestDiscoveryCoversKnownTokens:

    @pytest.mark.asyncio
    async def test_watching_known_mint_gets_covered(self, session):
        from workers.discovery_worker import DiscoveryWorker

        token = make_token(status=TokenStatus.WATCHING)
        session.add(token)
        await session.flush()

        worker = DiscoveryWorker(asyncio.Queue(), asyncio.Event())
        known_pool_data = {
            token.mint_address: (
                {"token": {"mint": token.mint_address, "symbol": "TEST", "name": "Test"},
                 "events": {"5m": {"priceChangePercentage": 10.0}}},
                {"liquidity": {"usd": 30000}, "price": {"usd": 0.002},
                 "marketCap": {"usd": 200000}, "txns": {"buys": 10, "sells": 2, "volume": 5000},
                 "lpBurn": 100, "security": {}, "createdAt": 0},
            )
        }

        with patch("workers.discovery_worker.get_session") as mock_gs, \
             patch("workers.shared_snapshot.get_session") as mock_gs2:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_gs2.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs2.return_value.__aexit__ = AsyncMock(return_value=False)
            covered = await worker._cover_known_tokens(known_pool_data)

        assert covered == 1
        result = await session.execute(
            select(TokenSnapshot).where(TokenSnapshot.token_id == token.id)
        )
        assert len(result.scalars().all()) == 1

    @pytest.mark.asyncio
    async def test_rejected_known_mint_is_not_covered(self, session):
        from workers.discovery_worker import DiscoveryWorker

        token = make_token(status=TokenStatus.REJECTED, rejection_reason="WASH_TRADING")
        session.add(token)
        await session.flush()

        worker = DiscoveryWorker(asyncio.Queue(), asyncio.Event())
        known_pool_data = {
            token.mint_address: (
                {"token": {"mint": token.mint_address, "symbol": "TEST", "name": "Test"},
                 "events": {}},
                {"liquidity": {"usd": 30000}, "price": {"usd": 0.002},
                 "marketCap": {"usd": 200000}, "txns": {"buys": 1, "sells": 1, "volume": 100},
                 "lpBurn": 100, "security": {}, "createdAt": 0},
            )
        }

        with patch("workers.discovery_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            covered = await worker._cover_known_tokens(known_pool_data)

        assert covered == 0
        result = await session.execute(
            select(TokenSnapshot).where(TokenSnapshot.token_id == token.id)
        )
        assert result.scalars().all() == []

    @pytest.mark.asyncio
    async def test_all_mints_fresh_before_any_individual_write_completes(self, session):
        """Regression test for the 2026-09-21 race: a sampling cycle that
        runs mid-way through _cover_known_tokens's per-token write loop
        must see ALL of this poll's mints as fresh, not just the ones
        whose individual write has already landed."""
        from workers.discovery_worker import DiscoveryWorker

        tokens = [make_token(status=TokenStatus.WATCHING) for _ in range(3)]
        session.add_all(tokens)
        await session.flush()

        worker = DiscoveryWorker(asyncio.Queue(), asyncio.Event())
        known_pool_data = {
            t.mint_address: (
                {"token": {"mint": t.mint_address, "symbol": "TEST", "name": "Test"},
                 "events": {}},
                {"liquidity": {"usd": 30000}, "price": {"usd": 0.002},
                 "marketCap": {"usd": 200000}, "txns": {"buys": 1, "sells": 1, "volume": 100},
                 "lpBurn": 100, "security": {}, "createdAt": 0},
            ) for t in tokens
        }

        seen_fresh_counts = []
        real_write = shared_snapshot.write_snapshot_and_notify

        async def spying_write(db_token, snap, now, is_observing, s1_queue):
            # Snapshot how many of the 3 mints are fresh RIGHT NOW, before
            # this particular mint's own write has even started — a
            # concurrently-running sampling tick would see this same view.
            seen_fresh_counts.append(
                sum(shared_snapshot.is_fresh(t.mint_address, now) for t in tokens)
            )
            return await real_write(db_token, snap, now, is_observing, s1_queue)

        with patch("workers.discovery_worker.get_session") as mock_gs, \
             patch("workers.shared_snapshot.get_session") as mock_gs2, \
             patch("workers.shared_snapshot.write_snapshot_and_notify", side_effect=spying_write):
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_gs2.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs2.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._cover_known_tokens(known_pool_data)

        # Before the fix, the Nth write would see only N-1 mints fresh
        # (itself not yet marked). With the batch pre-mark, every write
        # sees all 3 fresh from the very first iteration.
        assert seen_fresh_counts == [3, 3, 3]


# ── 4: SamplingWorker skips a mint discovery already covered ──────────────

class TestSamplingSkipsFreshMints:

    @pytest.mark.asyncio
    async def test_fresh_mint_is_not_fetched_but_still_ages_out(self, session):
        from workers.sampling_worker import SamplingWorker

        fresh_token = make_token(
            watch_started_at=datetime.now(timezone.utc) - timedelta(minutes=99999),
        )
        stale_token = make_token()
        session.add_all([fresh_token, stale_token])
        await session.flush()

        # discovery already covered fresh_token moments ago
        shared_snapshot._covered_at[fresh_token.mint_address] = datetime.now(timezone.utc)

        worker = SamplingWorker(asyncio.Queue(), asyncio.Event())
        worker._fetch_batch = AsyncMock(side_effect=lambda client, chunk: {
            m: {
                "price_usd": Decimal("0.001"), "liquidity_usd": Decimal("20000"),
                "market_cap_usd": Decimal("100000"), "volume_usd": Decimal("1000"),
                "buy_pressure": Decimal("0.5"), "volume_mult": Decimal("1.0"),
                "price_change_1m": 0.0, "price_change_5m": 0.0,
            } for m in chunk
        })

        with patch("workers.sampling_worker.get_session") as mock_gs, \
             patch("workers.shared_snapshot.get_session") as mock_gs2:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_gs2.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs2.return_value.__aexit__ = AsyncMock(return_value=False)
            await worker._sample_cycle(client=None)

        # fresh_token: never in a fetch call, aged out via TIER1_MAX_AGE_MINUTES
        fetched_mints = {m for call in worker._fetch_batch.call_args_list for m in call.args[1]}
        assert fresh_token.mint_address not in fetched_mints
        assert stale_token.mint_address in fetched_mints

        await session.refresh(fresh_token)
        assert fresh_token.status == TokenStatus.REJECTED
        assert fresh_token.rejection_reason == "WATCHING_TIMEOUT"
