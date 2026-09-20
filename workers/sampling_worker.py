"""
workers/sampling_worker.py
==========================
Sampling worker — SolanaTracker POST /tokens/multi

Fires every SAMPLE_INTERVAL_SECONDS (default 60s). For each WATCHING token,
fetches a fresh snapshot from SolanaTracker via a single batch call, writes
a TokenSnapshot row, and signals the scoring worker.

Price momentum signals from ST events replace the old volume_mult/baseline
approach. The scorer still uses the same SnapshotWindow structure but now
vm_p1/p2/p3 carry price_change_5m values across the rolling window, which
is a more direct momentum signal than volume relative to an arbitrary baseline.

Request budget: 1 call per sampling cycle regardless of token count (batch).

Logging contract
----------------
info:
  sampling_worker.started / stopped
  sampling_worker.cycle_complete — watched, sampled, errors, elapsed_ms
  sampling_worker.snapshot       — per-token: symbol, price, liquidity,
                                   buy_pressure, price_change_1m/5m
  sampling_worker.token_gone     — mint no longer in ST response
  sampling_worker.aged_out       — exceeded TIER1_MAX_AGE_MINUTES
debug:
  sampling_worker.no_watching_tokens
errors:
  sampling_worker.fetch_error
  sampling_worker.cycle_error
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal

import httpx
from sqlalchemy import select

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from models.orm import Token, TokenSnapshot, TokenStatus
from workers.events import SnapshotEvent
from workers.http_queue import get_http_queue

log = get_logger(__name__)

_ST_BASE      = "https://data.solanatracker.io"
_MULTI_PATH   = "/tokens/multi"
_HTTP_TIMEOUT = httpx.Timeout(timeout=15.0, connect=5.0)


def _best_pool(token_data: dict) -> dict | None:
    return next(
        (p for p in token_data.get("pools", []) if p.get("market") == "pumpfun-amm"),
        None,
    )


class SamplingWorker:

    def __init__(
        self,
        scoring_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
        s1_queue: asyncio.Queue | None = None,
    ) -> None:
        self._scoring_queue = scoring_queue
        self._shutdown      = shutdown_event
        self._s1_queue      = s1_queue
        self._interval      = settings.SAMPLE_INTERVAL_SECONDS
        # snapshot_number per mint — anchored to DB on startup, incremented in memory.
        # Re-anchoring on restart ensures S1WaveWorker always sees the true
        # snapshot number regardless of how many times the bot has restarted.
        self._snapshot_counts: dict[str, int] = {}

    async def run(self) -> None:
        log.info("sampling_worker.started", interval=self._interval)

        # Anchor snapshot counts to DB before first cycle.
        # Any token already in WATCHING state with existing snapshots gets
        # its real count here — prevents S1 from firing on snapshot N as if
        # it were snapshot 1 after a restart.
        await self._load_snapshot_counts_from_db()

        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            while not self._shutdown.is_set():
                try:
                    await self._sample_cycle(client)
                except Exception as exc:
                    log.error("sampling_worker.cycle_error",
                              error=str(exc), exc_info=True)

                await self._scoring_queue.put("CYCLE_COMPLETE")

                try:
                    await asyncio.wait_for(
                        self._shutdown.wait(),
                        timeout=float(self._interval),
                    )
                except asyncio.TimeoutError:
                    pass

        log.info("sampling_worker.stopped")

    async def _load_snapshot_counts_from_db(self) -> None:
        """
        Load real snapshot counts for all current WATCHING tokens from DB.
        Called once at startup. Anchors the in-memory counter to DB truth
        so a restart never causes S1WaveWorker to misfire on an old token.
        """
        from sqlalchemy import func as sa_func
        async with get_session() as session:
            # Get all WATCHING tokens with their snapshot counts
            result = await session.execute(
                select(
                    Token.mint_address,
                    sa_func.count(TokenSnapshot.id).label("snap_count"),
                )
                .join(TokenSnapshot, TokenSnapshot.token_id == Token.id, isouter=True)
                .where(Token.status == TokenStatus.WATCHING)
                .group_by(Token.mint_address)
            )
            rows = result.all()

        for mint, count in rows:
            self._snapshot_counts[mint] = count

        log.info(
            "sampling_worker.counts_anchored",
            tokens=len(rows),
            detail={mint: count for mint, count in rows} if rows else {},
        )

    async def _sample_cycle(self, client: httpx.AsyncClient) -> None:
        watching = await self._load_watching_tokens()
        if not watching:
            log.debug("sampling_worker.no_watching_tokens")
            return

        t0 = time.monotonic()

        # Single batch call for all watched tokens
        mints = [t.mint_address for t in watching]
        try:
            snapshots = await self._fetch_batch(client, mints)
        except Exception as exc:
            log.error("sampling_worker.fetch_error",
                      error=str(exc), exc_type=type(exc).__name__)
            return

        sampled = errors = 0
        now     = datetime.now(timezone.utc)

        for token in watching:
            is_observing = token.status == TokenStatus.OBSERVING

            # Age-out check — OBSERVING tokens get a short, bounded window
            # (OBSERVE_WINDOW_SECONDS) from discovery; WATCHING tokens keep
            # the existing long TIER1_MAX_AGE_MINUTES cutoff from when they
            # started being watched. Different clocks, different meanings:
            # one is "gave up on being tradeable a while ago", the other is
            # "control-group sample window closed".
            if is_observing:
                age_sec = (now - token.discovered_at).total_seconds()
                if age_sec > settings.OBSERVE_WINDOW_SECONDS:
                    self._clear_snapshot_count(token.mint_address)
                    await self._age_out(token.mint_address, "OBSERVE_WINDOW_ELAPSED")
                    continue
            elif token.watch_started_at:
                age_min = (now - token.watch_started_at).total_seconds() / 60
                if age_min > settings.TIER1_MAX_AGE_MINUTES:
                    self._clear_snapshot_count(token.mint_address)
                    await self._age_out(token.mint_address, "WATCHING_TIMEOUT")
                    continue

            snap_data = snapshots.get(token.mint_address)
            if snap_data is None:
                # "Gone from the API" means something different depending on
                # which group the token was in — a WATCHING token vanishing
                # is a strong rug signal; an OBSERVING token vanishing is
                # just confirmation the reject was correct. Keep them apart
                # so the derived-outcome analysis isn't looking at a blended
                # bucket of two different events.
                reason = "TOKEN_GONE_DURING_OBSERVATION" if is_observing else "TOKEN_GONE_LIKELY_RUG"
                log.info("sampling_worker.token_gone",
                         mint=token.mint_address, symbol=token.symbol,
                         status=token.status.value, reason=reason)
                self._clear_snapshot_count(token.mint_address)
                await self._age_out(token.mint_address, reason)
                errors += 1
                continue

            try:
                await self._write_snapshot(token, snap_data, now)
                sampled += 1

                # Track snapshot count per token
                count = self._snapshot_counts.get(token.mint_address, 0) + 1
                self._snapshot_counts[token.mint_address] = count

                log.info(
                    "sampling_worker.snapshot",
                    mint=token.mint_address,
                    symbol=token.symbol,
                    price_usd=str(snap_data["price_usd"]),
                    liquidity_usd=str(snap_data["liquidity_usd"]),
                    buy_pressure=str(snap_data["buy_pressure"]),
                    price_change_1m=snap_data["price_change_1m"],
                    price_change_5m=snap_data["price_change_5m"],
                )

                # Publish SnapshotEvent for S1WaveWorker — OBSERVING tokens
                # were already rejected at Tier1, so there's no entry
                # decision left to make on them here. They're still worth
                # sampling (control-group data for the derived-outcome
                # analysis), just not worth running the S1 signal on.
                if self._s1_queue is not None and not is_observing:
                    await self._s1_queue.put(SnapshotEvent(
                        mint=token.mint_address,
                        symbol=token.symbol,
                        snapshot_number=count,
                        price_usd=snap_data["price_usd"],
                        buy_pressure=snap_data["buy_pressure"],
                        price_change_1m=snap_data["price_change_1m"],
                        price_change_5m=snap_data["price_change_5m"],
                        liquidity_usd=snap_data["liquidity_usd"],
                        sampled_at=now,
                    ))
            except Exception as exc:
                log.error("sampling_worker.snapshot_error",
                          mint=token.mint_address, error=str(exc))
                errors += 1

        elapsed_ms = round((time.monotonic() - t0) * 1000)
        log.info(
            "sampling_worker.cycle_complete",
            watched=len(watching),
            sampled=sampled,
            errors=errors,
            elapsed_ms=elapsed_ms,
        )

    async def _fetch_batch(
        self,
        client: httpx.AsyncClient,
        mints: list[str],
    ) -> dict[str, dict]:
        """POST /tokens/multi → parse each into a normalised snapshot dict.
        Routed through the central rate-limited queue to prevent 429s when
        discovery and sampling workers fire simultaneously.
        """
        resp = await get_http_queue().submit(
            client.post,
            f"{_ST_BASE}{_MULTI_PATH}",
            json={"tokens": mints},
            headers={
                "x-api-key":    settings.SOLANA_TRACKER_API_KEY,
                "Content-Type": "application/json",
            },
        )
        if resp.status_code == 429:
            log.warning("sampling_worker.rate_limited")
            return {}
        resp.raise_for_status()
        data = resp.json()

        result: dict[str, dict] = {}
        for mint, token_data in data.get("tokens", {}).items():
            pool = _best_pool(token_data)
            if not pool:
                continue
            events = token_data.get("events", {})
            txns   = pool.get("txns", {})
            buys   = txns.get("buys", 0) or 0
            sells  = txns.get("sells", 0) or 0
            total  = buys + sells

            # price_change_5m drives vm_trend in the scorer
            price_5m = events.get("5m", {}).get("priceChangePercentage", 0.0) or 0.0

            result[mint] = {
                "price_usd":       Decimal(str(pool["price"]["usd"] or 0)),
                "liquidity_usd":   Decimal(str(pool["liquidity"]["usd"] or 0)),
                "market_cap_usd":  Decimal(str(pool["marketCap"]["usd"] or 0)),
                "volume_usd":      Decimal(str(txns.get("volume") or 0)),
                "buy_pressure":    Decimal(str(buys / max(total, 1))).quantize(Decimal("0.0001")),
                # price_change_5m normalised to 0–2 multiplier range for SnapshotWindow vm_p*
                # +100% → vm = 2.0, 0% → vm = 1.0, -50% → vm = 0.5
                "volume_mult":     Decimal(str(max(0.0, 1.0 + price_5m / 100))).quantize(Decimal("0.0001")),
                "price_change_1m": events.get("1m",  {}).get("priceChangePercentage", 0.0) or 0.0,
                "price_change_5m": price_5m,
                "price_change_15m":events.get("15m", {}).get("priceChangePercentage", 0.0) or 0.0,
            }
        return result

    async def _write_snapshot(
        self,
        token: Token,
        snap: dict,
        now: datetime,
    ) -> None:
        volume_usd    = snap["volume_usd"]
        liquidity_usd = snap["liquidity_usd"]
        vlr = (
            (volume_usd / liquidity_usd).quantize(Decimal("0.000001"))
            if liquidity_usd > Decimal("0")
            else Decimal("0")
        )

        async with get_session() as session:
            # Set baseline_volume_usd on first real snapshot
            result = await session.execute(
                select(Token)
                .where(Token.mint_address == token.mint_address)
                .with_for_update()
            )
            db_token = result.scalar_one_or_none()
            if db_token and (
                db_token.baseline_volume_usd is None
                or db_token.baseline_volume_usd == Decimal("0")
            ) and volume_usd > Decimal("0"):
                db_token.baseline_volume_usd = volume_usd

            snapshot = TokenSnapshot(
                token_id=token.id,
                sampled_at=now,
                price_usd=snap["price_usd"],
                liquidity_usd=liquidity_usd,
                market_cap_usd=snap["market_cap_usd"],
                volume_usd=volume_usd,
                buy_pressure=snap["buy_pressure"],
                # volume_mult carries price_change_5m as a multiplier:
                # drives vm_trend in the rolling window scorer
                volume_mult=snap["volume_mult"],
            )
            session.add(snapshot)

    async def _load_watching_tokens(self) -> list[Token]:
        # WATCHING (tradeable candidates) + OBSERVING (Tier1-rejected, still
        # sampled for a bounded window as the control group) both ride the
        # same batch call — this is the whole point of the design: the
        # extra observation doesn't cost a second API call. ENTERED tokens
        # are deliberately excluded: they're monitored by price_tick_worker
        # via WebSocket, and including them here caused zombie loops when a
        # token was stuck in ENTERED with no open trade (fired aged_out
        # 2,060×/session before this exclusion existed).
        async with get_session() as session:
            result = await session.execute(
                select(Token).where(
                    Token.status.in_((TokenStatus.WATCHING, TokenStatus.OBSERVING))
                )
            )
            return list(result.scalars().all())

    def _clear_snapshot_count(self, mint: str) -> None:
        """Remove snapshot counter when token leaves the watch queue."""
        self._snapshot_counts.pop(mint, None)

    async def _age_out(self, mint: str, reason: str) -> None:
        async with get_session() as session:
            result = await session.execute(
                select(Token).where(Token.mint_address == mint).with_for_update()
            )
            token = result.scalar_one_or_none()
            # Only age out WATCHING/OBSERVING, not ENTERED — an ENTERED
            # token has an open trade and must never be silently rejected.
            if token and token.status in (TokenStatus.WATCHING, TokenStatus.OBSERVING):
                token.status = TokenStatus.REJECTED
                token.rejection_reason = reason
        log.info("sampling_worker.aged_out", mint=mint, reason=reason)
