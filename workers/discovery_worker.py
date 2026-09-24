"""
workers/discovery_worker.py
===========================
Discovery worker — SolanaTracker graduated tokens endpoint.

Single source, single call per minute.
GET /tokens/multi/graduated?minCreatedAt=<30min ago>&limit=100&reduceSpam=true

Returns tokens that graduated from pump.fun to Raydium in the last 30 minutes,
with full enrichment baked in (liquidity, price, marketCap, volume, txns,
lpBurn, security authorities, price momentum events).

The 30-minute rolling window means:
  - Tokens graduating during a quiet period are still caught
  - Tokens that started below threshold are re-evaluated as liquidity builds
  - No enrichment worker, no retry loops, no DexScreener

Deduplication: once a token passes pre-filter and enters the filter_queue it
is added to _promoted and never re-queued. Tokens below threshold stay in the
window and are re-evaluated each poll until they hit threshold or age out.

Request budget: 1 call/min = 1,440/day. Free tier = 10,000 → 6.9 days.

Discovery/sampling sync (2026-09-21)
-------------------------------------
This response already contains full snapshot-quality data (price,
liquidity, market cap, buy/sell counts, price_change_1m/5m) for every
returned token — the same shape SamplingWorker writes into TokenSnapshot.
When an already-promoted mint reappears here and is currently WATCHING or
OBSERVING, that data is now used to write a real snapshot (via
workers.shared_snapshot) instead of being discarded, so SamplingWorker can
skip its own API call for that mint on its next 30s tick. See
workers/shared_snapshot.py and sampling_worker.py's own docstring for the
other half of this. Tokens older than this worker's 30-minute lookback
never appear here — sampling still fetches those itself.

Logging contract
----------------
info:
  discovery_worker.started              — config summary
  discovery_worker.token_queued         — passed pre-filter, queued for Tier 1
  discovery_worker.covered_known_token  — known WATCHING/OBSERVING mint got
                                          a free snapshot from this poll
  discovery_worker.poll_complete        — per-cycle: seen, queued,
                                          covered_by_discovery, elapsed_ms
  discovery_worker.poll_error           — HTTP/parse error, retries next cycle
  discovery_worker.rate_limited         — 429 received, backing off
debug:
  discovery_worker.pre_filter    — below threshold, still tracking
  discovery_worker.known_token   — already promoted, seen again this poll
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
from models.orm import Token, TokenStatus
from workers import shared_snapshot
from workers.http_queue import RateLimitedQueue

log = get_logger(__name__)

_ST_BASE        = "https://data.solanatracker.io"
_GRADUATED_PATH = "/tokens/multi/graduated"
_WINDOW_MS      = 30 * 60 * 1000   # 30 minutes look-back
_POLL_INTERVAL  = 60                # seconds
_HTTP_TIMEOUT   = httpx.Timeout(timeout=15.0, connect=5.0)


def _best_pool(token: dict) -> dict | None:
    """Return the pumpfun-amm (Raydium) pool — the one with real liquidity."""
    return next(
        (p for p in token.get("pools", []) if p.get("market") == "pumpfun-amm"),
        None,
    )


def _parse_st_token(token: dict, pool: dict) -> dict:
    """
    Convert a SolanaTracker token response into the normalised dict consumed
    by Tier1Worker and SamplingWorker.

    Replaces everything enrichment_worker used to fetch from DexScreener and
    Helius — all in a single SolanaTracker response.
    """
    events   = token.get("events", {})
    txns     = pool.get("txns", {})
    buys     = txns.get("buys", 0) or 0
    sells    = txns.get("sells", 0) or 0
    total    = buys + sells
    security = pool.get("security", {})

    return {
        # ── Identity ──────────────────────────────────────────────────────
        "mint":   token["token"]["mint"],
        "symbol": token["token"].get("symbol", "?"),
        "name":   token["token"].get("name", ""),

        # ── Liquidity / valuation ─────────────────────────────────────────
        "liquidity_usd":  Decimal(str(pool["liquidity"]["usd"] or 0)),
        "price_usd":      Decimal(str(pool["price"]["usd"] or 0)),
        "market_cap_usd": Decimal(str(pool["marketCap"]["usd"] or 0)),
        "volume_usd":     Decimal(str(txns.get("volume") or 0)),

        # ── Trade activity ────────────────────────────────────────────────
        "buys":         buys,
        "sells":        sells,
        "buy_pressure": Decimal(str(buys / max(total, 1))).quantize(Decimal("0.0001")),

        # ── Security — direct from ST, no Helius call needed ──────────────
        "lp_burn":          pool.get("lpBurn", 0),         # 100 = fully burned
        "mint_authority":   security.get("mintAuthority"), # None = renounced ✓
        "freeze_authority": security.get("freezeAuthority"),# None = renounced ✓

        # ── Price momentum events ─────────────────────────────────────────
        "price_change_1m":  events.get("1m",  {}).get("priceChangePercentage", 0.0),
        "price_change_5m":  events.get("5m",  {}).get("priceChangePercentage", 0.0),
        "price_change_15m": events.get("15m", {}).get("priceChangePercentage", 0.0),
        "price_change_1h":  events.get("1h",  {}).get("priceChangePercentage", 0.0),

        # ── Timing ────────────────────────────────────────────────────────
        "pool_created_at_ms": pool.get("createdAt", 0),
        "source": "solanatracker",
    }


class DiscoveryWorker:

    def __init__(
        self,
        filter_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
        s1_queue: asyncio.Queue | None = None,
    ) -> None:
        self._queue    = filter_queue
        self._shutdown = shutdown_event
        self._s1_queue = s1_queue          # for covering known WATCHING tokens — see _cover_known_tokens
        self._promoted: set[str] = set()   # mints already forwarded downstream

        # Own rate-limited queue and own API key — isolated from sampling's
        # (and s1_wave's) shared get_http_queue() lane. Previously all three
        # shared one queue and SOLANA_TRACKER_API_KEY; a burst from any one
        # of them could exhaust the others' share of a rate window together.
        # Both keys point at the same account for now (SOLANA_TRACKER_API_KEY
        # is a lifetime-capped free-tier quota, not a daily one — see the
        # module docstring), so this doesn't create independent quota today,
        # but the lane separation is real and a genuinely separate key drops
        # in here with no further changes.
        self._http = RateLimitedQueue()

    async def run(self) -> None:
        log.info(
            "discovery_worker.started",
            source="solanatracker",
            poll_interval_s=_POLL_INTERVAL,
            window_min=30,
            min_liquidity_usd=settings.TIER1_MIN_LIQUIDITY_USD,
            min_market_cap_usd=settings.TIER1_MIN_MARKET_CAP_USD,
        )
        self._http.start()

        try:
            async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
                while not self._shutdown.is_set():
                    t0 = time.monotonic()
                    try:
                        await self._poll(client)
                    except Exception as exc:
                        log.warning(
                            "discovery_worker.poll_error",
                            exc_type=type(exc).__name__,
                            error=str(exc),
                        )

                    elapsed = time.monotonic() - t0
                    try:
                        await asyncio.wait_for(
                            self._shutdown.wait(),
                            timeout=max(0.0, _POLL_INTERVAL - elapsed),
                        )
                    except asyncio.TimeoutError:
                        pass
        finally:
            self._http.stop()
            log.info("discovery_worker.stopped")

    async def _poll(self, client: httpx.AsyncClient) -> None:
        t0      = time.monotonic()
        now_ms  = int(time.time() * 1000)
        min_created = now_ms - _WINDOW_MS

        resp = await self._http.submit(
            client.get,
            f"{_ST_BASE}{_GRADUATED_PATH}",
            params={
                "limit":        100,
                "reduceSpam":   "true",
                "minCreatedAt": min_created,
            },
            headers={"x-api-key": settings.SOLANA_TRACKER_API_KEY_DISCOVERY},
        )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "60"))
            log.warning("discovery_worker.rate_limited", retry_after_s=retry_after)
            await asyncio.sleep(retry_after)
            return

        resp.raise_for_status()
        tokens = resp.json()

        queued = 0
        min_liq  = settings.TIER1_MIN_LIQUIDITY_USD
        min_mcap = settings.TIER1_MIN_MARKET_CAP_USD

        # mint -> (raw ST token dict, its pool dict), for mints already
        # promoted that reappeared in this poll. Handed to
        # _cover_known_tokens() after the loop so a WATCHING/OBSERVING
        # token gets a free snapshot from this response instead of the
        # data just being discarded. See workers/shared_snapshot.py.
        known_pool_data: dict[str, tuple[dict, dict]] = {}

        for token in tokens:
            pool = _best_pool(token)
            if not pool:
                continue

            mint = token["token"]["mint"]

            if mint in self._promoted:
                log.debug("discovery_worker.known_token", mint=mint)
                known_pool_data[mint] = (token, pool)
                continue

            liq  = float(pool["liquidity"]["usd"] or 0)
            mcap = float(pool["marketCap"]["usd"] or 0)

            if liq < min_liq or mcap < min_mcap:
                log.debug(
                    "discovery_worker.pre_filter",
                    mint=mint,
                    symbol=token["token"].get("symbol"),
                    liquidity_usd=round(liq, 2),
                    market_cap_usd=round(mcap, 2),
                )
                continue

            parsed = _parse_st_token(token, pool)
            self._promoted.add(mint)
            await self._queue.put(parsed)
            queued += 1

            age_min = (now_ms - pool.get("createdAt", now_ms)) / 60_000
            log.info(
                "discovery_worker.token_queued",
                mint=mint,
                symbol=parsed["symbol"],
                liquidity_usd=f"{liq:.0f}",
                market_cap_usd=f"{mcap:.0f}",
                age_minutes=round(age_min, 1),
                price_change_1m=parsed["price_change_1m"],
                price_change_5m=parsed["price_change_5m"],
                queue_size=self._queue.qsize(),
            )

        covered = await self._cover_known_tokens(known_pool_data) if known_pool_data else 0

        elapsed_ms = round((time.monotonic() - t0) * 1000)
        log.info(
            "discovery_worker.poll_complete",
            seen=len(tokens),
            queued=queued,
            covered_by_discovery=covered,
            promoted_total=len(self._promoted),
            elapsed_ms=elapsed_ms,
        )

    async def _cover_known_tokens(
        self, known_pool_data: dict[str, tuple[dict, dict]],
    ) -> int:
        """
        For already-promoted mints that reappear in this poll: if they're
        currently WATCHING or OBSERVING, write a snapshot straight from
        this response via workers.shared_snapshot instead of discarding
        the data — this is the discovery/sampling request sync. Any other
        status (REJECTED, ENTERED, CLOSED) is skipped; discovery has no
        business writing snapshots for tokens sampling isn't tracking.
        """
        now = datetime.now(timezone.utc)
        async with get_session() as session:
            result = await session.execute(
                select(Token).where(
                    Token.mint_address.in_(known_pool_data.keys()),
                    Token.status.in_((TokenStatus.WATCHING, TokenStatus.OBSERVING)),
                )
            )
            live_tokens = list(result.scalars().all())

        # Mark the whole batch fresh up front, before the per-token write
        # loop below (each iteration awaits its own DB session) — closes
        # the race window where a concurrently-running sampling cycle could
        # see only a partial prefix of this batch as covered. See
        # workers/shared_snapshot.py's mark_covered_batch docstring.
        shared_snapshot.mark_covered_batch([t.mint_address for t in live_tokens], now)

        covered = 0
        for db_token in live_tokens:
            st_token, pool = known_pool_data[db_token.mint_address]
            parsed = _parse_st_token(st_token, pool)
            price_5m = parsed["price_change_5m"]
            snap = {
                "price_usd":       parsed["price_usd"],
                "liquidity_usd":   parsed["liquidity_usd"],
                "market_cap_usd":  parsed["market_cap_usd"],
                "volume_usd":      parsed["volume_usd"],
                "buy_pressure":    parsed["buy_pressure"],
                # Same formula sampling_worker uses: price_change_5m
                # normalised to a 0-2 multiplier for SnapshotWindow vm_p*.
                "volume_mult":     Decimal(str(max(0.0, 1.0 + price_5m / 100))).quantize(Decimal("0.0001")),
                "price_change_1m": parsed["price_change_1m"],
                "price_change_5m": price_5m,
            }
            is_observing = db_token.status == TokenStatus.OBSERVING
            await shared_snapshot.write_snapshot_and_notify(
                db_token, snap, now, is_observing, self._s1_queue,
            )
            covered += 1
            log.info(
                "discovery_worker.covered_known_token",
                mint=db_token.mint_address,
                symbol=db_token.symbol,
                status=db_token.status.value,
            )
        return covered
