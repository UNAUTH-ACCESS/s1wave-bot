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

Logging contract
----------------
info:
  discovery_worker.started       — config summary
  discovery_worker.token_queued  — passed pre-filter, queued for Tier 1
  discovery_worker.poll_complete — per-cycle: seen, queued, elapsed_ms
  discovery_worker.poll_error    — HTTP/parse error, retries next cycle
  discovery_worker.rate_limited  — 429 received, backing off
debug:
  discovery_worker.pre_filter    — below threshold, still tracking
  discovery_worker.known_token   — already promoted, skipped
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import httpx

from config.logging import get_logger
from config.settings import settings
from workers.http_queue import get_http_queue

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
    ) -> None:
        self._queue    = filter_queue
        self._shutdown = shutdown_event
        self._promoted: set[str] = set()   # mints already forwarded downstream

    async def run(self) -> None:
        log.info(
            "discovery_worker.started",
            source="solanatracker",
            poll_interval_s=_POLL_INTERVAL,
            window_min=30,
            min_liquidity_usd=settings.TIER1_MIN_LIQUIDITY_USD,
            min_market_cap_usd=settings.TIER1_MIN_MARKET_CAP_USD,
        )

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

        log.info("discovery_worker.stopped")

    async def _poll(self, client: httpx.AsyncClient) -> None:
        t0      = time.monotonic()
        now_ms  = int(time.time() * 1000)
        min_created = now_ms - _WINDOW_MS

        resp = await get_http_queue().submit(
            client.get,
            f"{_ST_BASE}{_GRADUATED_PATH}",
            params={
                "limit":        100,
                "reduceSpam":   "true",
                "minCreatedAt": min_created,
            },
            headers={"x-api-key": settings.SOLANA_TRACKER_API_KEY},
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

        for token in tokens:
            pool = _best_pool(token)
            if not pool:
                continue

            mint = token["token"]["mint"]

            if mint in self._promoted:
                log.debug("discovery_worker.known_token", mint=mint)
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

        elapsed_ms = round((time.monotonic() - t0) * 1000)
        log.info(
            "discovery_worker.poll_complete",
            seen=len(tokens),
            queued=queued,
            promoted_total=len(self._promoted),
            elapsed_ms=elapsed_ms,
        )
