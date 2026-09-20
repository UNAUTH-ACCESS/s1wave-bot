"""
workers/trade_monitor_worker.py
================================
Trade monitor — per-second price feed for open positions.

Responsibilities (exactly three):
  1. Load OPEN trades from DB each cycle
  2. Batch-fetch current prices from SolanaTracker /tokens/multi
  3. Emit one MarketEvent per token onto monitor_queue

Nothing else. No risk logic. No exit decisions. No trade context.
The market doesn't know about your positions — and neither does this worker.

Rate limiting
-------------
Uses its OWN RateLimitedQueue instance with its own ST API key,
completely isolated from the discovery/sampling queue. This preserves
the full 1 req/s budget for discovery+sampling and gives the monitor
its own 1 req/s lane.

With 1-3 open trades (MAX_CONCURRENT_TRADES=3), all mints are batched
into a single /tokens/multi call — so each cycle costs exactly 1 request
regardless of trade count. Each trade gets a fresh price every ~1 second.

Logging contract
----------------
info:
  trade_monitor.started / stopped
  trade_monitor.cycle_complete  — open_trades, emitted, elapsed_ms
  trade_monitor.no_open_trades  — nothing to monitor
warning:
  trade_monitor.rate_limited    — 429 from ST (should not happen with queue)
  trade_monitor.token_missing   — mint not in ST response
error:
  trade_monitor.fetch_error
  trade_monitor.cycle_error
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
from models.orm import Trade, TradeStatus
from workers.events import MarketEvent
from workers.http_queue import RateLimitedQueue

log = get_logger(__name__)

_ST_BASE    = "https://data.solanatracker.io"
_MULTI_PATH = "/tokens/multi"
_TIMEOUT    = httpx.Timeout(timeout=10.0, connect=5.0)


def _best_pool(token_data: dict) -> dict | None:
    return next(
        (p for p in token_data.get("pools", []) if p.get("market") == "pumpfun-amm"),
        None,
    )


class TradeMonitorWorker:
    """
    Polls SolanaTracker for current prices on all open positions and
    emits MarketEvents onto monitor_queue.

    Parameters
    ----------
    monitor_queue  : asyncio.Queue[MarketEvent] — consumed by TradeRiskWorker
    shutdown_event : asyncio.Event
    api_key        : dedicated ST API key — isolated from discovery/sampling
    poll_interval  : seconds between cycles (default 1.0)
    """

    def __init__(
        self,
        monitor_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
        api_key: str,
        poll_interval: float = 1.0,
    ) -> None:
        self._queue         = monitor_queue
        self._shutdown      = shutdown_event
        self._api_key       = api_key
        self._poll_interval = poll_interval

        # Own rate-limited queue — completely isolated from discovery/sampling
        self._http = RateLimitedQueue()

    async def run(self) -> None:
        log.info("trade_monitor.started", poll_interval=self._poll_interval)
        self._http.start()

        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                while not self._shutdown.is_set():
                    try:
                        await self._cycle(client)
                    except Exception as exc:
                        log.error("trade_monitor.cycle_error",
                                  error=str(exc), exc_info=True)

                    try:
                        await asyncio.wait_for(
                            self._shutdown.wait(),
                            timeout=self._poll_interval,
                        )
                    except asyncio.TimeoutError:
                        pass
        finally:
            self._http.stop()
            log.info("trade_monitor.stopped")

    async def _cycle(self, client: httpx.AsyncClient) -> None:
        t0 = time.monotonic()

        # Load all open trades — just the mints we need to price
        mints = await self._load_open_mints()
        if not mints:
            log.debug("trade_monitor.no_open_trades")
            return

        # Batch fetch — one request regardless of trade count
        try:
            snapshots = await self._fetch(client, mints)
        except Exception as exc:
            log.error("trade_monitor.fetch_error",
                      error=str(exc), exc_type=type(exc).__name__)
            return

        now     = datetime.now(timezone.utc)
        emitted = 0

        for mint in mints:
            data = snapshots.get(mint)
            if data is None:
                log.warning("trade_monitor.token_missing", mint=mint)
                continue

            event = MarketEvent(
                mint=mint,
                price_usd=data["price_usd"],
                liquidity_usd=data["liquidity_usd"],
                buy_pressure=data["buy_pressure"],
                price_change_1m=data["price_change_1m"],
                price_change_5m=data["price_change_5m"],
                sampled_at=now,
            )
            await self._queue.put(event)
            emitted += 1

        elapsed_ms = round((time.monotonic() - t0) * 1000)
        log.info(
            "trade_monitor.cycle_complete",
            open_trades=len(mints),
            emitted=emitted,
            elapsed_ms=elapsed_ms,
        )

    async def _load_open_mints(self) -> list[str]:
        """Load mint addresses for all currently open trades."""
        async with get_session() as session:
            result = await session.execute(
                select(Trade.token_mint).where(Trade.status == TradeStatus.OPEN)
            )
            return list(result.scalars().all())

    async def _fetch(
        self,
        client: httpx.AsyncClient,
        mints: list[str],
    ) -> dict[str, dict]:
        """
        POST /tokens/multi for the given mints.
        Returns a dict of mint → normalised market snapshot.
        Routed through own RateLimitedQueue at 1 req/s.
        """
        resp = await self._http.submit(
            client.post,
            f"{_ST_BASE}{_MULTI_PATH}",
            json={"tokens": mints},
            headers={
                "x-api-key":    self._api_key,
                "Content-Type": "application/json",
            },
        )

        if resp.status_code == 429:
            log.warning("trade_monitor.rate_limited")
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

            result[mint] = {
                "price_usd":       Decimal(str(pool["price"]["usd"] or 0)),
                "liquidity_usd":   Decimal(str(pool["liquidity"]["usd"] or 0)),
                "buy_pressure":    Decimal(str(buys / max(total, 1))).quantize(Decimal("0.0001")),
                "price_change_1m": events.get("1m",  {}).get("priceChangePercentage", 0.0) or 0.0,
                "price_change_5m": events.get("5m",  {}).get("priceChangePercentage", 0.0) or 0.0,
            }

        return result
