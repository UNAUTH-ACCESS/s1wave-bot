"""
main.py
=======
SolanaBot v3 process entry point.

Architecture, post-2026-09-23 trim
-----------------------------------
The old scorer/S1Wave/CapitalEngine paper-trading pipeline (ScoringWorker,
CapitalEngine, RiskEngine, S1WaveWorker, TradeMonitorWorker,
TradeRiskWorker, engine/rolling_window.py) was removed entirely — it was
the strategy the shadow_trades research found catastrophic (-91.5%
median). Its old data (trades, shadow_trades, trade_price_observations)
is left in the database untouched as a historical record; nothing writes
to those tables anymore. What remains is the one validated, real branch:

  DiscoveryWorker  — polls ST /tokens/multi/graduated every 60s
                     → fully-enriched token dicts → filter_queue
  Tier1Worker      — hard gate (liquidity, mcap, age, lp_burn,
                     authorities, wash guard) → sampling_queue
  SamplingWorker   — ST POST /tokens/multi batch every 60s
                     → TokenSnapshot rows (also feeds momentum_signal.py
                     via shared_snapshot.write_snapshot_and_notify)
  ConfluenceShadowWorker — paper-only, real-time-monitored validation of
                     the confluence_entry_v1 signal (kept running
                     alongside the live worker as an ongoing benchmark)
  ConfluenceLiveWorker   — real, on-chain execution of confluence_entry_v1
                     via its own dedicated wallet, gated behind
                     settings.CONFLUENCE_LIVE_ENABLED (default False)
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from decimal import Decimal

import uvicorn

from config.logging import configure_logging, get_logger
from config.settings import settings
from database.init_db import init_db

log = get_logger(__name__)

from workers.discovery_worker import DiscoveryWorker
from filters.tier1_worker import Tier1Worker
from workers.sampling_worker import SamplingWorker
from workers.notification_worker import NotificationWorker
from workers.confluence_shadow_worker import ConfluenceShadowWorker
from workers.confluence_live_worker import ConfluenceLiveWorker
from workers.http_queue import get_http_queue

_shutdown_event = asyncio.Event()


def _handle_signal(sig: signal.Signals) -> None:
    log.warning("signal.received", signal=sig.name)
    _shutdown_event.set()


async def run_heartbeat() -> None:
    from database.engine import get_session
    from models.orm import SessionState
    from sqlalchemy import select
    from datetime import datetime, timezone
    import httpx

    while not _shutdown_event.is_set():
        try:
            # Fetch live SOL price from DexScreener — already in allowed domains.
            # On failure: last known price is preserved. settings.SOL_PRICE_USD
            # is only written on success, never cleared.
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(
                        "https://api.dexscreener.com/latest/dex/tokens/"
                        "So11111111111111111111111111111111111111112"
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        pairs = data.get("pairs") or []
                        # Find a SOL/USDC or SOL/USDT pair with meaningful volume
                        sol_price = 0.0
                        for pair in pairs:
                            if pair.get("quoteToken", {}).get("symbol") in ("USDC", "USDT"):
                                price_str = pair.get("priceUsd", "0")
                                try:
                                    sol_price = float(price_str)
                                    break
                                except:
                                    continue
                        if sol_price > 0:
                            settings.SOL_PRICE_USD = sol_price
                            log.debug("heartbeat.sol_price_updated", price=sol_price)
                    else:
                        log.warning("heartbeat.sol_price_bad_status",
                                    status=resp.status_code,
                                    cached=settings.SOL_PRICE_USD)
            except Exception as price_exc:
                # Network error, timeout, parse failure — keep last known price
                log.warning("heartbeat.sol_price_error",
                            error=str(price_exc),
                            cached_price=settings.SOL_PRICE_USD)

            async with get_session() as session:
                result = await session.execute(
                    select(SessionState).where(SessionState.id == 1)
                )
                state = result.scalar_one()
                state.last_heartbeat_at = datetime.now(timezone.utc)
            log.info("heartbeat.ok",
                     balance=str(state.available_balance),
                     sol_price_usd=settings.SOL_PRICE_USD)
        except Exception as exc:
            log.error("heartbeat.error", error=str(exc))
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=300)
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"))
    log.info("solanabot.starting", version="3.0.0")
    log.info("config.validated", host=settings.API_HOST, port=settings.API_PORT)

    initial_balance = Decimal(str(settings.INITIAL_BALANCE_USD))
    await init_db(initial_balance=initial_balance)

    import platform
    loop = asyncio.get_running_loop()
    if platform.system() != "Windows":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal, sig)

    tasks: list[asyncio.Task] = []
    tasks.append(asyncio.create_task(run_heartbeat(), name="heartbeat"))

    # ── Queues ────────────────────────────────────────────────────────────
    filter_queue:   asyncio.Queue = asyncio.Queue()   # discovery -> tier1
    sampling_queue: asyncio.Queue = asyncio.Queue()   # tier1 -> sampling

    # ── Workers ───────────────────────────────────────────────────────────
    discovery = DiscoveryWorker(filter_queue, _shutdown_event)

    tier1 = Tier1Worker(filter_queue, sampling_queue, _shutdown_event)

    sampling = SamplingWorker(shutdown_event=_shutdown_event)

    confluence_shadow = ConfluenceShadowWorker(shutdown_event=_shutdown_event)

    tasks.append(asyncio.create_task(discovery.run(),     name="discovery"))
    tasks.append(asyncio.create_task(tier1.run(),         name="tier1"))
    tasks.append(asyncio.create_task(sampling.run(),      name="sampling"))
    tasks.append(asyncio.create_task(confluence_shadow.run(), name="confluence_shadow"))

    # Real-money confluence_entry_v1 executor (Phase 16, 2026-09-23). Gated
    # on the dedicated wallet key being configured at all, not on
    # CONFLUENCE_LIVE_ENABLED — this lets the worker keep monitoring/exiting
    # any already-open real position even if the kill switch is later
    # flipped off, rather than abandoning it. With ENABLED=False and zero
    # open confluence_live_trades rows (the default, safe state until the
    # user funds the wallet and explicitly arms it), each cycle does
    # nothing: no entries attempted, no open trades to load, no DexScreener
    # calls, no execution calls. See workers/confluence_live_worker.py's
    # module docstring for the full isolation/safety-layer writeup.
    if settings.CONFLUENCE_LIVE_WALLET_PRIVATE_KEY:
        confluence_live = ConfluenceLiveWorker(shutdown_event=_shutdown_event)
        tasks.append(asyncio.create_task(confluence_live.run(), name="confluence_live"))
    else:
        log.info("confluence_live.not_started", reason="CONFLUENCE_LIVE_WALLET_PRIVATE_KEY not configured")

    notification = NotificationWorker(_shutdown_event)
    tasks.append(asyncio.create_task(notification.run(), name="notification"))

    from api.app import create_app  # noqa: F401
    config = uvicorn.Config(
        "api.app:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level="warning",
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    tasks.append(asyncio.create_task(server.serve(), name="api_server"))

    log.info("solanabot.running", worker_count=len(tasks))

    # Start central rate-limited HTTP queue (prevents ST 429s across workers)
    get_http_queue().start()

    await _shutdown_event.wait()
    log.warning("solanabot.shutting_down")

    get_http_queue().stop()

    for task in tasks:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for task, result in zip(tasks, results):
        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
            log.error("worker.shutdown_error",
                      worker=task.get_name(), error=str(result))

    from database.engine import close_engine
    await close_engine()
    log.info("solanabot.stopped")


if __name__ == "__main__":
    asyncio.run(main())
