"""
main.py
=======
SolanaBot v3 process entry point.

Architecture (SolanaTracker pipeline):
  DiscoveryWorker  — polls ST /tokens/multi/graduated every 60s
                     → fully-enriched token dicts → filter_queue
  Tier1Worker      — hard gate (liquidity, mcap, age, lp_burn,
                     authorities, wash guard) → sampling_queue
  SamplingWorker   — ST POST /tokens/multi batch every 60s
                     → TokenSnapshot rows → scoring_queue
  ScoringWorker    — rolling window scorer → entry_queue
  CapitalEngine    — position sizing, Jupiter execution → trades
  RiskEngine       — stop loss / take profit / time exit
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
from workers.scoring_worker import ScoringWorker
from engine.capital import CapitalEngine
from engine.risk import RiskEngine
from workers.trade_monitor_worker import TradeMonitorWorker
from workers.trade_risk_worker import TradeRiskWorker
from workers.s1_wave_worker import S1WaveWorker
from workers.notification_worker import NotificationWorker
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

    # Validate execution config on startup
    if not settings.PAPER_TRADING:
        if not settings.WALLET_PRIVATE_KEY:
            log.error("startup.missing_wallet_key")
            return
        if not settings.SOLANA_RPC_URL:
            log.error("startup.missing_rpc_url")
            return
        try:
            import base64
            import base58
            from solders.keypair import Keypair
            key_bytes = base58.b58decode(settings.WALLET_PRIVATE_KEY)
            kp = Keypair.from_bytes(key_bytes)
            log.info("startup.wallet_loaded", pubkey=str(kp.pubkey())[:16] + "...")
        except Exception as exc:
            log.error("startup.wallet_invalid", error=str(exc))
            return
    else:
        log.info("startup.paper_trading_mode")

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
    scoring_queue:  asyncio.Queue = asyncio.Queue()   # sampling -> scoring
    entry_queue:    asyncio.Queue = asyncio.Queue()   # scoring -> capital
    monitor_queue:  asyncio.Queue = asyncio.Queue()   # trade_monitor -> trade_risk
    s1_queue:       asyncio.Queue = asyncio.Queue()   # sampling -> s1_wave_worker

    # ── Workers ───────────────────────────────────────────────────────────
    risk = RiskEngine(_shutdown_event)

    discovery = DiscoveryWorker(filter_queue, _shutdown_event)

    tier1 = Tier1Worker(filter_queue, sampling_queue, _shutdown_event)

    sampling = SamplingWorker(
        scoring_queue=scoring_queue,
        shutdown_event=_shutdown_event,
        s1_queue=s1_queue,
    )

    scoring = ScoringWorker(scoring_queue, entry_queue, _shutdown_event)

    capital = CapitalEngine(entry_queue, _shutdown_event)

    trade_monitor = TradeMonitorWorker(
        monitor_queue=monitor_queue,
        shutdown_event=_shutdown_event,
        api_key=settings.SOLANA_TRACKER_API_KEY_MONITOR,
    )

    trade_risk = TradeRiskWorker(
        monitor_queue=monitor_queue,
        risk_engine=risk,
        shutdown_event=_shutdown_event,
    )

    s1_wave = S1WaveWorker(
        s1_queue=s1_queue,
        shutdown_event=_shutdown_event,
    )

    tasks.append(asyncio.create_task(discovery.run(),     name="discovery"))
    tasks.append(asyncio.create_task(tier1.run(),         name="tier1"))
    tasks.append(asyncio.create_task(sampling.run(),      name="sampling"))
    tasks.append(asyncio.create_task(scoring.run(),       name="scoring"))
    tasks.append(asyncio.create_task(capital.run(),       name="capital"))
    tasks.append(asyncio.create_task(s1_wave.run(),        name="s1_wave"))
    tasks.append(asyncio.create_task(trade_monitor.run(), name="trade_monitor"))
    tasks.append(asyncio.create_task(trade_risk.run(),    name="trade_risk"))

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
