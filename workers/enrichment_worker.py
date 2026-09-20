"""
workers/enrichment_worker.py
============================
Enrichment worker — PRD §4 Event ②

Consumes mint addresses from the enrichment queue. Fetches DEX Screener
data with retry backoff — tokens from PumpPortal arrive before DEX Screener
indexes them, so immediate attempts often return NO_DEX_PAIR.

Retry schedule (DEX Screener not indexed yet):
  Attempt 1: immediate
  Attempt 2: 10s later
  Attempt 3: 20s later
  Attempt 4: 40s later
  Attempt 5: 60s later
  Give up after attempt 5 (~130s total window)

Helius DAS is called once for token_decimals only.
mint_auth_renounced and freeze_auth_renounced are guaranteed by
pump.fun graduation protocol — not fetched from Helius.

Logging contract
----------------
info:
  enrichment.started            — mint, attempt number
  enrichment.dex_result         — liquidity, price, market_cap, volume
  enrichment.complete           — final values, elapsed_ms, attempts
  enrichment.rejected           — reason (NO_DEX_PAIR after retries, etc.)
  enrichment.retry              — attempt, delay_s, reason

errors:
  enrichment.source_failed      — source, exc_type, error
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from dexscreener.client import DexScreenerClient, DexTokenDetail
from helius.client import HeliusAssetInfo, HeliusClient, HeliusTxAnalysis
from models.orm import Token, TokenStatus

log = get_logger(__name__)

_MAX_CONCURRENT_ENRICHMENTS = 5
_PRE_LAUNCH_LIQUIDITY_FLOOR = Decimal("3000")

# Retry delays in seconds for NO_DEX_PAIR / Asset Not Found
# Total window: 0 + 10 + 20 + 40 + 60 = 130s max
_RETRY_DELAYS = [0, 10, 20, 40, 60]

# Default token decimals for pump.fun tokens — almost always 6
_DEFAULT_DECIMALS = 6


class EnrichmentWorker:

    def __init__(
        self,
        enrichment_queue: asyncio.Queue,
        dex_client: DexScreenerClient,
        helius_client: HeliusClient,
        shutdown_event: asyncio.Event,
        filter_queue: asyncio.Queue | None = None,
    ) -> None:
        self._queue      = enrichment_queue
        self._dex        = dex_client
        self._helius     = helius_client
        self._shutdown   = shutdown_event
        self._filter_queue = filter_queue
        self._semaphore  = asyncio.Semaphore(_MAX_CONCURRENT_ENRICHMENTS)

    async def run(self) -> None:
        log.info("enrichment_worker.started")

        while not self._shutdown.is_set():
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            # Queue carries either:
            #   str          — mint address only (stale requeue, trending source)
            #   (str, dict)  — mint + pre-fetched pair data from discovery
            if isinstance(item, tuple):
                mint, pair_data = item
            else:
                mint, pair_data = item, None

            asyncio.create_task(
                self._enrich_with_semaphore(mint, pair_data),
                name=f"enrich:{mint[:8]}",
            )

        log.info("enrichment_worker.stopped")

    async def _enrich_with_semaphore(self, mint: str, pair_data: dict | None = None) -> None:
        async with self._semaphore:
            await self._enrich_token(mint, pair_data)
        self._queue.task_done()

    async def _enrich_token(self, mint: str, pair_data: dict | None = None) -> None:
        t0 = time.monotonic()

        # ── DEX Screener — use pre-fetched data or fetch fresh ────────────
        if pair_data is not None:
            # Discovery already fetched full pair data — build a lightweight
            # pair object from the dict to avoid a redundant API call
            pair = self._pair_from_dict(pair_data)
            dex_detail = None  # not needed for write_enrichment

            log.info(
                "enrichment.started",
                mint=mint,
                attempt=1,
                source="pre_fetched",
            )
            log.info(
                "enrichment.dex_result",
                mint=mint,
                attempt=1,
                has_pair=True,
                liquidity=str(pair.liquidity_usd) if pair else None,
                price=str(pair.price_usd) if pair else None,
                market_cap=str(pair.market_cap_usd) if pair else None,
                volume_m5=str(pair.volume_m5_usd) if pair else None,
                buy_pressure=str(pair.buy_pressure) if pair else None,
            )
        else:
            # No pre-fetched data — fetch from DEX Screener with retry backoff
            dex_detail = None
            pair = None
            for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
                if delay > 0:
                    log.info(
                        "enrichment.retry",
                        mint=mint,
                        attempt=attempt,
                        delay_s=delay,
                        reason="NOT_INDEXED_YET",
                    )
                    await asyncio.sleep(delay)

                log.info("enrichment.started", mint=mint, attempt=attempt)

                try:
                    dex_detail = await self._dex.get_token_detail(mint)
                except Exception as exc:
                    log.error(
                        "enrichment.source_failed",
                        mint=mint,
                        source="dex",
                        attempt=attempt,
                        exc_type=type(exc).__name__,
                        error=str(exc),
                    )
                    await self._mark_rejected(mint, "ENRICHMENT_FAILED:DEX")
                    return

                pair = dex_detail.best_pair if dex_detail else None

                log.info(
                    "enrichment.dex_result",
                    mint=mint,
                    attempt=attempt,
                    has_pair=pair is not None,
                    liquidity=str(pair.liquidity_usd) if pair else None,
                    price=str(pair.price_usd) if pair else None,
                    market_cap=str(pair.market_cap_usd) if pair else None,
                    volume_m5=str(pair.volume_m5_usd) if pair else None,
                    buy_pressure=str(pair.buy_pressure) if pair else None,
                )

                if pair is not None:
                    break

                if attempt == len(_RETRY_DELAYS):
                    log.info(
                        "enrichment.rejected",
                        mint=mint,
                        reason="NO_DEX_PAIR",
                        attempts=attempt,
                    )
                    await self._mark_rejected(mint, "NO_DEX_PAIR")
                    return

        # ── Pre-launch liquidity check ────────────────────────────────────
        if (pair.liquidity_usd or Decimal("0")) < _PRE_LAUNCH_LIQUIDITY_FLOOR:
            log.info(
                "enrichment.rejected",
                mint=mint,
                reason="PRE_LAUNCH_LIQUIDITY",
                liquidity=str(pair.liquidity_usd),
                floor=str(_PRE_LAUNCH_LIQUIDITY_FLOOR),
            )
            await self._mark_rejected(mint, "PRE_LAUNCH_LIQUIDITY")
            return

        # ── Helius DAS — token_decimals only ─────────────────────────────
        # mint_auth and freeze_auth guaranteed by pump.fun graduation protocol
        token_decimals = _DEFAULT_DECIMALS
        try:
            asset_info = await self._helius.get_asset(mint)
            if asset_info.token_decimals is not None:
                token_decimals = asset_info.token_decimals
        except Exception as exc:
            # Non-fatal — default to 6, log and continue
            log.warning(
                "enrichment.helius_decimals_failed",
                mint=mint,
                error=str(exc),
                using_default=_DEFAULT_DECIMALS,
            )

        # ── Wash multiplier from DEX Screener h1 data ────────────────────
        h1_buys  = pair.txns_h1_buys  if pair else 0
        h1_sells = pair.txns_h1_sells if pair else 0

        if h1_buys == 0 and h1_sells == 0:
            dex_wash_mult = Decimal("1.0000")
        elif h1_sells == 0:
            dex_wash_mult = Decimal("9999.0000")
        else:
            dex_wash_mult = Decimal(str(round(h1_buys / h1_sells, 4)))

        # ── Write to DB ───────────────────────────────────────────────────
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        await self._write_enrichment(
            mint, pair, token_decimals, elapsed_ms, dex_wash_mult,
            h1_buys, h1_sells
        )

        if self._filter_queue is not None:
            await self._filter_queue.put(mint)

    async def _write_enrichment(
        self,
        mint: str,
        pair: object | None,
        token_decimals: int,
        elapsed_ms: int,
        dex_wash_mult: Decimal,
        h1_buys: int,
        h1_sells: int,
    ) -> None:

        async with get_session() as session:
            result = await session.execute(select(Token).where(Token.mint_address == mint))
            token = result.scalar_one_or_none()

            if token is None:
                log.warning("enrichment.token_missing_from_db", mint=mint)
                return

            token.liquidity_usd              = None
            token.market_cap_usd             = None
            token.mint_authority_renounced   = None
            token.freeze_authority_renounced = None
            token.holder_count               = None
            token.lp_locked_burned           = None
            token.wash_multiplier            = None

            if pair:
                token.liquidity_usd  = pair.liquidity_usd
                token.market_cap_usd = pair.market_cap_usd
                if pair.base_token_symbol and not token.symbol:
                    token.symbol = pair.base_token_symbol
                if pair.base_token_name and not token.name:
                    token.name = pair.base_token_name
                if token.baseline_volume_usd is None and pair.volume_m5_usd:
                    token.baseline_volume_usd = pair.volume_m5_usd

            # pump.fun graduation guarantees these — set directly
            token.mint_authority_renounced   = True
            token.freeze_authority_renounced = True
            token.lp_locked_burned           = True   # LP burned at graduation by pump.fun protocol

            token.holder_count  = None  # not fetching from Helius
            token.wash_multiplier = dex_wash_mult

            # Store token decimals for WS price calculation
            if token.token_decimals is None:
                token.token_decimals = token_decimals

        log.info(
            "enrichment.complete",
            mint=mint,
            liquidity=str(pair.liquidity_usd) if pair else None,
            mint_auth_renounced=True,
            freeze_auth_renounced=True,
            wash_mult=str(dex_wash_mult),
            h1_buys=h1_buys,
            h1_sells=h1_sells,
            token_decimals=token_decimals,
            elapsed_ms=elapsed_ms,
        )

    def _pair_from_dict(self, data: dict) -> object:
        """
        Build a lightweight pair object from a raw DEX Screener dict.
        Mirrors the fields used in _write_enrichment and wash multiplier calc.
        """
        from dataclasses import dataclass

        liq     = data.get("liquidity", {})
        vol     = data.get("volume", {})
        txns    = data.get("txns", {})
        base    = data.get("baseToken", {})

        class _Pair:
            pass

        p = _Pair()
        p.pair_address       = data.get("pairAddress")
        p.base_token_address = base.get("address")
        p.base_token_symbol  = base.get("symbol")
        p.base_token_name    = base.get("name")
        p.price_usd          = Decimal(str(data.get("priceUsd") or 0)) or None
        p.liquidity_usd      = Decimal(str(liq.get("usd") or 0)) or None
        p.market_cap_usd     = Decimal(str(data.get("marketCap") or data.get("fdv") or 0)) or None
        p.volume_m5_usd      = Decimal(str(vol.get("m5") or 0)) or None
        p.txns_h1_buys       = int(txns.get("h1", {}).get("buys") or 0)
        p.txns_h1_sells      = int(txns.get("h1", {}).get("sells") or 0)

        # buy_pressure = buys / (buys + sells) for m5
        m5_buys  = int(txns.get("m5", {}).get("buys") or 0)
        m5_sells = int(txns.get("m5", {}).get("sells") or 0)
        total_m5 = m5_buys + m5_sells
        p.buy_pressure = Decimal(str(round(m5_buys / total_m5, 4))) if total_m5 > 0 else None

        return p

    async def _mark_rejected(self, mint: str, reason: str) -> None:
        async with get_session() as session:
            result = await session.execute(select(Token).where(Token.mint_address == mint))
            token = result.scalar_one_or_none()
            if token:
                token.status = TokenStatus.REJECTED
                token.rejection_reason = reason
