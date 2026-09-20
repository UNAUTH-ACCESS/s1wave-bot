"""
filters/tier1_worker.py
========================
Tier 1 worker — hard gate filter for SolanaTracker-sourced tokens.

Receives fully-enriched dicts from discovery_worker (no DB token row needed).
Applies simplified Tier 1 constraints using data already in the ST response:
  1. liquidity_usd     >= TIER1_MIN_LIQUIDITY_USD
  2. market_cap_usd    >= TIER1_MIN_MARKET_CAP_USD
  3. token age         <= TIER1_MAX_AGE_MINUTES
  4. mint_authority    == None (renounced)
  5. freeze_authority  == None (renounced)
  6. lp_burn           == 100 (fully burned)
  7. buy/sell ratio    <= TIER1_MAX_WASH_MULTIPLIER (wash guard)

Tokens that pass are written to the DB as WATCHING and pushed to
sampling_queue for momentum scoring.

Logging contract
----------------
info:
  tier1_worker.started / stopped
  tier1.pass         — symbol, liquidity, market_cap, age_minutes
  tier1_worker.passed — symbol, age_minutes (after DB write)
  tier1_worker.rejected — symbol, reason, value, threshold
debug:
  tier1_worker.queue_empty
errors:
  tier1_worker.queue_error
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.dialects.postgresql import insert as pg_insert

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from models.orm import Token, TokenEvaluation, TokenStatus

log = get_logger(__name__)


class Tier1Worker:

    def __init__(
        self,
        filter_queue: asyncio.Queue,
        sampling_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._filter_queue   = filter_queue
        self._sampling_queue = sampling_queue
        self._shutdown       = shutdown_event

    async def run(self) -> None:
        log.info("tier1_worker.started")

        while not self._shutdown.is_set():
            try:
                token_dict = await asyncio.wait_for(
                    self._filter_queue.get(), timeout=5.0
                )
                self._filter_queue.task_done()
                await self._evaluate(token_dict)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("tier1_worker.queue_error", error=str(exc), exc_info=True)

        log.info("tier1_worker.stopped")

    async def _evaluate(self, t: dict) -> None:
        mint    = t["mint"]
        symbol  = t.get("symbol", "?")
        now     = datetime.now(timezone.utc)

        # ── Age ───────────────────────────────────────────────────────────
        created_ms  = t.get("pool_created_at_ms", 0)
        age_minutes = (now.timestamp() - created_ms / 1000) / 60 if created_ms else 0.0

        liq   = t.get("liquidity_usd", Decimal("0"))
        mcap  = t.get("market_cap_usd", Decimal("0"))
        buys  = t.get("buys", 0) or 0
        sells = t.get("sells", 0) or 0
        wash  = (buys / sells) if sells > 0 else (float(buys) if buys else None)
        lp_burn = t.get("lp_burn", 0)

        # Every constraint's raw value, gathered once up front — every
        # evaluation row (pass or fail) carries the full picture, not just
        # whichever field happened to trip the rejection.
        inputs = {
            "age_minutes":       round(age_minutes, 2),
            "liquidity_usd":     str(liq),
            "market_cap_usd":    str(mcap),
            "mint_authority_renounced":   t.get("mint_authority") is None,
            "freeze_authority_renounced": t.get("freeze_authority") is None,
            "lp_burn":           lp_burn,
            "buys":              buys,
            "sells":             sells,
            "wash_multiplier":   round(wash, 4) if wash is not None else None,
        }

        if age_minutes > settings.TIER1_MAX_AGE_MINUTES:
            await self._reject(t, now, "TOKEN_TOO_OLD", inputs)
            return

        if liq < Decimal(str(settings.TIER1_MIN_LIQUIDITY_USD)):
            await self._reject(t, now, "LOW_LIQUIDITY", inputs)
            return

        if mcap < Decimal(str(settings.TIER1_MIN_MARKET_CAP_USD)):
            await self._reject(t, now, "LOW_MARKET_CAP", inputs)
            return

        if t.get("mint_authority") is not None:
            await self._reject(t, now, "MINT_AUTHORITY_NOT_RENOUNCED", inputs)
            return

        if t.get("freeze_authority") is not None:
            await self._reject(t, now, "FREEZE_AUTHORITY_NOT_RENOUNCED", inputs)
            return

        if lp_burn < 100:
            await self._reject(t, now, "LP_NOT_BURNED", inputs)
            return

        if wash is not None and wash > settings.TIER1_MAX_WASH_MULTIPLIER:
            await self._reject(t, now, "WASH_TRADING", inputs)
            return

        # ── All constraints passed ────────────────────────────────────────
        log.info(
            "tier1.pass",
            mint=mint,
            symbol=symbol,
            liquidity=str(liq),
            market_cap=str(mcap),
            age_minutes=round(age_minutes, 1),
            lp_burn=lp_burn,
        )

        token_id = await self._write_watching(t, now)
        await self._write_evaluation(token_id, now, passed=True, reason_code=None, inputs=inputs)
        await self._sampling_queue.put(t)

        log.info(
            "tier1_worker.passed",
            mint=mint,
            symbol=symbol,
            age_minutes=round(age_minutes, 1),
        )

    async def _write_watching(self, t: dict, now: datetime):
        """Upsert token into DB as WATCHING. Returns the token's id."""
        async with get_session() as session:
            stmt = (
                pg_insert(Token)
                .values(
                    mint_address=t["mint"],
                    symbol=t.get("symbol"),
                    name=t.get("name"),
                    status=TokenStatus.WATCHING,
                    discovered_at=now,
                    watch_started_at=now,
                    # Enrichment fields from SolanaTracker
                    liquidity_usd=t.get("liquidity_usd"),
                    market_cap_usd=t.get("market_cap_usd"),
                    mint_authority_renounced=t.get("mint_authority") is None,
                    freeze_authority_renounced=t.get("freeze_authority") is None,
                    lp_locked_burned=t.get("lp_burn", 0) == 100,
                    # wash_multiplier: buys/sells ratio stored for diagnostics
                    wash_multiplier=Decimal(str(
                        (t["buys"] / max(t["sells"], 1))
                        if t.get("sells", 0) > 0 else t.get("buys", 0)
                    )).quantize(Decimal("0.0001")) if t.get("buys") else None,
                )
                .on_conflict_do_update(
                    index_elements=["mint_address"],
                    set_={
                        "status":          TokenStatus.WATCHING,
                        "watch_started_at": now,
                        "liquidity_usd":   t.get("liquidity_usd"),
                        "market_cap_usd":  t.get("market_cap_usd"),
                    },
                )
                .returning(Token.id)
            )
            result = await session.execute(stmt)
            return result.scalar_one()

    async def _write_observing(self, t: dict, now: datetime):
        """
        Upsert a REJECTED-from-Tier1 token as OBSERVING rather than dropping
        it entirely. This is the control group: sampling_worker will sample
        it for a short, bounded window (OBSERVE_WINDOW_SECONDS) before it
        finally moves to REJECTED. Returns the token's id.
        """
        async with get_session() as session:
            stmt = (
                pg_insert(Token)
                .values(
                    mint_address=t["mint"],
                    symbol=t.get("symbol"),
                    name=t.get("name"),
                    status=TokenStatus.OBSERVING,
                    discovered_at=now,
                    # watch_started_at deliberately left NULL — this token
                    # was never eligible to trade, so "started watching"
                    # never happened. sampling_worker uses discovered_at
                    # for OBSERVING tokens' age-out check instead.
                    liquidity_usd=t.get("liquidity_usd"),
                    market_cap_usd=t.get("market_cap_usd"),
                    mint_authority_renounced=t.get("mint_authority") is None,
                    freeze_authority_renounced=t.get("freeze_authority") is None,
                    lp_locked_burned=t.get("lp_burn", 0) == 100,
                    wash_multiplier=Decimal(str(
                        (t["buys"] / max(t["sells"], 1))
                        if t.get("sells", 0) > 0 else t.get("buys", 0)
                    )).quantize(Decimal("0.0001")) if t.get("buys") else None,
                )
                .on_conflict_do_update(
                    index_elements=["mint_address"],
                    set_={
                        "liquidity_usd":  t.get("liquidity_usd"),
                        "market_cap_usd": t.get("market_cap_usd"),
                    },
                )
                .returning(Token.id)
            )
            result = await session.execute(stmt)
            return result.scalar_one()

    async def _write_evaluation(
        self, token_id, evaluated_at: datetime, *, passed: bool,
        reason_code: str | None, inputs: dict,
    ) -> None:
        async with get_session() as session:
            session.add(TokenEvaluation(
                token_id=token_id,
                evaluated_at=evaluated_at,
                gate="TIER1",
                passed=passed,
                reason_code=reason_code,
                inputs_json=inputs,
            ))

    async def _reject(self, t: dict, now: datetime, reason: str, inputs: dict) -> None:
        mint, symbol = t["mint"], t.get("symbol", "?")
        log.info("tier1_worker.rejected", mint=mint, symbol=symbol, reason=reason, **inputs)

        # Was a hard rejection that dropped the token entirely. Now it
        # becomes the control group: recorded, then observed briefly.
        token_id = await self._write_observing(t, now)
        await self._write_evaluation(token_id, now, passed=False, reason_code=reason, inputs=inputs)
        await self._sampling_queue.put(t)
