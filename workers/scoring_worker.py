"""
workers/scoring_worker.py
=========================
Scoring worker — PRD §4 Event ⑤

Woken by the sampling_worker after each 30-second cycle. Scores all
WATCHING tokens and acts on the result.

Logging contract
----------------
info:
  scoring_worker.started / stopped
  scoring_worker.cycle_complete  — summary counts + elapsed_ms
  scoring_worker.token_scored    — per-token: symbol, score, signal,
                                   bp_trend, vm_trend, vm_zone,
                                   bp_p1/p2/p3, vm_p1/p2/p3
                                   (full signal breakdown — not just the score)
  scoring_worker.strong_buy      — full breakdown when entering capital engine
  scoring_worker.discarded       — score + reason when dropping below floor
  scoring_worker.watch           — score when staying in WATCH band (debug)

debug:
  scoring_worker.timeout_fallback

errors:
  scoring_worker.error

Capital engine blocking:
  capital_engine.entry_blocked   — reason why capital engine skipped entry
  capital_engine.no_scoring_context — token had no window at entry time
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
from engine.rolling_window import SnapshotWindow, ScoringResult, score_window
from models.orm import Token, TokenEvaluation, TokenSnapshot, TokenStatus

log = get_logger(__name__)

_DISCARD_REASON_TEMPLATE = "TIER2_DISCARD:score={score}"


class ScoringWorker:

    def __init__(
        self,
        scoring_queue: asyncio.Queue,
        entry_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._scoring_queue = scoring_queue
        self._entry_queue = entry_queue
        self._shutdown = shutdown_event

    async def run(self) -> None:
        log.info("scoring_worker.started")

        while not self._shutdown.is_set():
            try:
                await asyncio.wait_for(self._scoring_queue.get(), timeout=60.0)
                self._scoring_queue.task_done()
            except asyncio.TimeoutError:
                log.debug("scoring_worker.timeout_fallback")
            except asyncio.CancelledError:
                break

            try:
                await self._score_all_watching()
            except Exception as exc:
                log.error("scoring_worker.error", error=str(exc), exc_info=True)

        log.info("scoring_worker.stopped")

    async def _score_all_watching(self) -> None:
        watching = await self._load_watching_tokens()
        if not watching:
            return

        t0 = time.monotonic()
        strong_buys = watches = discards = skipped = 0

        for token in watching:
            result = await self._score_token(token)
            if result == "STRONG_BUY":
                strong_buys += 1
            elif result == "WATCH":
                watches += 1
            elif result == "DISCARD":
                discards += 1
            else:
                skipped += 1

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        log.info(
            "scoring_worker.cycle_complete",
            watching=len(watching),
            strong_buys=strong_buys,
            watches=watches,
            discards=discards,
            skipped_no_window=skipped,
            elapsed_ms=elapsed_ms,
        )

    async def _load_watching_tokens(self) -> list[Token]:
        async with get_session() as session:
            result = await session.execute(
                select(Token).where(Token.status == TokenStatus.WATCHING)
            )
            return list(result.scalars().all())

    async def _score_token(self, token: Token) -> str:
        window_data = await self._fetch_window(token)
        if window_data is None:
            return "SKIP"

        p1, p2, p3 = window_data

        def vlr(snap: TokenSnapshot) -> Decimal:
            if not snap.liquidity_usd or snap.liquidity_usd == Decimal("0"):
                return Decimal("0")
            return (snap.volume_usd / snap.liquidity_usd).quantize(Decimal("0.000001"))

        prev_score = await self._get_prev_score(token)

        window = SnapshotWindow(
            bp_p1=p1.buy_pressure, bp_p2=p2.buy_pressure, bp_p3=p3.buy_pressure,
            vm_p1=p1.volume_mult,  vm_p2=p2.volume_mult,  vm_p3=p3.volume_mult,
            vlr_p1=vlr(p1),        vlr_p2=vlr(p2),        vlr_p3=vlr(p3),
            age_minutes=self._token_age_minutes(token),
            prev_composite_score=prev_score,
        )

        result = score_window(window)

        # Per-signal breakdown — answers "which signals drove this score"
        log.info(
            "scoring_worker.token_scored",
            mint=token.mint_address,
            symbol=token.symbol,
            score=str(result.composite),
            signal=result.signal,
            bp_trend=result.bp_trend.value,
            vm_trend=result.vm_trend.value,
            vm_zone=result.vm_zone.value if result.vm_zone else None,
            bp_p1=str(p1.buy_pressure), bp_p2=str(p2.buy_pressure), bp_p3=str(p3.buy_pressure),
            vm_p1=str(p1.volume_mult),  vm_p2=str(p2.volume_mult),  vm_p3=str(p3.volume_mult),
            age_minutes=round(self._token_age_minutes(token), 1),
        )

        await self._act_on_result(token, result)
        return result.signal

    async def _fetch_window(
        self, token: Token
    ) -> tuple[TokenSnapshot, TokenSnapshot, TokenSnapshot] | None:
        async with get_session() as session:
            result = await session.execute(
                select(TokenSnapshot)
                .where(TokenSnapshot.token_id == token.id)
                .order_by(TokenSnapshot.sampled_at.desc())
                .limit(3)
            )
            snaps = list(result.scalars().all())

        if len(snaps) < 3:
            return None

        p3, p2, p1 = snaps[0], snaps[1], snaps[2]
        return p1, p2, p3

    async def _get_prev_score(self, token: Token) -> Decimal | None:
        async with get_session() as session:
            result = await session.execute(
                select(TokenSnapshot)
                .where(TokenSnapshot.token_id == token.id)
                .order_by(TokenSnapshot.sampled_at.desc())
                .limit(4)
            )
            snaps = list(result.scalars().all())

        if len(snaps) < 4:
            return None

        old_p1, old_p2, old_p3 = snaps[3], snaps[2], snaps[1]

        def vlr(snap: TokenSnapshot) -> Decimal:
            if not snap.liquidity_usd or snap.liquidity_usd == Decimal("0"):
                return Decimal("0")
            return (snap.volume_usd / snap.liquidity_usd).quantize(Decimal("0.000001"))

        prior_window = SnapshotWindow(
            bp_p1=old_p1.buy_pressure, bp_p2=old_p2.buy_pressure, bp_p3=old_p3.buy_pressure,
            vm_p1=old_p1.volume_mult,  vm_p2=old_p2.volume_mult,  vm_p3=old_p3.volume_mult,
            vlr_p1=vlr(old_p1),        vlr_p2=vlr(old_p2),        vlr_p3=vlr(old_p3),
            age_minutes=self._token_age_minutes(token),
            prev_composite_score=None,
        )
        return score_window(prior_window).composite

    async def _act_on_result(self, token: Token, result: ScoringResult) -> None:
        # Only the two terminal outcomes (enter / discard) are decision
        # points worth a row — WATCH just means "ask again next cycle" and
        # would otherwise write a row per token per 30s cycle for as long
        # as a token sits in rotation, for no analytical benefit.
        eval_inputs = {
            "score":    str(result.composite),
            "bp_trend": result.bp_trend.value,
            "vm_trend": result.vm_trend.value,
            "vm_zone":  result.vm_zone.value if result.vm_zone else None,
        }

        if result.is_strong_buy:
            # Check if an S1 wave position is already open for this mint
            existing_s1 = await self._get_open_s1_trade(token.mint_address)
            if existing_s1 is not None:
                # Scorer add-on: increase position on existing S1 trade
                await self._scorer_addon(token, existing_s1, result)
            else:
                # Normal scorer entry
                await self._entry_queue.put(token.mint_address)
                await self._write_evaluation(token.id, passed=True,
                                              reason_code="strong_buy", inputs=eval_inputs)
                log.info(
                    "scoring_worker.strong_buy",
                    mint=token.mint_address,
                    symbol=token.symbol,
                    score=str(result.composite),
                    bp_trend=result.bp_trend.value,
                    vm_trend=result.vm_trend.value,
                    vm_zone=result.vm_zone.value if result.vm_zone else None,
                )

        elif result.is_discard:
            async with get_session() as session:
                db_result = await session.execute(
                    select(Token)
                    .where(Token.mint_address == token.mint_address)
                    .with_for_update()
                )
                db_token = db_result.scalar_one_or_none()
                if db_token and db_token.status == TokenStatus.WATCHING:
                    db_token.status = TokenStatus.REJECTED
                    db_token.rejection_reason = _DISCARD_REASON_TEMPLATE.format(
                        score=str(result.composite)
                    )

            await self._write_evaluation(token.id, passed=False,
                                          reason_code="discard", inputs=eval_inputs)
            log.info(
                "scoring_worker.discarded",
                mint=token.mint_address,
                symbol=token.symbol,
                score=str(result.composite),
                bp_trend=result.bp_trend.value,
                vm_trend=result.vm_trend.value,
            )

        else:
            # WATCH band — stays in rotation
            log.debug(
                "scoring_worker.watch",
                mint=token.mint_address,
                symbol=token.symbol,
                score=str(result.composite),
            )


    async def _get_open_s1_trade(self, mint: str):
        """Return the open S1 wave trade for this mint, or None."""
        # get_session is deliberately NOT re-imported here — it's already
        # imported at module level, and tests patch it via
        # patch("workers.scoring_worker.get_session"). A local re-import
        # shadows that patch and silently hits whatever real DB
        # settings.DATABASE_URL points to instead of the test fixture.
        from models.orm import Trade, TradeStatus
        async with get_session() as session:
            result = await session.execute(
                select(Trade).where(
                    Trade.token_mint == mint,
                    Trade.status == TradeStatus.OPEN,
                    Trade.entry_source == "s1_wave",
                )
            )
            return result.scalar_one_or_none()

    async def _scorer_addon(self, token, existing_trade, result) -> None:
        """
        Add a standard position unit to an existing S1 wave trade.
        Does NOT alter trailing stop state or high watermark.
        Writes a BalanceHistory row so the ledger reconciles.
        Logs with entry_source=scorer_addon.
        """
        # get_session intentionally not re-imported — see the note in
        # _get_open_s1_trade above.
        from config.settings import settings
        from datetime import datetime, timezone
        from decimal import Decimal
        from models.orm import BalanceHistory, NotificationEvent, NotificationQueue, SessionState, Trade, TradeStatus
        from sqlalchemy import select
        import orjson

        async with get_session() as session:
            state_result = await session.execute(
                select(SessionState).where(SessionState.id == 1).with_for_update()
            )
            state = state_result.scalar_one()
            balance_before = state.available_balance
            allocation = Decimal(str(settings.TRADE_ALLOCATION_PCT))
            add_usd = (balance_before * allocation).quantize(Decimal("0.000001"))

            if add_usd < Decimal(str(settings.MIN_POSITION_USD)):
                log.info("scoring_worker.addon_insufficient_balance",
                         mint=token.mint_address)
                return

            # Increase position size on the existing trade under lock
            trade_result = await session.execute(
                select(Trade).where(Trade.id == existing_trade.id).with_for_update()
            )
            db_trade = trade_result.scalar_one_or_none()
            if db_trade is None or db_trade.status != TradeStatus.OPEN:
                return

            db_trade.position_size_usd += add_usd
            # Mark entry_source as scorer_addon for auditability
            db_trade.entry_source = "scorer_addon"

            balance_after = balance_before - add_usd
            state.available_balance = balance_after

            now = datetime.now(timezone.utc)

            # BalanceHistory row — ledger must reconcile for every capital event
            session.add(BalanceHistory(
                trade_id=db_trade.id,
                balance_before=balance_before,
                balance_after=balance_after,
                snapshot_at=now,
            ))

            # Notification so Telegram reflects the addon
            session.add(NotificationQueue(
                event_type=NotificationEvent.TRADE_OPEN,
                payload=orjson.dumps({
                    "trade_id":     str(db_trade.id),
                    "mint":         token.mint_address,
                    "symbol":       token.symbol,
                    "entry_source": "scorer_addon",
                    "add_usd":      str(add_usd),
                    "position_usd": str(db_trade.position_size_usd),
                    "balance_after": str(balance_after),
                    "score":        str(result.composite),
                    "timestamp":    now.isoformat(),
                }).decode(),
                trade_id=db_trade.id,
            ))

        log.info(
            "scoring_worker.scorer_addon",
            mint=token.mint_address,
            symbol=token.symbol,
            entry_source="scorer_addon",
            add_usd=str(add_usd),
            balance_after=str(balance_after),
            score=str(result.composite),
        )
    async def _write_evaluation(
        self, token_id, *, passed: bool, reason_code: str, inputs: dict,
    ) -> None:
        async with get_session() as session:
            session.add(TokenEvaluation(
                token_id=token_id,
                gate="SCORER",
                passed=passed,
                reason_code=reason_code,
                inputs_json=inputs,
            ))

    def _token_age_minutes(self, token: Token) -> float:
        if not token.watch_started_at:
            return 0.0
        return (datetime.now(timezone.utc) - token.watch_started_at).total_seconds() / 60
