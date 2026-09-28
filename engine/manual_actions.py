"""
engine/manual_actions.py
==========================
Real, on-chain manual position control for confluence_entry_v1 — added
2026-09-24 after this session's dashboard operator had to close 5 real
positions by hand via one-off scripts (SI, adidas, then BLK, CTNT, PEPE —
the last three specifically because a liquidity-guard finding showed the
dashboard's DexScreener-sourced "current price" had gone badly stale on
some of them). Extracted into one reusable, tested function instead of
writing a new ad-hoc script every time a human needs to override the
automated exit logic — this is what api/app.py's POST
/confluence/live/trades/{id}/close endpoint calls.

Mirrors ConfluenceLiveWorker._maybe_exit()'s successful-sell branch
exactly (status/exit fields/pnl_usd computation/notification), so a
manual close is recorded identically to an automated one, just with
exit_reason='MANUAL_CLOSE' and no dependency on the worker's own
in-memory state — this creates its own ExecutionEngine, safe to do
alongside the worker's (a wallet keypair + RPC client, no shared
mutable state).

CORRECTION, 2026-09-24: this docstring used to claim the two "can never
double-process the same trade since both check status == 'open' ...
right up to the DB write" — that protects the DATABASE ROW (only one
writer's status update ever lands), but it does NOT stop both paths from
submitting a REAL on-chain sell swap at the same time, which is a much
worse problem: found live the same day when the worker's own automatic
exit loop and a manual dashboard close both tried to sell the same
position within the same second. See engine/sell_coordination.py, which
this function now uses to make sure only one real sell is ever in flight
for a given trade at a time.
"""

from __future__ import annotations

from dataclasses import dataclass
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from config.logging import get_logger
from config.settings import settings
from database.engine import get_session
from engine.execution import ExecutionEngine
from engine.sell_coordination import finish_sell, try_start_sell
from models.orm import ConfluenceLiveTrade, ConfluenceNotification, Token

log = get_logger(__name__)


@dataclass
class ManualCloseResult:
    success: bool
    symbol: str | None = None
    pnl_usd: Decimal | None = None
    tx_signature: str | None = None
    error: str | None = None


async def close_trade_manually(trade_id: str, sol_price_usd: Decimal) -> ManualCloseResult:
    """
    Sell a single open ConfluenceLiveTrade's full recorded position back to
    SOL and mark it closed with exit_reason='MANUAL_CLOSE'. Real money,
    real on-chain transaction — the caller (an API endpoint gated by
    dashboard Basic Auth) is responsible for any user-facing confirmation.

    Returns ManualCloseResult(success=False, error=...) for every failure
    case (already closed, no token amount recorded, the sell itself
    failing) rather than raising — a manual-close attempt failing must
    never look like an unhandled server error to the dashboard.
    """
    try:
        trade_uuid = uuid.UUID(trade_id)
    except (ValueError, AttributeError, TypeError):
        return ManualCloseResult(success=False, error="Invalid trade id")

    async with get_session() as session:
        row = (await session.execute(
            select(ConfluenceLiveTrade, Token.symbol, Token.mint_address)
            .join(Token, Token.id == ConfluenceLiveTrade.token_id)
            .where(ConfluenceLiveTrade.id == trade_uuid)
        )).first()
        if row is None:
            return ManualCloseResult(success=False, error="Trade not found")
        trade, symbol, mint = row
        # 'unsellable' accepted alongside 'open' (2026-09-24) — a position
        # the worker downgraded there after too many failed real sells
        # (see confluence_live_worker.py's _UNSELLABLE_AFTER_S) is exactly
        # the kind of position a human might want to try manually closing;
        # refusing it here would block the one path meant to help.
        if trade.status not in ("open", "unsellable"):
            return ManualCloseResult(success=False, symbol=symbol, error=f"Trade is already '{trade.status}', not open")
        if not trade.entry_token_lamports:
            return ManualCloseResult(success=False, symbol=symbol, error="No recorded token amount — cannot size a sell")
        entry_token_lamports = trade.entry_token_lamports
        position_usd = trade.position_usd

    # See engine/sell_coordination.py — confluence_live_worker.py's own
    # ~1s automatic exit loop can be mid-sell for this exact trade right
    # now (e.g. it just independently tripped TIME_EXIT/HARD_FLOOR). Real
    # incident 2026-09-24: without this guard, both paths submitted a real
    # swap for the same token balance; whichever landed second was
    # rejected on-chain (custom program error 0x1788/0x1789) and reported
    # a scary SELL_FAILED_CRITICAL to the dashboard for a position that,
    # a moment later, the worker's own loop had already closed out fine.
    canonical_id = str(trade_uuid)
    if not try_start_sell(canonical_id):
        return ManualCloseResult(
            success=False, symbol=symbol,
            error="The bot's own exit logic is already closing this position right now "
                  "(likely tripped its own stop/target at the same moment) — check back "
                  "in a few seconds, it's very likely already closed.",
        )

    try:
        engine = ExecutionEngine(
            paper_override=False,
            wallet_private_key_override=settings.CONFLUENCE_LIVE_WALLET_PRIVATE_KEY,
            rpc_url_override=settings.HELIUS_RPC_URL,
        )

        # Read decimals BEFORE selling (the balance still exists) so exit_price
        # can be computed in real USD-per-whole-token terms afterward — added
        # 2026-09-24 after finding this method's exit_price was silently using
        # execution.py's known-wrong-unit sell-side actual_price (SOL-lamports
        # per raw-token-unit, documented there as "informational only... fix
        # before anything starts relying on it"). Nothing financial ever read
        # this field (pnl_usd is computed straight from real on-chain SOL
        # received, unaffected either way), but a manually-closed trade's
        # recorded exit_price was showing a nonsense multiple of entry_price
        # (a real one: $0.0000178 exit vs $0.0000021 entry on a trade that
        # actually closed near breakeven) — confirmed on Robinhood and FOMOCAT.
        balance_before_sell = await engine.get_token_balance_raw(mint)

        log.info("manual_actions.close_attempted", trade_id=trade_id, symbol=symbol, mint=mint)
        result = await engine.sell(mint, Decimal(str(entry_token_lamports)), trade_id=trade_id, symbol=symbol)
    finally:
        finish_sell(canonical_id)

    if not result.success:
        log.error("manual_actions.close_failed", trade_id=trade_id, symbol=symbol,
                  error_type=result.error_type, error_detail=result.error_detail)
        return ManualCloseResult(success=False, symbol=symbol,
                                  error=f"{result.error_type}: {result.error_detail}")

    exit_proceeds_usd = (Decimal(str(result.actual_amount)) / Decimal("1e9")) * sol_price_usd if result.actual_amount else None
    pnl_usd = (exit_proceeds_usd - position_usd) if exit_proceeds_usd is not None else None
    now = datetime.now(timezone.utc)

    if exit_proceeds_usd is not None and balance_before_sell is not None:
        _, decimals = balance_before_sell
        exit_price = exit_proceeds_usd / (Decimal(str(entry_token_lamports)) / (Decimal(10) ** decimals))
    else:
        # Balance lookup failed (rare) — fall back to the documented-wrong-
        # unit value rather than losing the field entirely; logged clearly
        # so it's never mistaken for a real, comparable price.
        exit_price = result.actual_price if result.actual_price else Decimal("0")
        log.warning("manual_actions.exit_price_unit_fallback", trade_id=trade_id, symbol=symbol,
                    reason="pre-sell balance lookup unavailable — exit_price is in the wrong unit")

    async with get_session() as session:
        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade_uuid))).scalar_one_or_none()
        if row is None or row.status not in ("open", "unsellable"):
            # Sold on-chain but lost the race for the DB row (should be
            # unreachable given the check above, but never silently drop a
            # real, already-executed sell's record).
            log.error("manual_actions.close_race_lost", trade_id=trade_id, symbol=symbol,
                      tx_signature=result.tx_signature)
            return ManualCloseResult(success=True, symbol=symbol, pnl_usd=pnl_usd,
                                      tx_signature=result.tx_signature,
                                      error="Sold on-chain, but the trade row had already changed status — check manually")
        row.status = "closed"
        row.exit_time = now
        row.exit_price = exit_price
        row.exit_reason = "MANUAL_CLOSE"
        row.exit_sol_lamports = int(result.actual_amount) if result.actual_amount else None
        row.exit_tx_signature = result.tx_signature
        row.pnl_usd = pnl_usd
        # Real, on-chain-verified net P&L (2026-09-28) — see
        # ConfluenceLiveTrade.entry_real_sol_lamports's docstring in
        # models/orm.py for the full audit behind this. Mirrors
        # confluence_live_worker.py's own successful-exit branch exactly.
        row.exit_real_sol_lamports = result.actual_sol_lamports
        row.reclaim_tx_signature = result.reclaim_tx_signature
        row.reclaim_sol_lamports = result.reclaim_sol_lamports
        if row.entry_real_sol_lamports is not None and result.actual_sol_lamports is not None:
            real_net_lamports = row.entry_real_sol_lamports + result.actual_sol_lamports + (result.reclaim_sol_lamports or 0)
            row.real_pnl_usd = (Decimal(real_net_lamports) / Decimal("1e9")) * sol_price_usd
        session.add(ConfluenceNotification(
            level="info" if (pnl_usd or 0) >= 0 else "warning",
            event="exit_filled",
            message=f"{symbol} closed (MANUAL_CLOSE): "
                    f"{f'${pnl_usd:+.4f}' if pnl_usd is not None else 'pnl unknown'}",
            trade_id=trade_uuid,
        ))

    log.info("manual_actions.close_succeeded", trade_id=trade_id, symbol=symbol,
              tx_signature=result.tx_signature, pnl_usd=str(pnl_usd) if pnl_usd is not None else None)
    return ManualCloseResult(success=True, symbol=symbol, pnl_usd=pnl_usd, tx_signature=result.tx_signature)
