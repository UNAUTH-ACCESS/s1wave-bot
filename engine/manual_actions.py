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
mutable state), and the two can never double-process the same trade
since both check `status == 'open'` immediately before acting and this
one holds that check right up to the DB write.
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
        if trade.status != "open":
            return ManualCloseResult(success=False, symbol=symbol, error=f"Trade is already '{trade.status}', not open")
        if not trade.entry_token_lamports:
            return ManualCloseResult(success=False, symbol=symbol, error="No recorded token amount — cannot size a sell")
        entry_token_lamports = trade.entry_token_lamports
        position_usd = trade.position_usd

    engine = ExecutionEngine(
        paper_override=False,
        wallet_private_key_override=settings.CONFLUENCE_LIVE_WALLET_PRIVATE_KEY,
        rpc_url_override=settings.HELIUS_RPC_URL,
    )
    log.info("manual_actions.close_attempted", trade_id=trade_id, symbol=symbol, mint=mint)
    result = await engine.sell(mint, Decimal(str(entry_token_lamports)), trade_id=trade_id, symbol=symbol)
    if not result.success:
        log.error("manual_actions.close_failed", trade_id=trade_id, symbol=symbol,
                  error_type=result.error_type, error_detail=result.error_detail)
        return ManualCloseResult(success=False, symbol=symbol,
                                  error=f"{result.error_type}: {result.error_detail}")

    exit_proceeds_usd = (Decimal(str(result.actual_amount)) / Decimal("1e9")) * sol_price_usd if result.actual_amount else None
    pnl_usd = (exit_proceeds_usd - position_usd) if exit_proceeds_usd is not None else None
    now = datetime.now(timezone.utc)

    async with get_session() as session:
        row = (await session.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.id == trade_uuid))).scalar_one_or_none()
        if row is None or row.status != "open":
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
        row.exit_price = result.actual_price if result.actual_price else Decimal("0")
        row.exit_reason = "MANUAL_CLOSE"
        row.exit_sol_lamports = int(result.actual_amount) if result.actual_amount else None
        row.exit_tx_signature = result.tx_signature
        row.pnl_usd = pnl_usd
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
