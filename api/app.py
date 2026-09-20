"""
api/app.py
==========
SolanaBot v3 — Read-only FastAPI dashboard  (Phase 9)

Endpoints
---------
GET /health             — bot status, uptime
GET /balance            — current balance, daily PnL, session stats
GET /trades/open        — open positions with unrealised PnL
GET /trades/history     — closed trades, filterable, paginated
GET /tokens/watching    — tokens currently being sampled
GET /tokens/rejected    — recent rejections with reasons
GET /stats              — win rate, avg PnL, best/worst trade
GET /logs/download      — download bot.log as .txt file

All responses are JSON.  No authentication — local use only.
Mount on port 8000 (settings.API_PORT).

Usage in main.py
----------------
    import uvicorn
    from api.app import create_app
    app = create_app()
    config = uvicorn.Config(app, host="0.0.0.0", port=8000)
    server = uvicorn.Server(config)
    await server.serve()
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import select, func, desc, and_

from config.logging import log_file_path
from config.settings import settings
from database.engine import get_session
from models.orm import (
    BalanceHistory,
    CircuitBreakerState,
    DailyLossState,
    ExitReason,
    SessionState,
    Token,
    TokenSnapshot,
    TokenStatus,
    Trade,
    TradeStatus,
)

_START_TIME = datetime.now(timezone.utc)


def create_app() -> FastAPI:
    app = FastAPI(
        title="SolanaBot v3 Dashboard",
        version="3.0.0",
        description="Read-only monitoring dashboard for SolanaBot v3",
        docs_url="/docs",
        redoc_url=None,
    )
    app.mount("/static", StaticFiles(directory="static"), name="static")

    # ── Health ────────────────────────────────────────────────────────────────

    @app.get("/health", tags=["system"])
    async def health() -> dict:
        """Bot status and uptime."""
        uptime_seconds = int(
            (datetime.now(timezone.utc) - _START_TIME).total_seconds()
        )
        h, rem = divmod(uptime_seconds, 3600)
        m, s = divmod(rem, 60)
        return {
            "status": "running",
            "version": "3.0.0",
            "uptime": f"{h}h {m}m {s}s",
            "uptime_seconds": uptime_seconds,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # ── Balance ───────────────────────────────────────────────────────────────

    @app.get("/balance", tags=["capital"])
    async def balance() -> dict:
        """Current balance, daily PnL, circuit breaker, and session stats."""
        async with get_session() as session:
            # Session state
            state_result = await session.execute(
                select(SessionState).where(SessionState.id == 1)
            )
            state = state_result.scalar_one()

            # Daily loss state
            dl_result = await session.execute(
                select(DailyLossState).where(DailyLossState.id == 1)
            )
            dl = dl_result.scalar_one()

            # Circuit breaker
            cb_result = await session.execute(
                select(CircuitBreakerState).where(CircuitBreakerState.id == 1)
            )
            cb = cb_result.scalar_one()

            # Open trade count
            open_count = await session.scalar(
                select(func.count(Trade.id)).where(Trade.status == TradeStatus.OPEN)
            )

            # Total trades
            total_trades = await session.scalar(
                select(func.count(Trade.id)).where(Trade.status == TradeStatus.CLOSED)
            )

            # All-time PnL
            total_pnl = await session.scalar(
                select(func.sum(Trade.pnl_usd)).where(Trade.status == TradeStatus.CLOSED)
            ) or Decimal("0")

        initial = Decimal(str(settings.INITIAL_BALANCE_USD))
        current = state.available_balance
        total_return_pct = (
            ((current - initial) / initial * 100).quantize(Decimal("0.01"))
            if initial > 0 else Decimal("0")
        )

        return {
            "available_balance": str(current),
            "initial_balance": str(initial),
            "total_pnl_usd": str(total_pnl.quantize(Decimal("0.01"))),
            "total_return_pct": str(total_return_pct),
            "open_positions": open_count,
            "closed_trades": total_trades,
            "daily": {
                "date_utc": dl.date_utc.isoformat(),
                "open_balance": str(dl.day_open_balance),
                "pnl_usd": str(dl.cumulative_pnl_usd.quantize(Decimal("0.01"))),
                "is_halted": dl.is_halted,
                "halted_at": dl.halted_at.isoformat() if dl.halted_at else None,
            },
            "circuit_breaker": {
                "consecutive_losses": cb.consecutive_losses,
                "is_paused": cb.is_paused,
                "resume_at": cb.resume_at.isoformat() if cb.resume_at else None,
            },
        }

    # ── Open trades ───────────────────────────────────────────────────────────

    @app.get("/trades/open", tags=["trades"])
    async def trades_open() -> list[dict]:
        """All open positions with unrealised PnL from latest snapshot."""
        async with get_session() as session:
            result = await session.execute(
                select(Trade).where(Trade.status == TradeStatus.OPEN)
                .order_by(Trade.entry_time.desc())
            )
            trades = list(result.scalars().all())

            rows = []
            for trade in trades:
                # Get latest snapshot price for unrealised PnL
                snap_result = await session.execute(
                    select(TokenSnapshot)
                    .join(Token, Token.id == TokenSnapshot.token_id)
                    .where(Token.mint_address == trade.token_mint)
                    .order_by(TokenSnapshot.sampled_at.desc())
                    .limit(1)
                )
                snap = snap_result.scalar_one_or_none()

                current_price = snap.price_usd if snap else None
                unrealised_pnl_usd = None
                unrealised_pnl_pct = None

                if current_price and trade.entry_price > 0:
                    pct = (current_price - trade.entry_price) / trade.entry_price
                    unrealised_pnl_pct = float(round(pct * 100, 2))
                    unrealised_pnl_usd = float(
                        (trade.position_size_usd * pct).quantize(Decimal("0.01"))
                    )

                now = datetime.now(timezone.utc)
                hold_seconds = int((now - trade.entry_time).total_seconds())

                rows.append({
                    "trade_id": str(trade.id),
                    "mint": trade.token_mint,
                    "symbol": await _get_symbol(session, trade.token_mint),
                    "entry_price": str(trade.entry_price),
                    "current_price": str(current_price) if current_price else None,
                    "position_usd": str(trade.position_size_usd),
                    "unrealised_pnl_usd": unrealised_pnl_usd,
                    "unrealised_pnl_pct": unrealised_pnl_pct,
                    "entry_score": str(trade.entry_composite_score),
                    "vm_zone": trade.entry_vm_zone.value,
                    "bp_trend": trade.entry_bp_trend.value,
                    "entry_time": trade.entry_time.isoformat(),
                    "hold_seconds": hold_seconds,
                    "hold_human": _fmt_duration(hold_seconds),
                    "stop_loss_price": str(
                        (trade.entry_price * Decimal(str(1 + settings.STOP_LOSS_PCT)))
                        .quantize(Decimal("0.000001"))
                    ),
                    "take_profit_price": str(
                        (trade.entry_price * Decimal(str(1 + settings.TAKE_PROFIT_PCT)))
                        .quantize(Decimal("0.000001"))
                    ),
                })

        return rows

    # ── Trade history ─────────────────────────────────────────────────────────

    @app.get("/trades/history", tags=["trades"])
    async def trades_history(
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        exit_reason: str | None = Query(default=None),
    ) -> dict:
        """Closed trades, paginated. Optionally filter by exit_reason."""
        async with get_session() as session:
            query = select(Trade).where(Trade.status == TradeStatus.CLOSED)

            if exit_reason:
                try:
                    reason_enum = ExitReason(exit_reason.upper())
                    query = query.where(Trade.exit_reason == reason_enum)
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid exit_reason. Valid: {[e.value for e in ExitReason]}"
                    )

            total = await session.scalar(
                select(func.count()).select_from(query.subquery())
            )

            result = await session.execute(
                query.order_by(desc(Trade.exit_time))
                .limit(limit)
                .offset(offset)
            )
            trades = list(result.scalars().all())

            rows = []
            for trade in trades:
                rows.append({
                    "trade_id": str(trade.id),
                    "mint": trade.token_mint,
                    "symbol": await _get_symbol(session, trade.token_mint),
                    "entry_price": str(trade.entry_price),
                    "exit_price": str(trade.exit_price) if trade.exit_price else None,
                    "position_usd": str(trade.position_size_usd),
                    "pnl_usd": str(trade.pnl_usd.quantize(Decimal("0.01"))) if trade.pnl_usd else None,
                    "pnl_pct": str(round(float(trade.pnl_pct or 0) * 100, 2)),
                    "exit_reason": trade.exit_reason.value if trade.exit_reason else None,
                    "entry_score": str(trade.entry_composite_score),
                    "vm_zone": trade.entry_vm_zone.value,
                    "entry_time": trade.entry_time.isoformat(),
                    "exit_time": trade.exit_time.isoformat() if trade.exit_time else None,
                    "hold_seconds": trade.hold_duration_seconds,
                    "hold_human": _fmt_duration(trade.hold_duration_seconds or 0),
                })

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "trades": rows,
        }

    # ── Watching tokens ───────────────────────────────────────────────────────

    @app.get("/tokens/watching", tags=["tokens"])
    async def tokens_watching() -> list[dict]:
        """Tokens currently in WATCHING status being sampled."""
        async with get_session() as session:
            result = await session.execute(
                select(Token)
                .where(Token.status == TokenStatus.WATCHING)
                .order_by(Token.watch_started_at.desc())
            )
            tokens = list(result.scalars().all())

            rows = []
            for token in tokens:
                # Latest snapshot
                snap_result = await session.execute(
                    select(TokenSnapshot)
                    .where(TokenSnapshot.token_id == token.id)
                    .order_by(TokenSnapshot.sampled_at.desc())
                    .limit(1)
                )
                snap = snap_result.scalar_one_or_none()

                # Snapshot count
                snap_count = await session.scalar(
                    select(func.count(TokenSnapshot.id))
                    .where(TokenSnapshot.token_id == token.id)
                )

                rows.append({
                    "mint": token.mint_address,
                    "symbol": token.symbol,
                    "watch_started_at": token.watch_started_at.isoformat() if token.watch_started_at else None,
                    "liquidity_usd": str(token.liquidity_usd) if token.liquidity_usd else None,
                    "market_cap_usd": str(token.market_cap_usd) if token.market_cap_usd else None,
                    "snapshot_count": snap_count,
                    "latest_price": str(snap.price_usd) if snap and snap.price_usd else None,
                    "latest_volume": str(snap.volume_usd) if snap and snap.volume_usd else None,
                    "latest_buy_pressure": str(snap.buy_pressure) if snap and snap.buy_pressure else None,
                    "latest_sampled_at": snap.sampled_at.isoformat() if snap else None,
                })

        return rows

    # ── Rejected tokens ───────────────────────────────────────────────────────

    @app.get("/tokens/rejected", tags=["tokens"])
    async def tokens_rejected(
        limit: int = Query(default=100, ge=1, le=500),
        reason: str | None = Query(default=None),
    ) -> dict:
        """Recent rejected tokens with rejection reasons."""
        async with get_session() as session:
            query = select(Token).where(Token.status == TokenStatus.REJECTED)

            if reason:
                query = query.where(Token.rejection_reason.ilike(f"%{reason}%"))

            total = await session.scalar(
                select(func.count()).select_from(query.subquery())
            )

            result = await session.execute(
                query.order_by(desc(Token.discovered_at))
                .limit(limit)
            )
            tokens = list(result.scalars().all())

        rows = [{
            "mint": t.mint_address,
            "symbol": t.symbol,
            "rejection_reason": t.rejection_reason,
            "liquidity_usd": str(t.liquidity_usd) if t.liquidity_usd else None,
            "market_cap_usd": str(t.market_cap_usd) if t.market_cap_usd else None,
            "discovered_at": t.discovered_at.isoformat(),
        } for t in tokens]

        return {"total": total, "tokens": rows}

    # ── Stats ─────────────────────────────────────────────────────────────────

    @app.get("/stats", tags=["capital"])
    async def stats() -> dict:
        """Aggregate performance statistics."""
        async with get_session() as session:
            closed = await session.execute(
                select(Trade).where(Trade.status == TradeStatus.CLOSED)
            )
            trades = list(closed.scalars().all())

        if not trades:
            return {"message": "No closed trades yet."}

        total = len(trades)
        wins = [t for t in trades if (t.pnl_usd or 0) > 0]
        losses = [t for t in trades if (t.pnl_usd or 0) <= 0]
        win_rate = round(len(wins) / total * 100, 1)

        pnls = [float(t.pnl_usd or 0) for t in trades]
        avg_pnl = round(sum(pnls) / total, 4)
        total_pnl = round(sum(pnls), 4)
        best = max(pnls)
        worst = min(pnls)

        # By exit reason
        by_reason: dict[str, dict] = {}
        for t in trades:
            r = t.exit_reason.value if t.exit_reason else "UNKNOWN"
            if r not in by_reason:
                by_reason[r] = {"count": 0, "total_pnl": 0.0}
            by_reason[r]["count"] += 1
            by_reason[r]["total_pnl"] = round(
                by_reason[r]["total_pnl"] + float(t.pnl_usd or 0), 4
            )

        # Average hold time
        hold_times = [t.hold_duration_seconds for t in trades if t.hold_duration_seconds]
        avg_hold = round(sum(hold_times) / len(hold_times)) if hold_times else 0

        # Peak balance from balance_history
        async with get_session() as session:
            peak = await session.scalar(
                select(func.max(BalanceHistory.balance_after))
            ) or Decimal(str(settings.INITIAL_BALANCE_USD))

        return {
            "total_trades": total,
            "win_rate_pct": win_rate,
            "wins": len(wins),
            "losses": len(losses),
            "total_pnl_usd": total_pnl,
            "avg_pnl_usd": avg_pnl,
            "best_trade_usd": best,
            "worst_trade_usd": worst,
            "avg_hold_seconds": avg_hold,
            "avg_hold_human": _fmt_duration(avg_hold),
            "peak_balance": str(peak),
            "by_exit_reason": by_reason,
        }

    # ── Log download ──────────────────────────────────────────────────────────

    @app.get("/logs/sessions", tags=["system"])
    async def logs_sessions() -> dict:
        """List all session log files available for download."""
        from config.logging import _LOG_DIR
        log_dir = _LOG_DIR
        if not log_dir.exists():
            return {"sessions": []}

        sessions = sorted(
            [f.name for f in log_dir.glob("session_*.log")],
            reverse=True
        )
        return {"sessions": sessions, "count": len(sessions)}

    @app.get("/logs/download", tags=["system"])
    async def logs_download(session: str = Query(default=None)) -> FileResponse:
        """
        Download a session log file.
        If session is omitted, downloads the current session.
        Use /logs/sessions to list available session filenames.
        """
        from config.logging import _LOG_DIR, _LOG_FILE
        if session:
            log_path = _LOG_DIR / session
            if not log_path.exists() or not log_path.name.startswith("session_"):
                raise HTTPException(status_code=404, detail=f"Session {session} not found.")
            filename = session
        else:
            log_path = _LOG_FILE
            if not log_path.exists():
                raise HTTPException(status_code=404, detail="No active session log found.")
            filename = log_path.name

        return FileResponse(
            path=str(log_path),
            media_type="text/plain",
            filename=filename.replace(".log", ".txt"),
        )

    @app.get("/logs/tail", tags=["system"])
    async def logs_tail(
        lines: int = Query(default=100, ge=1, le=1000)
    ) -> dict:
        """Return the last N lines of the current session log as JSON."""
        from config.logging import _LOG_FILE
        log_path = _LOG_FILE
        if not log_path.exists():
            raise HTTPException(status_code=404, detail="Log file not found.")

        with open(log_path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()

        tail = [line.rstrip() for line in all_lines[-lines:]]
        return {"lines": len(tail), "log": tail}

    return app


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _get_symbol(session, mint: str) -> str | None:
    result = await session.execute(
        select(Token.symbol).where(Token.mint_address == mint)
    )
    return result.scalar_one_or_none()


def _fmt_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


# Module-level app instance — imported by uvicorn in main.py
app = create_app()
