"""
api/app.py
==========
SolanaBot v3 — Read-only FastAPI dashboard  (Phase 9)

Endpoints
---------
Confluence (the current, real strategy — see api/app.py's own section
comment below for why the ones after it are frozen/historical):
GET /confluence/status          — kill switch, wallet balance, equity, gates
GET /confluence/live/trades     — real, on-chain confluence_entry_v1 trades
GET /confluence/live/stats      — real-money win rate/rug rate, current-filters vs all-time (2026-09-25)
GET /confluence/notifications   — in-app notification history (2026-09-24)
GET /confluence/stream          — SSE: live status + open trades w/ live
                                   price + notifications, ~1s cadence (2026-09-24)
GET /confluence/shadow/stats    — paper-benchmark win rate, rug rate, etc.
GET /confluence/shadow/positions — paper-only open/closed positions
GET /confluence/shadow/curve    — cumulative pnl_pct series for a chart
GET /confluence/summary         — one-shot plain-text report, e.g. `curl
                                   .../confluence/summary` from a phone
                                   terminal (2026-09-28)

Legacy (old scorer/S1Wave pipeline, removed 2026-09-23 — these tables are
frozen historical data, not live):
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

import asyncio
import json
import re
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, PlainTextResponse, StreamingResponse
from sqlalchemy import select, func, desc, and_

from engine.execution import ExecutionEngine
from engine.halt_override import acknowledge_halt, apply_override, get_halt_override
from engine.live_equity import DEPOSIT_USD, compute_equity_usd, is_daily_halted, is_permanently_halted
from engine.manual_actions import close_trade_manually
from workers.entry_filters import CURRENT_FILTER_REGIME_SINCE

from config.logging import get_logger, log_file_path
from config.settings import settings
from database.engine import get_session
from models.orm import (
    BalanceHistory,
    CircuitBreakerState,
    ConfluenceLiveObservation,
    ConfluenceLiveTrade,
    ConfluenceNotification,
    ConfluenceShadowPosition,
    DailyLossState,
    ExitReason,
    MomentumSignalEvent,
    SessionState,
    Token,
    TokenSnapshot,
    TokenStatus,
    Trade,
    TradeStatus,
)

log = get_logger(__name__)

_START_TIME = datetime.now(timezone.utc)
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _persist_env_var(key: str, value: str) -> None:
    """Rewrite one KEY=value line in .env in place, preserving every other
    line untouched (2026-09-24, for POST /confluence/toggle) — so a manual
    dashboard action survives a crash/restart rather than only affecting the
    current in-memory process. Appends the line if the key isn't present
    yet rather than silently no-op'ing."""
    lines = _ENV_PATH.read_text().splitlines(keepends=True)
    pattern = re.compile(rf"^{re.escape(key)}=")
    out = []
    found = False
    for line in lines:
        if pattern.match(line):
            out.append(f"{key}={value}\n")
            found = True
        else:
            out.append(line)
    if not found:
        out.append(f"{key}={value}\n")
    _ENV_PATH.write_text("".join(out))


async def _build_confluence_status() -> dict:
    """Overall confluence_entry_v1 status: kill switch, wallet, equity,
    concurrency, and which safety gate (if any) is currently blocking new
    entries. Equity is the LIVE wallet balance (engine/live_equity.py,
    2026-09-24) — the same shared formula workers/confluence_live_worker.py
    enforces, so this can never silently drift from what actually gates
    trading the way a separately-reimplemented copy could. Shared by
    GET /confluence/status and GET /confluence/stream (SSE) so both read
    the exact same snapshot logic."""
    async with get_session() as session:
        all_time_pnl = (await session.execute(
            select(func.coalesce(func.sum(ConfluenceLiveTrade.pnl_usd), 0))
            .where(ConfluenceLiveTrade.status == "closed")
        )).scalar_one()
        all_time_pnl = Decimal(str(all_time_pnl))

        start_of_day = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_pnl = (await session.execute(
            select(func.coalesce(func.sum(ConfluenceLiveTrade.pnl_usd), 0))
            .where(ConfluenceLiveTrade.status == "closed", ConfluenceLiveTrade.exit_time >= start_of_day)
        )).scalar_one()
        today_pnl = Decimal(str(today_pnl))

        open_count = (await session.execute(
            select(func.count()).select_from(ConfluenceLiveTrade).where(ConfluenceLiveTrade.status == "open")
        )).scalar_one()

        closed_count = (await session.execute(
            select(func.count()).select_from(ConfluenceLiveTrade).where(ConfluenceLiveTrade.status == "closed")
        )).scalar_one()

        # 'unsellable' (2026-09-24) — see confluence_live_worker.py's
        # _UNSELLABLE_AFTER_S comment: a position whose real sell kept
        # failing long enough is downgraded here, freeing its concurrency
        # slot (open_trades below stays exactly 'open'-only, matching what
        # the worker's own gate counts) while remaining real, tracked
        # money — surfaced as its own count so it's never just invisible.
        unsellable_count = (await session.execute(
            select(func.count()).select_from(ConfluenceLiveTrade).where(ConfluenceLiveTrade.status == "unsellable")
        )).scalar_one()

        # 'balance_zero' (2026-09-25) — see confluence_live_worker.py's
        # balance-already-zero reconciliation: a position confirmed already
        # resolved on-chain with real proceeds unknown without a manual
        # forensic lookup. Was previously invisible on the dashboard —
        # surfaced here the same way 'unsellable' already is, so it's
        # never silently forgotten.
        balance_zero_count = (await session.execute(
            select(func.count()).select_from(ConfluenceLiveTrade).where(ConfluenceLiveTrade.status == "balance_zero")
        )).scalar_one()

        # Real, on-chain-verified account reconciliation (2026-09-28) — see
        # engine/live_equity.py's DEPOSIT_USD docstring for the full audit:
        # all_time_realized_pnl_usd above (a sum of per-trade pnl_usd,
        # itself built from INTENDED swap amounts) understated the real
        # loss by ~6x. real_pnl_known_trades / closed_count tells the
        # dashboard how much of the picture is backed by verified on-chain
        # data vs. trades closed before this fix shipped.
        real_pnl_sum = (await session.execute(
            select(func.coalesce(func.sum(ConfluenceLiveTrade.real_pnl_usd), 0))
            .where(ConfluenceLiveTrade.status == "closed")
        )).scalar_one()
        real_pnl_known_trades = (await session.execute(
            select(func.count()).select_from(ConfluenceLiveTrade)
            .where(ConfluenceLiveTrade.status == "closed", ConfluenceLiveTrade.real_pnl_usd.is_not(None))
        )).scalar_one()

    wallet_sol: float | None = None
    wallet_error: str | None = None
    wallet_address: str | None = None
    if settings.CONFLUENCE_LIVE_WALLET_PRIVATE_KEY:
        try:
            from solana.rpc.async_api import AsyncClient
            from solders.keypair import Keypair
            import base58
            key_bytes = base58.b58decode(settings.CONFLUENCE_LIVE_WALLET_PRIVATE_KEY)
            pubkey = Keypair.from_bytes(key_bytes).pubkey()
            wallet_address = str(pubkey)
            client = AsyncClient(settings.HELIUS_RPC_URL)
            resp = await client.get_balance(pubkey)
            wallet_sol = resp.value / 1e9
            await client.close()
        except Exception as exc:
            wallet_error = str(exc)

    equity = compute_equity_usd(
        Decimal(str(wallet_sol)) if wallet_sol is not None else None,
        Decimal(str(settings.SOL_PRICE_USD)),
    )
    daily_limit = None
    if equity is not None:
        start_of_day_equity = max(Decimal("0"), equity - today_pnl)
        daily_limit = start_of_day_equity * Decimal(str(settings.CONFLUENCE_LIVE_DAILY_LOSS_LIMIT_PCT))

    # Resume-after-halt override (2026-09-28) — see engine/halt_override.py.
    override = await get_halt_override()
    effective_deposit, effective_pnl = apply_override(override, DEPOSIT_USD, all_time_pnl)

    return {
        "enabled": settings.CONFLUENCE_LIVE_ENABLED,
        "wallet_address": wallet_address,
        "wallet_sol": wallet_sol,
        "wallet_usd": round(wallet_sol * settings.SOL_PRICE_USD, 2) if wallet_sol is not None and settings.SOL_PRICE_USD else None,
        "wallet_error": wallet_error,
        "sol_price_usd": settings.SOL_PRICE_USD,
        "equity_usd": str(equity.quantize(Decimal("0.01"))) if equity is not None else None,
        "all_time_realized_pnl_usd": str(all_time_pnl.quantize(Decimal("0.01"))),
        "today_realized_pnl_usd": str(today_pnl.quantize(Decimal("0.01"))),
        "open_trades": open_count,
        "unsellable_trades": unsellable_count,
        "balance_zero_trades": balance_zero_count,
        "closed_trades": closed_count,
        "max_concurrent": settings.CONFLUENCE_LIVE_MAX_CONCURRENT,
        "exposure_pct": settings.CONFLUENCE_LIVE_EXPOSURE_PCT,
        "permanently_halted": is_permanently_halted(equity, effective_pnl, deposit_usd=effective_deposit),
        "halt_override_active": override is not None,
        "halt_override_acknowledged_at": override.acknowledged_at.isoformat() if override else None,
        "daily_halted": is_daily_halted(equity, today_pnl),
        "daily_loss_limit_usd": str(daily_limit.quantize(Decimal("0.01"))) if daily_limit is not None else None,
        "min_tradeable_usd": settings.CONFLUENCE_LIVE_MIN_TRADEABLE_USD,
        "max_loss_pct": settings.CONFLUENCE_LIVE_MAX_LOSS_PCT,
        "max_loss_usd": str((effective_deposit * Decimal(str(settings.CONFLUENCE_LIVE_MAX_LOSS_PCT))).quantize(Decimal("0.01"))),
        # Real account-level reconciliation (2026-09-28) — see
        # engine/live_equity.py's DEPOSIT_USD docstring for the audit this
        # closes. deposit_vs_balance_gap_usd is the ground-truth number:
        # current wallet value minus the one real deposit ever made,
        # unaffected by any per-trade accounting gap. real_all_time_pnl_usd
        # sums the verified-on-chain per-trade figure (only available for
        # trades closed after this fix shipped — see real_pnl_known_trades).
        "deposit_usd": str(DEPOSIT_USD.quantize(Decimal("0.01"))),
        "deposit_vs_balance_gap_usd": (
            str((equity - DEPOSIT_USD).quantize(Decimal("0.01"))) if equity is not None else None
        ),
        "real_all_time_pnl_usd": str(Decimal(str(real_pnl_sum)).quantize(Decimal("0.01"))),
        "real_pnl_known_trades": real_pnl_known_trades,
    }


def _compute_pct_stats(rows: list[tuple]) -> dict:
    """
    Shared win-rate/rug-rate/mean-pnl computation (2026-09-25), used by
    both the shadow and live stats endpoints so the two are computed the
    same way and can never quietly drift apart. `rows` is a list of
    (pnl_pct, exit_reason) tuples — pnl_pct as a plain float or None.

    Two trades in the shadow dataset are single-tick jumps to a price
    still live hours later with real volume (not reverted glitches) but
    both show $0 recorded liquidity — a real sell order would not
    realistically fill near that price. Capping each trade's counted gain
    at +100% (see analysis/sl_tp_and_concurrency_sweep.py's
    REALISTIC_FILL_CAP) is the honest number for decision-making; the raw
    mean is reported too, but the capped one is what should actually be
    trusted.
    """
    n = len(rows)
    pnls = [float(p) for p, _ in rows if p is not None]
    wins = [p for p in pnls if p > 0]
    rugs = [p for p in pnls if p <= -0.40]
    reasons: dict[str, int] = {}
    for _, reason in rows:
        reasons[reason or "unknown"] = reasons.get(reason or "unknown", 0) + 1

    REALISTIC_FILL_CAP = 1.0
    capped = [min(p, REALISTIC_FILL_CAP) for p in pnls]

    return {
        "closed": n,
        "win_rate": round(len(wins) / n, 4) if n else None,
        "median_pnl_pct": round(sorted(pnls)[len(pnls) // 2], 4) if pnls else None,
        "mean_pnl_pct_raw": round(sum(pnls) / len(pnls), 4) if pnls else None,
        "mean_pnl_pct_capped": round(sum(capped) / len(capped), 4) if capped else None,
        "rug_rate": round(len(rugs) / n, 4) if n else None,
        "rug_avg_pnl_pct": round(sum(rugs) / len(rugs), 4) if rugs else None,
        "exit_reasons": reasons,
    }


def _build_fee_breakdown(t: ConfluenceLiveTrade) -> dict:
    """
    Itemized real fee breakdown for one trade (2026-09-28, "all fees and
    capital paid"). network_fee is the real Solana fee actually charged
    (entry + exit combined); other_onchain_cost is whatever's left in the
    real deltas beyond the intended swap amounts and those network fees —
    this wallet's own token-account rent, and/or the non-reclaimable
    Pump.fun protocol-fee account (see ConfluenceLiveTrade.
    entry_real_sol_lamports's docstring in models/orm.py). Every field is
    None unless every input it needs is known — never a partial guess.
    """
    network_fee_lamports = None
    if t.entry_network_fee_lamports is not None or t.exit_network_fee_lamports is not None:
        network_fee_lamports = (t.entry_network_fee_lamports or 0) + (t.exit_network_fee_lamports or 0)

    other_onchain_cost_lamports = None
    if (
        t.entry_real_sol_lamports is not None and t.exit_real_sol_lamports is not None
        and t.entry_sol_lamports is not None and t.exit_sol_lamports is not None
        and network_fee_lamports is not None
    ):
        real_total = abs(t.entry_real_sol_lamports) + abs(t.exit_real_sol_lamports)
        intended_total = abs(t.entry_sol_lamports) + abs(t.exit_sol_lamports)
        other_onchain_cost_lamports = real_total - intended_total - network_fee_lamports

    sol_price = Decimal(str(settings.SOL_PRICE_USD)) if settings.SOL_PRICE_USD else None

    def to_usd(lamports):
        if lamports is None or sol_price is None:
            return None
        return str((Decimal(lamports) / Decimal("1e9") * sol_price).quantize(Decimal("0.0001")))

    return {
        "network_fee_lamports": network_fee_lamports,
        "network_fee_usd": to_usd(network_fee_lamports),
        "other_onchain_cost_lamports": other_onchain_cost_lamports,
        "other_onchain_cost_usd": to_usd(other_onchain_cost_lamports),
        "reclaim_sol_lamports": t.reclaim_sol_lamports,
        "reclaim_usd": to_usd(t.reclaim_sol_lamports),
    }


async def _build_open_trades_live() -> list[dict]:
    """Open confluence_live_trades with a LIVE current price and live
    unrealized P&L — the current price comes from the most recent
    confluence_live_observations row (written every ~1s by the worker's own
    cycle, same data it uses to decide exits), not a separate fetch, so this
    is exactly what the worker itself is seeing. Small N (MAX_CONCURRENT=3
    by design) — one query per trade for its latest observation is simpler
    and plenty fast at this size, vs. a lateral-join query for marginal gain."""
    async with get_session() as session:
        rows = (await session.execute(
            select(ConfluenceLiveTrade, Token.symbol, Token.mint_address)
            .join(Token, Token.id == ConfluenceLiveTrade.token_id)
            # 'unsellable' included (2026-09-24) — a position whose real
            # sell kept failing long enough to free its concurrency slot
            # (see confluence_live_worker.py's _UNSELLABLE_AFTER_S) is
            # still real, tracked money and must stay visible here, never
            # silently disappear from the dashboard the moment its slot
            # frees up. The `status` field lets the frontend badge it.
            .where(ConfluenceLiveTrade.status.in_(["open", "unsellable"]))
            .order_by(ConfluenceLiveTrade.entry_time.asc())
        )).all()

        out = []
        for t, symbol, mint in rows:
            latest_obs = (await session.execute(
                select(ConfluenceLiveObservation.price_usd, ConfluenceLiveObservation.observed_at)
                .where(ConfluenceLiveObservation.trade_id == t.id)
                .order_by(desc(ConfluenceLiveObservation.observed_at))
                .limit(1)
            )).first()
            current_price = latest_obs.price_usd if latest_obs else None
            live_pnl_pct = None
            if current_price is not None and t.entry_price:
                live_pnl_pct = float((current_price - t.entry_price) / t.entry_price * 100)

            # Frozen-price detection (2026-09-24) — current_price_age_s below
            # only measures how recently the worker successfully POLLED, not
            # how long the VALUE has actually been unchanged. Found live: a
            # position sat at the exact same price for 5+ hours (DexScreener
            # kept answering, the underlying pool just went dead) while
            # every single poll refreshed current_price_age_s back to ~0 —
            # the "(stale)" warning (which only fires on polling lag) never
            # fired even once. This looks back for the most recent
            # observation with a DIFFERENT price to find how long the
            # current value has really been frozen.
            price_frozen_for_s = None
            if current_price is not None:
                last_different = (await session.execute(
                    select(ConfluenceLiveObservation.observed_at)
                    .where(
                        ConfluenceLiveObservation.trade_id == t.id,
                        ConfluenceLiveObservation.price_usd != current_price,
                    )
                    .order_by(desc(ConfluenceLiveObservation.observed_at))
                    .limit(1)
                )).scalar_one_or_none()
                frozen_since = last_different or t.entry_time
                price_frozen_for_s = (datetime.now(timezone.utc) - frozen_since).total_seconds()

            out.append({
                "id": str(t.id),
                "symbol": symbol,
                "mint": mint,
                "status": t.status,
                "entry_time": t.entry_time.isoformat(),
                "entry_price": str(t.entry_price),
                "position_usd": str(t.position_usd),
                "current_price": str(current_price) if current_price is not None else None,
                "current_price_age_s": (
                    (datetime.now(timezone.utc) - latest_obs.observed_at).total_seconds() if latest_obs else None
                ),
                "price_frozen_for_s": price_frozen_for_s,
                "live_pnl_pct": round(live_pnl_pct, 2) if live_pnl_pct is not None else None,
                "live_pnl_usd": (
                    round(float(t.position_usd) * live_pnl_pct / 100, 4) if live_pnl_pct is not None else None
                ),
                "trailing_stop_floor": str(t.trailing_stop_floor) if t.trailing_stop_floor is not None else None,
                "high_watermark_price": str(t.high_watermark_price) if t.high_watermark_price is not None else None,
                # Real, verified price (2026-09-25) — an actual Jupiter sell
                # quote for the exact held size, refreshed on the liquidity
                # guard's ~60s cadence (workers/confluence_live_worker.py's
                # _check_liquidity_guard()). This is what the dashboard
                # should show as the trustworthy number: current_price above
                # is a raw DexScreener snapshot that can sit still ("frozen")
                # on thin liquidity even when nothing is wrong — real_price
                # is what a sell would actually get right now. NULL until
                # the first real check completes for a brand-new position.
                "real_price": str(t.real_price) if t.real_price is not None else None,
                "real_pnl_pct": float(t.real_pnl_pct) if t.real_pnl_pct is not None else None,
                "real_price_checked_at": t.real_price_checked_at.isoformat() if t.real_price_checked_at else None,
                "real_price_age_s": (
                    (datetime.now(timezone.utc) - t.real_price_checked_at).total_seconds()
                    if t.real_price_checked_at else None
                ),
            })
        return out


async def _build_recent_notifications(limit: int = 30) -> list[dict]:
    async with get_session() as session:
        rows = (await session.execute(
            select(ConfluenceNotification).order_by(desc(ConfluenceNotification.created_at)).limit(limit)
        )).scalars().all()
    return [
        {
            "id": str(n.id),
            "created_at": n.created_at.isoformat(),
            "level": n.level,
            "event": n.event,
            "message": n.message,
            "trade_id": str(n.trade_id) if n.trade_id else None,
        }
        for n in rows
    ]


def create_app() -> FastAPI:
    app = FastAPI(
        title="SolanaBot v3 Dashboard",
        version="3.0.0",
        description="Read-only monitoring dashboard for SolanaBot v3",
        docs_url="/docs",
        redoc_url=None,
    )

    # HTTP Basic Auth in front of EVERY request (dashboard, every JSON
    # endpoint, /docs, and the /static mount) — see settings.DASHBOARD_AUTH_*
    # comment. Middleware, not a per-route Depends(), specifically so the
    # StaticFiles mount is covered too (a route-level dependency wouldn't
    # touch it). Constant-time comparison on both fields so a partially-
    # correct guess can't be timed apart from a totally wrong one.
    if settings.DASHBOARD_AUTH_USER and settings.DASHBOARD_AUTH_PASSWORD:
        import base64
        import secrets as _secrets

        @app.middleware("http")
        async def basic_auth(request, call_next):
            header = request.headers.get("authorization", "")
            ok = False
            if header.startswith("Basic "):
                try:
                    decoded = base64.b64decode(header[6:]).decode("utf-8")
                    user, _, pw = decoded.partition(":")
                    ok = _secrets.compare_digest(user, settings.DASHBOARD_AUTH_USER) and _secrets.compare_digest(
                        pw, settings.DASHBOARD_AUTH_PASSWORD
                    )
                except Exception:
                    ok = False
            if not ok:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Authentication required."},
                    headers={"WWW-Authenticate": 'Basic realm="S1Wave Dashboard"'},
                )
            return await call_next(request)

    app.mount("/static", StaticFiles(directory="static"), name="static")

    @app.get("/", include_in_schema=False)
    async def dashboard() -> FileResponse:
        return FileResponse("static/s1wave_dashboard.html")

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

    # ── Confluence (current strategy, 2026-09-23) ───────────────────────────
    # Everything above this point queries the OLD scorer/S1Wave pipeline's
    # tables (Trade, DailyLossState, CircuitBreakerState) — that pipeline was
    # removed 2026-09-23; those tables are frozen historical data, not the
    # live system. Below is the real, current strategy.

    @app.get("/confluence/status", tags=["confluence"])
    async def confluence_status() -> dict:
        return await _build_confluence_status()

    @app.post("/confluence/toggle", tags=["confluence"])
    async def confluence_toggle(enabled: bool = Query(...)) -> dict:
        """
        Manually arm/disarm live trading from the dashboard (2026-09-24) —
        the button next to CONFLUENCE_LIVE_ENABLED. Two effects, both real:
          1. Mutates settings.CONFLUENCE_LIVE_ENABLED in-memory for
             IMMEDIATE effect — the worker's _cycle() rereads this setting
             fresh every ~1s, so this takes effect on the very next cycle,
             no restart needed.
          2. Persists the same value to .env (_persist_env_var) so a later
             crash/restart doesn't silently revert to whatever was last
             written to disk — a manual pause must stay paused.
        Same HTTP Basic Auth boundary as the rest of the dashboard secures
        this; there's no separate confirmation at this layer because the
        frontend already confirms before ever calling this to ARM real
        trading (disarming is the safe direction and needs none).
        """
        settings.CONFLUENCE_LIVE_ENABLED = enabled
        _persist_env_var("CONFLUENCE_LIVE_ENABLED", str(enabled))
        async with get_session() as session:
            session.add(ConfluenceNotification(
                level="warning" if enabled else "info",
                event="manual_toggle",
                message=f"Trading manually {'RESUMED' if enabled else 'PAUSED'} from the dashboard.",
            ))
        return {"enabled": settings.CONFLUENCE_LIVE_ENABLED}

    @app.post("/confluence/resume-halted", tags=["confluence"])
    async def confluence_resume_halted() -> dict:
        """
        Reset the drawdown-cap baseline to right now (2026-09-28) — the
        dashboard's RESUME HALTED TRADING button, and ALSO (2026-09-28,
        broadened) usable any time a deposit or other deliberate capital
        change means CONFLUENCE_LIVE_MAX_LOSS_PCT should be measured
        against the new balance instead of a stale one — a real gap found
        the same day a $7 top-up landed while an old halt's baseline
        ($4.33) was still in effect: the 30% cap stayed pinned to the old
        number instead of the fresh $11+ balance until this was called
        again. Originally required `permanently_halted` to be true; that
        restriction is gone since acknowledging a fresh baseline is a
        strictly safe operation whether or not a halt is currently active
        — see engine/halt_override.py and models.orm.ConfluenceLiveHaltOverride
        for the full baseline-reset design. Does NOT touch
        CONFLUENCE_LIVE_ENABLED — if trading was manually paused, it stays
        paused; this only resets is_permanently_halted()'s reference point.
        """
        status = await _build_confluence_status()
        was_halted = status["permanently_halted"]
        if status["equity_usd"] is None:
            raise HTTPException(status_code=503, detail="Wallet balance not available — try again in a moment.")
        equity_usd = Decimal(status["equity_usd"])
        all_time_pnl_usd = Decimal(status["all_time_realized_pnl_usd"])
        await acknowledge_halt(equity_usd, all_time_pnl_usd)
        new_max_loss_usd = equity_usd * Decimal(str(settings.CONFLUENCE_LIVE_MAX_LOSS_PCT))
        async with get_session() as session:
            session.add(ConfluenceNotification(
                level="warning",
                event="halt_resumed",
                message=(
                    (f"Halt acknowledged and trading RESUMED from the dashboard. " if was_halted
                     else "Drawdown-cap baseline recalibrated to the current balance. ") +
                    f"New baseline: equity ${equity_usd:.2f}, all-time P&L ${all_time_pnl_usd:.2f} — "
                    f"the {settings.CONFLUENCE_LIVE_MAX_LOSS_PCT:.0%} drawdown cap (${new_max_loss_usd:.2f}) now "
                    f"protects every dollar from this point forward."
                ),
            ))
        return await _build_confluence_status()

    @app.post("/confluence/withdraw", tags=["confluence"])
    async def confluence_withdraw(
        amount_usd: Decimal = Query(..., gt=0),
        destination_address: str = Query(...),
    ) -> dict:
        """
        Withdraw real SOL from the live trading wallet to an external
        address (2026-09-28) — the dashboard's withdrawal form. A real,
        on-chain, IRREVERSIBLE native System Program transfer
        (engine.execution.ExecutionEngine.withdraw_sol()), completely
        separate from every swap/close path — no token account, no
        Jupiter route. Amount is entered in USD (matching the rest of the
        dashboard) and converted to lamports at the current
        SOL_PRICE_USD; the frontend shows the exact resulting SOL amount
        in its confirmation dialog before ever calling this, since a USD
        estimate and the real lamports sent are two different numbers.

        Automatically re-baselines the halt-override (see
        engine/halt_override.py) to the POST-withdrawal equity — a
        withdrawal is a deliberate capital decision, not a trading loss,
        and must not itself count against CONFLUENCE_LIVE_MAX_LOSS_PCT's
        drawdown cap the way an unexplained balance drop should.
        """
        sol_price = Decimal(str(settings.SOL_PRICE_USD))
        if sol_price <= 0:
            raise HTTPException(status_code=503, detail="SOL price not yet available — try again in a moment")
        destination_address = destination_address.strip()
        if not destination_address:
            raise HTTPException(status_code=400, detail="Destination address is required.")
        amount_lamports = int((amount_usd / sol_price) * Decimal("1e9"))
        if amount_lamports <= 0:
            raise HTTPException(status_code=400, detail="Withdrawal amount is too small to send any lamports.")

        engine = ExecutionEngine(
            paper_override=False,
            wallet_private_key_override=settings.CONFLUENCE_LIVE_WALLET_PRIVATE_KEY,
            rpc_url_override=settings.HELIUS_RPC_URL,
        )
        try:
            tx_signature = await engine.withdraw_sol(destination_address, amount_lamports)
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        async with get_session() as session:
            session.add(ConfluenceNotification(
                level="warning",
                event="withdrawal_sent",
                message=(
                    f"Withdrew ${amount_usd:.2f} ({amount_lamports / 1e9:.6f} SOL) to "
                    f"{destination_address[:4]}...{destination_address[-4:]} — tx {tx_signature}."
                ),
            ))

        # Re-baseline (see this endpoint's docstring) — best-effort: the
        # withdrawal already succeeded and is irreversible either way, so a
        # failure to re-baseline must not read as the withdrawal itself
        # having failed.
        try:
            status = await _build_confluence_status()
            if status["equity_usd"] is not None:
                await acknowledge_halt(
                    Decimal(status["equity_usd"]), Decimal(status["all_time_realized_pnl_usd"]),
                )
        except Exception as exc:
            log.warning("api.withdraw_rebaseline_failed", error_type=type(exc).__name__, error=str(exc))

        return {"success": True, "tx_signature": tx_signature, "amount_lamports": amount_lamports,
                "solscan_url": f"https://solscan.io/tx/{tx_signature}"}

    @app.post("/confluence/live/trades/{trade_id}/close", tags=["confluence"])
    async def confluence_close_trade(trade_id: str) -> dict:
        """
        Manually close one open real position (2026-09-24) — the
        dashboard's per-row "Close" button. Real, on-chain sell via
        engine.manual_actions.close_trade_manually(), the same code path
        this session's operator ran by hand five times in a row (SI,
        adidas, BLK, CTNT, PEPE — the last three specifically because the
        liquidity guard found the dashboard's DexScreener-sourced price
        had already gone stale on them). Same HTTP Basic Auth boundary as
        the rest of the dashboard; the frontend confirms before ever
        calling this, since it's a real, irreversible trade.
        """
        sol_price = Decimal(str(settings.SOL_PRICE_USD))
        if sol_price <= 0:
            raise HTTPException(status_code=503, detail="SOL price not yet available — try again in a moment")
        result = await close_trade_manually(trade_id, sol_price)
        if not result.success:
            raise HTTPException(status_code=400, detail=result.error)
        return {
            "success": True, "symbol": result.symbol,
            "pnl_usd": str(result.pnl_usd) if result.pnl_usd is not None else None,
            "tx_signature": result.tx_signature,
        }

    @app.post("/confluence/live/close-all", tags=["confluence"])
    async def confluence_close_all() -> dict:
        """
        Emergency "close everything" (2026-09-24) — sells every currently
        open real position, one at a time, real transactions. Returns
        per-trade results rather than failing the whole request the
        moment one sell fails — a single stuck position must never block
        closing the rest.
        """
        sol_price = Decimal(str(settings.SOL_PRICE_USD))
        if sol_price <= 0:
            raise HTTPException(status_code=503, detail="SOL price not yet available — try again in a moment")
        async with get_session() as session:
            # 'unsellable' included (2026-09-24) — close-all should try
            # every real, still-tracked position, not just the ones still
            # counted against concurrency.
            open_ids = (await session.execute(
                select(ConfluenceLiveTrade.id).where(ConfluenceLiveTrade.status.in_(["open", "unsellable"]))
            )).scalars().all()
        results = []
        for trade_id in open_ids:
            result = await close_trade_manually(str(trade_id), sol_price)
            results.append({
                "trade_id": str(trade_id), "success": result.success, "symbol": result.symbol,
                "pnl_usd": str(result.pnl_usd) if result.pnl_usd is not None else None,
                "error": result.error,
            })
        return {"closed": sum(1 for r in results if r["success"]), "total": len(results), "results": results}

    @app.get("/confluence/notifications", tags=["confluence"])
    async def confluence_notifications(limit: int = Query(default=30, ge=1, le=200)) -> list[dict]:
        """In-app notification history (2026-09-24) — see
        models.orm.ConfluenceNotification. Persisted so this is still
        visible on a later visit, not just pushed live over the SSE
        stream below."""
        return await _build_recent_notifications(limit)

    @app.get("/confluence/stream", tags=["confluence"])
    async def confluence_stream(request: Request) -> StreamingResponse:
        """
        Server-Sent Events feed (2026-09-24) — the live, no-lag view of the
        money: status/equity, open trades with LIVE current price and
        unrealized P&L, and new in-app notifications, pushed roughly every
        second (matching the live worker's own poll interval — there is no
        point refreshing faster than the underlying data actually changes).
        One event type, 'snapshot', carrying the full current state each
        time — simpler and more robust than diffing, and cheap at this
        payload size. Reuses the exact same _build_confluence_status()/
        _build_open_trades_live() the REST endpoints call, so the live feed
        can never show something different from what a plain GET would.

        nginx (see ~/quantedge/nginx/active.conf) has proxy_buffering off
        for this path specifically — without it, nginx would buffer the
        whole response and every "live" update would arrive in one delayed
        burst instead of as it happens.
        """
        async def event_source():
            last_notification_id = None
            while True:
                if await request.is_disconnected():
                    break
                try:
                    status, open_trades, notifications = await asyncio.gather(
                        _build_confluence_status(), _build_open_trades_live(), _build_recent_notifications(10),
                    )
                    payload = {"status": status, "open_trades": open_trades, "notifications": notifications}
                    yield f"data: {json.dumps(payload)}\n\n"
                except Exception as exc:
                    yield f"data: {json.dumps({'error': str(exc)})}\n\n"
                await asyncio.sleep(1.0)

        return StreamingResponse(
            event_source(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",  # belt-and-suspenders alongside nginx's own proxy_buffering off
                "Connection": "keep-alive",
            },
        )

    @app.get("/confluence/live/stats", tags=["confluence"])
    async def confluence_live_stats() -> dict:
        """
        Real-money equivalent of /confluence/shadow/stats (2026-09-25) —
        same _compute_pct_stats() helper, same current-filters-vs-all-time
        split, so the two can be compared apples-to-apples and neither can
        silently drift from the other's definition of win_rate/rug_rate/etc.
        pnl_pct here is derived (pnl_usd / position_usd * 100) since
        ConfluenceLiveTrade stores absolute dollars, not a percentage.
        """
        async with get_session() as session:
            rows = (await session.execute(
                select(ConfluenceLiveTrade.pnl_usd, ConfluenceLiveTrade.position_usd,
                       ConfluenceLiveTrade.exit_reason, ConfluenceLiveTrade.entry_time)
                .where(ConfluenceLiveTrade.status == "closed")
            )).all()

        def to_pct_rows(source):
            return [
                (float(pnl_usd / position_usd * 100) if pnl_usd is not None and position_usd else None, reason)
                for pnl_usd, position_usd, reason, _ in source
            ]

        all_rows = to_pct_rows(rows)
        current_rows = to_pct_rows([r for r in rows if r[3] >= CURRENT_FILTER_REGIME_SINCE])

        current_stats = _compute_pct_stats(current_rows)
        return {
            "current_filters_since": CURRENT_FILTER_REGIME_SINCE.isoformat(),
            **current_stats,
            "all_time": _compute_pct_stats(all_rows),
        }

    @app.get("/confluence/live/trades", tags=["confluence"])
    async def confluence_live_trades(
        status: str = Query(default="open"),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> list[dict]:
        """Real, on-chain confluence_entry_v1 trades."""
        async with get_session() as session:
            q = select(ConfluenceLiveTrade, Token.symbol, Token.mint_address).join(
                Token, Token.id == ConfluenceLiveTrade.token_id
            )
            if status != "all":
                q = q.where(ConfluenceLiveTrade.status == status)
            q = q.order_by(desc(ConfluenceLiveTrade.entry_time)).limit(limit)
            rows = (await session.execute(q)).all()

        return [
            {
                "id": str(t.id),
                "symbol": symbol,
                "mint": mint,
                "status": t.status,
                "n_rules_cofiring": t.n_rules_cofiring,
                "entry_time": t.entry_time.isoformat(),
                "entry_price": str(t.entry_price),
                "entry_tx": t.entry_tx_signature,
                "exit_time": t.exit_time.isoformat() if t.exit_time else None,
                "exit_price": str(t.exit_price) if t.exit_price is not None else None,
                "exit_reason": t.exit_reason,
                "exit_tx": t.exit_tx_signature,
                "pnl_usd": str(t.pnl_usd) if t.pnl_usd is not None else None,
                "position_usd": str(t.position_usd),
                "error_detail": t.error_detail,
                # See _build_open_trades_live()'s comment — a real Jupiter
                # sell quote for the exact held size, not a market snapshot.
                "real_price": str(t.real_price) if t.real_price is not None else None,
                "real_pnl_pct": float(t.real_pnl_pct) if t.real_pnl_pct is not None else None,
                "real_price_checked_at": t.real_price_checked_at.isoformat() if t.real_price_checked_at else None,
                # Real, on-chain-verified audit trail (2026-09-28) — see
                # ConfluenceLiveTrade.entry_real_sol_lamports's docstring
                # in models/orm.py for the full incident this closes: the
                # fields above (pnl_usd, entry/exit_sol_lamports) are
                # INTENDED amounts and understated real spend by 6.36x in
                # aggregate. These four are read directly from each real
                # transaction's own balances — real_pnl_usd is the number
                # to trust; the others are NULL for trades closed before
                # 2026-09-28 (this session cannot retroactively know them
                # without a manual forensic lookup per trade).
                "entry_real_sol_lamports": t.entry_real_sol_lamports,
                "exit_real_sol_lamports": t.exit_real_sol_lamports,
                "reclaim_tx_signature": t.reclaim_tx_signature,
                "reclaim_sol_lamports": t.reclaim_sol_lamports,
                "real_pnl_usd": str(t.real_pnl_usd) if t.real_pnl_usd is not None else None,
                "real_pnl_pct_of_capital": (
                    str((t.real_pnl_usd / (Decimal(str(abs(t.entry_real_sol_lamports))) / Decimal("1e9") * Decimal(str(settings.SOL_PRICE_USD))) * 100).quantize(Decimal("0.01")))
                    if t.real_pnl_usd is not None and t.entry_real_sol_lamports and settings.SOL_PRICE_USD else None
                ),
                # Itemized fee breakdown (2026-09-28, "all fees and capital
                # paid") — network fee is the real Solana fee per
                # transaction; other_onchain_cost_lamports is whatever's
                # left in the real delta beyond the intended swap amount
                # and the network fee (rent for this wallet's own token
                # account, and/or the non-reclaimable Pump.fun protocol-fee
                # account — see entry_real_sol_lamports's docstring). Only
                # computed when every input is known.
                "fees": _build_fee_breakdown(t),
                "capital_paid_usd": (
                    str((Decimal(str(abs(t.entry_real_sol_lamports))) / Decimal("1e9") * Decimal(str(settings.SOL_PRICE_USD))).quantize(Decimal("0.0001")))
                    if t.entry_real_sol_lamports is not None and settings.SOL_PRICE_USD else None
                ),
                "solscan_url": f"https://solscan.io/token/{mint}",
            }
            for t, symbol, mint in rows
        ]

    @app.get("/confluence/shadow/stats", tags=["confluence"])
    async def confluence_shadow_stats() -> dict:
        """
        Aggregate stats for the confluence_entry_v1 paper benchmark.

        Top-level fields are scoped to entry_time >=
        entry_filters.CURRENT_FILTER_REGIME_SINCE (2026-09-25) — real
        problem this fixes: a single all-time number mixes trades from
        before and after each entry filter shipped, which can look
        misleadingly bad (a live win rate that looked like 36% turned out
        to be 66%, matching this same shadow benchmark, once trades from
        before all 3 current filters existed were excluded). `all_time`
        nests the same shape computed over every closed trade ever, for
        anyone who wants the full history. Update CURRENT_FILTER_REGIME_SINCE
        whenever a filter ships and both views stay honest without anyone
        having to remember to ask for this cut by hand again.
        """
        async with get_session() as session:
            open_count = (await session.execute(
                select(func.count()).select_from(ConfluenceShadowPosition).where(ConfluenceShadowPosition.status == "open")
            )).scalar_one()
            closed = (await session.execute(
                select(ConfluenceShadowPosition.pnl_pct, ConfluenceShadowPosition.exit_reason, ConfluenceShadowPosition.entry_time)
                .where(ConfluenceShadowPosition.status == "closed")
            )).all()

        all_rows = [(p, r) for p, r, _ in closed]
        current_rows = [(p, r) for p, r, t in closed if t >= CURRENT_FILTER_REGIME_SINCE]

        current_stats = _compute_pct_stats(current_rows)
        return {
            "open": open_count,
            "current_filters_since": CURRENT_FILTER_REGIME_SINCE.isoformat(),
            **current_stats,
            "all_time": _compute_pct_stats(all_rows),
        }

    @app.get("/confluence/summary", tags=["confluence"], response_class=PlainTextResponse)
    async def confluence_summary() -> str:
        """
        One-shot, plain-text performance report (2026-09-28) — "things that
        would make me prompt the software from Termux to see its
        performance": reading 3-4 separate JSON endpoints from a phone
        terminal to get the full picture is exactly the friction this
        removes. `curl <host>/confluence/summary` (with the same Basic Auth
        as the dashboard) gives the whole story in one screen: armed state,
        real wallet numbers (including the real-vs-deposit gap, not just
        the recorded figure), open positions, and both live and shadow
        performance since the current entry filters. Reuses the exact same
        building blocks as the JSON endpoints, so it can never show a
        different number than the dashboard does.
        """
        status, open_trades, live_stats, shadow_stats = await asyncio.gather(
            _build_confluence_status(), _build_open_trades_live(), confluence_live_stats(), confluence_shadow_stats(),
        )

        lines = []
        lines.append("S1WAVE — PERFORMANCE SUMMARY")
        lines.append(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append("")
        # A trade can only actually fire when BOTH the manual switch is
        # armed AND no safety gate is tripped — state these as one clear
        # verdict rather than two separately-true facts that read as
        # contradictory ("ARMED — HALTED").
        if status["permanently_halted"]:
            trading_line = "Trading: 🛑 HALTED by safety gate — POST /confluence/resume-halted (or the dashboard button) to clear it"
        elif status["daily_halted"]:
            trading_line = "Trading: ⏸️  PAUSED — daily loss limit hit, resumes automatically next UTC day"
        elif status["enabled"]:
            trading_line = "Trading: ✅ ARMED — may enter a position on the next qualifying signal"
        else:
            trading_line = "Trading: ⏸️  PAUSED — manually switched off from the dashboard"
        lines.append(trading_line)
        lines.append(f"Wallet:  {status['wallet_sol']:.4f} SOL (${status['wallet_usd']:.2f})" if status["wallet_sol"] is not None else "Wallet:  unknown (RPC error)")
        lines.append(f"Deposit: ${status['deposit_usd']} — real gap vs. deposit: ${status['deposit_vs_balance_gap_usd']}")
        real_pnl_note = (
            f"${status['real_all_time_pnl_usd']} across {status['real_pnl_known_trades']}/{status['closed_trades']} trades"
            if status["real_pnl_known_trades"] > 0
            else f"not yet known for any of {status['closed_trades']} trades — needs each one's rent-reclaim tx, "
                 "never captured before 2026-09-28; entry/exit real costs ARE known per-trade, see /confluence/live/trades"
        )
        lines.append(f"Recorded all-time P&L: ${status['all_time_realized_pnl_usd']}  (real, verified: {real_pnl_note})")
        lines.append(f"Today's P&L: ${status['today_realized_pnl_usd']}")
        lines.append("")
        lines.append(f"Positions: {status['open_trades']} open / {status['max_concurrent']} max"
                      + (f", {status['unsellable_trades']} unsellable" if status["unsellable_trades"] else "")
                      + (f", {status['balance_zero_trades']} need reconciliation" if status.get("balance_zero_trades") else "")
                      + f" — {status['closed_trades']} closed all-time")
        if open_trades:
            for t in open_trades:
                price_note = f"verified {round(t['real_price_age_s'])}s ago" if t.get("real_price_age_s") is not None else "checking…"
                pnl = t.get("real_pnl_pct") if t.get("real_price") is not None else t.get("live_pnl_pct")
                pnl_str = f"{pnl:+.1f}%" if pnl is not None else "—"
                lines.append(f"  · {t['symbol'] or t['mint'][:8]}: {pnl_str} (${t['position_usd']}, {price_note})"
                             + ("  [STUCK]" if t["status"] == "unsellable" else ""))
        # NOTE: _compute_pct_stats()'s own fields are NOT uniformly scaled —
        # win_rate/rug_rate are fractions (0.6667 = 66.67%) but
        # mean_pnl_pct_capped/median_pnl_pct are already percentages
        # (-5.2092 means -5.2092%, not -520.92%). _fmt_frac/_fmt_pct below
        # match that existing convention rather than silently "fixing" it
        # here and drifting from what the JSON endpoints report.
        def _fmt_frac(x):
            return f"{x*100:.1f}%" if x is not None else "—"

        def _fmt_pct(x):
            return f"{x:+.1f}%" if x is not None else "—"

        lines.append("")
        lines.append(f"LIVE   (since current filters, {live_stats['closed']} closed): "
                      f"win rate {_fmt_frac(live_stats['win_rate'])}, capped mean {_fmt_pct(live_stats['mean_pnl_pct_capped'])}, rug rate {_fmt_frac(live_stats['rug_rate'])}")
        lines.append(f"       all-time ({live_stats['all_time']['closed']} closed): "
                      f"win rate {_fmt_frac(live_stats['all_time']['win_rate'])}, capped mean {_fmt_pct(live_stats['all_time']['mean_pnl_pct_capped'])}")
        lines.append(f"SHADOW (since current filters, {shadow_stats['closed']} closed): "
                      f"win rate {_fmt_frac(shadow_stats['win_rate'])}, capped mean {_fmt_pct(shadow_stats['mean_pnl_pct_capped'])}, rug rate {_fmt_frac(shadow_stats['rug_rate'])}")
        lines.append(f"       all-time ({shadow_stats['all_time']['closed']} closed): "
                      f"win rate {_fmt_frac(shadow_stats['all_time']['win_rate'])}, capped mean {_fmt_pct(shadow_stats['all_time']['mean_pnl_pct_capped'])}")
        lines.append("")
        lines.append("Full detail: GET /confluence/status, /confluence/live/trades, /confluence/live/stats, /confluence/shadow/stats")
        return "\n".join(lines) + "\n"

    @app.get("/confluence/shadow/positions", tags=["confluence"])
    async def confluence_shadow_positions(
        status: str = Query(default="open"),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> list[dict]:
        """Paper-only confluence_entry_v1 positions (never real money)."""
        async with get_session() as session:
            q = select(ConfluenceShadowPosition, Token.symbol).join(
                Token, Token.id == ConfluenceShadowPosition.token_id
            )
            if status != "all":
                q = q.where(ConfluenceShadowPosition.status == status)
            q = q.order_by(desc(ConfluenceShadowPosition.entry_time)).limit(limit)
            rows = (await session.execute(q)).all()

        return [
            {
                "id": str(p.id),
                "symbol": symbol,
                "status": p.status,
                "n_rules_cofiring": p.n_rules_cofiring,
                "entry_time": p.entry_time.isoformat(),
                "entry_price": str(p.entry_price),
                "exit_time": p.exit_time.isoformat() if p.exit_time else None,
                "exit_price": str(p.exit_price) if p.exit_price is not None else None,
                "exit_reason": p.exit_reason,
                "pnl_pct": str(p.pnl_pct) if p.pnl_pct is not None else None,
            }
            for p, symbol in rows
        ]

    @app.get("/confluence/shadow/curve", tags=["confluence"])
    async def confluence_shadow_curve() -> dict:
        """Chronological closed-position pnl_pct series for a simple client-
        side equity-curve chart. Illustrative only — a straight cumulative
        sum of pnl_pct, not the real compounding/concurrency-aware
        simulation in analysis/trailing_stop_retroactive_replay.py.

        Each point's contribution is capped at +100% (same
        REALISTIC_FILL_CAP used in /confluence/shadow/stats and
        analysis/sl_tp_and_concurrency_sweep.py) — without it, the 2 known
        outlier trades (see that stats endpoint's own comment) alone sum to
        roughly +12,000 percentage points, which would render as one
        vertical spike and hide the actual shape of everything else. Both
        the capped and raw cumulative values are returned so the raw
        number is never hidden, just not what gets plotted as the primary
        line.
        """
        REALISTIC_FILL_CAP = 1.0
        async with get_session() as session:
            rows = (await session.execute(
                select(ConfluenceShadowPosition.exit_time, ConfluenceShadowPosition.pnl_pct)
                .where(ConfluenceShadowPosition.status == "closed")
                .order_by(ConfluenceShadowPosition.exit_time.asc())
            )).all()

        points = []
        cum = 0.0
        cum_raw = 0.0
        for exit_time, pnl_pct in rows:
            p = float(pnl_pct or 0)
            cum += min(p, REALISTIC_FILL_CAP)
            cum_raw += p
            points.append({
                "t": exit_time.isoformat(),
                "cum_pnl_pct": round(cum, 4),
                "cum_pnl_pct_raw": round(cum_raw, 4),
            })
        return {"points": points}

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
