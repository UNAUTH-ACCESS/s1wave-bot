"""Shadow positions priced with executable Jupiter quotes (2026-10-01)."""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from config.settings import settings
from engine.jupiter_quotes import Quote, QuoteResult
from models.orm import (
    ConfluenceShadowExecCheck, ConfluenceShadowPosition, MomentumSignalEvent, Token, TokenStatus,
)
from workers.confluence_shadow_worker import ConfluenceShadowWorker

W = "workers.confluence_shadow_worker"


def _token():
    return Token(mint_address=f"Mint{uuid.uuid4().hex[:36]}", symbol="T", status=TokenStatus.WATCHING,
                 discovered_at=datetime.now(timezone.utc) - timedelta(minutes=10))


def _q(out, impact="0.01", inn=3_000_000):
    return Quote(inn, out, impact, Decimal(impact))


def _patch_session(session):
    ctx = patch(f"{W}.get_session")
    m = ctx.start()
    m.return_value.__aenter__ = AsyncMock(return_value=session)
    m.return_value.__aexit__ = AsyncMock(return_value=False)
    return ctx


async def _open_position(session, **kw):
    tok = _token()
    session.add(tok)
    await session.flush()
    pos = ConfluenceShadowPosition(
        token_id=tok.id, experiment_version="confluence_entry_v1", entry_price=Decimal("1.00"),
        entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), n_rules_cofiring=2, status="open",
        exec_status="quoted", exec_entry_lamports=3_000_000, exec_entry_tokens_raw=5_000_000, **kw,
    )
    session.add(pos)
    await session.flush()
    d = dict(id=pos.id, mint=tok.mint_address, entry_price=pos.entry_price, entry_time=pos.entry_time,
             trailing_stop_floor=None, high_watermark_price=None, exec_status="quoted",
             exec_entry_lamports=3_000_000, exec_entry_tokens_raw=5_000_000)
    return pos, d


@pytest.mark.asyncio
async def test_entry_takes_executable_quote(session, monkeypatch):
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 100.0)
    tok = _token()
    session.add(tok)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    session.add(MomentumSignalEvent(
        token_id=tok.id, experiment_version="momentum_confluence_v1", triggered_at=datetime.now(timezone.utc),
        trigger_price=Decimal("0.001"), trailing_return_3min=Decimal("0.06"), n_rules_cofiring=2,
    ))
    await session.flush()
    ctx = _patch_session(session)
    try:
        with patch(f"{W}.jq.quote_buy", AsyncMock(return_value=QuoteResult(_q(7_000_000, "1"), "ok"))):
            await worker._open_new_positions()
    finally:
        ctx.stop()
    pos = (await session.execute(select(ConfluenceShadowPosition).where(
        ConfluenceShadowPosition.token_id == tok.id))).scalar_one()
    assert pos.status == "open" and pos.exec_status == "quoted"
    assert pos.exec_entry_tokens_raw == 7_000_000
    chk = (await session.execute(select(ConfluenceShadowExecCheck))).scalars().all()
    assert any(c.kind == "entry" and c.price_impact_raw == "1" for c in chk)


@pytest.mark.asyncio
async def test_entry_with_no_route_is_skipped(session, monkeypatch):
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 100.0)
    tok = _token()
    session.add(tok)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    session.add(MomentumSignalEvent(
        token_id=tok.id, experiment_version="momentum_confluence_v1", triggered_at=datetime.now(timezone.utc),
        trigger_price=Decimal("0.001"), trailing_return_3min=Decimal("0.06"), n_rules_cofiring=2,
    ))
    await session.flush()
    ctx = _patch_session(session)
    try:
        with patch(f"{W}.jq.quote_buy", AsyncMock(return_value=QuoteResult(None, "no_route"))):
            await worker._open_new_positions()
    finally:
        ctx.stop()
    pos = (await session.execute(select(ConfluenceShadowPosition).where(
        ConfluenceShadowPosition.token_id == tok.id))).scalar_one()
    assert pos.status == "no_route_skip"


@pytest.mark.asyncio
async def test_rate_limited_entry_fails_open_and_is_flagged(session, monkeypatch):
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 100.0)
    tok = _token()
    session.add(tok)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    session.add(MomentumSignalEvent(
        token_id=tok.id, experiment_version="momentum_confluence_v1", triggered_at=datetime.now(timezone.utc),
        trigger_price=Decimal("0.001"), trailing_return_3min=Decimal("0.06"), n_rules_cofiring=2,
    ))
    await session.flush()
    ctx = _patch_session(session)
    try:
        with patch(f"{W}.jq.quote_buy", AsyncMock(return_value=QuoteResult(None, "rate_limited"))):
            await worker._open_new_positions()
    finally:
        ctx.stop()
    pos = (await session.execute(select(ConfluenceShadowPosition).where(
        ConfluenceShadowPosition.token_id == tok.id))).scalar_one()
    assert pos.status == "open" and pos.exec_status == "no_quote"


@pytest.mark.asyncio
async def test_sentinel_impact_with_healthy_proceeds_does_not_close(session):
    pos, d = await _open_position(session)
    worker = ConfluenceShadowWorker(asyncio.Event())
    ctx = _patch_session(session)
    try:
        # proceeds 97% of cost, impact sentinel "1": must NOT be a crisis
        with patch(f"{W}.jq.quote_sell", AsyncMock(return_value=QuoteResult(_q(2_910_000, "1"), "ok"))):
            await worker._maybe_close(d, Decimal("1.01"), datetime.now(timezone.utc))
    finally:
        ctx.stop()
    await session.flush()
    await session.refresh(pos)
    assert pos.status == "open"


@pytest.mark.asyncio
async def test_real_crisis_closes_with_executable_pnl(session):
    pos, d = await _open_position(session)
    worker = ConfluenceShadowWorker(asyncio.Event())
    ctx = _patch_session(session)
    try:
        with patch(f"{W}.jq.quote_sell", AsyncMock(return_value=QuoteResult(_q(300_000, "1"), "ok"))):
            await worker._maybe_close(d, Decimal("1.05"), datetime.now(timezone.utc))
    finally:
        ctx.stop()
    await session.flush()
    await session.refresh(pos)
    assert pos.status == "closed" and pos.exit_reason == "LIQUIDITY_GUARD"
    assert pos.pnl_pct == Decimal("-0.9")  # executable, not the +5% snapshot
    assert pos.exec_exit_lamports == 300_000
    assert pos.exec_pnl_pct < Decimal("-0.9")  # net of fees


@pytest.mark.asyncio
async def test_normal_exit_records_net_executable_pnl(session):
    pos, d = await _open_position(session)
    worker = ConfluenceShadowWorker(asyncio.Event())
    worker._last_exec_check[pos.id] = 10**12  # suppress guard; exit quote only
    ctx = _patch_session(session)
    try:
        # hard floor on snapshot (-50%); exit quote pays 3.3M on 3.0M in
        with patch(f"{W}.jq.quote_sell", AsyncMock(return_value=QuoteResult(_q(3_300_000), "ok"))):
            await worker._maybe_close(d, Decimal("0.50"), datetime.now(timezone.utc))
    finally:
        ctx.stop()
    await session.flush()
    await session.refresh(pos)
    assert pos.status == "closed" and pos.exit_reason == "HARD_FLOOR"
    fees = 2 * settings.SHADOW_EXEC_FEE_LAMPORTS_PER_SIDE
    assert pos.exec_pnl_pct == (Decimal(3_300_000 - 3_000_000 - fees) / Decimal(3_000_000)).quantize(Decimal("0.000001"))
