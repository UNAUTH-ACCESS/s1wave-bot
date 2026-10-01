"""Shadow positions with modeled executable fills (2026-10-01; no network)."""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from config.settings import settings
from engine import shadow_exec_model as sem
from models.orm import (
    ConfluenceShadowExecCheck, ConfluenceShadowPosition, MomentumSignalEvent, Token, TokenStatus,
)
from workers.confluence_shadow_worker import ConfluenceShadowWorker

W = "workers.confluence_shadow_worker"


def _token():
    return Token(mint_address=f"Mint{uuid.uuid4().hex[:36]}", symbol="T", status=TokenStatus.WATCHING,
                 discovered_at=datetime.now(timezone.utc) - timedelta(minutes=10))


def _patch_session(session):
    ctx = patch(f"{W}.get_session")
    m = ctx.start()
    m.return_value.__aenter__ = AsyncMock(return_value=session)
    m.return_value.__aexit__ = AsyncMock(return_value=False)
    return ctx


async def _open_position(session):
    tok = _token()
    session.add(tok)
    await session.flush()
    pos = ConfluenceShadowPosition(
        token_id=tok.id, experiment_version="confluence_entry_v1", entry_price=Decimal("1.00"),
        entry_time=datetime.now(timezone.utc) - timedelta(minutes=1), n_rules_cofiring=2, status="open",
        exec_status="modeled", exec_notional_usd=Decimal("0.40"), exec_entry_liq_usd=Decimal("20000"),
    )
    session.add(pos)
    await session.flush()
    d = dict(id=pos.id, mint=tok.mint_address, entry_price=pos.entry_price, entry_time=pos.entry_time,
             trailing_stop_floor=None, high_watermark_price=None, exec_status="modeled",
             exec_notional_usd=Decimal("0.40"), exec_entry_liq_usd=Decimal("20000"))
    return pos, d


def test_model_thin_pool_pays_far_less_than_mid():
    fee = Decimal("0.005")
    deep, _ = sem.sell_proceeds(Decimal("0.4"), sem.side_reserve(Decimal("20000")), fee)
    thin, impact = sem.sell_proceeds(Decimal("0.4"), sem.side_reserve(Decimal("0.8")), fee)
    assert deep / Decimal("0.4") > Decimal("0.99")
    assert thin / Decimal("0.4") < Decimal("0.6") and impact > Decimal("0.4")
    assert sem.side_reserve(None) is None and sem.side_reserve(Decimal("0")) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("liq,expected", [(Decimal("25000"), "modeled"), (None, "no_quote")])
async def test_entry_models_fill_from_signal_liquidity(session, monkeypatch, liq, expected):
    tok = _token()
    session.add(tok)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    session.add(MomentumSignalEvent(
        token_id=tok.id, experiment_version="momentum_confluence_v1", triggered_at=datetime.now(timezone.utc),
        trigger_price=Decimal("0.001"), trailing_return_3min=Decimal("0.06"), n_rules_cofiring=2,
        liquidity_usd=liq,
    ))
    await session.flush()
    ctx = _patch_session(session)
    try:
        await worker._open_new_positions()
    finally:
        ctx.stop()
    pos = (await session.execute(select(ConfluenceShadowPosition).where(
        ConfluenceShadowPosition.token_id == tok.id))).scalar_one()
    assert pos.status == "open" and pos.exec_status == expected
    if liq:
        assert pos.exec_entry_liq_usd == liq and pos.exec_notional_usd > 0


@pytest.mark.asyncio
async def test_healthy_pool_does_not_trip_guard(session):
    pos, d = await _open_position(session)
    worker = ConfluenceShadowWorker(asyncio.Event())
    ctx = _patch_session(session)
    try:
        await worker._maybe_close(d, Decimal("1.01"), datetime.now(timezone.utc), Decimal("20000"))
    finally:
        ctx.stop()
    await session.flush()
    await session.refresh(pos)
    assert pos.status == "open"


@pytest.mark.asyncio
async def test_collapsed_liquidity_closes_with_modeled_pnl(session, monkeypatch):
    monkeypatch.setattr(settings, "SOL_PRICE_USD", 100.0)
    pos, d = await _open_position(session)
    worker = ConfluenceShadowWorker(asyncio.Event())
    ctx = _patch_session(session)
    try:
        # snapshot says +5% but the pool is now ~$0.80 deep
        await worker._maybe_close(d, Decimal("1.05"), datetime.now(timezone.utc), Decimal("0.80"))
    finally:
        ctx.stop()
    await session.flush()
    await session.refresh(pos)
    assert pos.status == "closed" and pos.exit_reason == "LIQUIDITY_GUARD"
    assert pos.pnl_pct < Decimal("-0.3")  # modeled, not the +5% snapshot
    assert pos.exec_pnl_pct < pos.pnl_pct  # net of network fees


@pytest.mark.asyncio
async def test_normal_exit_records_net_modeled_pnl(session):
    pos, d = await _open_position(session)
    worker = ConfluenceShadowWorker(asyncio.Event())
    worker._last_exec_check[pos.id] = 10**12  # suppress guard; exit model only
    ctx = _patch_session(session)
    try:
        await worker._maybe_close(d, Decimal("0.50"), datetime.now(timezone.utc), Decimal("20000"))
    finally:
        ctx.stop()
    await session.flush()
    await session.refresh(pos)
    assert pos.status == "closed" and pos.exit_reason == "HARD_FLOOR"
    assert Decimal("-0.56") < pos.exec_pnl_pct < Decimal("-0.50")  # -50% price, fees + tiny impact
