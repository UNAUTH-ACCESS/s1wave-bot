"""Exit-rule variants on shadow positions (2026-10-01; no network)."""
from __future__ import annotations

import asyncio
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from engine import exit_variants as xv
from models.orm import (
    ConfluenceShadowPosition, ConfluenceShadowVariantPosition, MomentumSignalEvent, Token, TokenStatus,
)
from workers.confluence_shadow_worker import ConfluenceShadowWorker

W = "workers.confluence_shadow_worker"
T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)


def test_base_variant_matches_live_rule_on_random_paths():
    worker = ConfluenceShadowWorker(asyncio.Event())
    base = xv.base_spec()
    rng = random.Random(7)
    for _ in range(300):
        entry = Decimal("1.0")
        floor = hwm = None
        floor_b = hwm_b = None
        price = entry
        for k in range(200):
            price = max(Decimal("0.01"), price * Decimal(str(round(rng.uniform(0.94, 1.07), 4))))
            now = T0 + timedelta(seconds=k * 150)  # crosses the 6h hold in some paths
            r1, f1, h1 = worker._check_exit(entry, T0, price, now, floor_b or xv.initial_floor(base, entry),
                                            hwm_b or entry)
            r2, f2, h2 = xv.step_exit(base, entry, T0, price, now, floor, hwm)
            assert (r1, f1, h1) == (r2, f2, h2)
            floor_b, hwm_b, floor, hwm = f1, h1, f2, h2
            if r1:
                break


def test_variant_knobs_differ_as_intended():
    specs = {s.name: s for s in xv.variant_specs()}
    e = Decimal("1.0")
    # -10% tick: tight_stop (hard floor -10%) exits, base and wide_stop hold
    r = {n: xv.step_exit(s, e, T0, Decimal("0.90"), T0, None, None)[0] for n, s in specs.items()}
    assert r["tight_stop"] == "HARD_FLOOR" and r["base"] is None and r["wide_stop"] is None
    # +60%: tp_50 sells, base keeps trailing
    assert xv.step_exit(specs["tp_50"], e, T0, Decimal("1.60"), T0, None, None)[0] == "TAKE_PROFIT"
    assert xv.step_exit(specs["base"], e, T0, Decimal("1.60"), T0, None, None)[0] is None
    # no_trail never ratchets: up 50% then back to 0.95 is held; base would have exited
    nt = specs["no_trail"]
    _, f, h = xv.step_exit(nt, e, T0, Decimal("1.50"), T0, None, None)
    assert f == xv.initial_floor(nt, e)
    assert xv.step_exit(specs["base"], e, T0, Decimal("0.95"), T0, Decimal("1.3"), Decimal("1.5"))[0] == "TRAILING_STOP"
    assert xv.step_exit(nt, e, T0, Decimal("0.95"), T0, f, h)[0] is None
    # hold_20m
    assert xv.step_exit(specs["hold_20m"], e, T0, Decimal("1.0"), T0 + timedelta(minutes=21), None, None)[0] == "TIME_EXIT"
    assert xv.step_exit(specs["hold_1h"], e, T0, Decimal("1.0"), T0 + timedelta(minutes=21), None, None)[0] is None


def _patch_session(session):
    ctx = patch(f"{W}.get_session")
    m = ctx.start()
    m.return_value.__aenter__ = AsyncMock(return_value=session)
    m.return_value.__aexit__ = AsyncMock(return_value=False)
    return ctx


@pytest.mark.asyncio
async def test_entry_creates_variants_and_they_exit_independently(session):
    tok = Token(mint_address=f"Mint{uuid.uuid4().hex[:36]}", symbol="T", status=TokenStatus.WATCHING,
                discovered_at=datetime.now(timezone.utc) - timedelta(minutes=10))
    session.add(tok)
    await session.flush()
    worker = ConfluenceShadowWorker(asyncio.Event())
    session.add(MomentumSignalEvent(
        token_id=tok.id, experiment_version="momentum_confluence_v1", triggered_at=datetime.now(timezone.utc),
        trigger_price=Decimal("1.00"), trailing_return_3min=Decimal("0.06"), n_rules_cofiring=2,
        liquidity_usd=Decimal("20000"),
    ))
    await session.flush()
    ctx = _patch_session(session)
    try:
        await worker._open_new_positions()
        await session.flush()
        hosts = await worker._load_open_variants()
        assert len(hosts) == 1
        host = hosts[tok.mint_address]
        host["pos"]["entry_time"] = host["pos"]["entry_time"].replace(tzinfo=timezone.utc)  # sqlite is naive
        assert {v["variant"] for v in host["variants"]} == {s.name for s in xv.variant_specs()} - {"base"}
        # -10% healthy-pool tick: tight_stop exits, the others stay open
        await worker._step_variants(host, host["pos"]["entry_price"] * Decimal("0.90"),
                                    datetime.now(timezone.utc), Decimal("20000"))
    finally:
        ctx.stop()
    await session.flush()
    rows = {r.variant: r for r in (await session.execute(select(ConfluenceShadowVariantPosition))).scalars()}
    assert rows["tight_stop"].status == "closed" and rows["tight_stop"].exit_reason == "HARD_FLOOR"
    assert rows["tight_stop"].exec_pnl_pct is not None and rows["tight_stop"].exec_pnl_pct < Decimal("-0.09")
    # base-rule floor (-12%) is not breached at -10%; wide_stop neither
    assert rows["wide_stop"].status == "open" and rows["fine_step"].status == "open"
