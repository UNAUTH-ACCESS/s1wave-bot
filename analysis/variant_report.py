"""
analysis/variant_report.py — compare exit-rule variants (engine/exit_variants.py).
==================================================================================
READ-ONLY. Two modes:

    cd /home/solana/s1wave-bot/solanabot && source .venv/bin/activate
    S1WAVE_ENV_FILE=.env PYTHONPATH=. python analysis/variant_report.py replay   # recorded 1s paths
    S1WAVE_ENV_FILE=.env PYTHONPATH=. python analysis/variant_report.py live     # variants run by the shadow worker

replay: re-runs every variant over the recorded 1-second paths of CLOSED shadow
  positions (confluence_shadow_observations), entries since the 2026-09-24 filter
  regime. Available immediately, but (a) paths stop when the primary exited, so a
  variant that would have held longer is CENSORED and marked at the last recorded
  price (optimistic/unknown for wide stops: read the 'resolved only' column too),
  (b) snapshot prices, no pool model, a flat per-trade cost is subtracted instead.
live: variants tracked by the worker since deployment, same entries/ticks as the
  primary, modeled fills (exec_pnl_pct). The trustworthy one once n is adequate.

Both: paired difference vs base on the same trades, bootstrap 95% CI, capped at
+100% per trade. The 'verdict' only flags a candidate for human review. Nothing
is ever auto-promoted (see CALIBRATION.md section 6).
"""
from __future__ import annotations

import asyncio
import random
import sys
from datetime import datetime, timezone
from decimal import Decimal
from statistics import mean, median

from sqlalchemy import select

from config.settings import settings
from database.engine import get_session
from engine import exit_variants as xv
from models.orm import (
    ConfluenceShadowObservation, ConfluenceShadowPosition, ConfluenceShadowVariantPosition,
)

CAP, RUG = 1.0, -0.40
SINCE = datetime(2026, 9, 24, tzinfo=timezone.utc)
MIN_N = 30


def trade_cost() -> float:
    """Flat round-trip cost as a fraction of a $0.40 position (pool fee + network)."""
    sol = settings.SOL_PRICE_USD or settings.CONFLUENCE_LIVE_DEPOSIT_SOL_PRICE_USD
    net = 2 * settings.SHADOW_EXEC_FEE_LAMPORTS_PER_SIDE / 1e9 * sol / settings.SHADOW_EXEC_NOTIONAL_USD
    return 2 * settings.SHADOW_EXEC_DEX_FEE_PCT + net


def boot_ci(diffs: list[float], iters: int = 2000) -> tuple[float, float]:
    rng = random.Random(1)
    n = len(diffs)
    means = sorted(mean(rng.choices(diffs, k=n)) for _ in range(iters))
    return means[int(iters * 0.025)], means[int(iters * 0.975) - 1]


def summarize(name: str, rets: dict, base: dict, censored: int = 0, resolved: dict | None = None) -> str:
    """rets / base: {trade_key: return}. Paired diff uses keys present in both."""
    vals = [min(v, CAP) for v in rets.values()]
    n = len(vals)
    if not n:
        return f"| {name} | 0 | | | | | | | |"
    keys = [k for k in rets if k in base]
    diffs = [min(rets[k], CAP) - min(base[k], CAP) for k in keys]
    if name == "base" or len(diffs) < 2:
        d = ci = verdict = "—"
    else:
        lo, hi = boot_ci(diffs)
        d, ci = f"{mean(diffs) * 100:+.2f}pp", f"[{lo * 100:+.1f}, {hi * 100:+.1f}]"
        verdict = ("candidate" if lo > 0 else "worse" if hi < 0 else "inconclusive") if len(diffs) >= MIN_N else "n too small"
    res = ""
    if resolved is not None:
        rv = [min(v, CAP) for v in resolved.values()]
        res = f"{mean(rv) * 100:+.1f}% (n={len(rv)})" if rv else "—"
    return (f"| {name} | {n} | {sum(v > 0 for v in vals) / n * 100:.0f}% | {mean(vals) * 100:+.1f}% | "
            f"{median(rets.values()) * 100:+.1f}% | {sum(v <= RUG for v in rets.values()) / n * 100:.0f}% | "
            f"{d} | {ci} | {verdict} |" + (f" {censored} | {res} |" if resolved is not None else ""))


HEAD = "| variant | n | win | capped mean | median | rug | vs base | 95% CI | verdict |"
SEP = "|---|---|---|---|---|---|---|---|---|"


async def replay() -> str:
    specs = xv.variant_specs()
    cost = trade_cost()
    async with get_session() as s:
        pos_rows = (await s.execute(
            select(ConfluenceShadowPosition.id, ConfluenceShadowPosition.entry_price,
                   ConfluenceShadowPosition.entry_time, ConfluenceShadowPosition.exit_reason,
                   ConfluenceShadowPosition.pnl_pct)
            .where(ConfluenceShadowPosition.status == "closed", ConfluenceShadowPosition.entry_time >= SINCE)
        )).all()
    pos = {r[0]: r for r in pos_rows}
    rets = {sp.name: {} for sp in specs}
    resolved = {sp.name: {} for sp in specs}
    censored = {sp.name: 0 for sp in specs}
    tally = {"match": 0, "total": 0, "positions": 0}

    def run_position(pid, ticks):
        _, entry, et, p_reason, p_pnl = pos[pid]
        if not entry or entry <= 0 or len(ticks) < 2:
            return
        tally["positions"] += 1
        for sp in specs:
            floor = hwm = None
            out = None
            for at, price in ticks:
                reason, floor, hwm = xv.step_exit(sp, entry, et, price, at, floor, hwm)
                if reason:
                    out = (reason, float((price - entry) / entry))
                    break
            if out is None:
                censored[sp.name] += 1
                last = float((ticks[-1][1] - entry) / entry)
                ret = float(p_pnl) if p_reason == "LIQUIDITY_GUARD" and p_pnl is not None else last
                rets[sp.name][pid] = ret - cost
            else:
                rets[sp.name][pid] = out[1] - cost
                resolved[sp.name][pid] = out[1] - cost
            if sp.name == "base":
                tally["total"] += 1
                tally["match"] += int(out is not None and out[0] == p_reason)

    # one position's path in memory at a time (the box has 2 GB shared with the live bots)
    async with get_session() as s:
        res = await s.stream(
            select(ConfluenceShadowObservation.position_id, ConfluenceShadowObservation.observed_at,
                   ConfluenceShadowObservation.price_usd)
            .where(ConfluenceShadowObservation.position_id.in_(list(pos)))
            .order_by(ConfluenceShadowObservation.position_id, ConfluenceShadowObservation.observed_at)
            .execution_options(yield_per=2000)
        )
        cur, ticks = None, []
        async for pid, at, price in res:
            if pid != cur:
                if cur is not None:
                    run_position(cur, ticks)
                cur, ticks = pid, []
            ticks.append((at, price))
        if cur is not None:
            run_position(cur, ticks)
    base_match, base_total, n_pos = tally["match"], tally["total"], tally["positions"]

    lines = [f"# Variant replay (recorded 1s paths, entries since {SINCE:%Y-%m-%d}, {n_pos} positions)", "",
             f"Flat per-trade cost subtracted: {cost * 100:.2f}%. Base replay reproduces the recorded exit reason on "
             f"{base_match}/{base_total} positions (differences: rule changes over time, ticks the worker held back).", "",
             HEAD + " censored | resolved only |", SEP + "---|---|"]
    base = rets["base"]
    for sp in specs:
        lines.append(summarize(sp.name, rets[sp.name], base, censored[sp.name], resolved[sp.name]))
    lines += ["", "Censored = path ended (primary exited) before this variant would have; marked at the last price "
              "(or the recorded modeled pnl for LIQUIDITY_GUARD). Wide/slow variants are biased by this: trust "
              "'resolved only' and the live mode more."]
    return "\n".join(lines)


async def live() -> str:
    async with get_session() as s:
        rows = (await s.execute(
            select(ConfluenceShadowVariantPosition.variant, ConfluenceShadowVariantPosition.status,
                   ConfluenceShadowVariantPosition.position_id, ConfluenceShadowVariantPosition.exec_pnl_pct,
                   ConfluenceShadowVariantPosition.pnl_pct)
        )).all()
        prim = {r[0]: r[1] for r in (await s.execute(
            select(ConfluenceShadowPosition.id, ConfluenceShadowPosition.exec_pnl_pct)
            .where(ConfluenceShadowPosition.status == "closed", ConfluenceShadowPosition.exec_pnl_pct.is_not(None))
        )).all()}
    by: dict[str, dict] = {}
    open_n: dict[str, int] = {}
    for name, status, pid, exec_pnl, _ in rows:
        if status == "open":
            open_n[name] = open_n.get(name, 0) + 1
        elif exec_pnl is not None:
            by.setdefault(name, {})[pid] = float(exec_pnl)
    base = {pid: float(v) for pid, v in prim.items() if any(pid in d for d in by.values())}
    lines = ["# Live variants (shadow worker, modeled fills net of fees)", ""]
    if not by:
        lines += ["No closed variant trades yet. They accrue as new signals fire (needs working sampling keys).", ""]
        lines += [f"Open variant rows: {sum(open_n.values())}"]
        return "\n".join(lines)
    lines += [HEAD, SEP, summarize("base", base, base)]
    for sp in xv.variant_specs()[1:]:
        if sp.name in by:
            lines.append(summarize(sp.name, by[sp.name], base))
    lines += ["", f"Open variant rows: {sum(open_n.values())}. 'candidate' needs n >= {MIN_N} paired trades and a 95% CI "
              "entirely above 0. It only flags a variant for human review."]
    return "\n".join(lines)


async def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "replay"
    print(await (live() if mode == "live" else replay()))


if __name__ == "__main__":
    asyncio.run(main())
