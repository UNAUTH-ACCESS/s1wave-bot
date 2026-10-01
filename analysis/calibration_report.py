"""
analysis/calibration_report.py — the standard calibration battery.
====================================================================
READ-ONLY (SELECT only). Run first in any calibration session:

    cd /home/solana/s1wave-bot/solanabot && source .venv/bin/activate
    S1WAVE_ENV_FILE=.env PYTHONPATH=. python analysis/calibration_report.py
    (S1WAVE_ENV_FILE=.env.second / .env.efetobo for the other accounts)

Prints Markdown and saves it to analysis/output/calibration_<UTC>.md.
Method, definitions and pitfalls: CALIBRATION.md at the repo root.

Sections: 1 data health | 2 live (real money) | 3 shadow | 4 live vs shadow
paired by token (how wrong is the paper number?) | 5 entry-signal buckets.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean, median

from sqlalchemy import func, select

from database.engine import get_session
from models.orm import (
    ConfluenceLiveTrade, ConfluenceShadowPosition, MomentumSignalEvent, TokenSnapshot,
)

CAP = 1.0                      # cap each trade's counted gain at +100% (phantom $0-liquidity peaks)
RUG = -0.40                    # a "rug" = pnl <= -40%
SOURCE_VERSION = "momentum_confluence_v1"
FILTER_REGIME_SINCE = datetime(2026, 9, 24, tzinfo=timezone.utc)   # all 3 entry filters live
GUARD_FIX_SINCE = datetime(2026, 10, 1, 6, 11, tzinfo=timezone.utc)  # guard judges executable proceeds
EXEC_SHADOW_SINCE = datetime(2026, 10, 1, 10, 58, tzinfo=timezone.utc)  # shadow has modeled fills

out: list[str] = []


def p(line: str = "") -> None:
    out.append(line)


def f(x, pct=False, nd=2):
    if x is None:
        return "—"
    return f"{x * 100:.1f}%" if pct else f"{x:.{nd}f}"


def stats(pnls: list[float]) -> dict:
    n = len(pnls)
    if not n:
        return {"n": 0}
    return {
        "n": n, "win": sum(x > 0 for x in pnls) / n, "mean": mean(pnls),
        "capped": mean(min(x, CAP) for x in pnls), "median": median(pnls),
        "rug": sum(x <= RUG for x in pnls) / n,
    }


def row(label: str, s: dict) -> str:
    if not s["n"]:
        return f"| {label} | 0 | | | | | |"
    return (f"| {label} | {s['n']} | {f(s['win'], True)} | {f(s['mean'], True)} | "
            f"{f(s['capped'], True)} | {f(s['median'], True)} | {f(s['rug'], True)} |")


HDR = "| group | n | win rate | mean | capped mean | median | rug rate |\n|---|---|---|---|---|---|---|"


def live_pct(t) -> float | None:
    """Real-money return on capital: verified real_pnl_usd when known, else intended pnl_usd."""
    pnl = t.real_pnl_usd if t.real_pnl_usd is not None else t.pnl_usd
    return float(pnl / t.position_usd) if pnl is not None and t.position_usd else None


async def main() -> None:
    now = datetime.now(timezone.utc)
    async with get_session() as s:
        live = (await s.execute(select(ConfluenceLiveTrade).where(ConfluenceLiveTrade.status == "closed"))).scalars().all()
        shadow = (await s.execute(select(ConfluenceShadowPosition).where(ConfluenceShadowPosition.status == "closed"))).scalars().all()
        status_mix = (await s.execute(select(ConfluenceShadowPosition.status, func.count()).group_by(ConfluenceShadowPosition.status))).all()
        live_mix = (await s.execute(select(ConfluenceLiveTrade.status, func.count()).group_by(ConfluenceLiveTrade.status))).all()
        last_snap = (await s.execute(select(func.max(TokenSnapshot.sampled_at)))).scalar()
        last_sig = (await s.execute(select(func.max(MomentumSignalEvent.triggered_at)))).scalar()
        sigs = {r.token_id: r for r in (await s.execute(select(MomentumSignalEvent).where(
            MomentumSignalEvent.experiment_version == SOURCE_VERSION))).scalars().all()}

    p(f"# Calibration report — {now:%Y-%m-%d %H:%M}Z\n")
    p("## 1. Data health (check this before believing anything below)")
    p(f"- Latest token snapshot: {last_snap}  |  latest momentum signal: {last_sig}")
    p("  If these are stale, sampling/discovery keys are exhausted and NO new data is arriving.")
    p(f"- Shadow rows by status: {dict(status_mix)}")
    p(f"- Live rows by status: {dict(live_mix)}")
    modeled = [x for x in shadow if x.exec_status == "modeled"]
    p(f"- Shadow closed: {len(shadow)} (with modeled fills: {len(modeled)}, flagged no_quote: "
      f"{sum(x.exec_status == 'no_quote' for x in shadow)})")
    p(f"- Live closed: {len(live)} (on-chain verified real_pnl_usd: {sum(t.real_pnl_usd is not None for t in live)})\n")

    p("## 2. Live (real money) — return on capital per trade")
    p("Verified = real_pnl_usd present (trust this); the rest use the intended pnl_usd (understates spend).\n")
    p(HDR)
    ver = [t for t in live if t.real_pnl_usd is not None]
    for label, grp in (
        ("all closed", live), ("verified only", ver),
        ("verified, since guard fix", [t for t in ver if t.entry_time >= GUARD_FIX_SINCE]),
    ):
        p(row(label, stats([x for x in map(live_pct, grp) if x is not None])))
    reasons: dict[str, list[float]] = {}
    for t in live:
        x = live_pct(t)
        if x is not None:
            reasons.setdefault(t.exit_reason or "unknown", []).append(x)
    p("\nBy exit reason:\n")
    p(HDR)
    for k, v in sorted(reasons.items(), key=lambda kv: -len(kv[1])):
        p(row(k, stats(v)))
    tot = sum(float(t.real_pnl_usd if t.real_pnl_usd is not None else (t.pnl_usd or 0)) for t in live)
    p(f"\nCumulative real P&L: ${tot:.2f} over {len(live)} closed trades.\n")

    p("## 3. Shadow (paper) — snapshot pnl_pct vs modeled executable exec_pnl_pct")
    p("pnl_pct is DexScreener-snapshot based (optimistic: no fees/slippage, frozen thin prices). "
      "exec_pnl_pct is net of modeled pool impact + fees. Compare the two on the SAME rows.\n")
    p(HDR)
    cur = [x for x in shadow if x.entry_time >= FILTER_REGIME_SINCE]
    p(row("all closed", stats([float(x.pnl_pct) for x in shadow if x.pnl_pct is not None])))
    p(row("current filter regime", stats([float(x.pnl_pct) for x in cur if x.pnl_pct is not None])))
    mm = [x for x in modeled if x.exec_pnl_pct is not None]
    p(row("modeled rows: snapshot pnl_pct", stats([float(x.pnl_pct) for x in mm])))
    p(row("modeled rows: exec_pnl_pct", stats([float(x.exec_pnl_pct) for x in mm])))
    r2: dict[str, list[float]] = {}
    for x in cur:
        if x.pnl_pct is not None:
            r2.setdefault(x.exit_reason or "unknown", []).append(float(x.pnl_pct))
    p("\nBy exit reason (current filter regime, snapshot pnl_pct):\n")
    p(HDR)
    for k, v in sorted(r2.items(), key=lambda kv: -len(kv[1])):
        p(row(k, stats(v)))
    p()

    p("## 4. Live vs shadow, paired by token (the calibration error of paper trading)")
    sh_by_tok = {x.token_id: x for x in shadow}
    pairs = [(t, sh_by_tok[t.token_id]) for t in live if t.token_id in sh_by_tok and live_pct(t) is not None
             and sh_by_tok[t.token_id].pnl_pct is not None]
    if pairs:
        lv = [live_pct(t) for t, _ in pairs]
        sv = [min(float(x.pnl_pct), CAP) for _, x in pairs]  # capped: raw shadow has phantom 100x peaks
        gap = [b - a for a, b in zip(lv, sv)]
        p(f"{len(pairs)} tokens traded by both (shadow capped at +100%). Mean live {f(mean(lv), True)} vs shadow {f(mean(sv), True)}; "
          f"mean gap (shadow - live) {f(mean(gap), True)}, median gap {f(median(gap), True)}.")
        p(f"Live won / shadow won: {sum(a > 0 for a in lv)} / {sum(b > 0 for b in sv)}. "
          f"Shadow won while live lost: {sum(a <= 0 < b for a, b in zip(lv, sv))}.")
        p("This gap is what the shadow model must close. Entry timing differs (live enters later and pays "
          "fees and rent), so a nonzero gap is expected — track whether it shrinks as the model improves.\n")
    else:
        p("No tokens traded by both yet.\n")

    p("## 5. Entry-signal buckets (shadow, snapshot pnl_pct, current filter regime)")
    p("Used to test whether a filter threshold still discriminates. Cross-tab before trusting two filters as independent.\n")
    for name, key, edges in (
        ("buy_pressure", lambda g: g.buy_pressure, [0.9, 0.95, 0.97, 0.99]),
        ("liquidity_usd", lambda g: g.liquidity_usd, [5_000, 10_000, 20_000, 30_000]),
        ("n_rules_cofiring", lambda g: g.n_rules_cofiring, [2, 3, 4]),
    ):
        buckets: dict[str, list[float]] = {}
        for x in cur:
            sg = sigs.get(x.token_id)
            v = key(sg) if sg else None
            if v is None or x.pnl_pct is None:
                continue
            v = float(v)
            lab = next((f"< {e}" for e in edges if v < e), f">= {edges[-1]}")
            buckets.setdefault(lab, []).append(float(x.pnl_pct))
        p(f"**{name}**\n")
        p(HDR)
        for lab in sorted(buckets, key=lambda l: (l.startswith(">="), float(l.split()[1]))):
            p(row(lab, stats(buckets[lab])))
        p()

    text = "\n".join(out)
    print(text)
    d = Path(__file__).parent / "output"
    d.mkdir(exist_ok=True)
    (d / f"calibration_{now:%Y%m%dT%H%M%SZ}.md").write_text(text)


if __name__ == "__main__":
    asyncio.run(main())
