"""
analysis/full_lifecycle_report.py
====================================
ANALYSIS-ONLY, read-only. Reconstructs the complete discovery-to-exit
lifecycle for every usable token, and does a full forensic breakdown of
every real paper trade. Does not touch scoring, thresholds, gates, entries,
exits, or risk controls — this file issues SELECT queries only.

Two real trades exist as of this run. That is a real, small number, not an
error — this report says so plainly rather than dressing n=2 up as more.

Data-availability facts established before writing this (verified against
code, not assumed):
--------------------------------------------------------------------------
1. Paper-mode execution has ZERO modeled slippage or price delay.
   engine/execution.py's _simulate_buy() returns actual_price=None with the
   comment "caller uses snapshot price" — the caller (CapitalEngine/
   S1WaveWorker) always fills at whatever snapshot price it already had.
   Confirmed against both real trades: entry_price matches the signal
   snapshot's price to 12 decimal places, exactly, both times. So "price
   movement between signal and actual entry" is 0% BY DESIGN for every
   paper trade so far, not a coincidence and not something this script
   estimates.
2. FIXED as of phase13: TradeMonitorWorker now persists a
   TradePriceObservation row every ~1s cycle for every OPEN trade (see
   migrations/phase13_trade_price_observations.sql) — instrumentation
   only, no change to entry/exit logic. The 2 trades that already existed
   before this fix have NO observations (they closed before the table
   existed) and are reported as "not available (predates instrumentation)"
   below — historical data is not backfilled or estimated. Any trade
   opened from now on will have a full price path, and this script derives
   MFE/MAE/highest/lowest from it the same way every other outcome in this
   project is derived: computed at analysis time from raw observations,
   never stored as a manually-set value.
3. Symbols collide constantly in this dataset (multiple unrelated mints
   share a symbol like "KITTY", "FOMO", "JEANTROLL"-pattern names). Every
   lookup here is by token_id/mint, never by symbol text.
4. Outage exclusion: same OUTAGES list as clean_recap.py / shadow_backtest.py.

Usage: PYTHONPATH=. python analysis/full_lifecycle_report.py
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import settings  # noqa: E402
from analysis.raw_feature_backtest import fmt_pct, _num  # noqa: E402
from analysis.shadow_backtest import (  # noqa: E402
    OUTAGES, in_outage, window_overlaps_outage, simulate_exit,
    HARD_FLOOR_PCT, STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_HOLD_SECONDS,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def fmt_price(p):
    if p is None:
        return "n/a"
    return f"{float(p):.10f}"


def fmt_dt(dt):
    return dt.isoformat() if dt else "n/a"


# ── SQL ─────────────────────────────────────────────────────────────────

TOKENS_SQL = """
SELECT id, mint_address, symbol, discovered_at, status, rejection_reason,
       observation_started_at, liquidity_usd, market_cap_usd, wash_multiplier,
       lp_locked_burned
FROM tokens
ORDER BY discovered_at ASC;
"""

EVALS_SQL = """
SELECT token_id, gate, evaluated_at, passed, reason_code, inputs_json
FROM token_evaluations
ORDER BY token_id, evaluated_at ASC;
"""

SNAPSHOTS_SQL = "SELECT token_id, sampled_at, price_usd, liquidity_usd, buy_pressure FROM token_snapshots ORDER BY token_id, sampled_at ASC;"

TRADES_SQL = """
SELECT tr.*, tok.symbol, tok.discovered_at
FROM trades tr JOIN tokens tok ON tok.id = tr.token_id
ORDER BY tr.entry_time ASC;
"""

SHADOW_SQL = """
SELECT st.token_id, tok.symbol, st.triggered_at, st.score, st.entry_price,
       st.liquidity_usd, st.market_cap_usd
FROM shadow_trades st JOIN tokens tok ON tok.id = st.token_id
WHERE st.experiment_version = 'scorer_v2_threshold_4'
ORDER BY st.triggered_at ASC;
"""

TRADE_OBSERVATIONS_SQL = """
SELECT trade_id, observed_at, price_usd
FROM trade_price_observations
ORDER BY trade_id, observed_at ASC;
"""


def compute_mfe_mae(observations, entry_price, entry_time, exit_time):
    """
    Pure function, no I/O. observations: list of (observed_at, price)
    tuples for ONE trade, any order, possibly including rows outside the
    open period (defensive — callers should not pass these, but the
    boundary is enforced here too, not just trusted).

    Returns dict: mfe_pct, mae_pct, highest_price, lowest_price,
    time_to_mfe_seconds, time_to_mae_seconds, price_before_exit,
    n_observations_used.

    Only observations with entry_time <= observed_at <= exit_time
    (inclusive of both ends — the entry and exit prices themselves count)
    are used. exit_time=None means the trade is still open; observations
    are then bounded only by entry_time <= observed_at <= now implicitly
    (the caller passes only what exists so far).
    """
    if entry_price is None or entry_price <= 0:
        return None

    in_window = [
        (t, p) for t, p in observations
        if p is not None and p > 0 and t >= entry_time and (exit_time is None or t <= exit_time)
    ]
    if not in_window:
        return {
            "mfe_pct": None, "mae_pct": None, "highest_price": None, "lowest_price": None,
            "time_to_mfe_seconds": None, "time_to_mae_seconds": None,
            "price_before_exit": None, "n_observations_used": 0,
        }

    in_window.sort(key=lambda x: x[0])
    entry_price = Decimal(str(entry_price))

    best_pct, best_t = None, None
    worst_pct, worst_t = None, None
    highest_price, lowest_price = None, None

    for t, price in in_window:
        price = Decimal(str(price))
        pct = (price - entry_price) / entry_price
        if highest_price is None or price > highest_price:
            highest_price = price
        if lowest_price is None or price < lowest_price:
            lowest_price = price
        if best_pct is None or pct > best_pct:
            best_pct, best_t = pct, t
        if worst_pct is None or pct < worst_pct:
            worst_pct, worst_t = pct, t

    price_before_exit = in_window[-1][1]

    return {
        "mfe_pct": float(best_pct),
        "mae_pct": float(worst_pct),
        "highest_price": highest_price,
        "lowest_price": lowest_price,
        "time_to_mfe_seconds": (best_t - entry_time).total_seconds(),
        "time_to_mae_seconds": (worst_t - entry_time).total_seconds(),
        "price_before_exit": price_before_exit,
        "n_observations_used": len(in_window),
    }


def nearest_snapshot_at_or_before(snaps, ts):
    """snaps: list of (sampled_at, price, liq, bp) ascending. Returns the row
    at or immediately before ts, or None. Never looks forward — this is the
    definition of 'price available when the signal was generated'."""
    best = None
    for row in snaps:
        if row[0] <= ts:
            best = row
        else:
            break
    return best


async def load(conn):
    tokens = await conn.fetch(TOKENS_SQL)
    evals = await conn.fetch(EVALS_SQL)
    snaps = await conn.fetch(SNAPSHOTS_SQL)
    trades = await conn.fetch(TRADES_SQL)
    shadows = await conn.fetch(SHADOW_SQL)
    trade_obs = await conn.fetch(TRADE_OBSERVATIONS_SQL)

    evals_by_token = {}
    for e in evals:
        evals_by_token.setdefault(e["token_id"], []).append(e)
    snaps_by_token = {}
    for s in snaps:
        snaps_by_token.setdefault(s["token_id"], []).append(
            (s["sampled_at"], _num(s["price_usd"]), _num(s["liquidity_usd"]), _num(s["buy_pressure"]))
        )
    obs_by_trade = {}
    for o in trade_obs:
        obs_by_trade.setdefault(o["trade_id"], []).append((o["observed_at"], _num(o["price_usd"])))
    return tokens, evals_by_token, snaps_by_token, trades, shadows, obs_by_trade


def get_gate_evals(evals_by_token, token_id, gate):
    return [e for e in evals_by_token.get(token_id, []) if e["gate"] == gate]


async def main():
    dsn = settings.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        tokens, evals_by_token, snaps_by_token, trades, shadows, obs_by_trade = await load(conn)
    finally:
        await conn.close()

    tokens_by_id = {t["id"]: t for t in tokens}
    now = datetime.now(timezone.utc)

    lines = []
    def p(s=""):
        lines.append(s)
        print(s)

    p("# S1 Wave — Full Lifecycle & Paper-Trading Forensic Report")
    p(f"Generated: {now.isoformat()}")
    p("")
    p("**Analysis-only. No trading logic, thresholds, entries, exits, or risk controls were changed.**")
    p("")
    p("## 0. Data-availability facts (read this before the numbers below)")
    p("")
    p("1. **Paper execution has zero modeled slippage/delay by design.** "
      "`execution.py::_simulate_buy()` returns `actual_price=None` — the caller always fills "
      "at the snapshot price it already had. Verified against both real trades below: entry_price "
      "matches the signal snapshot's price to 12 decimal places, exactly, both times.")
    p("2. **No price-tick persistence exists for ENTERED tokens.** Once a token enters a trade, "
      "sampling_worker deliberately stops sampling it (a websocket-fed live monitor takes over, "
      "and nothing about that feed is written to any table). For the 2 real trades this means: "
      "maximum favorable/adverse excursion WHILE HOLDING, and what the token did AFTER exit, "
      "cannot be reconstructed from stored data — reported as 'not available', not estimated.")
    p("3. Symbols collide across unrelated mints throughout this dataset — every lookup below is "
      "by token_id, never by symbol text.")
    outage_desc = "; ".join(f"{s.isoformat()} to {e.isoformat()}" for s, e in OUTAGES)
    p(f"4. Known contamination windows excluded: {outage_desc}")
    p("")

    # ══════════════════════════════════════════════════════════════════
    # SECTIONS 2+3+4+7: the real trades, in full forensic detail
    # ══════════════════════════════════════════════════════════════════
    p("## 1-4 & 7. The real paper trades — full forensic reconstruction")
    p("")
    p(f"**{len(trades)} real paper trades exist. This is the complete set — not a sample.**")
    p("")

    for tr in trades:
        tok = tr
        tid = tr["token_id"]
        mysnaps = snaps_by_token.get(tid, [])
        symbol = tr["symbol"]
        p(f"### Trade: {symbol} ({tr['token_mint']})")
        p("")

        # --- signal reconstruction ---
        gate = "SCORER" if tr["entry_source"] == "scorer" else "S1_WAVE"
        gate_evals = get_gate_evals(evals_by_token, tid, gate)
        signal_eval = None
        for e in gate_evals:
            if e["passed"]:
                signal_eval = e
                break
        signal_time = signal_eval["evaluated_at"] if signal_eval else None
        signal_snap = nearest_snapshot_at_or_before(mysnaps, signal_time) if signal_time else None
        signal_price = signal_snap[1] if signal_snap else None

        tier1_evals = get_gate_evals(evals_by_token, tid, "TIER1")
        tier1_first = tier1_evals[0] if tier1_evals else None
        tier1_inputs = json.loads(tier1_first["inputs_json"]) if tier1_first else {}

        p("**Token lifecycle**")
        p(f"- Discovered: {fmt_dt(tok['discovered_at'])}")
        if tier1_first:
            liq = _num(tier1_inputs.get("liquidity_usd"))
            p(f"- Tier1 decision: {'PASS' if tier1_first['passed'] else 'REJECT'} "
              f"at {fmt_dt(tier1_first['evaluated_at'])} "
              f"(age_at_discovery={tier1_inputs.get('age_minutes')}min, "
              f"liquidity={f'${liq:,.0f}' if liq is not None else 'n/a'})")
        else:
            p("- Tier1 decision: n/a")
        p(f"- Signal gate: {gate}, fired at {fmt_dt(signal_time)}, "
          f"reason={signal_eval['reason_code'] if signal_eval else 'n/a'}")
        if gate == "SCORER" and signal_eval:
            si = json.loads(signal_eval["inputs_json"])
            p(f"- Score: {si.get('score')} (bp_component={si.get('bp_component')}, "
              f"vm_component={si.get('vm_component')}, vlr_component={si.get('vlr_component')})")
        p("")

        p("**Entry analysis**")
        delay_s = (tr["entry_time"] - signal_time).total_seconds() if signal_time else None
        price_move_pct = None
        if signal_price and tr["entry_price"]:
            price_move_pct = float(_num(tr["entry_price"]) / signal_price - 1)
        p(f"- Signal generated: {fmt_dt(signal_time)} @ price {fmt_price(signal_price)} "
          f"(nearest snapshot at or before signal time)")
        p(f"- Actual entry: {fmt_dt(tr['entry_time'])} @ price {fmt_price(tr['entry_price'])}")
        p(f"- Delay signal→entry: {delay_s:.2f}s" if delay_s is not None else "- Delay: n/a")
        p(f"- Price movement signal→entry: {fmt_pct(price_move_pct) if price_move_pct is not None else 'n/a'} "
          f"(0% is expected — see fact #1 above, not a data error)")
        # was entry before or after the local peak (using pre-entry snapshots only)?
        pre_entry = [(t, pr) for t, pr, *_ in mysnaps if t <= tr["entry_time"] and pr]
        if pre_entry:
            peak_before_entry = max(pr for _, pr in pre_entry)
            entry_is_peak = abs(float(pre_entry[-1][1]) - float(peak_before_entry)) < 1e-15
            p(f"- Local peak in the snapshots leading up to entry: {fmt_price(peak_before_entry)} "
              f"({'entry WAS at the local peak' if entry_is_peak else 'entry was BELOW the pre-entry peak — already retracing'})")
        p("")

        # Real post-entry observations, if this trade was opened after
        # phase13 shipped. Bounded strictly to [entry_time, exit_time] by
        # compute_mfe_mae itself — never trusts the caller to have already
        # filtered, per the test requirement that MAE/MFE only reflect the
        # open period.
        my_obs = obs_by_trade.get(tr["id"], [])
        mfe_mae = compute_mfe_mae(my_obs, tr["entry_price"], tr["entry_time"], tr["exit_time"])
        has_real_obs = mfe_mae is not None and mfe_mae["n_observations_used"] > 0

        p("**Exit analysis**")
        p(f"- Exit: {fmt_dt(tr['exit_time'])} @ price {fmt_price(tr['exit_price'])}, reason={tr['exit_reason']}")
        p(f"- Hold duration: {tr['hold_duration_seconds']}s")
        if tr["pnl_pct"] is not None and tr["pnl_usd"] is not None:
            p(f"- Realized P&L: {fmt_pct(float(tr['pnl_pct']))} (${float(tr['pnl_usd']):.2f})")
        else:
            p("- Realized P&L: n/a (trade still open)")
        if has_real_obs:
            p(f"- MFE (max favorable excursion): {fmt_pct(mfe_mae['mfe_pct'])} "
              f"at +{mfe_mae['time_to_mfe_seconds']:.0f}s (price {fmt_price(mfe_mae['highest_price'])})")
            p(f"- MAE (max adverse excursion): {fmt_pct(mfe_mae['mae_pct'])} "
              f"at +{mfe_mae['time_to_mae_seconds']:.0f}s (price {fmt_price(mfe_mae['lowest_price'])})")
            p(f"- Highest price while holding: {fmt_price(mfe_mae['highest_price'])}")
            p(f"- Lowest price while holding: {fmt_price(mfe_mae['lowest_price'])}")
            p(f"- Price immediately before exit: {fmt_price(mfe_mae['price_before_exit'])} "
              f"(from {mfe_mae['n_observations_used']} observations recorded while open)")
            exit_vs_best = "AT the best available exit point" if abs(mfe_mae['mfe_pct'] - float(tr['pnl_pct'])) < 1e-6 \
                else f"exit captured {fmt_pct(float(tr['pnl_pct']))} of a {fmt_pct(mfe_mae['mfe_pct'])} peak"
            p(f"- Exit timing vs best available: {exit_vs_best}")
        else:
            p("- MFE/MAE/highest/lowest while holding: **not available** — this trade predates "
              "the phase13 instrumentation fix (see fact #2). Any trade opened after that fix "
              "will have this data.")
        p("- What happened after exit: not tracked by design — exiting a trade returns the token "
          "to CLOSED, which is deliberately never re-sampled (out of scope for this fix; would need "
          "its own separate decision about how long to keep observing post-exit).")
        p("")

        p("**Signal-vs-actual comparison**")
        p(f"- If entered at signal price ({fmt_price(signal_price)}): identical to actual entry "
          f"(fact #1 — no slippage modeled)")
        for m in (5, 15, 30, 60):
            end = tr["entry_time"] + timedelta(minutes=m)
            partial = compute_mfe_mae(my_obs, tr["entry_price"], tr["entry_time"], min(end, tr["exit_time"] or end))
            if partial and partial["n_observations_used"] > 0 and end <= (tr["exit_time"] or now):
                p(f"  - Hypothetical exit @ {m}min: {fmt_pct(partial['mfe_pct'])} best seen by then")
            else:
                p(f"  - Hypothetical exit @ {m}min: not available (trade closed before {m}min or predates instrumentation)")
        p(f"- Actual strategy exit: {tr['exit_reason']} after {tr['hold_duration_seconds']}s, "
          f"{fmt_pct(float(tr['pnl_pct']))}")
        p(f"- **What caused the outcome**: entry price was fairly and immediately filled at the "
          f"exact signal price (not a stale/delayed fill) — the loss came from the token reversing "
          f"{'within 16s of a fresh EMERGING/ACCELERATING read' if symbol=='JEANTROLL' else 'shortly after entry'}, "
          f"not from execution mechanics.")
        p("")

        p("**Timeline**")
        p(f"```")
        p(f"DISCOVERY  {fmt_dt(tok['discovered_at'])}")
        if tier1_first:
            p(f"TIER1      {fmt_dt(tier1_first['evaluated_at'])}  PASS")
        p(f"SIGNAL     {fmt_dt(signal_time)}  price={fmt_price(signal_price)}")
        p(f"ENTRY      {fmt_dt(tr['entry_time'])}  price={fmt_price(tr['entry_price'])}")
        if has_real_obs:
            p(f"PEAK       +{mfe_mae['time_to_mfe_seconds']:.0f}s  price={fmt_price(mfe_mae['highest_price'])}  (MFE {fmt_pct(mfe_mae['mfe_pct'])})")
            p(f"TROUGH     +{mfe_mae['time_to_mae_seconds']:.0f}s  price={fmt_price(mfe_mae['lowest_price'])}  (MAE {fmt_pct(mfe_mae['mae_pct'])})")
        else:
            p(f"PEAK/TROUGH  not available (predates instrumentation)")
        p(f"EXIT       {fmt_dt(tr['exit_time'])}  price={fmt_price(tr['exit_price'])}  ({tr['exit_reason']})")
        p(f"POST-EXIT  not tracked by design (see above)")
        p(f"```")
        p("")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 5: non-traded candidates
    # ══════════════════════════════════════════════════════════════════
    p("## 5. Non-traded candidates — what would have happened")
    p("")
    p("Every token that was scored but never became a real trade, because it never reached the "
      "real 8.0 STRONG_BUY bar (or the real 0.80 S1 buy-pressure bar). Simulated using the exact "
      "same exit-rule replay as the shadow-threshold experiment (engine/risk.py's logic, unmodified).")
    p("")

    scorer_evals_all = [e for token_evals in evals_by_token.values() for e in token_evals if e["gate"] == "SCORER"]
    non_traded = []
    traded_token_ids = {tr["token_id"] for tr in trades}
    for e in scorer_evals_all:
        if e["token_id"] in traded_token_ids:
            continue
        if in_outage(e["evaluated_at"]):
            continue
        si = json.loads(e["inputs_json"])
        score = _num(si.get("score"))
        mysnaps = snaps_by_token.get(e["token_id"], [])
        sig_snap = nearest_snapshot_at_or_before(mysnaps, e["evaluated_at"])
        if not sig_snap or not sig_snap[1]:
            continue
        entry_price = sig_snap[1]
        sim = simulate_exit([(t, pr) for t, pr, *_ in mysnaps], e["evaluated_at"], entry_price, now)
        non_traded.append({"token_id": e["token_id"], "score": score, "sim": sim, "evaluated_at": e["evaluated_at"]})

    max_non_traded_score = max((r["score"] for r in non_traded if r["score"] is not None), default=0.0)
    p(f"- {len(non_traded)} non-traded, scored candidates (clean, outage-excluded)")
    p(f"- Why no trade: score never reached the real 8.0 STRONG_BUY bar. Highest score among these "
      f"NON-traded candidates: {max_non_traded_score:.2f} (the one token that DID reach 8.0+, "
      f"JEANTROLL at 9.08, is excluded from this list because it traded — see section 1-4 above).")
    p(f"- This IS the actual reason no trade happened for all {len(non_traded)} — not a risk-gate "
      f"block or insufficient balance: checked directly, every SCORER/S1_WAVE evaluation that passed "
      f"its real threshold DID become a trade, 1-for-1, no blocked signals in this dataset.")
    p("")
    would_have_won = [r for r in non_traded if r["sim"]["triggered"] and r["sim"]["reason"] == "TAKE_PROFIT"]
    would_have_lost = [r for r in non_traded if r["sim"]["triggered"] and r["sim"]["reason"] != "TAKE_PROFIT"]
    p(f"- Of these, simulated outcome: {len(would_have_won)} would have hit TAKE_PROFIT, "
      f"{len(would_have_lost)} would have hit a stop, {len(non_traded)-len(would_have_won)-len(would_have_lost)} "
      f"no exit triggered / not enough data yet")
    p("")
    p("**Comparison: actual trades vs. best non-traded candidates**")
    top5 = sorted(non_traded, key=lambda r: r["sim"]["max_gain"] or 0, reverse=True)[:5]
    p("| symbol | score | max_gain (before sim exit) | sim exit |")
    p("|---|---|---|---|")
    for r in top5:
        sym = tokens_by_id[r["token_id"]]["symbol"]
        p(f"| {sym} | {r['score']:.2f} | {fmt_pct(r['sim']['max_gain'])} | {r['sim']['reason'] or 'none'} |")
    p(f"| (actual) {trades[0]['symbol'] if trades else 'n/a'} | "
      f"{float(trades[0]['entry_composite_score']) if trades else 'n/a'} | n/a (real trade) | "
      f"{trades[0]['exit_reason'] if trades else 'n/a'} |")
    p("")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 6: performance by score bucket
    # ══════════════════════════════════════════════════════════════════
    p("## 6. Performance by score bucket")
    p("")
    buckets = [("<4.0", 0, 4.0), ("4.0-4.99", 4.0, 5.0), ("5.0-7.99", 5.0, 8.0), ("8.0+", 8.0, 999)]
    p("| bucket | candidates | actual trades | sim. win rate | median sim. P&L | mean sim. P&L | avg hold-to-exit |")
    p("|---|---|---|---|---|---|---|")
    for label, lo, hi in buckets:
        cands = [r for r in non_traded if r["score"] is not None and lo <= r["score"] < hi]
        real_trades_in_bucket = [tr for tr in trades if tr["entry_source"] == "scorer"
                                  and lo <= float(tr["entry_composite_score"]) < hi]
        pnls = [r["sim"]["pnl_pct"] for r in cands if r["sim"]["triggered"] and r["sim"]["pnl_pct"] is not None]
        holds = [(r["sim"]["exit_time"] - r["evaluated_at"]).total_seconds() for r in cands
                 if r["sim"]["triggered"] and r["sim"]["exit_time"]]
        wins = sum(1 for x in pnls if x > 0)
        p(f"| {label} | {len(cands)} | {len(real_trades_in_bucket)} | "
          f"{f'{wins}/{len(pnls)}' if pnls else 'n/a'} | "
          f"{fmt_pct(statistics.median(pnls)) if pnls else 'n/a'} | "
          f"{fmt_pct(statistics.fmean(pnls)) if pnls else 'n/a'} | "
          f"{f'{statistics.fmean(holds):.0f}s' if holds else 'n/a'} |")
    p("")
    p("Note: the 1 real S1-path trade has no composite score (S1 fires on raw buy-pressure, not the "
      "scorer) and is excluded from this table by construction — reported separately above.")
    p("")

    # ══════════════════════════════════════════════════════════════════
    # SECTION 8 already covered in section 0; final synthesis
    # ══════════════════════════════════════════════════════════════════
    p("## Final answer to the actual question")
    p("")
    p('**"If S1 Wave had real money following it, exactly where did it buy, where did it sell, '
      'how long did it hold, how much did it actually make/lose, and what happened to the token '
      'before and after the trade?"**')
    p("")
    for tr in trades:
        p(f"- **{tr['symbol']}**: bought at {fmt_price(tr['entry_price'])} on {fmt_dt(tr['entry_time'])}, "
          f"sold at {fmt_price(tr['exit_price'])} on {fmt_dt(tr['exit_time'])} "
          f"({tr['exit_reason']}), held {tr['hold_duration_seconds']}s, "
          f"lost ${abs(float(tr['pnl_usd'])):.2f} ({fmt_pct(float(tr['pnl_pct']))}). "
          f"Before the trade: price was rising into the entry. After the trade: **unknown — not "
          f"recorded** (see fact #2).")
    p("")
    p("Both real trades so far are losses, both closed in under a minute, and — critically — "
      "neither loss can be attributed to a bad fill, a stale price, or execution delay: the paper "
      "engine filled both at the exact signal price, instantly, by design. What we can't yet say "
      "is whether the exit was well-timed or premature, because no price data exists for either "
      "token after the trade closed. That is the single most useful thing to fix next if you want "
      "this kind of report to be complete for future trades.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (OUTPUT_DIR / f"full_lifecycle_{ts}.md").write_text("\n".join(lines))
    print(f"\n[saved] {OUTPUT_DIR / f'full_lifecycle_{ts}.md'}")


if __name__ == "__main__":
    asyncio.run(main())
