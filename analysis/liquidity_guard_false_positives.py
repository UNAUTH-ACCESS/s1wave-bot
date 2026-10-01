"""
analysis/liquidity_guard_false_positives.py
=============================================
ANALYSIS-ONLY, READ-ONLY. Changes nothing about live trading — issues
SELECT queries and one public, unauthenticated Jupiter price quote
(no signing, no keys, no orders).

THE FINDING (2026-10-01)
-------------------------
`workers/confluence_live_worker.py::_check_liquidity_guard()` exits a
position when EITHER condition holds:

    quote["price_impact_pct"] >= 0.35   (_LIQUIDITY_CRISIS_IMPACT_PCT)
    real_pnl_pct <= -0.30               (_LIQUIDITY_CRISIS_PNL_FLOOR_PCT)

The second condition is sound and earns its keep — it catches real rug
pulls (~40 firings at -97%..-99% real P&L).

The FIRST condition is firing on a value that is not a measurement.
Jupiter's `priceImpactPct` comes back as the literal string "1" for
pump.fun-style thin pools — 564 of 575 guard firings saw exactly "1" —
so `>= 0.35` is permanently satisfied and the guard force-closes
essentially every live position within seconds of entry.

Two independent proofs that "1" is not a real impact measurement:

  1. INTERNAL CONTRADICTION at firing time. The same quote that reported
     price_impact_pct == 1 (i.e. "100% impact — the sell consumes the
     whole pool") simultaneously reported out_lamports worth 95%-99.8%
     of the position (the logged real_pnl_pct was -0.2%..-5%). You cannot
     both consume the entire pool and get ~96%-99.8% of your money back.
     The proceeds number is sane; the impact field is not.

  2. CONTROL QUOTE. A deep-liquidity pair (SOL->USDC, small size)
     returns priceImpactPct "0" from the same endpoint — so the field IS
     a fraction and the code's 0.35 threshold interprets the unit
     correctly. It is specifically these thin memecoin routes that
     return the clamped/sentinel "1". Reproduced live below.

CONSEQUENCE — the controlled comparison already in the data
------------------------------------------------------------
Shadow and live run the SAME signals, SAME entry filters, SAME exit
rules. The only material difference is that shadow has no liquidity
guard. Since 2026-09-27:

    SHADOW (no guard): n=112, win_rate 96%, exits mostly TIME_EXIT
    LIVE   (guard on): n=42,  win_rate  7%, exits 41/42 LIQUIDITY_GUARD

Read that honestly in BOTH directions:
  - The guard is why live cannot win. It is not the market, and it is
    not the SolanaTracker sampling outage — live also went 0/10 during
    the window when sampling was fully healthy (2026-09-29/30).
  - But shadow's 96% is NOT the achievable number. Shadow models zero
    slippage and zero fees, and prices off DexScreener snapshots that
    can sit frozen on thin liquidity — which is the exact failure the
    guard was built for (see the BLK and Gavel incidents in
    _check_liquidity_guard()'s own docstring). Some of that 96% is
    certainly paper. The true edge is unknown; it is simply not ~7%.

SUGGESTED FIX — deliberately NOT applied
-----------------------------------------
Per the user's explicit instruction ("don't change live trading yet...
I don't want us to get bad reaction when the market is actually good"),
nothing here is wired into the live path. The minimal change to propose:
stop treating `priceImpactPct` as trustworthy on its own. Options, in
increasing order of caution:

  a) Drop the impact branch; keep the P&L-floor branch (which is doing
     the real work on real rugs anyway).
  b) Keep the impact branch but require corroboration — only treat high
     impact as a crisis when real_pnl_pct is ALSO meaningfully negative
     (e.g. <= -10%), so a sentinel value alone can't liquidate a healthy
     position.
  c) Ignore the exact value 1 (and 0) as non-measurements, and act only
     on computed intermediate values.

(b) is the smallest, safest change: it preserves every genuine save
seen in the data while making a constant impact value harmless.

Usage: PYTHONPATH=. python analysis/liquidity_guard_false_positives.py
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from database.engine import get_session  # noqa: E402

# Public, unauthenticated quote endpoint — same one engine/execution.py uses.
_JUP = "https://lite-api.jup.ag/swap/v1/quote"
_SOL = "So11111111111111111111111111111111111111112"
_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

IMPACT_THRESHOLD = 0.35   # _LIQUIDITY_CRISIS_IMPACT_PCT
PNL_FLOOR = -0.30         # _LIQUIDITY_CRISIS_PNL_FLOOR_PCT
# asyncpg binds this as a real timestamptz — a bare string raises DataError.
REGIME_START = datetime(2026, 9, 27, tzinfo=timezone.utc)


async def guard_firing_breakdown() -> None:
    """How many guard-closed trades were actually in crisis territory on
    the trustworthy number (real P&L), vs. merely tripped by the impact
    field. real_pnl_pct is stored *100 (see the worker's write site)."""
    print("=== Guard-closed live trades, by REAL P&L at the time of closing ===")
    async with get_session() as session:
        res = await session.execute(text("""
            SELECT
              COUNT(*) AS n,
              SUM(CASE WHEN real_pnl_pct <= -30 THEN 1 ELSE 0 END) AS genuine_crisis,
              SUM(CASE WHEN real_pnl_pct > -10 THEN 1 ELSE 0 END)  AS healthy_at_close,
              ROUND(AVG(real_pnl_pct), 2) AS avg_real_pnl_pct
            FROM confluence_live_trades
            WHERE exit_reason = 'LIQUIDITY_GUARD' AND real_pnl_pct IS NOT NULL
        """))
        r = res.first()
    if not r or not r.n:
        print("  (no guard-closed trades with a recorded real P&L)")
        return
    print(f"  n={r.n}")
    print(f"  genuinely in crisis (real P&L <= -30%): {r.genuine_crisis}"
          f"  ({r.genuine_crisis / r.n:.0%}) — these are the guard's real saves")
    print(f"  still healthy (real P&L > -10%):        {r.healthy_at_close}"
          f"  ({r.healthy_at_close / r.n:.0%}) — these are the false positives")
    print(f"  average real P&L at close: {r.avg_real_pnl_pct}%")
    print()


async def shadow_vs_live() -> None:
    """The controlled comparison: identical signals/filters/exits, the
    guard being the material difference."""
    print(f"=== Shadow (no guard) vs Live (guard on), entries since {REGIME_START:%Y-%m-%d} ===")
    async with get_session() as session:
        shadow = (await session.execute(text("""
            SELECT COUNT(*) n, SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) wins
            FROM confluence_shadow_positions
            WHERE status = 'closed' AND entry_time >= :start
        """), {"start": REGIME_START})).first()
        live = (await session.execute(text("""
            SELECT COUNT(*) n, SUM(CASE WHEN COALESCE(real_pnl_usd, pnl_usd) > 0 THEN 1 ELSE 0 END) wins
            FROM confluence_live_trades
            WHERE status = 'closed' AND entry_time >= :start
        """), {"start": REGIME_START})).first()
        live_exits = (await session.execute(text("""
            SELECT exit_reason, COUNT(*) n FROM confluence_live_trades
            WHERE status = 'closed' AND entry_time >= :start
            GROUP BY exit_reason ORDER BY n DESC
        """), {"start": REGIME_START})).all()

    if shadow and shadow.n:
        print(f"  SHADOW (no guard): n={shadow.n}, win_rate={shadow.wins / shadow.n:.0%}"
              "   <- optimistic: zero fees/slippage modeled, snapshot prices")
    if live and live.n:
        print(f"  LIVE   (guard on): n={live.n}, win_rate={live.wins / live.n:.0%}")
        print(f"  live exits: {{{', '.join(f'{e.exit_reason}: {e.n}' for e in live_exits)}}}")
    print()


FIX_DEPLOYED = datetime(2026, 10, 1, 6, 11, tzinfo=timezone.utc)  # restart that shipped the fix


async def prospective_since_fix() -> None:
    """Live vs shadow for entries AFTER the executable-proceeds fix."""
    print(f"=== Prospective: entries since fix ({FIX_DEPLOYED:%Y-%m-%d %H:%M}Z) ===")
    async with get_session() as session:
        sh = (await session.execute(text("""
            SELECT COUNT(*) n, SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) wins, ROUND(AVG(pnl_pct),2) avg
            FROM confluence_shadow_positions WHERE status='closed' AND entry_time >= :t"""),
            {"t": FIX_DEPLOYED})).first()
        lv = (await session.execute(text("""
            SELECT COUNT(*) n, SUM(CASE WHEN COALESCE(real_pnl_usd,pnl_usd) > 0 THEN 1 ELSE 0 END) wins,
                   ROUND(SUM(COALESCE(real_pnl_usd,pnl_usd)),4) usd
            FROM confluence_live_trades WHERE status='closed' AND entry_time >= :t"""),
            {"t": FIX_DEPLOYED})).first()
        ex = (await session.execute(text("""
            SELECT exit_reason, COUNT(*) n FROM confluence_live_trades
            WHERE status='closed' AND entry_time >= :t GROUP BY 1 ORDER BY 2 DESC"""),
            {"t": FIX_DEPLOYED})).all()
    print(f"  shadow: n={sh.n} wins={sh.wins} avg_pnl_pct={sh.avg}")
    print(f"  live:   n={lv.n} wins={lv.wins} real_pnl_usd={lv.usd}")
    print(f"  live exits: {{{', '.join(f'{e.exit_reason}: {e.n}' for e in ex)}}}")
    print("  (judge once n>=30 live; shadow remains optimistic - no fees/slippage)\n")


async def _quote(client: httpx.AsyncClient, input_mint: str, output_mint: str, amount: int) -> dict:
    resp = await client.get(_JUP, params={
        "inputMint": input_mint, "outputMint": output_mint, "amount": amount,
    })
    resp.raise_for_status()
    return resp.json()


async def live_control_quote() -> None:
    """Proof 2: the field is a fraction and reads correctly on a deep
    pool, but comes back as the sentinel "1" on the thin memecoin routes
    the bot actually trades."""
    print("=== Live Jupiter control (read-only, no keys, no orders) ===")
    async with get_session() as session:
        mints = (await session.execute(text("""
            SELECT t.mint_address, clt.entry_token_lamports
            FROM confluence_live_trades clt JOIN tokens t ON t.id = clt.token_id
            WHERE clt.exit_reason = 'LIQUIDITY_GUARD' AND clt.entry_token_lamports IS NOT NULL
            ORDER BY clt.entry_time DESC LIMIT 3
        """))).all()

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            control = await _quote(client, _SOL, _USDC, 10_000_000)  # 0.01 SOL, deep pool
            print(f"  deep pool  SOL->USDC 0.01 SOL : priceImpactPct={control.get('priceImpactPct')!r}"
                  "   <- sane, so the unit is a FRACTION and 0.35 is read correctly")
        except Exception as exc:
            print(f"  control quote failed: {exc}")

        for m in mints:
            try:
                q = await _quote(client, m.mint_address, _SOL, int(m.entry_token_lamports))
                print(f"  guarded mint {m.mint_address[:12]}… : "
                      f"priceImpactPct={q.get('priceImpactPct')!r}, outAmount={q.get('outAmount')}"
                      f"{'   <- route exists and returns real proceeds' if q.get('outAmount') else ''}")
            except Exception as exc:
                print(f"  {m.mint_address[:12]}… quote failed: {exc}")
    print()


async def main() -> None:
    print(__doc__.split("Usage:")[0].strip()[:0] or "", end="")  # keep stdout clean; docstring is the write-up
    print("liquidity_guard_false_positives — ANALYSIS ONLY, nothing changed\n")
    await guard_firing_breakdown()
    await shadow_vs_live()
    await live_control_quote()
    await prospective_since_fix()
    print("Suggested fix is described in this file's module docstring and is")
    print("deliberately NOT applied — live trading is unchanged by this script.")


if __name__ == "__main__":
    asyncio.run(main())
