# S1Wave — orientation for whoever picks this up next

**If you are an AI model reading this cold: read this whole file before touching anything. This trades real money.**

## Current standing orders (2026-09-24, from the user, still in force until told otherwise)

1. **S1Wave is the priority.** Claude usage resets Monday (2026-09-28) — until then, spend effort here before contentpipe or anything else.
2. **Goal: make the account profitable and stable by Sept 28.** Not "keep it running" — actually improve the real numbers. Use the live dataset (shadow + live trades) to keep finding and shipping data-backed entry/exit improvements, the same way the wash-trading and liquidity-ceiling filters got built (see "How past improvements got made" below) — that pattern is the playbook, keep running it.
3. **Keep the ledger accurate and keep reclaiming stranded money.** The rent-auto-reclaim fix (below) must keep working — verify it after any deploy that touches `engine/execution.py` or `engine/manual_actions.py`. See "Known accounting gap" below for a real, still-open imprecision in the displayed P&L.
4. **Don't run standing background monitors.** Check status on demand (`GET /confluence/status`, `/confluence/live/trades`) instead of a persistent log-tail — burns usage for little benefit. See `/home/solana/.claude/projects/-home-solana/memory/feedback_no_proactive_monitoring.md`.
5. **Everything must survive a reboot and resume correctly blind.** Confirmed as of 2026-09-24: `s1wave-bot.service` is `enabled` (systemd --user) and `loginctl show-user solana` reports `Linger=yes`, so it starts on boot without a login session. `CONFLUENCE_LIVE_ENABLED` and all thresholds live in `.env`, not just process memory, so a restart preserves the armed/paused state. If you're resuming after a gap: run the "Resuming after a blind period" checklist below before assuming anything about current state.

## What this is

A real-money Solana memecoin trading bot. `workers/confluence_live_worker.py` is the **only** component that can move real funds — dedicated wallet (`CONFLUENCE_LIVE_WALLET_PRIVATE_KEY` in `.env`, public address `7yyvL2cSnnbtxbosL9kdVZbWZLJFpzLKUhYevvxZLmqS` — safe to share; **never echo the private key**). `workers/confluence_shadow_worker.py` runs the exact same entry/exit rules on paper, no money, purely as a larger-sample benchmark for whether the rules have real edge.

Entry signal (`workers/momentum_signal.py`): a >5.3% price move over a trailing 3-minute window, with `n_rules_cofiring >= 2` secondary rules also true at that moment (`workers/entry_filters.py` — see below — then further screens the candidate before it's allowed to actually open).

Dashboard: https://s1wave-solana.duckdns.org/ (Basic Auth `solana`/`DUGqntc_fudiuXxw`) — live SSE stream, Pause/Resume, per-position Close + Close All buttons.

## Real money, right now (verify fresh, don't trust this number as still-current)

As of 2026-09-24 ~18:30 UTC: wallet ~$6.19, all-time realized P&L ~-$1.27 (see accounting gap note below — the real all-in picture is somewhat worse than this field alone suggests), 3 open positions, ~24 closed. Original deposit: exactly 0.09277473 SOL, confirmed on-chain, received 2026-09-24 08:27:31 UTC — the wallet's only-ever deposit.

**Always re-check via the API, never trust a number in this doc or in your own memory of a past conversation**: `GET /confluence/status`, `GET /confluence/live/trades?status=open`.

## Architecture map

| File | Role |
|---|---|
| `workers/confluence_live_worker.py` | The only thing that spends real money. Entry, exit, halt gates, notifications. |
| `workers/confluence_shadow_worker.py` | Paper-trading twin — same rules, no money, bigger sample size. |
| `workers/momentum_signal.py` + `shared_snapshot.py` | The entry signal itself. |
| `workers/entry_filters.py` | Two data-backed skip conditions checked before either worker opens a position — **read this file's docstring first**, it has the full analysis behind both filters. |
| `engine/execution.py` | Real Jupiter swaps (buy/sell), token balance reads, `get_sell_quote()` (read-only liquidity check), `close_token_account()` (auto rent reclaim). |
| `engine/manual_actions.py` | `close_trade_manually()` — real on-chain sell, backs the dashboard's Close/Close All buttons. |
| `engine/sell_coordination.py` | In-process guard (2026-09-24) so the worker's automatic exit loop and a manual close can never both submit a real sell for the same trade at once — see "Real race" note below. |
| `engine/live_equity.py` | Equity = **live wallet SOL balance only**, deliberately excludes open-position value. This is why "wallet + realized P&L" will never exactly equal a deposit while positions are open — see the balance-reconciliation section of NOTEBOOK.md if this confuses a future session again. |
| `engine/trailing_stop.py` | The 10%-step trailing-stop staircase. `_INITIAL_STOP_PCT = 0.12` here is the ONLY real enforcement point — `settings.STOP_LOSS_PCT` is a separate, must-match-by-hand field used only for a validator, not actual behavior. |
| `api/app.py` | Dashboard backend — status, trades, SSE stream, toggle, manual close(s). |
| `static/s1wave_dashboard.html` | The dashboard itself. |

## The three live entry filters (as of 2026-09-24)

All three implemented in `workers/entry_filters.py`, checked in order, in both workers, right before a position would open:

1. `is_wash_trading_rejected()` — skip if the token's most recent TIER1 evaluation before the signal was a `WASH_TRADING` rejection.
2. `is_liquidity_too_high()` — skip if that same evaluation reports `liquidity_usd >= $30,000`.
3. `is_buy_pressure_too_low()` — skip if the triggering MomentumSignalEvent's `buy_pressure < 0.97`. The cleanest, most monotonic discriminator found yet (11.7% vs ~35-44% HARD_FLOOR rate) — pure/synchronous, no DB query, since the value is already on the row both workers query.

Skipped candidates are recorded (`status='wash_skipped'` / `'high_liq_skip'` / `'low_bp_skip'`), not silently dropped, specifically so a future analysis pass can measure whether each filter is actually helping. **Read the module docstring for the full numbers before changing any threshold** — they came from analyzing the shadow dataset (262, then 304 trades) against every available metric, not guesses.

## MAJOR: real on-chain audit trail, and a real accounting gap of ~$6.71 found (2026-09-28)

A 3-day dry-spell investigation (by "Toni") led to a full wallet reconciliation, independently verified in a separate pass (direct RPC balance-delta computation, not just re-reading their numbers). **Confirmed real: `entry_sol_lamports`/`exit_sol_lamports`/`pnl_usd` are all built from INTENDED swap amounts, and understated real spend by 6.36x in aggregate across 71 trades.** Root cause, confirmed via a direct on-chain instruction trace: some sells must create a Pump.fun protocol-fee token account owned by Pump.fun's fee collector (`JCRGumoE9Qi5BBgULTgdgTLjSgkCMSbF62ZZfGs84JeU`), never this wallet, never reclaimable (~$0.18, confirmed NOT universal — looks like a first-sell-per-mint cost). On one confirmed trade this fee alone exceeded the entire gain, turning a "+126% winner" by the old accounting into a real net loss. **The real, ground-truth account-level loss is deposit-vs-current-balance, not a sum of `pnl_usd`** — check `GET /confluence/status`'s `deposit_vs_balance_gap_usd` field for the honest number; do not trust `all_time_realized_pnl_usd` alone anymore.

Fixed: every real swap now captures its actual wallet balance delta directly from the confirmed transaction (`ExecutionEngine.actual_sol_lamports`, via `_confirm_tx_with_delta()` — no extra RPC cost), plus the separate rent-reclaim transaction's real signature and amount (`close_token_account_with_amount()`). Five new columns on `confluence_live_trades` (`entry_real_sol_lamports`, `exit_real_sol_lamports`, `reclaim_tx_signature`, `reclaim_sol_lamports`, `real_pnl_usd`) are now populated automatically on every close, in both the automatic and manual-close paths. `real_pnl_usd` is the number to trust going forward — it's NULL (never a fabricated partial number) for trades closed before 2026-09-28, since those can't be retroactively known without a manual forensic lookup per trade (I tried a quick automated version of this and got it wrong — see the git commit for what I got wrong and why).

Dashboard now shows "REAL vs. DEPOSIT" next to the old recorded P&L, and the trade history table shows `real_pnl_usd` (✓-marked) as the primary number once known, flagging "≠rec" when it disagrees with the old figure. Every trade row links to Solscan for the mint and both transactions.

**If someone hands you a forensic report like Toni's again: verify it independently before acting, using a DIFFERENT method than they used** — this is exactly how the -$19 bug in my own first reconciliation attempt (forgetting reclaims are a separate, unlisted transaction) got caught before it was reported as real.

## Stats are now scoped to the current filter regime, not misleading all-time mixes (2026-09-25)

`GET /confluence/shadow/stats` and the new `GET /confluence/live/stats` both return numbers scoped to `entry_time >= workers/entry_filters.py::CURRENT_FILTER_REGIME_SINCE` at the top level (the honest, "with today's rules" number), with the full all-time history nested under `all_time`. Real problem this fixes: a live win rate that looked like 36% turned out to be 66% once trades from before all 3 current filters existed were excluded — don't ever eyeball a raw all-time win rate again without checking whether it's mixing filter eras. **Update `CURRENT_FILTER_REGIME_SINCE` whenever a filter ships or a threshold changes materially** — both endpoints' honesty depends on that constant staying current. Note: the "since current filters" sample can be small for a while after a filter change (it only counts what's actually happened since then) — don't over-read a tiny-n number; use the retroactive-reapplication method (join closed trades against `TokenEvaluation`/`MomentumSignalEvent` and recompute what would have passed today's rules against the FULL historical set) for a bigger validation sample, same as how the buy-pressure filter itself was originally derived.

## SOLVED: SEND was never a drained pool (2026-09-25)

The "unsellable" SEND position from 2026-09-24 (see below) was never actually stuck on a drained pool — that was a wrong diagnosis. Real forensic on-chain trace found the truth: the liquidity guard's real sell actually confirmed on-chain 5 seconds after entry, but the service happened to restart mid-confirmation-poll at that exact moment, so the DB write never happened. Every retry since then failed only because we no longer held any tokens — not because the pool was drained. Reconciled the ledger with the real on-chain data (exit_reason `RECONCILED_ONCHAIN`) and reclaimed the stranded rent. Two standing fixes now guard against this recurring: (1) once a sell has failed once, the real on-chain balance is checked before retrying again — a confirmed zero moves the trade to a new `balance_zero` status instead of retrying forever; (2) `ExecutionEngine.sweep_dead_token_accounts()` + `_maybe_sweep_rent()` run every 30 minutes, closing any zero-balance token account regardless of how it went to zero. If you ever see status `balance_zero` on a trade, that means the position is already resolved on-chain but its exact real P&L needs a manual forensic lookup (same method used for SEND: find the wallet's real sell transaction from around entry_time via `get_signatures_for_address`/`get_transaction`) before the ledger can be closed out accurately — never fabricate a number for it.

## Unsellable positions (as of 2026-09-24)

A position whose real sell keeps failing (confirmed real on-chain rejection, e.g. a fully-drained pool — not a bug on our end) gets downgraded from `status='open'` to `status='unsellable'` after `_UNSELLABLE_AFTER_S` (10 min) of continuous failure. This frees its `CONFLUENCE_LIVE_MAX_CONCURRENT` slot for a new trade (`_open_trade_count()` only counts `'open'`) while it keeps being priced and keeps getting real sell attempts every cycle (`_load_open_trades()` includes both). The **only** way out is a real successful sell — never a fabricated close. Surfaced on the dashboard as a red "⛔ STUCK" badge and a separate `unsellable_trades` count on `/confluence/status`, so stuck money is never just invisible. **Known limitation**: the failure-duration clock lives in-process memory, so a service restart resets it for any currently-stuck position — acceptable for now, but worth remembering if a stuck position seems to "reset" after a deploy.

## Ring-fenced capital: two protection fixes (2026-09-25, "protect our money first")

1. **Entry cost ceiling.** Real incident: two buys cost ~$0.55 instead of ~$0.19 — both were the first-ever trade against a just-migrated pool, and Solana made our transaction pay to create the pool's OWN vault accounts (never ours, never reclaimable). `engine/execution.py::_check_entry_cost()` now simulates every buy (free — no fee, no rent) before ever signing it, and refuses to submit if the projected real cost exceeds `sol_lamports + _MAX_ENTRY_RENT_OVERHEAD_LAMPORTS` (2.2M lamports). Fails closed on any simulation error. If you see `error_type: "entry_too_expensive"` on a skipped entry, this is why — real money was protected, not lost.
2. **Faster liquidity check when in real profit.** Real incident: a position's DexScreener snapshot climbed to +40% over 4 minutes while the REAL price had already collapsed to -33% underneath — the 60s-throttled guard only caught it once, at the very end. Now checks every 30s instead of 60s once a position's snapshot-based unrealized gain crosses 15% (`_LIQUIDITY_CHECK_INTERVAL_FAST_S`/`_LIQUIDITY_CHECK_FAST_THRESHOLD_PCT` in `confluence_live_worker.py`). Deliberately not applied to every position — a faster universal interval already caused a real rate-limit incident earlier this session.

## Dashboard now shows real, verified prices instead of the raw snapshot (2026-09-25)

The dashboard used to show `current_price` — a raw DexScreener snapshot that can sit still ("frozen") on thin liquidity for a while even though the position is perfectly fine and tradeable. That kept reading as broken to whoever's watching it. Now `_check_liquidity_guard()` (already fetching a real Jupiter sell quote every ~60s per position) persists `real_price`/`real_pnl_pct`/`real_price_checked_at` onto the trade row every time it runs, not just on a crisis, and the dashboard shows that as the primary number, labeled "(verified Ns ago)". `current_price` still exists in the API response for reference but is no longer the trusted number. A brand-new position (<~60s old) shows "(checking…)" with the snapshot until its first real check lands.

## Fixed: a stuck position's endless retries were starving new trades of API quota (2026-09-25)

User asked "why no trades" and the real answer wasn't the entry filters being strict — SEND's own endless sell-retry loop (already `unsellable` from the day before) was hitting Jupiter's shared `/swap` endpoint every ~6 seconds forever, and a genuinely fillable new buy (Luckin) got 429'd at the exact same instant SEND's retry loop did, confirmed in the journal down to the millisecond. Fixed with `_SELL_BACKOFF_AFTER_S`/`_SELL_BACKOFF_INTERVAL_S` (30s each, in `confluence_live_worker.py`): once a sell has been failing that long, real attempts space out to at most once per 30s instead of every cycle. If trades seem to have stopped and there's an `unsellable` position, check for `execution.swap_build_error`/`quote_error` 429s in the journal near the time of any real `entry_failed` — this exact pattern is why.

## Fixed: manual close vs. automatic exit could race (2026-09-24)

User report: "there are positions active but cant close there." Real cause, found live: the worker's own ~1s exit loop and the dashboard's manual close endpoint could both submit a real sell for the same trade at the same instant (a position tripping TIME_EXIT/HARD_FLOOR right as a human clicks Close). Whichever swap landed second was rejected on-chain (real custom program errors) and reported a scary failure for a position that, a moment later, usually wasn't actually stuck — the other path had already closed it fine. Fixed with `engine/sell_coordination.py`, a simple in-process claim both paths check before calling `ExecutionEngine.sell()`. If you ever see a manual close return "the bot's own exit logic is already closing this position," that's this guard working as intended — wait a few seconds and re-check status, don't retry immediately.

## Known live issue: sampling worker's API key is out of credits (2026-09-24, urgent, needs a human)

`SOLANA_TRACKER_API_KEY` (used only by `workers/sampling_worker.py`, separate from `SOLANA_TRACKER_API_KEY_DISCOVERY` which `discovery_worker.py` uses and which still has credits) is returning `403 {"error":"Insufficient credits for this request"}` on **every single call** — confirmed via a direct curl test, not just log noise. Over 1,000 failed calls in the hour before this was found. This degrades price-snapshot coverage for any WATCHING/OBSERVING token not already covered by `discovery_worker`'s own 60s poll (see `sampling_worker.py`'s module docstring on the discovery/sampling sync) — meaning `MomentumSignalEvent` (the real entry-signal table, see its corrected docstring in `models/orm.py`) may be missing trigger opportunities on tokens discovery isn't independently re-polling. **This needs a human to top up SolanaTracker credits** — it's a billing action, not something to route around in code without checking with the user first. Don't silently point `sampling_worker` at the discovery key as a workaround without confirming the discovery key has spare quota for the added load; they were deliberately split so one budget can't starve the other.

## Known accounting gap (real, open, low priority but real)

`ConfluenceLiveTrade.pnl_usd` = `exit_proceeds_usd - position_usd` (the *intended* entry size), not the real total lamports debited at entry (which is larger by that trade's token-account rent + network fee). Rent gets fully reclaimed on exit (confirmed working, zero stranded accounts as of 2026-09-24), so it's not a permanent loss — but it means the displayed all-time P&L is somewhat rosier than the true all-in cash-flow picture, especially at these tiny (~$0.03-0.04) position sizes where the fixed ~$0.17-0.18 rent cost per new token account is often bigger than the position itself. Full derivation (real on-chain ledger reconciliation, lamport-exact) is in NOTEBOOK.md's 2026-09-24 "balance reconciliation" sections. Not fixed as of this writing — a real, scoped potential improvement if someone wants to take it on, but don't rush a change to this formula without full test coverage; it feeds the halt-threshold math (`CONFLUENCE_LIVE_MAX_LOSS_USD`).

## How past improvements got made (the playbook — repeat this)

1. Pull real closed trades (shadow for volume, live for ground truth) joined against `TokenEvaluation` (TIER1's real inputs: age, liquidity, market cap, lp_burn, wash_multiplier, mint/freeze authority, buy_pressure, volume_mult) and `MomentumSignalEvent` (n_rules_cofiring, trailing_return_3min).
2. Bucket by each metric, compute rug rate / capped mean (cap each trade at +100% — a couple of historical trades have unrealistic $0-liquidity "peak" prices that inflate a raw mean) / win rate per bucket.
3. Cross-tab candidate filters against each other before trusting either (a lot of "liquidity" and "LP burn" signal turned out to be the same underlying population — check overlap before claiming two independent filters).
4. Verify a real quote-level sanity check where possible (a dashboard "current price" can be badly stale — see the BLK incident in NOTEBOOK.md — always cross-check a real Jupiter quote before trusting a DexScreener snapshot for anything action-worthy).
5. Implement as a narrow, specific, tested skip condition (see `workers/entry_filters.py`'s two functions as the template), never a broad "block everything TIER1 didn't like."
6. **Watch the status column length.** `ConfluenceLiveTrade.status` and `ConfluenceShadowPosition.status` are `VARCHAR(16)`. A skip-status literal over 16 chars crashed the shadow worker for ~5 minutes the same day the wash-trading filter shipped. Keep new status values short and check `len()` before deploying.
7. Deploy, then watch the *rate limit*, not just correctness — the liquidity guard's first deploy was checking Jupiter's free-tier API too often (3 positions x one check/20s) and got 100% 429'd for several minutes before anyone noticed it was silently providing zero protection. Real external APIs have real limits; a background check that "should be cheap" can still saturate a shared quota.

## Resuming after a blind period (reboot, crash, long gap)

1. `systemctl --user status s1wave-bot.service` — should be `active`. If not, `systemctl --user start s1wave-bot.service` (should also auto-start on boot given it's enabled + linger is on — investigate why it didn't if you find it stopped after a real reboot).
2. `curl -s -u solana:DUGqntc_fudiuXxw https://s1wave-solana.duckdns.org/confluence/status` — check `enabled` (armed or paused?), `wallet_usd`, `open_trades`, `permanently_halted`, `daily_halted`. Don't assume any of these match what an old conversation said.
3. `GET /confluence/live/trades?status=open` — for each open position, get a **real** Jupiter sell quote for its exact `entry_token_lamports` before trusting the dashboard's displayed P&L (see the liquidity-guard/BLK incident — a stale snapshot price can show a fake winner). The guard should be doing this automatically every 60s per position; spot-check it's actually happening (`journalctl --user -u s1wave-bot.service --since "5 min ago" | grep liquidity`) rather than assuming.
4. Check `journalctl --user -u s1wave-bot.service --since "10 min ago"` for anything alarming (`sell_failed_critical`, repeated `quote_error`/429, `cycle_error`) before doing anything else.
5. Only then decide whether to keep operating as-is, pause, or intervene on a specific position.

## Testing discipline (keep this up)

237 tests as of 2026-09-24 (`pytest -q` from `/home/solana/s1wave-bot/solanabot`, venv at `.venv`). Every real bug fix and every new filter in this codebase's history has a regression test using **real recorded numbers from the actual incident**, not synthetic round numbers — keep that convention. Run the full suite before every deploy; restart the service (`systemctl --user restart s1wave-bot.service`) after; confirm a clean start (`journalctl --user -u s1wave-bot.service --since "10 sec ago"`) before considering a change shipped.

## Where the rest of the history lives

`/home/solana/NOTEBOOK.md` has the full narrative — every bug, every real number, every decision, in the order it happened, across this and every other project on this machine. This file is the orientation; that file is the record. Update both when you make a real change.
