"""
config/settings.py
==================
Single source of truth for all configuration.

All values are loaded from environment variables (or a .env file).
Validation happens at process startup — a missing or malformed value
raises immediately rather than at the point of first use.

Usage
-----
    from config.settings import settings

    poll_interval = settings.DEXSCREENER_POLL_INTERVAL
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # ── Helius ──────────────────────────────────────────────────────────────
    HELIUS_API_KEY: str
    HELIUS_RPC_URL: str
    HELIUS_WS_URL: str

    # ── Database ────────────────────────────────────────────────────────────
    DATABASE_URL: str  # must use asyncpg driver: postgresql+asyncpg://...

    # ── Telegram ────────────────────────────────────────────────────────────
    # Optional — leave both blank to run with alerts disabled entirely.
    # NotificationWorker and telegram_alerts.py both no-op cleanly when unset.
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # ── SolanaTracker ────────────────────────────────────────────────────────
    SOLANA_TRACKER_API_KEY: str = ""            # sampling + S1 Wave's price re-check
    # Discovery's own key/rate-limit lane, isolated from sampling's above.
    # Previously named SOLANA_TRACKER_API_KEY_MONITOR and used by
    # TradeMonitorWorker — that worker now uses DexScreener (free, no key)
    # instead, so this slot was repurposed for discovery. As of 2026-09-21
    # this holds a genuinely separate SolanaTracker account/key, so
    # discovery now has real quota separation from sampling, not just a
    # separate rate-limit queue — see discovery_worker.py.
    SOLANA_TRACKER_API_KEY_DISCOVERY: str = ""

    # ── Execution (Phase 9) ───────────────────────────────────────────────
    PAPER_TRADING: bool = True               # True = simulation only, False = live
    WALLET_PRIVATE_KEY: str = ""             # base58 encoded keypair
    SOLANA_RPC_URL: str = ""                 # Helius HTTP RPC endpoint
    # 2026-09-24 — found live, during the first real confluence_live trade
    # attempts: the old quote-api.jup.ag/v6 base is DEAD (curl: connection
    # failure / HTTP status 000, not a 404 — Jupiter has since moved their
    # free tier to lite-api.jup.ag). engine/execution.py's _get_quote()
    # catches any exception and returns None, which the caller reports as
    # the generic "No route found" — so this looked exactly like "this
    # token has no liquidity" for every single attempt, when the real cause
    # was the bot never reaching Jupiter at all. Confirmed real liquidity
    # and a real route exist for the 3 tokens that failed this way
    # (verified directly against the new endpoint: a live 200 quote via
    # "Pump.fun Amm", and the matching /swap endpoint answers real
    # requests too) before changing this.
    JUPITER_API_URL: str = "https://lite-api.jup.ag/swap/v1"
    SLIPPAGE_BPS: int = 500                  # 5% slippage tolerance for memecoins
    SOL_PRICE_USD: float = 0.0               # Fallback — updated by heartbeat worker

    # ── Confluence live trading (Phase 16, 2026-09-23) ─────────────────────
    # A second, fully isolated real-money path for the confluence_entry_v1
    # strategy alone — never touches PAPER_TRADING, WALLET_PRIVATE_KEY,
    # CapitalEngine, or the STRONG_BUY/S1_WAVE paths in any way. Defaults
    # here are deliberately the safest possible values: disabled — do not
    # raise the percentages/ceilings below without a fresh, explicit
    # instruction from the user, they are real risk decisions, not tuning
    # knobs.
    CONFLUENCE_LIVE_ENABLED: bool = False                    # master kill switch — stays False until the user explicitly arms it
    CONFLUENCE_LIVE_WALLET_PRIVATE_KEY: str = ""             # dedicated wallet, isolated from WALLET_PRIVATE_KEY
    # 2026-09-24: equity is now the LIVE on-chain wallet balance, not a
    # fixed stake figure — see engine/live_equity.py for the full model and
    # why (the user's own instruction: "$7 deposited -> trade with $7,
    # $15 deposited -> trade with $15 ... size trades with available
    # wallet capital"). Fund the wallet with whatever amount you want at
    # risk; the bot sizes and halts against that real balance automatically,
    # with no config change needed per deposit.
    #
    # Dust floor for the permanent-halt gate (engine/live_equity.py's
    # is_permanently_halted()) — below this, swap fees/slippage would
    # dominate any trade, so treat the account as tapped out rather than
    # attempt an ever-shrinking sequence of sub-dollar trades.
    CONFLUENCE_LIVE_MIN_TRADEABLE_USD: Annotated[float, Field(gt=0)] = 1.0
    # 2026-09-24, user's explicit instruction for the first real test run:
    # "set max loss to $3". Absolute-dollar lifetime cap, independent of the
    # dust floor above — once ALL-TIME REALIZED LOSS across every closed
    # confluence_live_trades row reaches this amount, halt PERMANENTLY, same
    # severity as the dust-floor halt, regardless of how much equity is
    # still technically in the wallet. This is a real risk decision for a
    # live test phase, not a tuning knob — do not raise it without a fresh,
    # explicit instruction.
    CONFLUENCE_LIVE_MAX_LOSS_USD: Annotated[float, Field(gt=0)] = 3.0
    # Outer sanity ceiling on a single position, independent of how much
    # the account has compounded or how much is deposited — NOT the
    # everyday position size (see CONFLUENCE_LIVE_EXPOSURE_PCT below for
    # that). A backstop against a mis-sized trade if equity is ever
    # unexpectedly large, not a number meant to bind in normal operation.
    CONFLUENCE_LIVE_MAX_POSITION_USD: Annotated[float, Field(gt=0)] = 50.0
    # Raised from 1 to 3 (2026-09-23) after analysis/sl_tp_and_concurrency_
    # sweep.py replayed all 200 closed confluence_shadow_positions rows:
    # one-at-a-time trading only ever captured 15 of 196 real signals (the
    # rest were skipped as overlapping) and that small, concentrated
    # sequence was what produced the near-total wipeout found earlier
    # (a real losing streak landing on a single large bet). Splitting the
    # same capital across 3 concurrent, proportionally smaller positions
    # captured 37 signals in the same replay and cut max drawdown from
    # 99% to 34% — the best point found; 5+ slots captured more signals
    # but did not reduce drawdown further or improve the final result, so
    # there's no data-backed case for going wider than 3.
    #
    # 2026-09-24: lowered to 1 for the user's first live test run ("single
    # trade at a time so we confirm it works"), then raised back to 3 the
    # same day once that was verified end-to-end ("reduce position sizing
    # so we can run 3 trades at a time") — CONFLUENCE_LIVE_EXPOSURE_PCT
    # stayed at 2% total, so raising this back to 3 automatically shrinks
    # each individual trade to ~0.67% of equity (2% / 3), not a separate
    # change — see that setting's comment for the exact math.
    #
    # 2026-09-24, later the same day: raised 3 -> 5 per the user's explicit
    # instruction to "slightly decrease position sizing then increase
    # concurrent trades to 5" — after a live "why are we losing" review
    # found the two live entry filters (workers/entry_filters.py) don't
    # yet have enough post-deploy live volume to judge (1 real closed
    # trade), and the user chose to lean on the shadow benchmark (302+
    # closed paper trades, zero capital/overhead distortion) as the primary
    # study dataset going forward, using live trading mainly to validate
    # real execution at reduced per-trade size. NOTE: the 2026-09-23
    # concurrency sweep (analysis/sl_tp_and_concurrency_sweep.py, above)
    # found 5+ slots captured more signals than 3 but did NOT reduce
    # drawdown or improve the final replayed result any further — this
    # change is explicitly for signal coverage/data volume, not because
    # new evidence overturned that finding.
    CONFLUENCE_LIVE_MAX_CONCURRENT: Annotated[int, Field(ge=1)] = 5
    # Exposure-percentage sizing (2026-09-23, replacing the earlier fixed-
    # stake-plus-profit-share formula the same day; 2026-09-24, lowered
    # 10% -> 2% per the user's instruction "each trade 2% of available
    # balance" while CONFLUENCE_LIVE_MAX_CONCURRENT was still 1; lowered
    # again 2% -> 1.5% the same day once MAX_CONCURRENT went back to 3, per
    # the user's explicit instruction "each trade should be 0.5% now ... so
    # we can meet up with our research profit factor and not hold the
    # software back" — i.e. size each individual slot at 0.5% directly,
    # which at 3 slots means 1.5% total).
    # Position sizing (engine/live_equity.py's compute_position_usd()):
    #   equity          = live wallet SOL balance * SOL_PRICE_USD  (2026-09-24)
    #   total_exposure  = equity * CONFLUENCE_LIVE_EXPOSURE_PCT
    #   position_usd    = total_exposure / CONFLUENCE_LIVE_MAX_CONCURRENT
    # i.e. never more than this fraction of CURRENT total capital is at
    # risk across every open position combined, split evenly across the
    # concurrent slots — at MAX_CONCURRENT=3, 1.5% total / 3 slots = 0.5%
    # of equity per trade; left unchanged at 1.5% total when
    # MAX_CONCURRENT went 3->5 (2026-09-24), so each slot's real size
    # dropped to 0.3% of equity (1.5% / 5) as a direct, intended
    # consequence of that change, not a separate edit here.
    # This compounds automatically (equity moves with
    # the wallet's real balance — deposits, withdrawals, and realized P&L
    # all show up in it for free) without a separate profit-redeployment
    # knob, and is inherently protective: after a loss, equity is smaller,
    # so the very next position is automatically smaller too.
    #
    # 2026-09-28: raised 1.5% -> 50% (total, still / 5 slots = 10% of
    # equity per slot), per the user's explicit instruction after the
    # real on-chain audit trail found the account's real losses were
    # driven almost entirely by a fixed-dollar fee floor, not bad signal
    # quality: average real fee per trade (~$0.25, mostly the
    # non-reclaimable Pump.fun protocol-fee account) was ~91% of the
    # average ~$0.29 real position size at 1.5%. At the post-top-up
    # equity (~$11.30, after the user added $7), 50% total / 5 slots
    # produces ~$1.13/trade — fee ratio drops to ~22%, the level judged
    # to actually give the entry signal's real edge (validated the same
    # day against a 3x-bigger 403-trade shadow sample — see NOTEBOOK.md's
    # 2026-09-28 entry) room to show up in the real dollar P&L instead of
    # being pre-determined by fixed costs. MAX_CONCURRENT stayed at 5
    # (user's explicit instruction, not raised alongside this).
    CONFLUENCE_LIVE_EXPOSURE_PCT: Annotated[float, Field(gt=0, le=1)] = 0.50
    # Real daily-loss circuit breaker, separate from the paper one
    # CapitalEngine/RiskEngine already have — halts new live entries for
    # the rest of the UTC day if today's realized real P&L drops below
    # this fraction of *today's starting* live equity (see
    # engine/live_equity.py's is_daily_halted()).
    CONFLUENCE_LIVE_DAILY_LOSS_LIMIT_PCT: Annotated[float, Field(gt=0, le=1)] = 0.5

    # ── Dashboard auth (2026-09-23) ─────────────────────────────────────────
    # HTTP Basic Auth in front of the whole api/app.py FastAPI app (dashboard
    # + every JSON endpoint) — added specifically because the user is about
    # to open the firewall on settings.API_PORT to the public internet. The
    # dashboard shows the confluence-live wallet address/balance/trading
    # activity; it was built as "local use only, no auth" and must not be
    # left that way once it's internet-reachable. Empty = auth disabled
    # (matches the original local-only default); set both to require login.
    DASHBOARD_AUTH_USER: str = ""
    DASHBOARD_AUTH_PASSWORD: str = ""

    # ── Discovery ───────────────────────────────────────────────────────────
    DEXSCREENER_POLL_INTERVAL: Annotated[int, Field(ge=10, le=300)] = 60
    SAMPLE_INTERVAL_SECONDS: Annotated[int, Field(ge=10, le=300)] = 30

    # How long a Tier1-rejected or Scorer-discarded token stays in OBSERVING
    # and keeps getting sampled for free (piggybacking the same batch call
    # as WATCHING tokens) before it's written off to REJECTED. Widened from
    # 90s to 1800s (30min) so peak-return-at-5/15/30-minutes is actually
    # computable for the control group, not just a near-instant snapshot.
    # Still hard-bounded (never unlimited) — this is the control group, not
    # a second watch list, so it must not grow the sampling batch without
    # limit. It never becomes eligible for WATCH, trading, or alerts
    # regardless of how long the window is.
    OBSERVE_WINDOW_SECONDS: Annotated[int, Field(ge=30, le=3600)] = 1800

    # Longer window for tokens flagged by a shadow-evaluation experiment
    # (see workers/scoring_worker.py's SHADOW_EXPERIMENT_*) — long enough to
    # support a 1-hour outcome horizon. Deliberately scoped to ONLY
    # shadow-flagged tokens (a small subset of OBSERVING), not applied
    # globally, specifically because of the SolanaTracker 20-tokens-per-
    # request limit discovered during the OBSERVE_WINDOW_SECONDS incident —
    # sampling_worker now chunks requests so any batch size works
    # correctly, but a longer window still means more concurrent tokens on
    # average, so it stays opt-in per-token rather than raising the
    # default for everyone.
    SHADOW_OBSERVE_WINDOW_SECONDS: Annotated[int, Field(ge=60, le=7200)] = 3600

    # ── Tier 1 hard gate ────────────────────────────────────────────────────
    TIER1_MIN_LIQUIDITY_USD: Annotated[float, Field(gt=0)] = 12_000.0
    TIER1_MIN_MARKET_CAP_USD: Annotated[float, Field(gt=0)] = 15_000.0
    TIER1_MIN_AGE_MINUTES: Annotated[int, Field(ge=0)] = 0
    TIER1_MAX_AGE_MINUTES: Annotated[int, Field(ge=60)] = 1440
    # wash_multiplier check:
    #   buy_count / sell_count > TIER1_MAX_WASH_MULTIPLIER → likely wash trading → REJECT
    #   buy_count / sell_count <= TIER1_MAX_WASH_MULTIPLIER → organic activity → PASS
    TIER1_MAX_WASH_MULTIPLIER: Annotated[float, Field(gt=1.0)] = 2.5

    # ── Tier 2 momentum scoring ─────────────────────────────────────────────
    TIER2_STRONG_BUY_THRESHOLD: Annotated[float, Field(ge=0, le=10)] = 8.0
    TIER2_WATCH_THRESHOLD: Annotated[float, Field(ge=0, le=10)] = 5.0

    # ── Capital ─────────────────────────────────────────────────────────────
    INITIAL_BALANCE_USD: Annotated[float, Field(gt=0)] = 1_000.0
    TRADE_ALLOCATION_PCT: Annotated[float, Field(gt=0, le=1)] = 0.05
    MAX_CONCURRENT_TRADES: Annotated[int, Field(ge=1, le=10)] = 3
    MIN_POSITION_USD: Annotated[float, Field(gt=0)] = 1.0

    # ── Exit rules ──────────────────────────────────────────────────────────
    # All stored as decimals, e.g. -0.06 = -6%
    #
    # 2026-09-24, widened -6%/-7% -> -12%/-15% per the user's explicit
    # instruction, after two real live trades both exited within ~1 second
    # via HARD_FLOOR on what turned out to be ordinary first-second
    # volatility for a brand-new pump.fun token (real fill P&L came back
    # to roughly breakeven once the sell confirmed a few seconds later).
    # STOP_LOSS_PCT itself does NOT drive the actual initial-stop distance
    # — that's engine/trailing_stop.py's own hardcoded _INITIAL_STOP_PCT,
    # kept dependency-free on purpose. This field is only used below (the
    # HARD_FLOOR-must-be-stricter validation) and for display/logging —
    # keep it equal to trailing_stop.py's constant by hand if either changes.
    STOP_LOSS_PCT: Annotated[float, Field(le=0)] = -0.12
    HARD_FLOOR_PCT: Annotated[float, Field(le=0)] = -0.15
    TAKE_PROFIT_PCT: Annotated[float, Field(gt=0)] = 0.30
    MAX_HOLD_HOURS: Annotated[int, Field(ge=1, le=48)] = 6

    # ── Risk controls ───────────────────────────────────────────────────────
    MAX_DAILY_LOSS_PCT: Annotated[float, Field(gt=0, le=1)] = 0.20
    CB_LOSS_COUNT: Annotated[int, Field(ge=1)] = 3
    CB_PAUSE_MINUTES: Annotated[int, Field(ge=1)] = 60

    # ── API server ──────────────────────────────────────────────────────────
    API_HOST: str = "0.0.0.0"
    API_PORT: Annotated[int, Field(ge=1024, le=65535)] = 8000

    # ── Cross-field validation ───────────────────────────────────────────────

    @model_validator(mode="after")
    def hard_floor_must_be_below_stop_loss(self) -> "Settings":
        if self.HARD_FLOOR_PCT >= self.STOP_LOSS_PCT:
            raise ValueError(
                f"HARD_FLOOR_PCT ({self.HARD_FLOOR_PCT}) must be strictly below "
                f"STOP_LOSS_PCT ({self.STOP_LOSS_PCT}). "
                "Hard floor is an emergency exit — it must trigger after stop loss."
            )
        return self

    @model_validator(mode="after")
    def watch_threshold_must_be_below_strong_buy(self) -> "Settings":
        if self.TIER2_WATCH_THRESHOLD >= self.TIER2_STRONG_BUY_THRESHOLD:
            raise ValueError(
                f"TIER2_WATCH_THRESHOLD ({self.TIER2_WATCH_THRESHOLD}) must be "
                f"below TIER2_STRONG_BUY_THRESHOLD ({self.TIER2_STRONG_BUY_THRESHOLD})."
            )
        return self

    @model_validator(mode="after")
    def age_range_is_valid(self) -> "Settings":
        if self.TIER1_MIN_AGE_MINUTES >= self.TIER1_MAX_AGE_MINUTES:
            raise ValueError(
                f"TIER1_MIN_AGE_MINUTES ({self.TIER1_MIN_AGE_MINUTES}) must be "
                f"below TIER1_MAX_AGE_MINUTES ({self.TIER1_MAX_AGE_MINUTES})."
            )
        return self

    @field_validator("DATABASE_URL")
    @classmethod
    def database_url_must_use_asyncpg(cls, v: str) -> str:
        if not v.startswith("postgresql+asyncpg://"):
            raise ValueError(
                "DATABASE_URL must use the asyncpg driver: "
                "postgresql+asyncpg://user:pass@host:port/dbname"
            )
        return v

    # ── Convenience helpers ──────────────────────────────────────────────────

    @property
    def max_hold_seconds(self) -> int:
        return self.MAX_HOLD_HOURS * 3600

    @property
    def cb_pause_seconds(self) -> int:
        return self.CB_PAUSE_MINUTES * 60


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton settings instance. Cached after first call."""
    return Settings()  # type: ignore[call-arg]


# Module-level singleton — import this everywhere.
settings = get_settings()
