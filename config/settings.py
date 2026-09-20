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
    SOLANA_TRACKER_API_KEY: str = ""         # discovery + sampling
    SOLANA_TRACKER_API_KEY_MONITOR: str = "" # trade monitor (separate rate limit lane)

    # ── Execution (Phase 9) ───────────────────────────────────────────────
    PAPER_TRADING: bool = True               # True = simulation only, False = live
    WALLET_PRIVATE_KEY: str = ""             # base58 encoded keypair
    SOLANA_RPC_URL: str = ""                 # Helius HTTP RPC endpoint
    JUPITER_API_URL: str = "https://quote-api.jup.ag/v6"
    SLIPPAGE_BPS: int = 500                  # 5% slippage tolerance for memecoins
    SOL_PRICE_USD: float = 0.0               # Fallback — updated by heartbeat worker

    # ── Discovery ───────────────────────────────────────────────────────────
    DEXSCREENER_POLL_INTERVAL: Annotated[int, Field(ge=10, le=300)] = 60
    SAMPLE_INTERVAL_SECONDS: Annotated[int, Field(ge=10, le=300)] = 30

    # How long a Tier1-rejected token stays in OBSERVING and keeps getting
    # sampled for free (piggybacking the same batch call as WATCHING tokens)
    # before it's written off to REJECTED. Bounded deliberately — this is
    # the control group, not a second watch list, so it must not grow the
    # sampling batch without limit.
    OBSERVE_WINDOW_SECONDS: Annotated[int, Field(ge=30, le=600)] = 90

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
    STOP_LOSS_PCT: Annotated[float, Field(le=0)] = -0.06
    HARD_FLOOR_PCT: Annotated[float, Field(le=0)] = -0.07
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
