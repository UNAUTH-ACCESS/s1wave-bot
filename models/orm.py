"""
models/orm.py
=============
SQLAlchemy 2.0 ORM models.

Every table in PRD §9 is defined here.  Relationships are declared so
joined loads work in the API layer without extra queries.

Column naming follows snake_case throughout.  All primary keys are UUIDs
generated server-side by PostgreSQL (gen_random_uuid()).  All timestamps
are TIMESTAMPTZ so they store UTC and convert correctly in any timezone.
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    CheckConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

# JSONB in Postgres (production), plain JSON everywhere else — the test
# suite's fixtures run against an in-memory sqlite engine, which has no
# JSONB type at all.
JSONType = JSONB().with_variant(JSON(), "sqlite")
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# ── Enums ────────────────────────────────────────────────────────────────────

class TokenStatus(str, enum.Enum):
    """Lifecycle states for a discovered token."""
    ENRICHED  = "ENRICHED"   # on-chain data fetched, awaiting Tier 1
    WATCHING  = "WATCHING"   # passed Tier 1, sampling in progress
    OBSERVING = "OBSERVING"  # failed a gate but still sampled for a short,
                             # bounded window (see sampling_worker.py's
                             # OBSERVE_WINDOW_SECONDS) — this is the control
                             # group: what happens to tokens the bot never
                             # traded, not just the ones that passed Tier1.
    REJECTED  = "REJECTED"   # failed Tier 1, or observation window/watch ended
    ENTERED   = "ENTERED"    # open position exists
    CLOSED    = "CLOSED"     # position fully closed


class TrendDirection(str, enum.Enum):
    """Momentum direction classification used in Tier 2 scoring."""
    ACCELERATING = "ACCELERATING"   # P1<P2<P3, rate increasing
    EMERGING     = "EMERGING"       # P1≈P2<P3, signal appearing
    SPIKE        = "SPIKE"          # P3 >> P1,P2 with no prior build
    FLAT         = "FLAT"           # P1≈P2≈P3
    DECELERATING = "DECELERATING"   # P1>P2>P3 or P3<P2


class VMZone(str, enum.Enum):
    """
    Volume momentum zone at entry.  Used for post-trade analysis and
    future exit timing refinement.

    Classification rules (applied in scoring_worker):
        EARLY       — score rising, token age < 10 minutes
        STRONG      — score >= TIER2_STRONG_BUY_THRESHOLD, sustained momentum
        LATE        — score plateauing or declining, age > 30 minutes
        EXHAUSTION  — volume collapsing (VM DECELERATING), sell pressure rising
                      (BP DECELERATING)
    """
    EARLY      = "EARLY"
    STRONG     = "STRONG"
    LATE       = "LATE"
    EXHAUSTION = "EXHAUSTION"


class LiquidityFlag(str, enum.Enum):
    """Position size relative to pool liquidity — logged but never gates entry."""
    DEEP    = "DEEP"     # position < 0.5% of liquidity
    NORMAL  = "NORMAL"   # 0.5–2%
    THIN    = "THIN"     # 2–5%
    SHALLOW = "SHALLOW"  # > 5%


class LiquidityTrend(str, enum.Enum):
    """Liquidity direction across the 3 scoring snapshots."""
    GROWING  = "GROWING"
    STABLE   = "STABLE"
    DRAINING = "DRAINING"


class ExitReason(str, enum.Enum):
    """Why a position was closed.  Evaluated in strict priority order."""
    HARD_FLOOR     = "HARD_FLOOR"     # Priority 1: -7% gap
    STOP_LOSS      = "STOP_LOSS"      # Priority 2: -6%
    TAKE_PROFIT    = "TAKE_PROFIT"    # Priority 3: +30%
    TIME_EXIT      = "TIME_EXIT"      # Priority 4: 6-hour max hold
    TRAILING_STOP  = "TRAILING_STOP"  # S1 wave: trailing staircase floor hit


class TradeStatus(str, enum.Enum):
    OPEN   = "OPEN"
    CLOSED = "CLOSED"


class EntrySource(str, enum.Enum):
    """How a trade was originated."""
    SCORER       = "scorer"        # 3-window rolling scorer
    S1_WAVE      = "s1_wave"       # snapshot-1 buy pressure signal
    SCORER_ADDON = "scorer_addon"  # scorer STRONG_BUY add to existing s1_wave


class NotificationEvent(str, enum.Enum):
    """All alert notification types."""
    TRADE_OPEN            = "TRADE_OPEN"
    TRADE_CLOSE           = "TRADE_CLOSE"
    CIRCUIT_BREAKER       = "CIRCUIT_BREAKER"
    DAILY_HALT            = "DAILY_HALT"
    HEARTBEAT             = "HEARTBEAT"
    SELL_FAILED_CRITICAL  = "SELL_FAILED_CRITICAL"
    VELOCITY_BREAKER      = "VELOCITY_BREAKER"
    CB_RESUMED            = "CB_RESUMED"
    # Phase 7 — WebSocket health events (queued)
    WS_CONNECTED      = "WS_CONNECTED"
    WS_DISCONNECTED   = "WS_DISCONNECTED"
    WS_RECONNECTING   = "WS_RECONNECTING"
    NO_PRICE_UPDATES  = "NO_PRICE_UPDATES"


class NotificationStatus(str, enum.Enum):
    PENDING    = "PENDING"
    SENT       = "SENT"
    FAILED     = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"  # failed 3 times, abandoned


import uuid as _uuid


def _new_uuid() -> _uuid.UUID:
    return _uuid.uuid4()


# ── Base ─────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


# ── tokens ───────────────────────────────────────────────────────────────────

class Token(Base):
    """One row per discovered token.  Status tracks lifecycle."""

    __tablename__ = "tokens"
    __table_args__ = (
        Index("ix_tokens_status", "status"),
        Index("ix_tokens_watch_started_at", "watch_started_at"),
        Index("ix_tokens_discovered_at", "discovered_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=_new_uuid
    )
    mint_address: Mapped[str] = mapped_column(String(44), unique=True, nullable=False)
    symbol: Mapped[str | None] = mapped_column(String(32))
    name: Mapped[str | None] = mapped_column(String(128))

    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    status: Mapped[TokenStatus] = mapped_column(
        SAEnum(TokenStatus, name="token_status"), nullable=False, default=TokenStatus.ENRICHED
    )
    rejection_reason: Mapped[str | None] = mapped_column(Text)
    watch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # ── DEX Screener fields (at enrichment time) ──────────────────────────
    liquidity_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    market_cap_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))

    # ── Helius DAS fields ─────────────────────────────────────────────────
    mint_authority_renounced: Mapped[bool | None] = mapped_column(Boolean)
    freeze_authority_renounced: Mapped[bool | None] = mapped_column(Boolean)
    holder_count: Mapped[int | None] = mapped_column(Integer)

    # ── Helius TX history fields ──────────────────────────────────────────
    lp_locked_burned: Mapped[bool | None] = mapped_column(Boolean)
    # wash_multiplier = buy_count / sell_count from last 50 TXs.
    # PASS condition: wash_multiplier <= TIER1_MAX_WASH_MULTIPLIER (2.5)
    # FAIL condition: wash_multiplier >  2.5 (buys overwhelmingly dominant → wash trading)
    wash_multiplier: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))

    # ── Volume baseline (anchored to token debut) ─────────────────────────
    # This is set on the FIRST snapshot ever taken and never updated.
    # volume_mult in subsequent snapshots = snapshot.volume_usd / this value.
    # Using the first snapshot — not the rolling window P1 — preserves
    # historical context across multiple scoring windows.
    baseline_volume_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))

    # ── Phase 7: token decimals for WS price calculation ───────────────────
    # Fetched from Helius DAS at enrichment time. Stored here so the
    # WebSocket price calculator never needs to re-fetch per tick.
    # Standard SPL tokens use 6 decimals; some use 9.
    token_decimals: Mapped[int | None] = mapped_column(Integer)

    # ── Relationships ─────────────────────────────────────────────────────
    snapshots: Mapped[list["TokenSnapshot"]] = relationship(
        back_populates="token", cascade="all, delete-orphan", lazy="select"
    )
    trades: Mapped[list["Trade"]] = relationship(
        back_populates="token", cascade="all, delete-orphan", lazy="select"
    )
    evaluations: Mapped[list["TokenEvaluation"]] = relationship(
        back_populates="token", cascade="all, delete-orphan", lazy="select"
    )

    def __repr__(self) -> str:
        return f"<Token {self.symbol} {self.mint_address[:8]}… {self.status}>"


# ── token_snapshots ───────────────────────────────────────────────────────────

class TokenSnapshot(Base):
    """
    One row per 30-second DEX Screener sample for a WATCHING token.

    The rolling window (P1/P2/P3 in Tier 2 scoring) is always the last
    3 rows ordered by sampled_at DESC for a given token_id.

    volume_mult = volume_usd / token.baseline_volume_usd
    This multiplier is computed at write time by the sampling_worker and
    stored here so the scoring_worker reads pre-computed values.
    """

    __tablename__ = "token_snapshots"
    __table_args__ = (
        Index("ix_snapshots_token_id_sampled_at", "token_id", "sampled_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=_new_uuid
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tokens.id", ondelete="CASCADE"), nullable=False
    )
    sampled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    price_usd: Mapped[Decimal] = mapped_column(Numeric(30, 12), nullable=False)
    liquidity_usd: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    market_cap_usd: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    # DEX Screener m5 volume
    volume_usd: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    # buy_volume / (buy_volume + sell_volume) from DEX Screener txns data
    buy_pressure: Mapped[Decimal] = mapped_column(Numeric(6, 4), nullable=False)
    # volume_usd / token.baseline_volume_usd (computed at write time)
    volume_mult: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)

    # ── Relationships ─────────────────────────────────────────────────────
    token: Mapped["Token"] = relationship(back_populates="snapshots")

    def __repr__(self) -> str:
        return f"<Snapshot token={self.token_id} at={self.sampled_at.isoformat()}>"


# ── token_evaluations ─────────────────────────────────────────────────────────

class TokenEvaluation(Base):
    """
    One row per gate look at a token — Tier1, S1 Wave, and later the Scorer —
    whether or not it led to a trade.

    token_snapshots holds what the market did. trades holds what we did
    about it. This table holds why a gate said yes or no, with the exact
    inputs it saw at that moment — not a single overwritable reason string
    on the token row, which only ever remembers the most recent gate that
    touched it.

    inputs_json is intentionally schema-less: each gate looks at different
    fields, and this table shouldn't need a migration every time a gate's
    feature set changes.

    Outcome labels (rug / faded / pumped, peak multiple, time-to-death) are
    never stored here. They're derived on demand from token_snapshots by
    the analysis pass — always a reproducible formula over raw data, never
    a value anyone sets by hand.
    """

    __tablename__ = "token_evaluations"
    __table_args__ = (
        Index("ix_token_evaluations_token_id", "token_id", "evaluated_at"),
        Index("ix_token_evaluations_gate", "gate", "passed"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=_new_uuid
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tokens.id", ondelete="CASCADE"), nullable=False
    )
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    gate: Mapped[str] = mapped_column(String(16), nullable=False)  # TIER1 | S1_WAVE
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64))
    inputs_json: Mapped[dict] = mapped_column(JSONType, nullable=False, default=dict)

    token: Mapped["Token"] = relationship(back_populates="evaluations")

    def __repr__(self) -> str:
        return f"<TokenEvaluation {self.gate} passed={self.passed} token={self.token_id}>"


# ── trades ───────────────────────────────────────────────────────────────────

class Trade(Base):
    """One row per simulated (and later, real) position."""

    __tablename__ = "trades"
    __table_args__ = (
        Index("ix_trades_status", "status"),
        Index("ix_trades_token_id", "token_id"),
        Index("ix_trades_entry_time", "entry_time"),
        CheckConstraint("pnl_pct IS NULL OR (pnl_pct >= -1)", name="ck_pnl_pct_sane"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=_new_uuid
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    # Denormalised for fast lookups (avoids join on every price tick).
    token_mint: Mapped[str] = mapped_column(String(44), nullable=False, index=True)

    # ── Entry ─────────────────────────────────────────────────────────────
    entry_price: Mapped[Decimal] = mapped_column(Numeric(30, 12), nullable=False)
    entry_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    entry_composite_score: Mapped[Decimal] = mapped_column(Numeric(5, 2), nullable=False)
    entry_bp_trend: Mapped[TrendDirection] = mapped_column(
        SAEnum(TrendDirection, name="trend_direction"), nullable=False
    )
    entry_vm_trend: Mapped[TrendDirection] = mapped_column(
        SAEnum(TrendDirection, name="trend_direction", create_type=False), nullable=False
    )
    # VMZone classification — see VMZone docstring for rules.
    entry_vm_zone: Mapped[VMZone] = mapped_column(
        SAEnum(VMZone, name="vm_zone"), nullable=False
    )
    # Liquidity awareness — logged for analysis, never gates entry in v3.
    entry_liquidity_flag: Mapped[LiquidityFlag] = mapped_column(
        SAEnum(LiquidityFlag, name="liquidity_flag"), nullable=False
    )
    entry_liquidity_trend: Mapped[LiquidityTrend] = mapped_column(
        SAEnum(LiquidityTrend, name="liquidity_trend"), nullable=False
    )
    position_size_usd: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)

    # ── S1 Wave fields ────────────────────────────────────────────────────
    entry_source: Mapped[str] = mapped_column(
        SAEnum("scorer", "s1_wave", "scorer_addon",
               name="entry_source", create_type=False),
        nullable=False, default="scorer"
    )
    trailing_stop_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    trailing_stop_floor: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    high_watermark_price: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))

    # ── Execution fields (NULL in paper trading mode) ─────────────────────
    token_amount_bought: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    tx_signature_entry:  Mapped[str | None]     = mapped_column(String(128))
    tx_signature_exit:   Mapped[str | None]     = mapped_column(String(128))

    # ── Exit (NULL until closed) ──────────────────────────────────────────
    exit_price: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    exit_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exit_reason: Mapped[ExitReason | None] = mapped_column(
        SAEnum(ExitReason, name="exit_reason")
    )
    pnl_usd: Mapped[Decimal | None] = mapped_column(Numeric(20, 6))
    pnl_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    hold_duration_seconds: Mapped[int | None] = mapped_column(Integer)

    status: Mapped[TradeStatus] = mapped_column(
        SAEnum(TradeStatus, name="trade_status"),
        nullable=False,
        default=TradeStatus.OPEN,
    )

    # ── Relationships ─────────────────────────────────────────────────────
    token: Mapped["Token"] = relationship(back_populates="trades")
    balance_record: Mapped["BalanceHistory | None"] = relationship(
        back_populates="trade", uselist=False, cascade="all, delete-orphan"
    )
    notifications: Mapped[list["NotificationQueue"]] = relationship(
        back_populates="trade", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Trade {self.token_mint[:8]}… {self.status} entry={self.entry_price}>"


# ── balance_history ───────────────────────────────────────────────────────────

class BalanceHistory(Base):
    """One row per trade close — the full compounding ledger."""

    __tablename__ = "balance_history"
    __table_args__ = (
        Index("ix_balance_history_snapshot_at", "snapshot_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=_new_uuid
    )
    trade_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("trades.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,  # one balance snapshot per trade
    )
    balance_before: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    balance_after: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    snapshot_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    # ── Relationships ─────────────────────────────────────────────────────
    trade: Mapped["Trade"] = relationship(back_populates="balance_record")

    def __repr__(self) -> str:
        return (
            f"<BalanceHistory {self.balance_before}→{self.balance_after} "
            f"at={self.snapshot_at.isoformat()}>"
        )


# ── circuit_breaker_state ─────────────────────────────────────────────────────

class CircuitBreakerState(Base):
    """
    Singleton row (id=1).  Tracks consecutive losses and pause state.

    consecutive_losses resets to 0 on any winning trade.
    is_paused → True when consecutive_losses reaches CB_LOSS_COUNT.
    Auto-resumes at resume_at timestamp (set to now() + CB_PAUSE_MINUTES).
    """

    __tablename__ = "circuit_breaker_state"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, default=1,
        comment="Singleton row — always id=1"
    )
    consecutive_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    pause_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resume_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return (
            f"<CircuitBreaker losses={self.consecutive_losses} "
            f"paused={self.is_paused} resume={self.resume_at}>"
        )


# ── daily_loss_state ──────────────────────────────────────────────────────────

class DailyLossState(Base):
    """
    Singleton row (id=1).  Tracks today's PnL against the daily loss limit.

    Resets at midnight UTC: date_utc updates, day_open_balance = current
    balance, cumulative_pnl_usd = 0, is_halted = False.
    """

    __tablename__ = "daily_loss_state"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, default=1,
        comment="Singleton row — always id=1"
    )
    date_utc: Mapped[date] = mapped_column(Date, nullable=False)
    day_open_balance: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    cumulative_pnl_usd: Mapped[Decimal] = mapped_column(
        Numeric(20, 6), nullable=False, default=Decimal("0")
    )
    is_halted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    halted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return (
            f"<DailyLoss date={self.date_utc} pnl={self.cumulative_pnl_usd} "
            f"halted={self.is_halted}>"
        )


# ── session_state ─────────────────────────────────────────────────────────────

class SessionState(Base):
    """
    Singleton row (id=1).  Mutable bot-level state persisted across restarts.

    available_balance is the authoritative capital figure.  Every trade open
    and close updates this value atomically.  The balance_history table holds
    the full audit log.
    """

    __tablename__ = "session_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    available_balance: Mapped[Decimal] = mapped_column(Numeric(20, 6), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"<SessionState balance={self.available_balance}>"


# ── notification_queue ────────────────────────────────────────────────────────

class NotificationQueue(Base):
    """
    Outbox table for alert notifications.

    Dispatch rules:
        TRADE_OPEN / TRADE_CLOSE → immediate (notification_worker skips
            the 60s queue and fires these inline at the point of event).
        All other events → queued, dispatched by notification_worker every 60s.

    At-least-once delivery: notification_worker retries on failure.
    After 3 failures: status → DEAD_LETTER, attempts logged.
    """

    __tablename__ = "notification_queue"
    __table_args__ = (
        Index("ix_notif_status_event", "status", "event_type"),
        Index("ix_notif_created_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=_new_uuid
    )
    event_type: Mapped[NotificationEvent] = mapped_column(
        SAEnum(NotificationEvent, name="notification_event"), nullable=False
    )
    # JSON payload — content varies by event_type (see notifications/payloads.py)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[NotificationStatus] = mapped_column(
        SAEnum(NotificationStatus, name="notification_status"),
        nullable=False,
        default=NotificationStatus.PENDING,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)

    # Optional FK — only set for TRADE_OPEN / TRADE_CLOSE events
    trade_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("trades.id", ondelete="SET NULL")
    )
    trade: Mapped["Trade | None"] = relationship(back_populates="notifications")

    def __repr__(self) -> str:
        return f"<Notification {self.event_type} {self.status} id={self.id}>"
