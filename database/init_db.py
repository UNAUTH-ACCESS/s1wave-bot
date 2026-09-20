"""
database/init_db.py
===================
Database initialisation utilities.

Called once at process startup (or by the Alembic env when running
migrations).  Creates all tables that don't exist and seeds the three
singleton rows that must always be present:

    - session_state       (id=1)
    - circuit_breaker_state (id=1)
    - daily_loss_state    (id=1)

These are upserted so re-running init_db is idempotent.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, date
from decimal import Decimal

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import settings
from database.engine import get_engine, get_session
from models.orm import (
    Base,
    CircuitBreakerState,
    DailyLossState,
    SessionState,
)

log = logging.getLogger(__name__)


async def create_tables() -> None:
    """Create all ORM tables if they don't exist (dev/test convenience)."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    log.info("Database tables created (or already exist).")


async def seed_singletons(initial_balance: Decimal | None = None) -> None:
    """
    Upsert the three singleton rows.

    session_state.available_balance is only set on INSERT — it is never
    overwritten by a re-run, preserving the live balance across restarts.
    """
    balance = initial_balance or Decimal(str(settings.INITIAL_BALANCE_USD))
    now = datetime.now(timezone.utc)
    today = date.today()

    async with get_session() as session:
        # ── session_state ────────────────────────────────────────────────
        stmt = (
            pg_insert(SessionState)
            .values(id=1, available_balance=balance, started_at=now)
            .on_conflict_do_update(
                index_elements=["id"],
                set_={"last_heartbeat_at": now, "started_at": now},  # record restart boundary
            )
        )
        await session.execute(stmt)

        # ── circuit_breaker_state ────────────────────────────────────────
        stmt = (
            pg_insert(CircuitBreakerState)
            .values(id=1, consecutive_losses=0, is_paused=False)
            .on_conflict_do_nothing(index_elements=["id"])
        )
        await session.execute(stmt)

        # ── daily_loss_state ─────────────────────────────────────────────
        # On conflict (daily reset will handle date changes at midnight),
        # do nothing — the midnight reset task owns this row after init.
        stmt = (
            pg_insert(DailyLossState)
            .values(
                id=1,
                date_utc=today,
                day_open_balance=balance,
                cumulative_pnl_usd=Decimal("0"),
                is_halted=False,
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
        await session.execute(stmt)

    log.info("Singleton rows seeded (session_state, circuit_breaker, daily_loss).")


async def init_db(initial_balance: Decimal | None = None) -> None:
    """
    Full startup init: create tables then seed singletons.

    Call once from main() before starting workers.
    """
    await create_tables()
    await seed_singletons(initial_balance)
    log.info("Database initialisation complete.")


async def reset_daily_loss(current_balance: Decimal) -> None:
    """
    Midnight UTC reset for daily loss tracking.

    Called by the heartbeat worker at the start of each UTC day.
    Resets cumulative_pnl_usd to 0, updates day_open_balance to the
    current live balance, and clears the halt flag.
    """
    today = date.today()
    async with get_session() as session:
        result = await session.execute(select(DailyLossState).where(DailyLossState.id == 1))
        state = result.scalar_one()
        state.date_utc = today
        state.day_open_balance = current_balance
        state.cumulative_pnl_usd = Decimal("0")
        state.is_halted = False
        state.halted_at = None
    log.info("Daily loss state reset for %s. Open balance: %s", today, current_balance)
