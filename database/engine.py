"""
database/engine.py
==================
Async SQLAlchemy engine and session management.

Usage
-----
    # In a worker or service:
    async with get_session() as session:
        result = await session.execute(select(Token))

    # In a FastAPI dependency:
    async def endpoint(session: AsyncSession = Depends(get_db_session)):
        ...

Design notes
------------
- Single engine per process, created once at startup.
- NullPool is used so connections are not held open across await points;
  asyncpg manages its own internal pool.
- All session operations are within explicit transactions.
  Use session.begin() or rely on the context manager which auto-commits
  on clean exit and rolls back on exception.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from config.settings import settings

# ── Engine ───────────────────────────────────────────────────────────────────

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _build_engine() -> AsyncEngine:
    return create_async_engine(
        settings.DATABASE_URL,
        poolclass=NullPool,   # asyncpg manages its own connection pool
        echo=False,           # set to True for SQL debug logging
        future=True,
    )


def get_engine() -> AsyncEngine:
    """Return the process-wide async engine, creating it on first call."""
    global _engine
    if _engine is None:
        _engine = _build_engine()
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Return the process-wide session factory."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,  # prevents lazy-load errors after commit
            autobegin=True,
            autoflush=False,
        )
    return _session_factory


# ── Context manager helpers ───────────────────────────────────────────────────

@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Provide a transactional async session.

    Commits on clean exit, rolls back on any exception.  Always call this
    via 'async with' — never use the factory directly in application code.

        async with get_session() as session:
            token = await session.get(Token, token_id)
            token.status = TokenStatus.WATCHING
            # auto-committed on exit
    """
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            yield session


# ── FastAPI dependency ────────────────────────────────────────────────────────

async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that yields a session per request.

    Usage:
        from fastapi import Depends
        from database.engine import get_db_session

        async def my_route(db: AsyncSession = Depends(get_db_session)):
            ...
    """
    async with get_session() as session:
        yield session


# ── Lifecycle ─────────────────────────────────────────────────────────────────

async def close_engine() -> None:
    """Dispose the engine.  Call on process shutdown."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
