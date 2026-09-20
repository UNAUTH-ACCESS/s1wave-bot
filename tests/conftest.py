from __future__ import annotations

import os

_TEST_ENV = {
    "HELIUS_API_KEY":       "test-key-dummy",
    "HELIUS_RPC_URL":       "https://mainnet.helius-rpc.com/?api-key=test-key-dummy",
    "HELIUS_WS_URL":        "wss://mainnet.helius-rpc.com/?api-key=test-key-dummy",
    "DATABASE_URL":         "postgresql+asyncpg://user:password@localhost:5432/test",
    "TELEGRAM_BOT_TOKEN":   "test-bot-token",
    "TELEGRAM_CHAT_ID":     "12345",
    "INITIAL_BALANCE_USD":  "1000.00",
}
for _k, _v in _TEST_ENV.items():
    os.environ.setdefault(_k, _v)

try:
    from config.settings import get_settings
    get_settings.cache_clear()
except Exception:
    pass

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool
from models.orm import Base

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

@pytest_asyncio.fixture(scope="function")
async def engine():
    _engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield _engine
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await _engine.dispose()

@pytest_asyncio.fixture(scope="function")
async def session(engine) -> AsyncSession:
    factory = async_sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )
    async with factory() as session:
        async with session.begin():
            yield session
            await session.rollback()