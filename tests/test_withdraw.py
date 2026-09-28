"""
tests/test_withdraw.py
========================
engine/execution.py's withdraw_sol() (2026-09-28) — a real, native System
Program transfer letting capital actually leave the live trading wallet on
request. Unlike the swap/close paths, a failure here must be LOUD
(RuntimeError, never a swallowed None) since this moves real money out on
purpose.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from solders.hash import Hash
from solders.keypair import Keypair

from engine.execution import ExecutionEngine


def make_engine() -> ExecutionEngine:
    return ExecutionEngine(
        paper_override=False,
        wallet_private_key_override=str(Keypair()),
        rpc_url_override="http://127.0.0.1:1",
    )


class FakeBalanceResp:
    def __init__(self, value: int):
        self.value = value


class FakeBlockhashResp:
    value = type("V", (), {"blockhash": Hash.default()})()


@pytest.mark.asyncio
async def test_paper_mode_refuses_to_withdraw():
    engine = ExecutionEngine(paper_override=True)
    with pytest.raises(RuntimeError, match="paper mode"):
        await engine.withdraw_sol(str(Keypair().pubkey()), 1_000_000)


@pytest.mark.asyncio
async def test_rejects_a_malformed_destination_address():
    engine = make_engine()
    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(1_000_000_000))
    with pytest.raises(RuntimeError, match="not a valid Solana address"):
        await engine.withdraw_sol("not-a-real-address", 1_000_000)


@pytest.mark.asyncio
async def test_rejects_a_non_positive_amount():
    engine = make_engine()
    with pytest.raises(RuntimeError, match="positive"):
        await engine.withdraw_sol(str(Keypair().pubkey()), 0)


@pytest.mark.asyncio
async def test_rejects_a_withdrawal_that_would_leave_nothing_for_the_network_fee():
    """The real reserve check — amount + fee reserve must fit inside the
    REAL current balance, not a stale cached one."""
    engine = make_engine()
    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(1_000_000))  # 0.001 SOL
    with pytest.raises(RuntimeError, match="Insufficient balance"):
        await engine.withdraw_sol(str(Keypair().pubkey()), 999_999)  # leaves only 1 lamport for the fee reserve


@pytest.mark.asyncio
async def test_successful_withdrawal_returns_the_real_confirmed_tx_signature():
    engine = make_engine()
    destination = Keypair().pubkey()
    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(1_000_000_000))  # 1 SOL

    submitted = {}

    async def fake_send_transaction(tx, opts=None):
        submitted["tx"] = tx
        return type("R", (), {"value": "WITHDRAWSIG111"})()

    engine._rpc.get_latest_blockhash = AsyncMock(return_value=FakeBlockhashResp())
    engine._rpc.send_transaction = fake_send_transaction
    engine._confirm_tx = AsyncMock(return_value=True)

    sig = await engine.withdraw_sol(str(destination), 500_000_000)

    assert sig == "WITHDRAWSIG111"
    submitted_tx = submitted["tx"]
    # Signed by our real wallet, sending to the real destination, not some
    # other account substituted along the way.
    assert str(submitted_tx.message.account_keys[0]) == str(engine._keypair.pubkey())
    assert str(destination) in [str(k) for k in submitted_tx.message.account_keys]


@pytest.mark.asyncio
async def test_unconfirmed_withdrawal_raises_instead_of_silently_returning_none():
    """A withdrawal that never confirms must be loud — unlike the best-
    effort rent-reclaim path, silently returning None here would hide a
    real, unresolved money-movement attempt."""
    engine = make_engine()
    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(1_000_000_000))
    engine._rpc.get_latest_blockhash = AsyncMock(return_value=FakeBlockhashResp())
    engine._rpc.send_transaction = AsyncMock(return_value=type("R", (), {"value": "UNCONFIRMEDSIG"})())
    engine._confirm_tx = AsyncMock(return_value=False)

    with pytest.raises(RuntimeError, match="not confirmed"):
        await engine.withdraw_sol(str(Keypair().pubkey()), 500_000_000)
