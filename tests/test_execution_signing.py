"""
tests/test_execution_signing.py
=================================
engine/execution.py's real-money execution path — regression coverage for
THREE real bugs found live on 2026-09-24, in order, during the first real
confluence_live trade (each one only surfaced after fixing the last):

1. _sign_and_submit(): solders.VersionedTransaction has no in-place .sign()
   method in the installed version (0.29.0) — every submission failed with
   "'VersionedTransaction' object has no attribute 'sign'". Fixed by
   building a NEW VersionedTransaction from the unsigned one's .message and
   the real signer, rather than mutating the (immutable, Rust-backed)
   deserialized object.

2. _confirm_tx(): get_transaction() requires a solders.Signature object,
   not a plain str — every poll attempt raised TypeError, silently
   swallowed by a bare `except: pass` (a second bug on its own — this was
   invisible in the logs). The real transaction had ALREADY succeeded
   on-chain the whole time; the loop just never once successfully checked,
   and reported a false "not confirmed within 60s" after a real fill —
   leaving actual purchased tokens with no ConfluenceLiveTrade row tracking
   them. Fixed by converting to Signature.from_string() first, and the
   silent except now logs.

3. actual_price: computed as sol_lamports/out_amount (SOL-lamports per raw
   token unit) — not the same unit as the USD-per-whole-token price
   (DexScreener's price_usd) the exit-decision logic compares it against.
   Confirmed on the real trade: produced 0.00427 against that token's real
   $0.0005419 DexScreener price. Fixed by querying the real post-trade
   wallet balance (raw amount + decimals) and computing
   position_usd / (raw_amount / 10**decimals) instead.

All three use a REAL throwaway keypair and REAL, well-formed solders
objects (not pure mocks) wherever the bug was in how this codebase used
the library, so these tests exercise the actual API surface this session
hit rather than an idealized mock that could hide the same bug again.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.signature import Signature
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

from engine.execution import ExecutionEngine, ExecutionResult


def make_jupiter_shaped_tx_bytes(fee_payer: Keypair) -> bytes:
    """A real, well-formed VersionedTransaction serialized to bytes, shaped
    the way Jupiter's /swap endpoint response is: a compiled message with a
    placeholder signature, not yet signed by the real wallet — exactly what
    _sign_and_submit() receives as its tx_bytes argument."""
    ix = transfer(TransferParams(from_pubkey=fee_payer.pubkey(), to_pubkey=fee_payer.pubkey(), lamports=0))
    msg = MessageV0.try_compile(fee_payer.pubkey(), [ix], [], Hash.default())
    placeholder = VersionedTransaction.populate(msg, [Keypair().sign_message(bytes(msg))])
    return bytes(placeholder)


@pytest.mark.asyncio
async def test_sign_and_submit_produces_a_validly_signed_transaction(monkeypatch):
    """The core regression: this must not raise
    AttributeError("'VersionedTransaction' object has no attribute 'sign'")
    and must submit a transaction actually signed by OUR wallet, with our
    pubkey as fee payer — not the placeholder from the unsigned bytes."""
    wallet = Keypair()
    engine = ExecutionEngine(
        paper_override=False,
        wallet_private_key_override=str(wallet),  # base58 secret, matches settings' format
        rpc_url_override="http://127.0.0.1:1",  # never actually dialed — send_transaction is mocked below
    )

    submitted = {}

    async def fake_send_transaction(tx, opts=None):
        submitted["tx"] = tx
        class _Resp:
            value = "FAKESIGNATURE111"
        return _Resp()

    engine._rpc.send_transaction = fake_send_transaction

    tx_bytes = make_jupiter_shaped_tx_bytes(wallet)
    sig = await engine._sign_and_submit(tx_bytes)

    assert sig == "FAKESIGNATURE111"
    submitted_tx = submitted["tx"]
    assert str(submitted_tx.message.account_keys[0]) == str(wallet.pubkey())
    assert len(submitted_tx.signatures) == 1
    # Signed by our real wallet, not the placeholder keypair the "Jupiter" fixture used.
    assert submitted_tx.signatures[0] != VersionedTransaction.from_bytes(tx_bytes).signatures[0]


@pytest.mark.asyncio
async def test_sign_and_submit_returns_none_on_rpc_error(monkeypatch):
    """A real submission failure (RPC rejects it) must still return None,
    not raise — _execute_buy()/_execute_sell() rely on this to report a
    clean submit_failed rather than crashing the cycle."""
    wallet = Keypair()
    engine = ExecutionEngine(
        paper_override=False,
        wallet_private_key_override=str(wallet),
        rpc_url_override="http://127.0.0.1:1",
    )

    async def failing_send_transaction(tx, opts=None):
        raise RuntimeError("simulated RPC rejection")

    engine._rpc.send_transaction = failing_send_transaction

    tx_bytes = make_jupiter_shaped_tx_bytes(wallet)
    sig = await engine._sign_and_submit(tx_bytes)
    assert sig is None


def make_engine() -> ExecutionEngine:
    return ExecutionEngine(
        paper_override=False,
        wallet_private_key_override=str(Keypair()),
        rpc_url_override="http://127.0.0.1:1",
    )


class FakeTxMeta:
    def __init__(self, err=None):
        self.err = err


class FakeTxResp:
    def __init__(self, err=None):
        self.transaction = type("T", (), {"meta": FakeTxMeta(err)})()


class FakeGetTxResult:
    def __init__(self, value):
        self.value = value


@pytest.mark.asyncio
async def test_confirm_tx_calls_rpc_with_a_real_signature_object_not_a_string():
    """The core regression: get_transaction() must be called with a
    solders.Signature, not the plain str tx_sig — that TypeError was the
    real bug, previously invisible because the except swallowed it silently."""
    engine = make_engine()
    sig_str = str(Keypair().sign_message(b"x"))
    seen_args = {}

    async def fake_get_transaction(sig_arg, commitment=None, max_supported_transaction_version=None):
        seen_args["sig"] = sig_arg
        return FakeGetTxResult(FakeTxResp(err=None))

    engine._rpc.get_transaction = fake_get_transaction
    result = await engine._confirm_tx(sig_str)

    assert result is True
    assert isinstance(seen_args["sig"], Signature)
    assert str(seen_args["sig"]) == sig_str


@pytest.mark.asyncio
async def test_confirm_tx_returns_false_on_real_onchain_error():
    engine = make_engine()
    sig_str = str(Keypair().sign_message(b"x"))

    async def fake_get_transaction(sig_arg, commitment=None, max_supported_transaction_version=None):
        return FakeGetTxResult(FakeTxResp(err={"InstructionError": [0, "Custom"]}))

    engine._rpc.get_transaction = fake_get_transaction
    result = await engine._confirm_tx(sig_str)
    assert result is False


# ── _confirm_tx_with_delta() — real SOL delta capture (2026-09-28) ──────────
#
# Real incident: entry_sol_lamports (the intended swap amount) understated
# real spend by 6.36x in aggregate across 71 real trades. Root cause,
# confirmed via a direct on-chain instruction trace: a mandatory Pump.fun
# protocol-fee token account (owned by Pump.fun's fee collector, never
# this wallet, never reclaimable) that some sells must create — on one
# confirmed trade this fee alone exceeded the entire quoted gain. This
# reads the wallet's own real pre/post balance from the same confirm
# response, at no extra RPC cost.

class FakeMetaWithBalances:
    def __init__(self, err, pre_balances, post_balances, fee=5000):
        self.err = err
        self.pre_balances = pre_balances
        self.post_balances = post_balances
        self.fee = fee


class FakeTxWithBalances:
    def __init__(self, meta, account_keys):
        self.transaction = type("Inner", (), {"message": type("Msg", (), {"account_keys": account_keys})()})()
        self.meta = meta


class FakeTxRespWithBalances:
    def __init__(self, meta, account_keys):
        self.transaction = FakeTxWithBalances(meta, account_keys)


@pytest.mark.asyncio
async def test_confirm_tx_with_delta_reads_the_real_wallet_balance_change():
    engine = make_engine()
    wallet = engine._keypair.pubkey()
    other = Keypair().pubkey()
    sig_str = str(Keypair().sign_message(b"x"))

    async def fake_get_transaction(sig_arg, commitment=None, max_supported_transaction_version=None):
        meta = FakeMetaWithBalances(err=None, pre_balances=[10_000_000, 500_000], post_balances=[8_500_000, 1_500_000], fee=5000)
        return FakeGetTxResult(FakeTxRespWithBalances(meta, [wallet, other]))

    engine._rpc.get_transaction = fake_get_transaction
    confirmed, delta, fee = await engine._confirm_tx_with_delta(sig_str)

    assert confirmed is True
    assert delta == -1_500_000  # 8_500_000 - 10_000_000, this wallet's own real delta
    assert fee == 5000


@pytest.mark.asyncio
async def test_confirm_tx_with_delta_returns_none_delta_on_real_onchain_error():
    engine = make_engine()
    sig_str = str(Keypair().sign_message(b"x"))

    async def fake_get_transaction(sig_arg, commitment=None, max_supported_transaction_version=None):
        meta = FakeMetaWithBalances(err={"InstructionError": [0, "Custom"]}, pre_balances=[1], post_balances=[1])
        return FakeGetTxResult(FakeTxRespWithBalances(meta, [engine._keypair.pubkey()]))

    engine._rpc.get_transaction = fake_get_transaction
    confirmed, delta, fee = await engine._confirm_tx_with_delta(sig_str)
    assert confirmed is False
    assert delta is None
    assert fee is None


@pytest.mark.asyncio
async def test_confirm_tx_with_delta_returns_none_when_wallet_not_found():
    """Should be unreachable (this wallet is always the fee payer), but
    must fail safe rather than mistake someone else's balance for ours."""
    engine = make_engine()
    other = Keypair().pubkey()
    sig_str = str(Keypair().sign_message(b"x"))

    async def fake_get_transaction(sig_arg, commitment=None, max_supported_transaction_version=None):
        meta = FakeMetaWithBalances(err=None, pre_balances=[500_000], post_balances=[600_000])
        return FakeGetTxResult(FakeTxRespWithBalances(meta, [other]))

    engine._rpc.get_transaction = fake_get_transaction
    confirmed, delta, fee = await engine._confirm_tx_with_delta(sig_str)
    assert confirmed is True
    assert delta is None
    assert fee == 5000  # the fee itself is still known even if the wallet's own balance row wasn't found


# ── get_real_tx_delta() — one-shot backfill lookup (2026-09-28) ─────────────
#
# The backfill counterpart to _confirm_tx_with_delta(): no polling loop,
# since it's used for trades that already closed, sometimes days ago.

@pytest.mark.asyncio
async def test_get_real_tx_delta_returns_delta_and_fee():
    engine = make_engine()
    wallet = engine._keypair.pubkey()
    sig_str = str(Keypair().sign_message(b"x"))

    async def fake_get_transaction(sig_arg, commitment=None, max_supported_transaction_version=None):
        meta = FakeMetaWithBalances(err=None, pre_balances=[10_000_000], post_balances=[8_500_000], fee=7500)
        return FakeGetTxResult(FakeTxRespWithBalances(meta, [wallet]))

    engine._rpc.get_transaction = fake_get_transaction
    result = await engine.get_real_tx_delta(sig_str)
    assert result == (-1_500_000, 7500)


@pytest.mark.asyncio
async def test_get_real_tx_delta_returns_none_on_error():
    engine = make_engine()
    sig_str = str(Keypair().sign_message(b"x"))

    async def fake_get_transaction(sig_arg, commitment=None, max_supported_transaction_version=None):
        meta = FakeMetaWithBalances(err={"InstructionError": [0, "Custom"]}, pre_balances=[1], post_balances=[1])
        return FakeGetTxResult(FakeTxRespWithBalances(meta, [engine._keypair.pubkey()]))

    engine._rpc.get_transaction = fake_get_transaction
    result = await engine.get_real_tx_delta(sig_str)
    assert result is None


@pytest.mark.asyncio
async def test_get_real_tx_delta_returns_none_when_tx_not_found():
    engine = make_engine()
    sig_str = str(Keypair().sign_message(b"x"))

    async def fake_get_transaction(sig_arg, commitment=None, max_supported_transaction_version=None):
        return FakeGetTxResult(None)

    engine._rpc.get_transaction = fake_get_transaction
    result = await engine.get_real_tx_delta(sig_str)
    assert result is None


@pytest.mark.asyncio
async def test_get_real_tx_delta_swallows_rpc_errors():
    engine = make_engine()
    sig_str = str(Keypair().sign_message(b"x"))
    engine._rpc.get_transaction = AsyncMock(side_effect=RuntimeError("RPC outage"))
    result = await engine.get_real_tx_delta(sig_str)
    assert result is None


class FakeTokenAmount:
    def __init__(self, amount: str, decimals: int):
        self.amount = amount
        self.decimals = decimals


class FakeParsedAccount:
    def __init__(self, amount: str, decimals: int):
        self.account = type("Acc", (), {
            "data": type("Data", (), {"parsed": {"info": {"tokenAmount": {"amount": amount, "decimals": decimals}}}})()
        })()


class FakeTokenAccountsResp:
    def __init__(self, rows):
        self.value = rows


@pytest.mark.asyncio
async def test_get_token_balance_raw_returns_real_amount_and_decimals():
    engine = make_engine()

    async def fake_get_accounts(owner, opts, commitment=None):
        return FakeTokenAccountsResp([FakeParsedAccount("2167016274", 6)])

    engine._rpc.get_token_accounts_by_owner_json_parsed = fake_get_accounts
    result = await engine.get_token_balance_raw("8m1yzDofuxPyG6qn8MNny3nQJT1Cqq3r1qTzWnTpump")
    assert result == (2167016274, 6)


@pytest.mark.asyncio
async def test_get_token_balance_raw_retries_once_then_returns_none(monkeypatch):
    import engine.execution as exec_module
    monkeypatch.setattr(exec_module.asyncio, "sleep", AsyncMock())  # skip the real 2s wait in tests
    engine = make_engine()
    calls = {"n": 0}

    async def always_empty(owner, opts, commitment=None):
        calls["n"] += 1
        return FakeTokenAccountsResp([])

    engine._rpc.get_token_accounts_by_owner_json_parsed = always_empty
    result = await engine.get_token_balance_raw(str(Keypair().pubkey()))  # valid-format mint, just never returns a row
    assert result is None
    assert calls["n"] == 2  # one retry, per the docstring


@pytest.mark.asyncio
async def test_execute_buy_computes_dimensionally_correct_price_from_real_balance(monkeypatch):
    """End-to-end: with a mocked quote/build/submit/confirm all succeeding,
    actual_price must come from position_usd / (real_raw_amount /
    10**decimals) — NOT sol_lamports/out_amount (the old, wrong formula)."""
    engine = make_engine()
    engine._get_quote = AsyncMock(return_value={"outAmount": "1709821287"})
    engine._build_swap_tx = AsyncMock(return_value=b"fake-tx-bytes")
    engine._check_entry_cost = AsyncMock(return_value=None)  # safe to proceed — not what this test covers
    engine._sign_and_submit = AsyncMock(return_value="FAKESIG")
    engine._confirm_tx_with_delta = AsyncMock(return_value=(True, -9282473, 5000))
    engine.get_token_balance_raw = AsyncMock(return_value=(2167016274, 6))  # the real LUCKYCATT numbers

    result = await engine._execute_buy(
        mint="8m1yzDofuxPyG6qn8MNny3nQJT1Cqq3r1qTzWnTpump",
        sol_lamports=9277473,
        position_usd=Decimal("1.05"),
    )

    assert result.success is True
    assert result.actual_amount == Decimal("2167016274")
    expected_price = Decimal("1.05") / (Decimal("2167016274") / Decimal(10 ** 6))
    assert result.actual_price == expected_price
    # The old, wrong formula would have given ~0.0054 — sanity-check we're nowhere near it.
    wrong_old_formula = Decimal("9277473") / Decimal("1709821287")
    assert abs(result.actual_price - wrong_old_formula) > Decimal("0.0001")


@pytest.mark.asyncio
async def test_execute_buy_falls_back_when_real_balance_unavailable(monkeypatch):
    """If the post-trade balance lookup fails (rare), a real on-chain-
    confirmed purchase must still be recorded with SOME usable price rather
    than silently dropped — using the quoted out_amount with the documented
    6-decimals assumption."""
    engine = make_engine()
    engine._get_quote = AsyncMock(return_value={"outAmount": "1709821287"})
    engine._build_swap_tx = AsyncMock(return_value=b"fake-tx-bytes")
    engine._check_entry_cost = AsyncMock(return_value=None)  # safe to proceed — not what this test covers
    engine._sign_and_submit = AsyncMock(return_value="FAKESIG")
    engine._confirm_tx_with_delta = AsyncMock(return_value=(True, -9282473, 5000))
    engine.get_token_balance_raw = AsyncMock(return_value=None)

    result = await engine._execute_buy(
        mint="SomeMint",
        sol_lamports=9277473,
        position_usd=Decimal("1.05"),
    )

    assert result.success is True
    assert result.actual_amount == Decimal("1709821287")
    expected_price = Decimal("1.05") / (Decimal("1709821287") / Decimal(10 ** 6))
    assert result.actual_price == expected_price


# ── _check_entry_cost() — freshly-migrated-pool cost ceiling (2026-09-25) ───
#
# Real incident: two buys (Bybit, NUUC) each cost ~$0.55 instead of their
# intended ~$0.19 — both were the first-ever trade against a just-migrated
# pool, and Solana made our transaction pay to create the pool's own
# internal vault accounts (not ours, never reclaimable). These tests cover
# the pre-flight simulation that now catches this BEFORE any real money
# moves.

class FakeBalanceResp:
    def __init__(self, value: int):
        self.value = value


class FakeSimAccount:
    def __init__(self, lamports: int):
        self.lamports = lamports


class FakeSimResult:
    def __init__(self, accounts=None, err=None):
        self.accounts = accounts
        self.err = err


class FakeSimResp:
    def __init__(self, value: FakeSimResult):
        self.value = value


@pytest.mark.asyncio
async def test_entry_cost_check_allows_a_normal_single_account_entry():
    """Real numbers from a normal trade (ROLL): ~111K lamports swap
    principal + ~1.49M rent + a small fee — comfortably under the ceiling."""
    engine = make_engine()
    tx_bytes = make_jupiter_shaped_tx_bytes(engine._keypair)
    sol_lamports = 111_067
    pre = 10_000_000
    projected_post = pre - 111_067 - 1_488_440 - 5_000  # normal total real cost

    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(pre))
    engine._rpc.simulate_transaction = AsyncMock(
        return_value=FakeSimResp(FakeSimResult(accounts=[FakeSimAccount(projected_post)]))
    )

    result = await engine._check_entry_cost(tx_bytes, sol_lamports)
    assert result is None  # safe to proceed


@pytest.mark.asyncio
async def test_entry_cost_check_rejects_a_freshly_migrated_pool():
    """Real numbers from the actual Bybit incident: ~4.75M lamports real
    cost against a ~135K intended swap — must be rejected, not submitted.

    2026-09-28: this ceiling was raised to 5.0M (which would have ALLOWED
    this exact case) and reverted back to 2.2M twice in one day — see
    _MAX_ENTRY_RENT_OVERHEAD_LAMPORTS's full-history comment. Settled back
    on the original 2.2M: the configuration with the actual longer live
    track record, after a real-money incident on each side (DISNEY/
    LOADPAD/xSOL at 5.0M with a zero-delay guard; GEMINI at 5.0M with a
    60s grace period) without enough sample to tell which effect
    dominates. Do not raise this again without a much bigger live sample
    under controlled conditions."""
    engine = make_engine()
    tx_bytes = make_jupiter_shaped_tx_bytes(engine._keypair)
    sol_lamports = 134_730
    pre = 10_000_000
    projected_post = pre - 4_785_561  # the real Bybit incident's total cost

    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(pre))
    engine._rpc.simulate_transaction = AsyncMock(
        return_value=FakeSimResp(FakeSimResult(accounts=[FakeSimAccount(projected_post)]))
    )

    result = await engine._check_entry_cost(tx_bytes, sol_lamports)
    assert result is not None
    assert result.success is False
    assert result.error_type == "entry_too_expensive"


@pytest.mark.asyncio
async def test_entry_cost_check_rejects_something_even_more_extreme():
    """A cost well beyond even the Bybit-incident magnitude (e.g.
    multiple missing accounts, not just one pool's vaults) must still be
    caught."""
    engine = make_engine()
    tx_bytes = make_jupiter_shaped_tx_bytes(engine._keypair)
    sol_lamports = 134_730
    pre = 10_000_000
    projected_post = pre - 9_000_000  # far beyond anything observed so far

    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(pre))
    engine._rpc.simulate_transaction = AsyncMock(
        return_value=FakeSimResp(FakeSimResult(accounts=[FakeSimAccount(projected_post)]))
    )

    result = await engine._check_entry_cost(tx_bytes, sol_lamports)
    assert result is not None
    assert result.success is False
    assert result.error_type == "entry_too_expensive"


@pytest.mark.asyncio
async def test_entry_cost_check_rejects_when_simulation_shows_a_real_failure():
    engine = make_engine()
    tx_bytes = make_jupiter_shaped_tx_bytes(engine._keypair)

    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(10_000_000))
    engine._rpc.simulate_transaction = AsyncMock(
        return_value=FakeSimResp(FakeSimResult(accounts=None, err={"InstructionError": [2, "Custom"]}))
    )

    result = await engine._check_entry_cost(tx_bytes, 100_000)
    assert result is not None
    assert result.error_type == "simulated_failure"


@pytest.mark.asyncio
async def test_entry_cost_check_fails_closed_when_unverifiable():
    """No account data back from the simulation — must abort rather than
    proceed blind, per the 'protect the money first' priority."""
    engine = make_engine()
    tx_bytes = make_jupiter_shaped_tx_bytes(engine._keypair)

    engine._rpc.get_balance = AsyncMock(return_value=FakeBalanceResp(10_000_000))
    engine._rpc.simulate_transaction = AsyncMock(
        return_value=FakeSimResp(FakeSimResult(accounts=None, err=None))
    )

    result = await engine._check_entry_cost(tx_bytes, 100_000)
    assert result is not None
    assert result.error_type == "entry_cost_unverifiable"


@pytest.mark.asyncio
async def test_entry_cost_check_fails_closed_on_rpc_error():
    engine = make_engine()
    tx_bytes = make_jupiter_shaped_tx_bytes(engine._keypair)

    engine._rpc.get_balance = AsyncMock(side_effect=RuntimeError("RPC hiccup"))

    result = await engine._check_entry_cost(tx_bytes, 100_000)
    assert result is not None
    assert result.error_type == "entry_cost_check_error"


@pytest.mark.asyncio
async def test_execute_buy_aborts_before_ever_signing_when_too_expensive():
    """Integration: _execute_buy() must never call _sign_and_submit() at
    all when the cost check rejects — this is what makes it free (no real
    transaction submitted) rather than just a nicer error message."""
    engine = make_engine()
    engine._get_quote = AsyncMock(return_value={"outAmount": "1709821287"})
    engine._build_swap_tx = AsyncMock(return_value=b"fake-tx-bytes")
    engine._check_entry_cost = AsyncMock(return_value=ExecutionResult(
        success=False, error_type="entry_too_expensive", error_detail="too expensive",
    ))
    engine._sign_and_submit = AsyncMock(side_effect=AssertionError("must never sign a rejected entry"))

    result = await engine._execute_buy(mint="SomeMint", sol_lamports=100_000, position_usd=Decimal("0.02"))

    assert result.success is False
    assert result.error_type == "entry_too_expensive"
    engine._sign_and_submit.assert_not_awaited()


# ── close_token_account() — rent-reclaim fix (2026-09-24) ───────────────────
#
# Real bug found live: every entry creates a token account (~0.0015-0.0021
# SOL rent), and nothing ever closed it after the matching exit sold the
# position to zero. Confirmed on-chain: 5 already-exited positions had left
# 5 dead, still-rent-bearing accounts behind (~$0.86 total) that showed up
# in none of equity_usd, open-position value, or realized P&L. These tests
# cover the reclaim method itself and its wiring into _execute_sell().

TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


class FakeCloseableAccountEntry:
    def __init__(self, pubkey, amount: str, owner_program: str, lamports: int = 1488440):
        from solders.pubkey import Pubkey
        self.pubkey = pubkey
        self.account = type("Acc", (), {
            "data": type("Data", (), {"parsed": {"info": {"tokenAmount": {"amount": amount, "decimals": 6}}}})(),
            "owner": Pubkey.from_string(owner_program),
            "lamports": lamports,
        })()


@pytest.mark.asyncio
async def test_close_token_account_closes_a_zero_balance_account():
    """Core path: a fully-drained account gets a real CloseAccount
    instruction submitted, addressed to the account's own owning program
    (Token-2022 here), with the wallet as both rent destination and signer."""
    engine = make_engine()
    dead_account = Keypair().pubkey()

    async def fake_get_accounts(owner, opts, commitment=None):
        return FakeTokenAccountsResp([FakeCloseableAccountEntry(dead_account, "0", TOKEN_2022_PROGRAM_ID)])

    class FakeBlockhashResp:
        value = type("V", (), {"blockhash": Hash.default()})()

    submitted = {}

    async def fake_send_transaction(tx, opts=None):
        submitted["tx"] = tx
        return type("R", (), {"value": "CLOSESIG111"})()

    engine._rpc.get_token_accounts_by_owner_json_parsed = fake_get_accounts
    engine._rpc.get_latest_blockhash = AsyncMock(return_value=FakeBlockhashResp())
    engine._rpc.send_transaction = fake_send_transaction
    engine._confirm_tx = AsyncMock(return_value=True)

    sig = await engine.close_token_account(str(Keypair().pubkey()))

    assert sig == "CLOSESIG111"
    ix = submitted["tx"].message.instructions[0]
    assert str(submitted["tx"].message.account_keys[ix.program_id_index]) == TOKEN_2022_PROGRAM_ID
    assert bytes(ix.data) == bytes([9])  # CloseAccount opcode


@pytest.mark.asyncio
async def test_close_token_account_refuses_a_nonzero_balance():
    """CloseAccount fails on-chain for a nonzero balance — must not even
    attempt the transaction, since that would waste a real fee on a
    guaranteed-to-fail submission."""
    engine = make_engine()

    async def fake_get_accounts(owner, opts, commitment=None):
        return FakeTokenAccountsResp([FakeCloseableAccountEntry(Keypair().pubkey(), "42", TOKEN_2022_PROGRAM_ID)])

    engine._rpc.get_token_accounts_by_owner_json_parsed = fake_get_accounts
    engine._rpc.send_transaction = AsyncMock(side_effect=AssertionError("must not submit"))

    sig = await engine.close_token_account(str(Keypair().pubkey()))
    assert sig is None


@pytest.mark.asyncio
async def test_close_token_account_returns_none_when_no_account_exists():
    engine = make_engine()

    async def fake_get_accounts(owner, opts, commitment=None):
        return FakeTokenAccountsResp([])

    engine._rpc.get_token_accounts_by_owner_json_parsed = fake_get_accounts
    sig = await engine.close_token_account(str(Keypair().pubkey()))
    assert sig is None


@pytest.mark.asyncio
async def test_close_token_account_swallows_failures_and_returns_none():
    """A close failure must never bubble up — it runs right after an
    already-successful sell and must not turn that into a reported error."""
    engine = make_engine()

    async def fake_get_accounts(owner, opts, commitment=None):
        raise RuntimeError("simulated RPC outage")

    engine._rpc.get_token_accounts_by_owner_json_parsed = fake_get_accounts
    sig = await engine.close_token_account(str(Keypair().pubkey()))
    assert sig is None


@pytest.mark.asyncio
async def test_execute_sell_reclaims_rent_after_a_confirmed_sell(monkeypatch):
    """Wiring check: a successful _execute_sell() must call
    close_token_account_with_amount() with the sold mint exactly once, and
    surface the real reclaim signature/amount on the result (2026-09-28)."""
    engine = make_engine()
    engine._get_quote = AsyncMock(return_value={"outAmount": "142280"})
    engine._build_swap_tx = AsyncMock(return_value=b"fake-tx-bytes")
    engine._sign_and_submit = AsyncMock(return_value="FAKESIG")
    engine._confirm_tx_with_delta = AsyncMock(return_value=(True, 142280, 5000))
    engine.close_token_account_with_amount = AsyncMock(return_value=("CLOSESIG", 1488440))

    result = await engine._execute_sell(mint="SoldOutMint", token_lamports=1746290)

    assert result.success is True
    engine.close_token_account_with_amount.assert_awaited_once_with("SoldOutMint")
    assert result.reclaim_tx_signature == "CLOSESIG"
    assert result.reclaim_sol_lamports == 1488440
    assert result.actual_sol_lamports == 142280


# ── sweep_dead_token_accounts() — periodic rent sweep (2026-09-25) ──────────
#
# Real incident: SEND's real sell succeeded on-chain seconds after entry,
# but this process happened to restart mid-confirmation-poll, so the DB
# write never happened and close_token_account() (called only right after
# a sell THIS engine itself just executed) never ran for it — the account
# sat unreclaimed for a full day. This is the general safety net: find
# every zero-balance account this wallet owns, regardless of how it went
# to zero, and close it.

class FakeAccountEntry:
    def __init__(self, mint: str, amount: str):
        self.account = type("Acc", (), {
            "data": type("Data", (), {"parsed": {"info": {"mint": mint, "tokenAmount": {"amount": amount, "decimals": 6}}}})(),
        })()


class FakeListResp:
    def __init__(self, rows):
        self.value = rows


@pytest.mark.asyncio
async def test_sweep_closes_only_zero_balance_accounts():
    """Scans both the classic Token program and Token-2022 — here only the
    classic program has any accounts (matching every real trade so far),
    Token-2022 comes back empty."""
    engine = make_engine()
    from engine.execution import _TOKEN_PROGRAM_ID

    async def fake_list(owner, opts, commitment=None):
        if str(opts.program_id) == _TOKEN_PROGRAM_ID:
            return FakeListResp([
                FakeAccountEntry("DeadMint1111111111111111111111111111111111", "0"),
                FakeAccountEntry("AliveMint111111111111111111111111111111111", "500"),
                FakeAccountEntry("DeadMint2222222222222222222222222222222222", "0"),
            ])
        return FakeListResp([])  # Token-2022 — nothing here

    engine._rpc.get_token_accounts_by_owner_json_parsed = fake_list
    engine.close_token_account = AsyncMock(side_effect=["SIG1", "SIG2"])

    reclaimed = await engine.sweep_dead_token_accounts()

    assert reclaimed == ["SIG1", "SIG2"]
    calls = [c.args[0] for c in engine.close_token_account.await_args_list]
    assert "DeadMint1111111111111111111111111111111111" in calls
    assert "DeadMint2222222222222222222222222222222222" in calls
    assert "AliveMint111111111111111111111111111111111" not in calls


@pytest.mark.asyncio
async def test_sweep_returns_empty_when_nothing_to_reclaim():
    engine = make_engine()

    async def fake_list(owner, opts, commitment=None):
        return FakeListResp([FakeAccountEntry("AliveMint111111111111111111111111111111111", "500")])

    engine._rpc.get_token_accounts_by_owner_json_parsed = fake_list
    engine.close_token_account = AsyncMock(side_effect=AssertionError("must not close a live account"))

    reclaimed = await engine.sweep_dead_token_accounts()
    assert reclaimed == []


@pytest.mark.asyncio
async def test_sweep_survives_a_listing_failure():
    engine = make_engine()

    async def fake_list(owner, opts, commitment=None):
        raise RuntimeError("simulated RPC outage")

    engine._rpc.get_token_accounts_by_owner_json_parsed = fake_list
    reclaimed = await engine.sweep_dead_token_accounts()
    assert reclaimed == []


@pytest.mark.asyncio
async def test_sweep_returns_empty_in_paper_mode():
    engine = ExecutionEngine(paper_override=True)
    reclaimed = await engine.sweep_dead_token_accounts()
    assert reclaimed == []


# ── get_sell_quote() — liquidity guard fix (2026-09-24) ─────────────────────
#
# Real bug found live: a DexScreener-sourced "current price" showed BLK
# +19.5% unrealized, while a real Jupiter quote for the exact held size —
# routed through the pool actually trading now — showed 100% price impact
# and a real -76.5% loss. get_sell_quote() is the read-only building block
# workers/confluence_live_worker.py's liquidity guard uses to catch this.

@pytest.mark.asyncio
async def test_get_sell_quote_returns_real_impact_and_amount():
    engine = make_engine()
    engine._get_quote = AsyncMock(return_value={"outAmount": "79842", "priceImpactPct": "1"})

    quote = await engine.get_sell_quote("SomeMint", 565147983)

    assert quote == {"out_lamports": 79842, "price_impact_pct": Decimal("1")}


@pytest.mark.asyncio
async def test_get_sell_quote_returns_none_when_no_route():
    engine = make_engine()
    engine._get_quote = AsyncMock(return_value=None)

    quote = await engine.get_sell_quote("SomeMint", 1000)
    assert quote is None


@pytest.mark.asyncio
async def test_get_sell_quote_returns_none_in_paper_mode():
    engine = ExecutionEngine(paper_override=True)
    quote = await engine.get_sell_quote("SomeMint", 1000)
    assert quote is None


@pytest.mark.asyncio
async def test_get_sell_quote_swallows_errors():
    engine = make_engine()
    engine._get_quote = AsyncMock(side_effect=RuntimeError("network blip"))
    quote = await engine.get_sell_quote("SomeMint", 1000)
    assert quote is None


@pytest.mark.asyncio
async def test_execute_sell_succeeds_even_if_reclaim_raises(monkeypatch):
    """A rent-reclaim failure must never turn a real, confirmed sell into a
    reported failure — close_token_account_with_amount() already swallows
    its own errors, but the call site in _execute_sell() guards it too in
    case that ever changes."""
    engine = make_engine()
    engine._get_quote = AsyncMock(return_value={"outAmount": "142280"})
    engine._build_swap_tx = AsyncMock(return_value=b"fake-tx-bytes")
    engine._sign_and_submit = AsyncMock(return_value="FAKESIG")
    engine._confirm_tx_with_delta = AsyncMock(return_value=(True, 142280, 5000))
    engine.close_token_account_with_amount = AsyncMock(side_effect=RuntimeError("should not happen, but must not break the sell"))

    result = await engine._execute_sell(mint="SoldOutMint", token_lamports=1746290)
    assert result.success is True
    assert result.reclaim_tx_signature is None  # reclaim failed, but that must never fail the sell itself
