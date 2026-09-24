"""
tests/test_phase2.py
====================
Phase 2 — Discovery and Enrichment tests.

All HTTP calls are mocked with httpx.MockTransport / respx so no real
API keys or network access are needed.

Test surface:
  - DexScreenerClient: profile parsing, pair selection, rate-limit retry
  - HeliusClient: DAS parsing, safe-default-on-failure, TX wash multiplier
  - wash_multiplier direction (critical — the exact boundary behaviour)
  - LP burn detection from transaction data
  - DiscoveryWorker: deduplication, queue population, stale re-queue
  - EnrichmentWorker: parallel fetch, DB write, failure isolation
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from dexscreener.client import DexScreenerClient, _parse_pair, _parse_token_detail
from helius.client import HeliusClient, _analyse_transactions, HeliusTxAnalysis
from models.orm import Token, TokenStatus


# ── Fixtures — canned API responses ──────────────────────────────────────────

SOLANA_PROFILE_RESPONSE = [
    {
        "chainId": "solana",
        "tokenAddress": "TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "symbol": "AAAA",
        "name": "Token AAAA",
        "icon": "https://example.com/icon.png",
    },
    {
        "chainId": "solana",
        "tokenAddress": "TokenBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "symbol": "BBBB",
        "name": "Token BBBB",
    },
    {
        "chainId": "ethereum",  # should be filtered out
        "tokenAddress": "0xNotSolana",
        "symbol": "ETH",
    },
]

SOLANA_PAIR_RESPONSE = {
    "pairs": [
        {
            "chainId": "solana",
            "pairAddress": "PairXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            "baseToken": {
                "address": "TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "symbol": "AAAA",
                "name": "Token AAAA",
            },
            "priceUsd": "0.00123",
            "liquidity": {"usd": "45000.50"},
            "marketCap": "92000.00",
            "volume": {"m5": "8500.00", "h1": "42000.00"},
            "txns": {
                "m5": {"buys": 72, "sells": 28},
                "h1": {"buys": 310, "sells": 140},
            },
            "pairCreatedAt": 1746057600000,
        },
        {
            "chainId": "solana",
            "pairAddress": "PairYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY",
            "baseToken": {
                "address": "TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "symbol": "AAAA",
                "name": "Token AAAA",
            },
            "priceUsd": "0.00122",
            "liquidity": {"usd": "12000.00"},
            "marketCap": "91000.00",
            "volume": {"m5": "2000.00", "h1": "9000.00"},
            "txns": {
                "m5": {"buys": 15, "sells": 10},
                "h1": {"buys": 60, "sells": 40},
            },
            "pairCreatedAt": 1746057600000,
        },
    ]
}

HELIUS_ASSET_RESPONSE = {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "id": "TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "ownership": {"owner": None},       # mint authority renounced
        "authorities": [],                   # no remaining authorities
        "token_info": {
            "freeze_authority": None,        # freeze authority renounced
            "holder_count": 842,
        },
        "supply": {},
    }
}

HELIUS_ASSET_RESPONSE_NOT_RENOUNCED = {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "id": "TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "ownership": {"owner": "SomeMintAuthority111111111111111111111111"},
        "authorities": [{"scopes": ["mint"], "address": "SomeMintAuthority"}],
        "token_info": {
            "freeze_authority": "SomeFreezeAuth1111111111111111111111111111",
            "holder_count": 10,
        },
        "supply": {},
    }
}

HELIUS_TX_RESPONSE_WITH_LP_BURN = {
    "jsonrpc": "2.0",
    "id": 1,
    "result": [
        {
            "type": "SWAP",
            "description": "burn lp tokens",
            "feePayer": "UserWalletAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "tokenTransfers": [
                {
                    "toUserAccount": "1nc1nerator11111111111111111111111111111111",
                    "tokenAmount": "1000",
                }
            ],
            "nativeTransfers": [],
            "instructions": [],
        },
        *[
            {
                "type": "SWAP",
                "description": "",
                "feePayer": f"Buyer{i:04d}AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "tokenTransfers": [
                    {
                        "toUserAccount": f"Buyer{i:04d}AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "tokenAmount": "500",
                    }
                ],
                "nativeTransfers": [],
                "instructions": [],
            }
            for i in range(40)  # 40 buys
        ],
        *[
            {
                "type": "SWAP",
                "description": "",
                "feePayer": f"Seller{i:04d}AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "tokenTransfers": [
                    {
                        "toUserAccount": "LiqPoolAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                        "tokenAmount": "200",
                    }
                ],
                "nativeTransfers": [],
                "instructions": [],
            }
            for i in range(20)  # 20 sells → wash_mult = 40/20 = 2.0 (passes)
        ],
    ]
}


# ── DexScreenerClient tests ───────────────────────────────────────────────────

class TestDexScreenerClient:

    @pytest.mark.asyncio
    async def test_filters_solana_only(self):
        """Non-Solana tokens in the response are discarded."""
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = SOLANA_PROFILE_RESPONSE
            mock_get.return_value = mock_resp

            client = DexScreenerClient()
            client._http = MagicMock()
            client._http.get = AsyncMock(return_value=mock_resp)

            profiles = await client.get_latest_token_profiles()

        solana_profiles = [p for p in profiles if p.chain_id == "solana"]
        assert len(solana_profiles) == 2
        assert all(p.chain_id == "solana" for p in profiles)

    def test_parse_pair_buy_pressure_m5(self):
        """buy_pressure_m5 = m5 buys / (m5 buys + m5 sells)"""
        pair_data = SOLANA_PAIR_RESPONSE["pairs"][0]
        pair = _parse_pair(pair_data)
        # 72 buys, 28 sells → BP_m5 = 72/100 = 0.72
        assert pair.buy_pressure_m5 == Decimal("0.7200")

    def test_parse_pair_buy_pressure_h1(self):
        """buy_pressure_h1 uses the 1-hour window — noise-resistant for enrichment."""
        pair_data = SOLANA_PAIR_RESPONSE["pairs"][0]
        pair = _parse_pair(pair_data)
        # 310 buys, 140 sells → BP_h1 = 310/450 ≈ 0.6889
        assert pair.buy_pressure_h1 == Decimal("0.6889")

    def test_buy_pressure_property_returns_m5(self):
        """Back-compat property: .buy_pressure returns m5 value for sampling_worker."""
        pair_data = SOLANA_PAIR_RESPONSE["pairs"][0]
        pair = _parse_pair(pair_data)
        assert pair.buy_pressure == pair.buy_pressure_m5

    def test_parse_pair_zero_m5_transactions(self):
        """buy_pressure_m5 is None when both m5 counts are 0."""
        pair_data = {**SOLANA_PAIR_RESPONSE["pairs"][0]}
        pair_data["txns"] = {"m5": {"buys": 0, "sells": 0}, "h1": {"buys": 50, "sells": 30}}
        pair = _parse_pair(pair_data)
        assert pair.buy_pressure_m5 is None
        # h1 still has data
        assert pair.buy_pressure_h1 is not None

    def test_parse_token_detail_picks_highest_liquidity(self):
        """When multiple pairs exist, canonical pair = highest liquidity."""
        detail = _parse_token_detail(
            "TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            SOLANA_PAIR_RESPONSE["pairs"],
        )
        assert detail.best_pair is not None
        # Pair 0 has liquidity 45000.50, Pair 1 has 12000 — Pair 0 wins
        assert detail.best_pair.liquidity_usd == Decimal("45000.50")

    def test_parse_missing_decimal_fields(self):
        """Missing numeric fields produce None, not an error."""
        pair_data = {
            "chainId": "solana",
            "pairAddress": "PairZZZZ",
            "baseToken": {"address": "MintZZZZ", "symbol": "ZZZ", "name": "Token Z"},
            "txns": {"m5": {"buys": 5, "sells": 5}},
            # priceUsd, liquidity, marketCap, volume intentionally absent
        }
        pair = _parse_pair(pair_data)
        assert pair.price_usd is None
        assert pair.liquidity_usd is None
        assert pair.market_cap_usd is None
        assert pair.volume_m5_usd is None


# ── HeliusClient tests ────────────────────────────────────────────────────────

class TestHeliusClient:

    def _make_mock_http(self, response_data: dict):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = response_data
        mock_resp.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.post = AsyncMock(return_value=mock_resp)
        return mock_http

    @pytest.mark.asyncio
    async def test_get_asset_renounced_authorities(self):
        client = HeliusClient()
        client._http = self._make_mock_http(HELIUS_ASSET_RESPONSE)

        info = await client.get_asset("TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

        assert info.mint_authority_renounced is True
        assert info.freeze_authority_renounced is True
        assert info.holder_count == 842

    @pytest.mark.asyncio
    async def test_get_asset_not_renounced(self):
        client = HeliusClient()
        client._http = self._make_mock_http(HELIUS_ASSET_RESPONSE_NOT_RENOUNCED)

        info = await client.get_asset("TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

        assert info.mint_authority_renounced is False
        assert info.freeze_authority_renounced is False

    @pytest.mark.asyncio
    async def test_get_asset_rpc_error_returns_safe_default(self):
        """On RPC error, returns False (not renounced) — safe for Tier 1 rejection."""
        error_response = {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32600, "message": "Invalid request"},
        }
        client = HeliusClient()
        client._http = self._make_mock_http(error_response)

        info = await client.get_asset("TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

        # Safe defaults — these will cause Tier 1 rejection, which is correct
        assert info.mint_authority_renounced is False
        assert info.freeze_authority_renounced is False

    @pytest.mark.asyncio
    async def test_analyse_transactions_returns_safe_default_stub(self):
        """
        Phase 12: analyse_transactions() bypasses real TX parsing entirely
        for PumpSwap tokens and always returns safe defaults — LP burn is
        guaranteed by pump.fun's graduation protocol, and wash multiplier
        is computed from DEX Screener h1 data instead (see
        EnrichmentWorker._enrich_token / TestWashMultiplierAnalysis for the
        real parsing logic, _analyse_transactions(), which still exists
        and is still tested but is no longer called by this method). This
        replaces two tests written before that change that fed it mock TX
        data expecting it to be parsed — it never even looks at the
        response body now, so the specific mock data doesn't matter.
        """
        client = HeliusClient()
        client._http = self._make_mock_http(HELIUS_TX_RESPONSE_WITH_LP_BURN)

        analysis = await client.analyse_transactions("TokenAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

        assert analysis.lp_locked_burned is False
        assert analysis.wash_multiplier == float("inf")
        assert analysis.buy_count == 0
        assert analysis.sell_count == 0


# ── Wash multiplier boundary tests ───────────────────────────────────────────

class TestWashMultiplierAnalysis:
    """
    Direct tests of _analyse_transactions to verify wash multiplier
    direction is correct at and around the 2.5 boundary.
    """

    def _make_txs(self, buy_count: int, sell_count: int) -> list[dict]:
        """Build minimal fake TX list with given buy/sell counts."""
        txs = []
        for i in range(buy_count):
            wallet = f"Buyer{i:04d}AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            txs.append({
                "type": "SWAP",
                "description": "",
                "feePayer": wallet,
                "tokenTransfers": [{"toUserAccount": wallet, "tokenAmount": "100"}],
                "nativeTransfers": [],
                "instructions": [],
            })
        for i in range(sell_count):
            wallet = f"Seller{i:04d}AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            txs.append({
                "type": "SWAP",
                "description": "",
                "feePayer": wallet,
                "tokenTransfers": [{"toUserAccount": "LiqPool", "tokenAmount": "100"}],
                "nativeTransfers": [],
                "instructions": [],
            })
        return txs

    def test_exactly_2_5_passes(self):
        """buy/sell = 2.5 exactly → boundary is inclusive → PASS"""
        txs = self._make_txs(buy_count=25, sell_count=10)
        analysis = _analyse_transactions(txs)
        assert analysis.wash_multiplier == pytest.approx(2.5)
        assert analysis.wash_multiplier <= 2.5  # passes

    def test_above_2_5_fails(self):
        """buy/sell = 3.0 → wash trading → FAIL"""
        txs = self._make_txs(buy_count=30, sell_count=10)
        analysis = _analyse_transactions(txs)
        assert analysis.wash_multiplier == pytest.approx(3.0)
        assert analysis.wash_multiplier > 2.5  # fails

    def test_zero_sells_is_infinity(self):
        """0 sells → infinite multiplier → always fails"""
        txs = self._make_txs(buy_count=50, sell_count=0)
        analysis = _analyse_transactions(txs)
        assert analysis.wash_multiplier == float("inf")
        assert analysis.wash_multiplier > 2.5  # fails

    def test_equal_buys_sells(self):
        """50/50 split → 1.0 → well within organic range"""
        txs = self._make_txs(buy_count=25, sell_count=25)
        analysis = _analyse_transactions(txs)
        assert analysis.wash_multiplier == pytest.approx(1.0)
        assert analysis.wash_multiplier <= 2.5  # passes

    def test_no_transactions_returns_infinity(self):
        """Empty TX list → infinite wash mult → Tier 1 fails safely"""
        analysis = _analyse_transactions([])
        assert analysis.wash_multiplier == float("inf")


# ── DiscoveryWorker tests ─────────────────────────────────────────────────────
#
# The two tests that used to live here (test_new_tokens_inserted_and_queued,
# test_known_tokens_are_skipped) tested a DexTokenProfile-based
# DiscoveryWorker(queue, dex_client, shutdown) with a _filter_new_tokens()
# method — none of which exist anymore. DiscoveryWorker was fully rewritten
# to poll SolanaTracker's /tokens/multi/graduated directly (see
# workers/discovery_worker.py's docstring), with its own internal HTTP
# client rather than an injected DexScreenerClient, and mint dedup now
# lives in an in-memory `_promoted` set plus `_cover_known_tokens()`, not a
# DB-lookup filter function. Removed 2026-09-23 rather than rewritten,
# since equivalent, CURRENT-architecture coverage of the "a known mint
# gets covered instead of re-queued" behavior already exists in
# tests/test_discovery_sampling_sync.py's TestDiscoveryCoversKnownTokens.


# ── EnrichmentWorker tests ────────────────────────────────────────────────────

class TestEnrichmentWorker:

    @pytest.mark.asyncio
    async def test_enrichment_writes_all_fields(self, session):
        """Enrichment writes DEX + Helius DAS + Helius TX data to token row."""
        from workers.enrichment_worker import EnrichmentWorker
        from dexscreener.client import DexTokenDetail, DexPairSnapshot
        from helius.client import HeliusAssetInfo, HeliusTxAnalysis

        mint = "MintENRICHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        token = Token(
            mint_address=mint,
            symbol="ENRICH",
            status=TokenStatus.ENRICHED,
            discovered_at=datetime.now(timezone.utc),
        )
        session.add(token)
        await session.flush()

        mock_dex = AsyncMock()
        mock_dex.get_token_detail.return_value = DexTokenDetail(
            token_address=mint,
            symbol="ENRICH",
            name="Enrich Token",
            best_pair=DexPairSnapshot(
                pair_address="PairENRICH",
                base_token_address=mint,
                base_token_symbol="ENRICH",
                base_token_name="Enrich Token",
                price_usd=Decimal("0.001"),
                liquidity_usd=Decimal("35000"),
                market_cap_usd=Decimal("70000"),
                volume_m5_usd=Decimal("5000"),
                volume_h1_usd=Decimal("20000"),
                buy_pressure_m5=Decimal("0.65"),
                buy_pressure_h1=Decimal("0.60"),
                pair_created_at_ms=1746057600000,
                txns_m5_buys=65,
                txns_m5_sells=35,
                txns_h1_buys=250,
                txns_h1_sells=150,
            ),
        )

        mock_helius = AsyncMock()
        mock_helius.get_asset.return_value = HeliusAssetInfo(
            mint_address=mint,
            mint_authority_renounced=True,
            freeze_authority_renounced=True,
            holder_count=500,
            token_decimals=6,
        )

        queue = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = EnrichmentWorker(queue, mock_dex, mock_helius, shutdown)

        # _write_enrichment's signature changed (Phase 12): `pair` is now
        # the DexPairSnapshot directly (dex_detail.best_pair), not the
        # DexTokenDetail wrapper; token_decimals is a plain int from
        # Helius (not the whole HeliusAssetInfo); wash multiplier and h1
        # buy/sell counts now come from DEX Screener h1 data, not from
        # Helius TX analysis (removed — see TestHeliusClient's
        # analyse_transactions stub tests).
        pair = mock_dex.get_token_detail.return_value.best_pair
        dex_wash_mult = Decimal(str(round(pair.txns_h1_buys / pair.txns_h1_sells, 4)))

        with patch("workers.enrichment_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            await worker._write_enrichment(
                mint, pair, 6, 100, dex_wash_mult, pair.txns_h1_buys, pair.txns_h1_sells,
            )

        # Re-fetch from session to check writes
        result = await session.execute(select(Token).where(Token.mint_address == mint))
        updated = result.scalar_one()

        assert updated.liquidity_usd == Decimal("35000")
        assert updated.mint_authority_renounced is True
        assert updated.freeze_authority_renounced is True
        # pump.fun graduation guarantees these — hardcoded True regardless
        # of any Helius TX data (see enrichment_worker.py's docstring).
        assert updated.lp_locked_burned is True
        assert updated.wash_multiplier == dex_wash_mult
        # Baseline volume set on first enrichment
        assert updated.baseline_volume_usd == Decimal("5000")
        assert updated.token_decimals == 6

    @pytest.mark.asyncio
    async def test_enrichment_failure_marks_rejected(self, session):
        """If a data source raises, token is marked REJECTED not stuck ENRICHED."""
        from workers.enrichment_worker import EnrichmentWorker

        mint = "MintFAILAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        token = Token(
            mint_address=mint,
            symbol="FAIL",
            status=TokenStatus.ENRICHED,
            discovered_at=datetime.now(timezone.utc),
        )
        session.add(token)
        await session.flush()

        mock_dex = AsyncMock()
        mock_dex.get_token_detail.side_effect = Exception("DEX Screener timeout")

        mock_helius = AsyncMock()
        mock_helius.get_asset.return_value = MagicMock()
        mock_helius.analyse_transactions.return_value = MagicMock()

        queue = asyncio.Queue()
        shutdown = asyncio.Event()
        worker = EnrichmentWorker(queue, mock_dex, mock_helius, shutdown)

        with patch("workers.enrichment_worker.get_session") as mock_gs:
            mock_gs.return_value.__aenter__ = AsyncMock(return_value=session)
            mock_gs.return_value.__aexit__ = AsyncMock(return_value=False)

            await worker._mark_rejected(mint, "ENRICHMENT_FAILED:DEX")

        result = await session.execute(select(Token).where(Token.mint_address == mint))
        updated = result.scalar_one()
        assert updated.status == TokenStatus.REJECTED
        assert "ENRICHMENT_FAILED" in (updated.rejection_reason or "")
