-- Phase 16 migration
-- Real, on-chain live trading for confluence_entry_v1 ONLY.
--
-- Fully isolated from the real `trades` table, CapitalEngine, RiskEngine's
-- production decisions, and STRONG_BUY/S1_WAVE. Executed via
-- engine/execution.py's ExecutionEngine against a dedicated wallet
-- (settings.CONFLUENCE_LIVE_WALLET_PRIVATE_KEY), never the main
-- WALLET_PRIVATE_KEY, and gated behind settings.CONFLUENCE_LIVE_ENABLED
-- (default False).
--
-- Built 2026-09-23 after the user reviewed a mixed
-- confluence_shadow_positions evaluation (n=167: real edge but
-- inconsistent across time, 17% rug rate, one outlier dominating the raw
-- headline number) and explicitly chose to proceed with $10 total capital
-- at risk, one position at a time.

CREATE TABLE IF NOT EXISTS confluence_live_trades (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_id               UUID NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    n_rules_cofiring       INTEGER NOT NULL,
    status                 VARCHAR(16) NOT NULL DEFAULT 'open',
    entry_time             TIMESTAMPTZ NOT NULL,
    entry_price            NUMERIC(30,12) NOT NULL,
    entry_sol_lamports     INTEGER,
    entry_token_lamports   INTEGER,
    entry_tx_signature     VARCHAR(128),
    exit_time              TIMESTAMPTZ,
    exit_price             NUMERIC(30,12),
    exit_reason            VARCHAR(32),
    exit_sol_lamports      INTEGER,
    exit_tx_signature      VARCHAR(128),
    pnl_usd                NUMERIC(12,6),
    position_usd           NUMERIC(12,6) NOT NULL,
    error_detail           TEXT
);

CREATE INDEX IF NOT EXISTS ix_confluence_live_status
    ON confluence_live_trades(status);

CREATE TABLE IF NOT EXISTS confluence_live_observations (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id     UUID NOT NULL REFERENCES confluence_live_trades(id) ON DELETE CASCADE,
    observed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    price_usd    NUMERIC(30,12) NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_confluence_live_observations_trade_id
    ON confluence_live_observations(trade_id, observed_at);

COMMENT ON TABLE confluence_live_trades IS
    'Real, on-chain trades for confluence_entry_v1 only. Isolated from the trades table, CapitalEngine, RiskEngine production decisions, and STRONG_BUY/S1_WAVE. Gated behind settings.CONFLUENCE_LIVE_ENABLED (default False) and a dedicated wallet.';
COMMENT ON TABLE confluence_live_observations IS
    'Raw 1-second price path for OPEN confluence_live_trades rows.';
