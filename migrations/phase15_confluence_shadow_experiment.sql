-- Phase 15 migration
-- Real-time-monitored paper-trade experiment: "confluence_entry_v1"
--
-- Purely additive, purely observational. Neither table here is ever read
-- by CapitalEngine, RiskEngine, execution.py, or any production decision
-- path. Nothing in this migration changes TokenStatus, Trade, or any
-- existing table. A position opening or closing here changes nothing
-- about what the real bot does, consumes no MAX_CONCURRENT_TRADES slot,
-- and has no effect on the daily-loss circuit breaker.
--
-- Opened the moment a momentum_signal_events row (see phase14) records
-- n_rules_cofiring >= 2 (research: analysis/pump_signal_quality.py,
-- 2026-09-22, found this confluence roughly doubles win rate). Monitored
-- at the same 1-second DexScreener cadence a real trade gets via
-- TradeMonitorWorker — closing the gap where the retrospective expectancy
-- backtest (analysis/momentum_signal_expectancy.py) could only simulate
-- exits against 30-60s WATCHING-tier token_snapshots, since no historical
-- 1-second data exists for tokens that were never actually entered.

CREATE TABLE IF NOT EXISTS confluence_shadow_positions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_id            UUID NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    experiment_version  VARCHAR(64) NOT NULL,
    entry_price         NUMERIC(30,12) NOT NULL,
    entry_time          TIMESTAMPTZ NOT NULL,
    n_rules_cofiring    INTEGER NOT NULL,
    status              VARCHAR(16) NOT NULL DEFAULT 'open',
    exit_price          NUMERIC(30,12),
    exit_time           TIMESTAMPTZ,
    exit_reason         VARCHAR(32),
    pnl_pct             NUMERIC(10,6),

    -- One shadow position per token per experiment — mirrors how a real
    -- strategy only enters once per token.
    UNIQUE (token_id, experiment_version)
);

CREATE INDEX IF NOT EXISTS ix_confluence_shadow_status
    ON confluence_shadow_positions(status);

CREATE TABLE IF NOT EXISTS confluence_shadow_observations (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    position_id  UUID NOT NULL REFERENCES confluence_shadow_positions(id) ON DELETE CASCADE,
    observed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    price_usd    NUMERIC(30,12) NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_confluence_shadow_observations_position_id
    ON confluence_shadow_observations(position_id, observed_at);

COMMENT ON TABLE confluence_shadow_positions IS
    'Real-time-monitored (1s DexScreener cadence) paper-trade experiment for the momentum-confluence entry rule. Never read by production trading code, never touches real trades/capital/concurrency.';
COMMENT ON TABLE confluence_shadow_observations IS
    'Raw 1-second price path for OPEN confluence_shadow_positions rows. Analysis-only, same convention as trade_price_observations.';
