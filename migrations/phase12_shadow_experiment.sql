-- Phase 12 migration
-- Shadow-evaluation experiment: "scorer_v2_threshold_4"
--
-- Purely additive, purely observational. This table is never read by
-- CapitalEngine, RiskEngine, execution.py, or any production decision
-- path. Nothing in this migration changes TokenStatus, Trade, or any
-- existing table. A row here means "the real scorer produced this score
-- for this token at this moment" — recorded so an experimental,
-- lower-threshold entry policy can be backtested against real subsequent
-- price action, without ever routing through the real (even paper)
-- capital/risk engines and without ever touching the real 5.0/8.0
-- thresholds. Safe on a live DB.

CREATE TABLE IF NOT EXISTS shadow_trades (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_id          UUID NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    experiment_version VARCHAR(64) NOT NULL,
    triggered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    score             NUMERIC(6,4) NOT NULL,
    tier1_passed      BOOLEAN NOT NULL,
    entry_price       NUMERIC(24,12) NOT NULL,
    liquidity_usd     NUMERIC(20,6),
    market_cap_usd    NUMERIC(20,6),
    buys              INTEGER,
    sells             INTEGER,
    wash_multiplier   NUMERIC(10,4),
    inputs_json       JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- One shadow entry per token per experiment — the FIRST time this
    -- token's score crossed this experiment's threshold, mirroring how a
    -- real strategy only enters once, not once per scoring cycle.
    UNIQUE (token_id, experiment_version)
);

CREATE INDEX IF NOT EXISTS ix_shadow_trades_experiment
    ON shadow_trades(experiment_version, triggered_at);

COMMENT ON TABLE shadow_trades IS
    'Analysis-only shadow entries for threshold-lowering experiments (e.g. scorer_v2_threshold_4). Records the decision-time snapshot only; outcomes (returns, drawdown, exit-rule simulation) are always derived later from token_snapshots by the analysis script, never stored here. Never read by production trading code.';
