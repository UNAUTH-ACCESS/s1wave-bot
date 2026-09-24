-- Phase 14 migration
-- Forward-tracking shadow experiment: "momentum_confluence_v1"
--
-- Purely additive, purely observational. This table is never read by
-- CapitalEngine, RiskEngine, execution.py, or any production decision
-- path. Nothing in this migration changes TokenStatus, Trade, or any
-- existing table.
--
-- Records the first moment a token's price rises >5.3% over a trailing
-- 3-minute window (research: analysis/pump_timing_research.py,
-- analysis/pump_signal_quality.py, 2026-09-22) — a signal found to
-- reliably precede a token's peak by a median ~8 minutes, with the
-- number of co-firing secondary rules (n_rules_cofiring) found to
-- roughly double the odds the move holds rather than dumping. This table
-- exists to validate that finding PROSPECTIVELY, on tokens discovered
-- from here forward, using the token_snapshots the real pipeline already
-- writes — no extra API calls, no change to any real threshold. Safe on
-- a live DB.

CREATE TABLE IF NOT EXISTS momentum_signal_events (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_id              UUID NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
    experiment_version    VARCHAR(64) NOT NULL,
    triggered_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    trigger_price         NUMERIC(30,12) NOT NULL,
    trailing_return_3min  NUMERIC(10,6) NOT NULL,
    trailing_return_5min  NUMERIC(10,6),
    buy_pressure          NUMERIC(6,4),
    volume_mult           NUMERIC(12,4),
    n_rules_cofiring      INTEGER NOT NULL,
    liquidity_usd         NUMERIC(20,6),
    market_cap_usd        NUMERIC(20,6),

    -- One signal event per token per experiment — the FIRST time the
    -- primary rule crossed, mirroring a real strategy that only enters
    -- once, not once per snapshot.
    UNIQUE (token_id, experiment_version)
);

CREATE INDEX IF NOT EXISTS ix_momentum_signal_experiment
    ON momentum_signal_events(experiment_version, triggered_at);

COMMENT ON TABLE momentum_signal_events IS
    'Analysis-only, forward-tracking shadow experiment for the pump-timing signal (momentum_confluence_v1). Records the trigger-moment snapshot only; outcomes are always derived later from token_snapshots by an analysis script, never stored here. Never read by production trading code.';
