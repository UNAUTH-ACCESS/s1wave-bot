-- Phase 13 migration
-- Post-entry price instrumentation. Purely additive: one new table, no
-- existing column touched, no historical row rewritten. Fills the gap
-- found during the full-lifecycle report: once a token became ENTERED,
-- sampling_worker stopped sampling it, and the live monitor that takes
-- over (TradeMonitorWorker/TradeRiskWorker) never persisted anything —
-- meaning MFE/MAE and post-entry price path were unrecoverable for every
-- past trade. This does not change entry logic, exit logic, thresholds,
-- risk controls, or paper execution — TradeMonitorWorker already fetches
-- this exact price data every cycle for risk evaluation; this table just
-- keeps a copy of it instead of discarding it after use.

CREATE TABLE IF NOT EXISTS trade_price_observations (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id       UUID NOT NULL REFERENCES trades(id) ON DELETE CASCADE,
    observed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    price_usd      NUMERIC(30,12) NOT NULL,
    liquidity_usd  NUMERIC(20,6),
    buy_pressure   NUMERIC(10,4)
);

CREATE INDEX IF NOT EXISTS ix_trade_price_observations_trade_id
    ON trade_price_observations(trade_id, observed_at);

COMMENT ON TABLE trade_price_observations IS
    'Raw price observations for a trade while it is OPEN, one row per TradeMonitorWorker cycle (~1s). Written only while status=OPEN — the existing OPEN-only load query means writes naturally stop the cycle after a trade closes, with no separate stop condition needed. MFE/MAE/highest/lowest are always derived from this table at analysis time, never stored here, consistent with every other outcome in this project.';
