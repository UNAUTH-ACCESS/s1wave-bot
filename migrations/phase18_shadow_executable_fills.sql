-- Phase 18: executable ("realistic") fills for shadow positions. Additive only.
ALTER TABLE confluence_shadow_positions
  ADD COLUMN IF NOT EXISTS exec_status VARCHAR(16),
  ADD COLUMN IF NOT EXISTS exec_notional_usd NUMERIC(20,6),
  ADD COLUMN IF NOT EXISTS exec_entry_lamports BIGINT,
  ADD COLUMN IF NOT EXISTS exec_entry_tokens_raw BIGINT,
  ADD COLUMN IF NOT EXISTS exec_exit_lamports BIGINT,
  ADD COLUMN IF NOT EXISTS exec_pnl_pct NUMERIC(10,6);

CREATE TABLE IF NOT EXISTS confluence_shadow_exec_checks (
  id UUID PRIMARY KEY,
  position_id UUID NOT NULL REFERENCES confluence_shadow_positions(id) ON DELETE CASCADE,
  checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  kind VARCHAR(8) NOT NULL,
  out_lamports BIGINT,
  exec_pnl_pct NUMERIC(10,6),
  price_impact_raw VARCHAR(40)
);
CREATE INDEX IF NOT EXISTS ix_confluence_shadow_exec_checks_position
  ON confluence_shadow_exec_checks (position_id, checked_at);
