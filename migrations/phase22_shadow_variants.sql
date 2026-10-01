-- Phase 22: exit-rule variants riding on shadow positions. Additive only.
CREATE TABLE IF NOT EXISTS confluence_shadow_variant_positions (
  id UUID PRIMARY KEY,
  position_id UUID NOT NULL REFERENCES confluence_shadow_positions(id) ON DELETE CASCADE,
  variant VARCHAR(24) NOT NULL,
  status VARCHAR(8) NOT NULL DEFAULT 'open',
  trailing_stop_floor NUMERIC(30,12),
  high_watermark_price NUMERIC(30,12),
  exit_price NUMERIC(30,12),
  exit_time TIMESTAMPTZ,
  exit_reason VARCHAR(32),
  pnl_pct NUMERIC(10,6),
  exec_pnl_pct NUMERIC(10,6),
  CONSTRAINT uq_shadow_variant_position UNIQUE (position_id, variant)
);
CREATE INDEX IF NOT EXISTS ix_shadow_variant_status ON confluence_shadow_variant_positions (status);
