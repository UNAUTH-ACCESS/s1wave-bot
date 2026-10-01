-- Phase 19: modeled shadow fills need the entry pool liquidity. Additive only.
ALTER TABLE confluence_shadow_positions
  ADD COLUMN IF NOT EXISTS exec_entry_liq_usd NUMERIC(20,6);
