-- =============================================================================
-- aidq_metadata  |  Migration 007: LLM call log for model routing
-- Requires: 001_agent_governance.sql
-- Plan: docs/model_routing_plan.md (owner decision 2026-10-13: Postgres, not Delta)
--
-- Schema only, identical in dev and prod: grants are environment configuration
-- (grants/dev.yml metadata_roles), not part of this file.
--   * llm_calls: one row per model call made through src/aidq_llm (agent, task,
--     model, escalation, tokens, latency, illustrative cost, trace id, outcome).
--   * prune_llm_calls(keep_days): the 90-day retention rule. Deletes rows older
--     than keep_days (at least 1) and returns how many; SECURITY DEFINER so a
--     writer can prune without DELETE on the table. The router calls it once per
--     run after writing its rows.
-- Runs inside the migration runner's transaction (tools/migrate.py): no BEGIN/COMMIT.
-- =============================================================================
SET LOCAL search_path = aidq_metadata;

CREATE TABLE IF NOT EXISTS llm_calls (
  call_id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  called_at         timestamptz NOT NULL DEFAULT now(),
  agent             text NOT NULL,                 -- e.g. 'aidq_monitor@0.4'
  task              text NOT NULL,                 -- routing.yml task, e.g. 'root_cause'
  model             text NOT NULL,
  escalated         boolean NOT NULL DEFAULT false,  -- this call ran on the fallback model
  escalation_reason text,
  input_tokens      integer,
  output_tokens     integer,
  latency_ms        integer,
  est_cost_usd      numeric(12, 6),                -- illustrative list price (Free Edition is free)
  trace_id          text,                          -- the run's agent_trace_id until MLflow tracing exists
  outcome           text NOT NULL CHECK (outcome IN ('ok', 'error', 'schema_invalid', 'timeout')),
  error             text
);
CREATE INDEX IF NOT EXISTS llm_calls_called_at_idx ON llm_calls (called_at);
CREATE INDEX IF NOT EXISTS llm_calls_task_model_idx ON llm_calls (task, model, called_at);

CREATE OR REPLACE FUNCTION prune_llm_calls(keep_days integer DEFAULT 90) RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = aidq_metadata, pg_temp AS $$
DECLARE
  n integer;
BEGIN
  IF keep_days IS NULL OR keep_days < 1 THEN
    RAISE EXCEPTION 'keep_days must be at least 1 (got %)', keep_days;
  END IF;
  DELETE FROM aidq_metadata.llm_calls WHERE called_at < now() - make_interval(days => keep_days);
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END $$;

REVOKE EXECUTE ON FUNCTION prune_llm_calls(integer) FROM PUBLIC;
