-- =============================================================================
-- aidq_metadata  |  Migration 001: agent governance layer
-- Target: Lakebase Autoscaling (Postgres 17), database databricks_postgres
--
-- Purpose: let AI agents PROPOSE metadata changes that a human approves,
-- with every applied change logged and reversible.
--
-- Safe for the current pipeline: additive only. No existing column is dropped
-- or renamed; the pipeline's current reads and writes keep working.
-- Run on a Lakebase BRANCH first, then on production.
-- =============================================================================
SET LOCAL search_path = aidq_metadata;

-- -----------------------------------------------------------------------------
-- 1. Harden existing tables
-- -----------------------------------------------------------------------------

-- run_audit had no key. Surrogate key works even with existing duplicate rows.
ALTER TABLE run_audit
  ADD COLUMN IF NOT EXISTS audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;

-- FK for new rows only (NOT VALID keeps existing orphans). Clean orphans, then:
--   ALTER TABLE run_audit VALIDATE CONSTRAINT run_audit_table_fk;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                  WHERE conname = 'run_audit_table_fk'
                    AND conrelid = 'aidq_metadata.run_audit'::regclass) THEN
    ALTER TABLE aidq_metadata.run_audit
      ADD CONSTRAINT run_audit_table_fk FOREIGN KEY (table_id)
      REFERENCES aidq_metadata.source_table_def (table_id) NOT VALID;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS run_audit_table_layer_time_idx
  ON run_audit (table_id, layer, started_at DESC);

-- Severity must be one the pipeline understands (new rows only until validated).
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                  WHERE conname = 'dq_rules_severity_chk'
                    AND conrelid = 'aidq_metadata.data_quality_rules'::regclass) THEN
    ALTER TABLE aidq_metadata.data_quality_rules
      ADD CONSTRAINT dq_rules_severity_chk CHECK (severity IN ('HARD', 'SOFT')) NOT VALID;
  END IF;
END $$;

-- Rule lifecycle. NOTE: dq.py must add "AND is_active" to its rule query
-- before deactivation has any effect.
ALTER TABLE data_quality_rules
  ADD COLUMN IF NOT EXISTS is_active   boolean     NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS created_by  text        NOT NULL DEFAULT 'human',
  ADD COLUMN IF NOT EXISTS created_at  timestamptz NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS proposal_id bigint;

-- rule_id already has an automatic id: it is a serial column (default
-- nextval('aidq_metadata.data_quality_rules_rule_id_seq')), so approved proposals can insert
-- rules as they are. ADD GENERATED ... AS IDENTITY is not used: Postgres refuses it on a
-- column that already has a default. Move the sequence past the existing ids, which were
-- inserted explicitly.
SELECT setval(pg_get_serial_sequence('aidq_metadata.data_quality_rules', 'rule_id'),
              GREATEST((SELECT max(rule_id) FROM data_quality_rules), 1));

-- watermark_col: allow NULL instead of '' for full loads. Existing '' rows
-- are left alone. Before converting them, change metadata.py to
-- (value or "").strip(), otherwise None.strip() will fail.
ALTER TABLE source_table_def ALTER COLUMN watermark_col DROP NOT NULL;

-- -----------------------------------------------------------------------------
-- 2. Proposals: the only table agents may write to
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS config_proposals (
  proposal_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  proposal_type     text NOT NULL CHECK (proposal_type IN (
                      'ADD_DQ_RULE', 'DEACTIVATE_DQ_RULE', 'ADD_COLUMN')),
  table_id          int  NOT NULL REFERENCES source_table_def (table_id),
  proposed_change   jsonb NOT NULL,          -- e.g. {"rule_name":..., "rule_expr":..., "severity":...}
  rationale         text NOT NULL,           -- plain-language explanation for the reviewer
  evidence          jsonb,                   -- failing-row counts, profile stats, sample reasons
  proposed_by       text NOT NULL,           -- agent name + version, e.g. 'dq_agent@0.3.1'
  agent_trace_id    text,                    -- MLflow trace id for explainability
  confidence        numeric CHECK (confidence BETWEEN 0 AND 1),
  status            text NOT NULL DEFAULT 'PENDING' CHECK (status IN (
                      'PENDING', 'VALIDATION_FAILED', 'VALIDATED',
                      'APPROVED', 'REJECTED', 'APPLIED', 'ROLLED_BACK')),
  validation_result jsonb,                   -- output of the automatic dry run
  reviewed_by       text,
  reviewed_at       timestamptz,
  review_comment    text,
  applied_at        timestamptz,
  created_at        timestamptz NOT NULL DEFAULT now(),
  -- A decision needs a named reviewer, and nobody approves their own proposal.
  CONSTRAINT review_required CHECK (
    status NOT IN ('APPROVED', 'REJECTED', 'APPLIED') OR
    (reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL)),
  CONSTRAINT no_self_approval CHECK (reviewed_by IS DISTINCT FROM proposed_by)
);
CREATE INDEX IF NOT EXISTS config_proposals_status_idx ON config_proposals (status, created_at);

-- Enforce the lifecycle in the database, whatever tool writes to it:
-- new proposals start PENDING with no review, and nothing reaches APPROVED
-- without passing validation first.
CREATE OR REPLACE FUNCTION enforce_proposal_lifecycle() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.status <> 'PENDING' OR NEW.reviewed_by IS NOT NULL OR NEW.applied_at IS NOT NULL THEN
      RAISE EXCEPTION 'New proposals must be PENDING and unreviewed';
    END IF;
    RETURN NEW;
  END IF;
  IF NEW.proposed_change IS DISTINCT FROM OLD.proposed_change
     OR NEW.proposed_by IS DISTINCT FROM OLD.proposed_by
     OR NEW.table_id IS DISTINCT FROM OLD.table_id THEN
    RAISE EXCEPTION 'A proposal''s content cannot be edited; submit a new proposal';
  END IF;
  IF NEW.status IS DISTINCT FROM OLD.status AND NOT (
       (OLD.status = 'PENDING'           AND NEW.status IN ('VALIDATED', 'VALIDATION_FAILED', 'REJECTED'))
    OR (OLD.status = 'VALIDATION_FAILED' AND NEW.status = 'REJECTED')
    OR (OLD.status = 'VALIDATED'         AND NEW.status IN ('APPROVED', 'REJECTED'))
    OR (OLD.status = 'APPROVED'          AND NEW.status IN ('APPLIED', 'REJECTED'))
    OR (OLD.status = 'APPLIED'           AND NEW.status = 'ROLLED_BACK')) THEN
    RAISE EXCEPTION 'Invalid status change % -> % for proposal %', OLD.status, NEW.status, OLD.proposal_id;
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_proposal_lifecycle ON config_proposals;
CREATE TRIGGER trg_proposal_lifecycle BEFORE INSERT OR UPDATE
  ON config_proposals FOR EACH ROW EXECUTE FUNCTION enforce_proposal_lifecycle();

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint
                  WHERE conname = 'dq_rules_proposal_fk'
                    AND conrelid = 'aidq_metadata.data_quality_rules'::regclass) THEN
    ALTER TABLE aidq_metadata.data_quality_rules
      ADD CONSTRAINT dq_rules_proposal_fk FOREIGN KEY (proposal_id)
      REFERENCES aidq_metadata.config_proposals (proposal_id);
  END IF;
END $$;

-- -----------------------------------------------------------------------------
-- 3. Change log: every edit to config tables, by anyone, with before/after
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS config_change_log (
  change_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  table_name  text NOT NULL,
  operation   text NOT NULL,
  old_row     jsonb,
  new_row     jsonb,
  changed_by  text NOT NULL,
  proposal_id bigint,
  changed_at  timestamptz NOT NULL DEFAULT now()
);

-- Callers can set the actor for the transaction:  SET LOCAL aidq.actor = 'name';
CREATE OR REPLACE FUNCTION log_config_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO aidq_metadata.config_change_log
    (table_name, operation, old_row, new_row, changed_by, proposal_id)
  VALUES (
    TG_TABLE_NAME, TG_OP,
    CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) END,
    CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) END,
    COALESCE(NULLIF(current_setting('aidq.actor', true), ''), current_user),
    NULLIF(current_setting('aidq.proposal_id', true), '')::bigint);
  RETURN COALESCE(NEW, OLD);
END $$;

DROP TRIGGER IF EXISTS trg_log_dq_rules ON data_quality_rules;
CREATE TRIGGER trg_log_dq_rules AFTER INSERT OR UPDATE OR DELETE
  ON data_quality_rules FOR EACH ROW EXECUTE FUNCTION log_config_change();

DROP TRIGGER IF EXISTS trg_log_source_columns ON source_columns;
CREATE TRIGGER trg_log_source_columns AFTER INSERT OR UPDATE OR DELETE
  ON source_columns FOR EACH ROW EXECUTE FUNCTION log_config_change();

-- The pipeline updates status/watermark columns on source_table_def, so log
-- only changes to the configuration columns.
DROP TRIGGER IF EXISTS trg_log_source_table_def ON source_table_def;
CREATE TRIGGER trg_log_source_table_def AFTER UPDATE OF
  business_key, watermark_col, load_mode, scd_type, dest_schema, discard_threshold
  ON source_table_def FOR EACH ROW EXECUTE FUNCTION log_config_change();

-- -----------------------------------------------------------------------------
-- 4. Apply step: the only path from an approved proposal into live config
-- -----------------------------------------------------------------------------
-- SECURITY DEFINER: reviewers can apply approved proposals without holding
-- direct write access to the config tables.
CREATE OR REPLACE FUNCTION apply_proposal(p_id bigint, p_actor text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path = aidq_metadata, pg_temp AS $$
DECLARE
  p aidq_metadata.config_proposals%ROWTYPE;
  c jsonb;
BEGIN
  SELECT * INTO p FROM aidq_metadata.config_proposals
   WHERE proposal_id = p_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'Proposal % not found', p_id; END IF;
  IF p.status <> 'APPROVED' THEN
    RAISE EXCEPTION 'Proposal % is %, only APPROVED proposals can be applied', p_id, p.status;
  END IF;
  c := p.proposed_change;
  PERFORM set_config('aidq.actor', p_actor, true);
  PERFORM set_config('aidq.proposal_id', p_id::text, true);

  IF p.proposal_type = 'ADD_DQ_RULE' THEN
    INSERT INTO aidq_metadata.data_quality_rules
      (table_id, rule_name, rule_expr, severity, created_by, proposal_id)
    VALUES (p.table_id, c->>'rule_name', c->>'rule_expr',
            COALESCE(c->>'severity', 'SOFT'), p.proposed_by, p_id);

  ELSIF p.proposal_type = 'DEACTIVATE_DQ_RULE' THEN
    UPDATE aidq_metadata.data_quality_rules SET is_active = false
     WHERE rule_id = (c->>'rule_id')::int AND table_id = p.table_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'Rule % not found for table %', c->>'rule_id', p.table_id; END IF;

  ELSIF p.proposal_type = 'ADD_COLUMN' THEN
    INSERT INTO aidq_metadata.source_columns (table_id, column_name, ordinal, data_type, is_active)
    VALUES (p.table_id, c->>'column_name',
            COALESCE((c->>'ordinal')::int,
                     (SELECT COALESCE(max(ordinal), 0) + 1 FROM aidq_metadata.source_columns
                       WHERE table_id = p.table_id)),
            c->>'data_type', true);
  END IF;

  UPDATE aidq_metadata.config_proposals
     SET status = 'APPLIED', applied_at = now() WHERE proposal_id = p_id;
  RETURN format('Proposal %s (%s) applied by %s', p_id, p.proposal_type, p_actor);
END $$;

-- New functions are executable by PUBLIC by default. Only roles that are granted EXECUTE
-- explicitly (see the optional block at the end) may apply proposals.
REVOKE EXECUTE ON FUNCTION aidq_metadata.apply_proposal(bigint, text) FROM PUBLIC;

-- -----------------------------------------------------------------------------
-- 5. Incidents: output of the root-cause agent
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS incidents (
  incident_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_id             text,
  table_id           int REFERENCES source_table_def (table_id),
  layer              text,
  category           text CHECK (category IN (
                       'BAD_RULE_EXPR', 'SCHEMA_DRIFT', 'REJECT_THRESHOLD',
                       'SOURCE_UNAVAILABLE', 'CREDENTIAL_EXPIRED', 'DATA_VOLUME', 'OTHER')),
  summary            text NOT NULL,
  suggested_fix      text,
  linked_proposal_id bigint REFERENCES config_proposals (proposal_id),
  agent_trace_id     text,
  status             text NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')),
  detected_at        timestamptz NOT NULL DEFAULT now(),
  resolved_at        timestamptz
);

-- -----------------------------------------------------------------------------
-- 6. Health view: what the monitoring agent reads first
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_table_health AS
WITH latest AS (
  SELECT DISTINCT ON (table_id, layer) *
    FROM run_audit
   WHERE table_id IS NOT NULL
   ORDER BY table_id, layer, started_at DESC NULLS LAST, audit_id DESC
)
SELECT d.table_id, d.source_table, l.layer, l.run_id, l.status, l.error,
       l.rows_read, l.rows_written, l.rows_rejected,
       -- layer 'ledger' (ledger_check) stores ledger pairs in rows_read and mismatches in rows_rejected:
       -- status/error are meaningful for it, a reject rate and threshold are not.
       CASE WHEN l.layer <> 'ledger'
            THEN round(l.rows_rejected::numeric / NULLIF(l.rows_read, 0), 4) END AS reject_rate,
       d.discard_threshold,
       CASE WHEN l.layer <> 'ledger'
            THEN (d.discard_threshold IS NOT NULL
                  AND l.rows_rejected::numeric / NULLIF(l.rows_read, 0) > d.discard_threshold) END AS threshold_breached,
       (SELECT count(*) FROM data_quality_rules r
         WHERE r.table_id = d.table_id AND r.is_active)            AS active_rules,
       (SELECT count(*) FROM config_proposals p
         WHERE p.table_id = d.table_id AND p.status IN ('PENDING', 'VALIDATED')) AS open_proposals,
       l.started_at, l.ended_at
  FROM source_table_def d
  JOIN latest l USING (table_id);


-- =============================================================================
-- OPTIONAL (run separately): least-privilege roles.
-- On Lakebase, grant these group roles to the Postgres roles that map to your
-- agent's service principal and to human reviewers.
-- =============================================================================
-- CREATE ROLE aidq_agent NOLOGIN;
-- CREATE ROLE aidq_reviewer NOLOGIN;
-- GRANT USAGE ON SCHEMA aidq_metadata TO aidq_agent, aidq_reviewer;
-- GRANT SELECT ON ALL TABLES IN SCHEMA aidq_metadata TO aidq_agent, aidq_reviewer;
-- GRANT INSERT ON aidq_metadata.config_proposals, aidq_metadata.incidents TO aidq_agent;
-- -- The validator (an automated step) may mark PENDING -> VALIDATED / VALIDATION_FAILED;
-- -- the lifecycle trigger blocks it from anything else.
-- GRANT UPDATE (status, validation_result) ON aidq_metadata.config_proposals TO aidq_agent;
-- GRANT UPDATE (status, reviewed_by, reviewed_at, review_comment)
--   ON aidq_metadata.config_proposals TO aidq_reviewer;
-- GRANT EXECUTE ON FUNCTION aidq_metadata.apply_proposal(bigint, text) TO aidq_reviewer;
-- (EXECUTE was already revoked from PUBLIC above; the grant to reviewers is the only one needed.)
