-- =============================================================================
-- aidq_metadata  |  Migration 006: governance for the DQ rule recommender
-- Requires: 001_agent_governance.sql, 005_incident_categories_fingerprint.sql
-- Design: docs/dq_recommender_design_v2.md, section 3 ("Schema changes")
--
-- Schema only, identical in dev and prod: no roles, no grants (those are
-- environment configuration in grants/<target>.yml and the owner's role script).
--   * config_proposals.approved_severity: the reviewer's severity decision
--     (HARD/SOFT, nullable). proposed_change stays immutable, so the audit trail
--     keeps both the agent's proposal and the reviewer's decision.
--   * Lifecycle trigger:
--       - new proposals carry no validation_result and no approved_severity;
--       - validation_result changes only together with
--         PENDING -> VALIDATED / VALIDATION_FAILED;
--       - approved_severity changes only together with VALIDATED -> APPROVED.
--   * apply_proposal(): severity = COALESCE(approved_severity,
--     proposed_change->>'severity', 'SOFT').
--   * Incident category DQ_NOT_EXPRESSIBLE: a finding a row-level rule cannot
--     express (duplicates, orphans, late rows).
-- Runs inside the migration runner's transaction (tools/migrate.py): no BEGIN/COMMIT.
-- =============================================================================
SET LOCAL search_path = aidq_metadata;

ALTER TABLE config_proposals ADD COLUMN IF NOT EXISTS approved_severity text;
ALTER TABLE config_proposals DROP CONSTRAINT IF EXISTS config_proposals_approved_severity_chk;
ALTER TABLE config_proposals ADD CONSTRAINT config_proposals_approved_severity_chk
  CHECK (approved_severity IS NULL OR approved_severity IN ('HARD', 'SOFT'));

CREATE OR REPLACE FUNCTION enforce_proposal_lifecycle() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.status <> 'PENDING' OR NEW.reviewed_by IS NOT NULL OR NEW.applied_at IS NOT NULL
       OR NEW.validation_result IS NOT NULL OR NEW.approved_severity IS NOT NULL THEN
      RAISE EXCEPTION 'New proposals must be PENDING, unreviewed and unvalidated';
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
  IF NEW.validation_result IS DISTINCT FROM OLD.validation_result AND NOT (
       OLD.status = 'PENDING' AND NEW.status IN ('VALIDATED', 'VALIDATION_FAILED')) THEN
    RAISE EXCEPTION 'validation_result of proposal % can only be set with PENDING -> VALIDATED / VALIDATION_FAILED',
      OLD.proposal_id;
  END IF;
  IF NEW.approved_severity IS DISTINCT FROM OLD.approved_severity AND NOT (
       OLD.status = 'VALIDATED' AND NEW.status = 'APPROVED') THEN
    RAISE EXCEPTION 'approved_severity of proposal % can only be set with VALIDATED -> APPROVED', OLD.proposal_id;
  END IF;
  RETURN NEW;
END $$;

-- Same body as in 001 except the severity line.
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
            COALESCE(p.approved_severity, c->>'severity', 'SOFT'), p.proposed_by, p_id);

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

-- CREATE OR REPLACE keeps existing privileges; restate the 001 / 004 revokes so the file stands on its own.
REVOKE EXECUTE ON FUNCTION apply_proposal(bigint, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION enforce_proposal_lifecycle() FROM PUBLIC;

ALTER TABLE incidents DROP CONSTRAINT IF EXISTS incidents_category_check;
ALTER TABLE incidents ADD CONSTRAINT incidents_category_check CHECK (category IN (
  'BAD_RULE_EXPR', 'SCHEMA_DRIFT', 'REJECT_THRESHOLD', 'SOURCE_UNAVAILABLE',
  'CREDENTIAL_EXPIRED', 'DATA_VOLUME', 'COMPUTE_QUOTA', 'ORCHESTRATION',
  'DATA_COMPLETENESS', 'DQ_NOT_EXPRESSIBLE', 'OTHER'));
