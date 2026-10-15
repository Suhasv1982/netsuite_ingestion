-- =============================================================================
-- aidq_metadata  |  Migration 008: shared foundation for the next agents
-- Requires: 001 .. 007
-- Design: docs/agents_next_design.md, section 1 ("Schema changes"), 3.5, 5.5
-- Owner approval 2026-10-15: schema only.
--
-- Schema only, identical in dev and prod: no roles, no grants (environment configuration in grants/<target>.yml).
--   * config_proposals.proposal_type: + DEACTIVATE_COLUMN (schema drift agent), + ONBOARD_TABLE (onboarding agent).
--   * config_proposals.table_id: may be NULL only for ONBOARD_TABLE (the table does not exist yet); every other
--     type still needs it (CHECK).
--   * config_proposals.requires_rebuild: set by the validator when approval changes an incremental table's hashed
--     column set (the bronze guard then fails closed until a rebuild). Like validation_result, it can only change
--     together with PENDING -> VALIDATED / VALIDATION_FAILED (lifecycle trigger).
--   * incidents.category: + SCHEMA_TYPE_CHANGE (a type change is reported, never auto-proposed).
--   * apply_proposal():
--       DEACTIVATE_COLUMN sets is_active = false (the row stays: history); refuses the business key and the
--         watermark column, and a column that is not active;
--       ONBOARD_TABLE inserts the source_table_def row and all its source_columns rows in this one transaction;
--         refuses a table that is already defined and an empty column list. proposed_change:
--         {"table": {source_table, dest_catalog, dest_schema, business_key, watermark_col, load_mode, scd_type,
--                    business_area_id, discard_threshold},
--          "columns": [{"column_name", "data_type", "ordinal"}, ...]}
--   * Change log: inserts into source_table_def are logged too (the onboarded row carries its proposal_id there).
--   * source_table_def's id sequence is moved past the highest existing table_id (the first rows were inserted with
--     explicit ids, so on dev the next id collided with table 1: found by the 008 integration test). It only ever
--     moves forward; no row changes.
-- Runs inside the migration runner's transaction (tools/migrate.py): no BEGIN/COMMIT.
-- =============================================================================
SET LOCAL search_path = aidq_metadata;

ALTER TABLE config_proposals DROP CONSTRAINT IF EXISTS config_proposals_proposal_type_check;
ALTER TABLE config_proposals ADD CONSTRAINT config_proposals_proposal_type_check CHECK (proposal_type IN (
  'ADD_DQ_RULE', 'DEACTIVATE_DQ_RULE', 'ADD_COLUMN', 'DEACTIVATE_COLUMN', 'ONBOARD_TABLE'));

ALTER TABLE config_proposals ALTER COLUMN table_id DROP NOT NULL;
ALTER TABLE config_proposals DROP CONSTRAINT IF EXISTS config_proposals_table_id_chk;
ALTER TABLE config_proposals ADD CONSTRAINT config_proposals_table_id_chk
  CHECK (table_id IS NOT NULL OR proposal_type = 'ONBOARD_TABLE');

ALTER TABLE config_proposals ADD COLUMN IF NOT EXISTS requires_rebuild boolean;

ALTER TABLE incidents DROP CONSTRAINT IF EXISTS incidents_category_check;
ALTER TABLE incidents ADD CONSTRAINT incidents_category_check CHECK (category IN (
  'BAD_RULE_EXPR', 'SCHEMA_DRIFT', 'REJECT_THRESHOLD', 'SOURCE_UNAVAILABLE',
  'CREDENTIAL_EXPIRED', 'DATA_VOLUME', 'COMPUTE_QUOTA', 'ORCHESTRATION',
  'DATA_COMPLETENESS', 'DQ_NOT_EXPRESSIBLE', 'SCHEMA_TYPE_CHANGE', 'OTHER'));

-- Lifecycle trigger: as in 006, plus requires_rebuild (new proposals carry none; it changes only with validation).
CREATE OR REPLACE FUNCTION enforce_proposal_lifecycle() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.status <> 'PENDING' OR NEW.reviewed_by IS NOT NULL OR NEW.applied_at IS NOT NULL
       OR NEW.validation_result IS NOT NULL OR NEW.approved_severity IS NOT NULL
       OR NEW.requires_rebuild IS NOT NULL THEN
      RAISE EXCEPTION 'New proposals must be PENDING, unreviewed and unvalidated';
    END IF;
    RETURN NEW;
  END IF;
  IF NEW.proposed_change IS DISTINCT FROM OLD.proposed_change
     OR NEW.proposed_by IS DISTINCT FROM OLD.proposed_by
     OR NEW.table_id IS DISTINCT FROM OLD.table_id
     OR NEW.proposal_type IS DISTINCT FROM OLD.proposal_type THEN
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
  IF (NEW.validation_result IS DISTINCT FROM OLD.validation_result
      OR NEW.requires_rebuild IS DISTINCT FROM OLD.requires_rebuild) AND NOT (
       OLD.status = 'PENDING' AND NEW.status IN ('VALIDATED', 'VALIDATION_FAILED')) THEN
    RAISE EXCEPTION 'validation_result / requires_rebuild of proposal % can only be set with PENDING -> VALIDATED / VALIDATION_FAILED',
      OLD.proposal_id;
  END IF;
  IF NEW.approved_severity IS DISTINCT FROM OLD.approved_severity AND NOT (
       OLD.status = 'VALIDATED' AND NEW.status = 'APPROVED') THEN
    RAISE EXCEPTION 'approved_severity of proposal % can only be set with VALIDATED -> APPROVED', OLD.proposal_id;
  END IF;
  RETURN NEW;
END $$;

-- Same body as in 006 plus the DEACTIVATE_COLUMN and ONBOARD_TABLE branches.
CREATE OR REPLACE FUNCTION apply_proposal(p_id bigint, p_actor text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER
SET search_path = aidq_metadata, pg_temp AS $$
DECLARE
  p aidq_metadata.config_proposals%ROWTYPE;
  c jsonb;
  t jsonb;
  d aidq_metadata.source_table_def%ROWTYPE;
  new_table_id int;
  col jsonb;
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

  ELSIF p.proposal_type = 'DEACTIVATE_COLUMN' THEN
    SELECT * INTO d FROM aidq_metadata.source_table_def WHERE table_id = p.table_id;
    IF c->>'column_name' IS NULL OR c->>'column_name' IN (d.business_key, d.watermark_col) THEN
      RAISE EXCEPTION 'Column % of table % cannot be deactivated (business key or watermark column)',
        c->>'column_name', p.table_id;
    END IF;
    UPDATE aidq_metadata.source_columns SET is_active = false
     WHERE table_id = p.table_id AND column_name = c->>'column_name' AND COALESCE(is_active, true);
    IF NOT FOUND THEN
      RAISE EXCEPTION 'Column % is not an active column of table %', c->>'column_name', p.table_id;
    END IF;

  ELSIF p.proposal_type = 'ONBOARD_TABLE' THEN
    t := c->'table';
    IF t IS NULL OR t->>'source_table' IS NULL OR t->>'business_key' IS NULL
       OR jsonb_typeof(c->'columns') <> 'array' OR jsonb_array_length(c->'columns') = 0 THEN
      RAISE EXCEPTION 'Proposal % needs table.source_table, table.business_key and a non-empty columns list', p_id;
    END IF;
    IF EXISTS (SELECT 1 FROM aidq_metadata.source_table_def WHERE source_table = t->>'source_table') THEN
      RAISE EXCEPTION 'Table % is already defined', t->>'source_table';
    END IF;
    INSERT INTO aidq_metadata.source_table_def
      (business_area_id, source_table, dest_catalog, dest_schema, business_key, watermark_col, load_mode,
       scd_type, discard_threshold)
    VALUES ((t->>'business_area_id')::int, t->>'source_table', t->>'dest_catalog', t->>'dest_schema',
            t->>'business_key', NULLIF(btrim(COALESCE(t->>'watermark_col', '')), ''), t->>'load_mode',
            t->>'scd_type', COALESCE((t->>'discard_threshold')::numeric, 0.05))
    RETURNING table_id INTO new_table_id;
    FOR col IN SELECT * FROM jsonb_array_elements(c->'columns') LOOP
      INSERT INTO aidq_metadata.source_columns (table_id, column_name, ordinal, data_type, is_active)
      VALUES (new_table_id, col->>'column_name', (col->>'ordinal')::int, col->>'data_type', true);
    END LOOP;
  END IF;

  UPDATE aidq_metadata.config_proposals
     SET status = 'APPLIED', applied_at = now() WHERE proposal_id = p_id;
  RETURN format('Proposal %s (%s) applied by %s', p_id, p.proposal_type, p_actor);
END $$;

-- CREATE OR REPLACE keeps existing privileges; restate the 001 / 004 revokes so the file stands on its own.
REVOKE EXECUTE ON FUNCTION apply_proposal(bigint, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION enforce_proposal_lifecycle() FROM PUBLIC;

-- The id sequence must hand out ids after the existing rows (ONBOARD_TABLE inserts without an explicit id).
DO $$
DECLARE
  seq text := pg_get_serial_sequence('aidq_metadata.source_table_def', 'table_id');
  next_free bigint;
  seq_next bigint;
BEGIN
  IF seq IS NOT NULL THEN
    SELECT COALESCE(max(table_id), 0) + 1 INTO next_free FROM aidq_metadata.source_table_def;
    EXECUTE format('SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END FROM %s', seq) INTO seq_next;
    IF seq_next < next_free THEN
      PERFORM setval(seq, next_free, false);
    END IF;
  END IF;
END $$;

-- Log inserts into source_table_def too (ONBOARD_TABLE), not only configuration updates.
DROP TRIGGER IF EXISTS trg_log_source_table_def ON source_table_def;
CREATE TRIGGER trg_log_source_table_def AFTER INSERT OR UPDATE OF
  business_key, watermark_col, load_mode, scd_type, dest_schema, discard_threshold
  ON source_table_def FOR EACH ROW EXECUTE FUNCTION log_config_change();
