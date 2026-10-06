-- =============================================================================
-- aidq_metadata  |  Migration 005: incident categories, fingerprint, evidence
-- Requires: 001_agent_governance.sql (incidents), 002_incident_compute_quota.sql
--
-- For the Phase 4 monitor / RCA agent (docs/phase4_agent_plan.md):
--   * categories ORCHESTRATION (a scheduled run that did not start, incident
--     2026-10-06) and DATA_COMPLETENESS (rows missing between source and bronze,
--     incident 2026-10-02);
--   * fingerprint: stable id of the signal an incident is about; at most one
--     OPEN incident per fingerprint, so a daily re-run cannot duplicate it;
--   * evidence: the agent's structured findings (tool, finding) as JSON.
-- Additive: no existing column or row changes. Runs inside the migration
-- runner's transaction (tools/migrate.py): no BEGIN/COMMIT.
-- =============================================================================
SET LOCAL search_path = aidq_metadata;

ALTER TABLE incidents DROP CONSTRAINT IF EXISTS incidents_category_check;
ALTER TABLE incidents ADD CONSTRAINT incidents_category_check CHECK (category IN (
  'BAD_RULE_EXPR', 'SCHEMA_DRIFT', 'REJECT_THRESHOLD', 'SOURCE_UNAVAILABLE',
  'CREDENTIAL_EXPIRED', 'DATA_VOLUME', 'COMPUTE_QUOTA', 'ORCHESTRATION',
  'DATA_COMPLETENESS', 'OTHER'));

ALTER TABLE incidents ADD COLUMN IF NOT EXISTS fingerprint text;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS evidence jsonb;
ALTER TABLE incidents DROP CONSTRAINT IF EXISTS incidents_fingerprint_not_blank;
ALTER TABLE incidents ADD CONSTRAINT incidents_fingerprint_not_blank CHECK (fingerprint IS NULL OR btrim(fingerprint) <> '');

CREATE UNIQUE INDEX IF NOT EXISTS incidents_open_fingerprint_uq
  ON incidents (fingerprint) WHERE status = 'OPEN' AND fingerprint IS NOT NULL;
