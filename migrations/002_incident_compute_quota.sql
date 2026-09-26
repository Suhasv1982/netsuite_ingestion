-- =============================================================================
-- aidq_metadata  |  Migration 002: COMPUTE_QUOTA incident category
-- Requires: 001_agent_governance.sql
--
-- Adds COMPUTE_QUOTA for failures caused by platform compute limits
-- (e.g. RESOURCE_EXHAUSTED on Free Edition serverless when a SQL warehouse,
-- a pipeline update and a job run at the same time).
-- =============================================================================
BEGIN;
SET search_path = aidq_metadata;

ALTER TABLE incidents DROP CONSTRAINT IF EXISTS incidents_category_check;
ALTER TABLE incidents ADD CONSTRAINT incidents_category_check CHECK (category IN (
  'BAD_RULE_EXPR', 'SCHEMA_DRIFT', 'REJECT_THRESHOLD', 'SOURCE_UNAVAILABLE',
  'CREDENTIAL_EXPIRED', 'DATA_VOLUME', 'COMPUTE_QUOTA', 'OTHER'));

COMMIT;
