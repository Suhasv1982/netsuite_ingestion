-- =============================================================================
-- aidq_metadata  |  Migration 004: no PUBLIC EXECUTE on the trigger functions
-- Requires: 001_agent_governance.sql (creates both functions)
--
-- Postgres grants EXECUTE on new functions to PUBLIC. These two are trigger
-- functions: nobody needs to call them directly, and Postgres does not check
-- EXECUTE when a trigger fires (only when a trigger is created), so revoking it
-- leaves the audit and lifecycle triggers working. aidq_owner keeps EXECUTE as owner.
-- Runs inside the migration runner's transaction (tools/migrate.py): no BEGIN/COMMIT.
-- =============================================================================
SET LOCAL search_path = aidq_metadata;

REVOKE EXECUTE ON FUNCTION aidq_metadata.enforce_proposal_lifecycle() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION aidq_metadata.log_config_change() FROM PUBLIC;
