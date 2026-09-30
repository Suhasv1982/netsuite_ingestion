-- =============================================================================
-- aidq_metadata  |  Migration 003: blank watermark_col -> NULL
-- Requires: nothing (source_table_def exists since the base schema)
--
-- A blank watermark_col has meant "FullLoad". From now on that is spelled NULL,
-- and a CHECK constraint keeps blank strings out. The pipeline already treats
-- blank and NULL the same (metadata._blank_to_none in read_table_defs), so this
-- migration can run before or after the code that expects it.
-- Runs inside the migration runner's transaction (tools/migrate.py): no BEGIN/COMMIT.
-- =============================================================================
SET LOCAL search_path = aidq_metadata;

UPDATE source_table_def
   SET watermark_col = NULL
 WHERE watermark_col IS NOT NULL AND btrim(watermark_col) = '';

ALTER TABLE source_table_def DROP CONSTRAINT IF EXISTS source_table_def_watermark_not_blank;
ALTER TABLE source_table_def ADD CONSTRAINT source_table_def_watermark_not_blank
  CHECK (watermark_col IS NULL OR btrim(watermark_col) <> '');
