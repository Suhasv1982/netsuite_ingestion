-- Remove true duplicates from the shared source (netsuite-sample/production, schema netsuite).
-- Plan and approval: docs/source_duplicate_cleanup_plan.md. Run by the OWNER (data-generator has no DELETE),
-- only after the Lakebase backup branch exists and nothing else is writing (not 05:00-05:15 UTC).
--
-- Rule: per incremental table and (business key, updated_date day), keep the row written first (lowest xmin, then
-- ctid) and delete every other row: (b) exact copies and (c) same key and day with different content. Versions on
-- other days (a) and NULL-key rows are not touched. One transaction: every count is checked against the dry run of
-- 2026-10-26, and any difference raises an error, which aborts the transaction (nothing is deleted).

BEGIN;

LOCK TABLE netsuite.netsuite_memberships, netsuite.netsuite_certifications, netsuite.netsuite_transactions,
           netsuite.netsuite_transaction_lines IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE cleanup_targets ON COMMIT DROP AS
WITH r AS (
    SELECT 'netsuite_memberships'::text AS tbl, ctid AS row_ctid, membership_internal_id::text AS k,
           updated_date::date AS d, md5(row_to_json(s)::text) AS h,
           row_number() OVER w AS rn, first_value(md5(row_to_json(s)::text)) OVER w AS keep_h
    FROM netsuite.netsuite_memberships s WHERE membership_internal_id IS NOT NULL
    WINDOW w AS (PARTITION BY membership_internal_id, updated_date::date ORDER BY xmin::text::bigint, ctid)
    UNION ALL
    SELECT 'netsuite_certifications', ctid, certification_internal_id::text, updated_date::date, md5(row_to_json(s)::text),
           row_number() OVER w, first_value(md5(row_to_json(s)::text)) OVER w
    FROM netsuite.netsuite_certifications s WHERE certification_internal_id IS NOT NULL
    WINDOW w AS (PARTITION BY certification_internal_id, updated_date::date ORDER BY xmin::text::bigint, ctid)
    UNION ALL
    SELECT 'netsuite_transactions', ctid, transaction_internal_id::text, updated_date::date, md5(row_to_json(s)::text),
           row_number() OVER w, first_value(md5(row_to_json(s)::text)) OVER w
    FROM netsuite.netsuite_transactions s WHERE transaction_internal_id IS NOT NULL
    WINDOW w AS (PARTITION BY transaction_internal_id, updated_date::date ORDER BY xmin::text::bigint, ctid)
    UNION ALL
    SELECT 'netsuite_transaction_lines', ctid, transaction_line_id::text, updated_date::date, md5(row_to_json(s)::text),
           row_number() OVER w, first_value(md5(row_to_json(s)::text)) OVER w
    FROM netsuite.netsuite_transaction_lines s WHERE transaction_line_id IS NOT NULL
    WINDOW w AS (PARTITION BY transaction_line_id, updated_date::date ORDER BY xmin::text::bigint, ctid)
)
SELECT tbl, row_ctid, k, d, CASE WHEN h = keep_h THEN 'b_exact' ELSE 'c_diff' END AS kind
FROM r WHERE rn > 1;

-- 1. The selection must equal the approved dry run exactly (716 rows).
DO $$
DECLARE
    expected text := 'netsuite_certifications|2026-07-11|c_diff|19,netsuite_certifications|2026-10-02|c_diff|5,'
                     'netsuite_certifications|2026-10-03|c_diff|1,netsuite_memberships|2026-07-11|c_diff|24,'
                     'netsuite_memberships|2026-10-02|c_diff|11,netsuite_memberships|2026-10-03|c_diff|13,'
                     'netsuite_transaction_lines|2026-07-11|c_diff|288,netsuite_transaction_lines|2026-10-02|b_exact|11,'
                     'netsuite_transaction_lines|2026-10-02|c_diff|79,netsuite_transaction_lines|2026-10-03|b_exact|10,'
                     'netsuite_transaction_lines|2026-10-03|c_diff|94,netsuite_transactions|2026-07-11|c_diff|98,'
                     'netsuite_transactions|2026-10-02|c_diff|34,netsuite_transactions|2026-10-03|c_diff|29';
    actual text;
BEGIN
    SELECT string_agg(tbl || '|' || d || '|' || kind || '|' || n, ',' ORDER BY tbl, d, kind) INTO actual
    FROM (SELECT tbl, d, kind, count(*) AS n FROM cleanup_targets GROUP BY 1, 2, 3) x;
    IF actual IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'selection differs from the approved dry run; nothing deleted. actual: %', actual;
    END IF;
END $$;

DELETE FROM netsuite.netsuite_memberships t USING cleanup_targets c
 WHERE c.tbl = 'netsuite_memberships' AND t.ctid = c.row_ctid;
DELETE FROM netsuite.netsuite_certifications t USING cleanup_targets c
 WHERE c.tbl = 'netsuite_certifications' AND t.ctid = c.row_ctid;
DELETE FROM netsuite.netsuite_transactions t USING cleanup_targets c
 WHERE c.tbl = 'netsuite_transactions' AND t.ctid = c.row_ctid;
DELETE FROM netsuite.netsuite_transaction_lines t USING cleanup_targets c
 WHERE c.tbl = 'netsuite_transaction_lines' AND t.ctid = c.row_ctid;

-- 2. Afterwards: no (key, updated_date day) held twice in any incremental table.
DO $$
DECLARE
    left_over bigint;
BEGIN
    SELECT (SELECT count(*) FROM (SELECT 1 FROM netsuite.netsuite_memberships WHERE membership_internal_id IS NOT NULL
                GROUP BY membership_internal_id, updated_date::date HAVING count(*) > 1) a)
         + (SELECT count(*) FROM (SELECT 1 FROM netsuite.netsuite_certifications WHERE certification_internal_id IS NOT NULL
                GROUP BY certification_internal_id, updated_date::date HAVING count(*) > 1) b)
         + (SELECT count(*) FROM (SELECT 1 FROM netsuite.netsuite_transactions WHERE transaction_internal_id IS NOT NULL
                GROUP BY transaction_internal_id, updated_date::date HAVING count(*) > 1) c)
         + (SELECT count(*) FROM (SELECT 1 FROM netsuite.netsuite_transaction_lines WHERE transaction_line_id IS NOT NULL
                GROUP BY transaction_line_id, updated_date::date HAVING count(*) > 1) d)
      INTO left_over;
    IF left_over <> 0 THEN
        RAISE EXCEPTION '% duplicate (key, day) groups remain; rolling back', left_over;
    END IF;
    RAISE NOTICE 'OK: 716 rows deleted (memberships 48, certifications 25, transactions 161, lines 482); no duplicate (key, day) left';
END $$;

COMMIT;
