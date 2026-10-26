# Plan: remove true duplicates from the source (incidents 7, 9, 11, 13)

Status: **plan approved by the owner on 2026-10-26**; runs after the quota reset (section 4). Dev only; prod stays on hold. Nothing has been
deleted or rebuilt. The generator fix in the same PR is unit-tested only (the Free Edition quota is exhausted).

## 1. Findings (read-only, netsuite-sample/production, 2026-10-26)

Duplicate keys classified per incremental table and `updated_date` day (the source has no batch column; each batch
writes its rows on its own `updated_date` day):

| Table | (a) versions: keys with >1 day (extra versions) | (b) exact copies | (c) same key and day, different content | (b)+(c) to delete |
|---|---|---|---|---|
| memberships | 3,563 (7,296) | 0 | 07-11: 24 · 10-02: 11 · 10-03: 13 | 48 |
| certifications | 2,875 (5,854) | 0 | 07-11: 19 · 10-02: 5 · 10-03: 1 | 25 |
| transactions | 14,279 (29,237) | 0 | 07-11: 98 · 10-02: 34 · 10-03: 29 | 161 |
| transaction_lines | 42,870 (87,706) | 10-02: 11 · 10-03: 10 | 07-11: 288 · 10-02: 79 · 10-03: 94 | 482 |
| **total** | kept | **21** | **695** | **716** |

No (key, day) holds more than 2 rows. Nothing after 10-03. NULL-key rows (25 / 20 / 99 / 291, the
`null_business_key` defect) are not duplicates and are not touched.

**Two causes, neither active in the daily runs:**

1. **07-11: the `duplicate_business_key` defect** of the base load (`--init --seed 42 --config
   defects_baseline.yaml`, rate 0.01). The counts equal its manifest exactly (24 / 19 / 98 / 288 = incidents 7, 9,
   11, 13) and every copy differs from its original only in `created_date` (one day earlier), as the defect writes
   it. Deliberate at the time, not a bug. The daily config (`tools/increment_daily.yaml`) has this rate at 0.
2. **10-02 / 10-03: incident 1 of 2026-10-06.** Generator run 2 (10-01) wrote versions dated up to two days ahead;
   the 10-02 / 10-03 batches (`--allow-before-watermark`) then wrote a second version of 287 of those keys on the
   same day. In lines, 21 of them got the same random quantity, so they are exact copies (b). This was a generator
   bug, fixed on 10-06 (#27: a version is never written on a (key, day) the source already holds). Each group's two
   rows come from two different write transactions.

Examples (key, day): memberships 5040, 5233, 5360 on 07-11; lines 90025_5, 90072_3, 90092_3 on 07-11. Full list
(git-ignored): `baseline/raw/cleanup_20261026_rows_to_delete.json`.

## 2. Generator fix (this PR)

So (b) and (c) cannot be written again, from any path:

* `duplicate_business_key` must be 0: a config with a higher rate is refused (`DUPLICATES_REMOVED`). The name stays
  known, so old configs and manifests still parse; the injection code is gone. `baseline/defects_baseline.yaml` is
  kept as a historical record and no longer loads.
* A fail-closed guard (`check_no_duplicate_versions`) runs on every generated batch, before anything is written:
  no (key, `updated_date` day) twice within the batch or next to a row the source already holds. Exact copies
  included; NULL keys exempt.
* Tests: the guard refuses both historical cases (the 07-11 copy, the 10-02/03 second version), allows versions on
  other days and NULL keys; init and increment with every other defect on produce no duplicate.

## 3. Cleanup (for the owner's approval; the owner runs it)

Rule: per (key, day) keep the row written first (lowest `xmin`, then `ctid`) and delete the other. On 07-11 that
deletes exactly the injected copies (`created_date` 07-10); on 10-02 / 10-03 it keeps the 10-01 row, which is what
the fixed generator would have done (skip a version on a day the source already holds).

1. **Backup branch first** (the project has 5 of 10 branches):
   ```
   databricks postgres create-branch projects/netsuite-sample pre-cleanup-backup-20261026 --profile DEFAULT \
     --json '{"spec": {"source_branch": "projects/netsuite-sample/branches/production", "no_expiry": true}}'
   ```
2. **Run** `tools/sql/cleanup_source_duplicates_20261026.sql` as the owner (data-generator has no DELETE), outside
   05:00-05:15 UTC. One transaction: it locks the four tables, selects the rows, **refuses unless the selection equals
   this dry run per table, day and kind (716 rows)**, deletes by `ctid`, then refuses unless no duplicate (key, day)
   remains. Either way to run it:
   ```
   databricks psql --project netsuite-sample --branch production --profile DEFAULT -- -d databricks_postgres -v ON_ERROR_STOP=1 -f tools/sql/cleanup_source_duplicates_20261026.sql
   ```
   (needs psql), or from the repo with the venv:
   ```
   .venv/Scripts/python -c "import sys; sys.path.insert(0, 'tools'); import pg_writer; c = pg_writer.connect('DEFAULT'); c.autocommit = True; c.execute(open('tools/sql/cleanup_source_duplicates_20261026.sql').read()); print('done')"
   ```
   The selection part was checked read-only on 2026-10-26: it matches the expected counts exactly. The deletes have
   not been executed.

## 4. After the quota reset (owner order)

1. Merge this PR alone; wait for its deploy-dev.
2. Backup branch, then the cleanup (section 3).
3. Dev bronze rebuild: `gh workflow run deploy-dev.yml -f bronze_rebuild=true` (bronze still holds the deleted
   rows; a normal run never removes rows).
4. `python tools/daily_check.py --date <that day> --profile DEFAULT` must PASS (bronze = source per table and day).
5. Resolve incidents 7, 9, 11, 13 with this plan as the resolution.

## 5. Eval ground truth and manifests (after step 4.4)

* **Manifests:** the 07-11 `duplicate_business_key` rows (429) of `workspace.generator.manifests` describe rows that
  no longer exist. Proposal: keep history, add a removal marker (a `cleanup` batch or a `removed_at` column, decided
  in that PR), and teach `verify_manifest.py` to skip removed rows.
* **DQ eval:** `evals/run_dq_eval.py` requires duplicates to be reported as DQ_NOT_EXPRESSIBLE at least once. With no
  duplicates left that target cannot pass: drop it from the coverage target until set-level checks exist (below).
  Orphans and late rows stay.

## 6. Not in this plan

* **Orphan lines (incident 14):** 584 line ids, all from the 07-11 base load: 294 are the `orphan_lines` defect
  (its manifest: 294) and 290 are lines of the 99 transactions whose key the `null_business_key` defect nulled
  (manifest: 99). Known defects, not a bug. The source holds 1,739 orphan rows because the daily runs write new
  versions of those lines (1,340 through 10-14 = the incident's count). The 290 are not in any manifest (a side
  effect of a recorded defect).
* **Set-level checks** (uniqueness, referential integrity) for real sources: after onboarding, if the owner agrees.
* Prod.
