# Before-agents baseline report

Pipeline: `netsuite_ingestion_poc` (prod bundle, job `netsuite_ingestion_daily`, catalog `poc_netsuite`), source: Lakebase `netsuite-sample` schema `netsuite`, metadata: Lakebase `aidq-metadata`. No agents were running. Every count below is computed from the saved snapshots in `baseline/` (see section 8).

## Summary

- Step A (clean data, full refresh) and step B (2,005 injected defect rows, full refresh) both succeeded. Step C (increment with normal updates, 223 late-arriving rows and the drift column, normal run) **failed**: all four silver flows stopped with "streaming sources added or removed", so silver is unchanged after it.
- Step B outcomes for the injected rows: 54 rejected, 1,031 passed to silver, 860 silently absorbed, 0 caused failure, 60 customers rows bronze-only.
- Only membership status, certification type and the certification start-date cutoff produce rejections. Negative amounts, amount mismatches, orphan lines and invalid transaction types pass through untouched; duplicate keys and NULL keys are merged away with no error (4 NULL-key rows remain in silver).
- Source, bronze and silver reconcile with 0 unexplained rows for every table in steps B and C (section 4).
- Findings 1 to 9 (section 5) cover the `run_audit` over-count, the incremental-run failure, the customers date columns dropped by bronze, and the drift column.

## 1. What was run

| Step | Data change | Backup taken before it (schema in `netsuite-sample`) | Job run id | Job result | Pipeline update |
|---|---|---|--:|---|---|
| A. Clean, full refresh | `--init --seed 42`, no defects | netsuite_backup_202609242053 | 306754157382911 | SUCCESS | d585c6cf… COMPLETED |
| B. Defects, full refresh | `--init --seed 42 --config defects_baseline.yaml` (2,005 defect rows) | netsuite_backup_202609251523 | 8535844756042 | SUCCESS | f242c30b… COMPLETED |
| C. Increment, normal run | `--increment --seed 43 --batch-date 2026-08-22 --apply-schema-drift` (late_arriving 10%) | netsuite_backup_202609251530 | 346077225702715 | SUCCESS_WITH_FAILURES | 342850e9… FAILED |

Steps A and B ran with `full_refresh: true` (the source data was replaced). Step C ran the job normally, with no full refresh. Each backup is a verified copy of the five tables plus a Lakebase branch `pre-synthetic-backup-<timestamp>`.

## 2. Headline numbers

| Step | Source rows (4 incremental tables) | Silver rows | Rows in `rejected_rows` | Pipeline failed |
|---|--:|--:|--:|---|
| A. Clean | 44,500 | 44,500 | 0 | no |
| B. Defects, full refresh | 44,929 | 44,015 | 54 | no |
| C. Increment, normal run | 51,559 | 44,015 | 57 | yes |

Per-table row counts:

| Table | A source | A silver | B source | B silver | C source | C silver |
|---|--:|--:|--:|--:|--:|--:|
| customers | 2,000 | - | 2,000 | - | 2,100 | - |
| memberships | 2,500 | 2,500 | 2,524 | 2,451 | 2,896 | 2,451 |
| certifications | 2,000 | 2,000 | 2,019 | 1,952 | 2,317 | 1,952 |
| transactions | 10,000 | 10,000 | 10,098 | 9,902 | 11,588 | 9,902 |
| transaction_lines | 30,000 | 30,000 | 30,288 | 29,710 | 34,758 | 29,710 |

Reject breakdown (`poc_reject.rejected_rows`):

| Step | Table | Reason (rule name) | Rows |
|---|---|---|--:|
| A | - | (none) | 0 |
| B | certifications | Date Range | 9 |
| B | certifications | Not in List | 20 |
| B | memberships | Not in List | 25 |
| C | certifications | Date Range | 9 |
| C | certifications | Not in List | 23 |
| C | memberships | Not in List | 25 |

## 3. Classification of every injected defect row

Outcomes: **Rejected** = a HARD DQ rule sent it to `poc_reject.rejected_rows`. **Passed to silver** = reached `poc_silver`. **Silently absorbed** = in neither silver nor rejects, with no error (merged away by the AUTO CDC key merge). **Caused failure** = the run failed and the error names the table. **Unresolved** = the run failed and the outcome cannot be determined. **Bronze only** = customers rows (FullLoad, no DQ rules, no silver table). Rows are matched by business key, or by count for NULL keys.

### 3a. Step B: defects, full refresh

By defect type:

| Defect | Rows injected | Rejected | Passed to silver | Silently absorbed | Caused failure | Unresolved | Bronze only (customers) |
|---|--:|--:|--:|--:|--:|--:|--:|
| customer_null_company_name | 20 | 0 | 0 | 0 | 0 | 0 | 20 |
| customer_malformed_email | 20 | 0 | 0 | 0 | 0 | 0 | 20 |
| customer_updated_before_created | 20 | 0 | 0 | 0 | 0 | 0 | 20 |
| invalid_enum | 145 | 45 | 100 | 0 | 0 | 0 | 0 |
| end_before_start | 45 | 9 | 36 | 0 | 0 | 0 | 0 |
| negative_amount | 300 | 0 | 300 | 0 | 0 | 0 | 0 |
| amount_mismatch | 297 | 0 | 297 | 0 | 0 | 0 | 0 |
| orphan_lines | 294 | 0 | 294 | 0 | 0 | 0 | 0 |
| null_business_key | 435 | 0 | 4 | 431 | 0 | 0 | 0 |
| duplicate_business_key | 429 | 0 | 0 | 429 | 0 | 0 | 0 |
| **Total** | **2,005** | **54** | **1,031** | **860** | **0** | **0** | **60** |

By table:

| Table | Rows injected | Rejected | Passed to silver | Silently absorbed | Caused failure | Unresolved | Bronze only (customers) |
|---|--:|--:|--:|--:|--:|--:|--:|
| netsuite_customers | 60 | 0 | 0 | 0 | 0 | 0 | 60 |
| netsuite_memberships | 99 | 25 | 26 | 48 | 0 | 0 | 0 |
| netsuite_transactions | 297 | 0 | 101 | 196 | 0 | 0 | 0 |
| netsuite_certifications | 79 | 29 | 12 | 38 | 0 | 0 | 0 |
| netsuite_transaction_lines | 1,470 | 0 | 892 | 578 | 0 | 0 | 0 |
| **Total** | **2,005** | **54** | **1,031** | **860** | **0** | **0** | **60** |

Defect by table:

| Defect | Table | Rows | Rejected | Passed to silver | Silently absorbed | Caused failure | Unresolved | Bronze only (customers) |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| customer_null_company_name | customers | 20 | 0 | 0 | 0 | 0 | 0 | 20 |
| customer_malformed_email | customers | 20 | 0 | 0 | 0 | 0 | 0 | 20 |
| customer_updated_before_created | customers | 20 | 0 | 0 | 0 | 0 | 0 | 20 |
| invalid_enum | memberships | 25 | 25 | 0 | 0 | 0 | 0 | 0 |
| invalid_enum | transactions | 100 | 0 | 100 | 0 | 0 | 0 | 0 |
| invalid_enum | certifications | 20 | 20 | 0 | 0 | 0 | 0 | 0 |
| end_before_start | memberships | 25 | 0 | 25 | 0 | 0 | 0 | 0 |
| end_before_start | certifications | 20 | 9 | 11 | 0 | 0 | 0 | 0 |
| negative_amount | transaction_lines | 300 | 0 | 300 | 0 | 0 | 0 | 0 |
| amount_mismatch | transaction_lines | 297 | 0 | 297 | 0 | 0 | 0 | 0 |
| orphan_lines | transaction_lines | 294 | 0 | 294 | 0 | 0 | 0 | 0 |
| null_business_key | memberships | 25 | 0 | 1 | 24 | 0 | 0 | 0 |
| duplicate_business_key | memberships | 24 | 0 | 0 | 24 | 0 | 0 | 0 |
| null_business_key | certifications | 20 | 0 | 1 | 19 | 0 | 0 | 0 |
| duplicate_business_key | certifications | 19 | 0 | 0 | 19 | 0 | 0 | 0 |
| null_business_key | transactions | 99 | 0 | 1 | 98 | 0 | 0 | 0 |
| duplicate_business_key | transactions | 98 | 0 | 0 | 98 | 0 | 0 | 0 |
| null_business_key | transaction_lines | 291 | 0 | 1 | 290 | 0 | 0 | 0 |
| duplicate_business_key | transaction_lines | 288 | 0 | 0 | 288 | 0 | 0 | 0 |

### 3b. Step C: increment, normal run

Only late-arriving rows are defects in this step (the updated versions and new rows are normal data).

| Defect | Rows injected | Rejected | Passed to silver | Silently absorbed | Caused failure | Unresolved | Bronze only (customers) |
|---|--:|--:|--:|--:|--:|--:|--:|
| late_arriving | 223 | 0 | 0 | 0 | 223 | 0 | 0 |
| **Total** | **223** | **0** | **0** | **0** | **223** | **0** | **0** |

By table:

| Table | Rows injected | Rejected | Passed to silver | Silently absorbed | Caused failure | Unresolved | Bronze only (customers) |
|---|--:|--:|--:|--:|--:|--:|--:|
| netsuite_memberships | 13 | 0 | 0 | 0 | 13 | 0 | 0 |
| netsuite_certifications | 10 | 0 | 0 | 0 | 10 | 0 | 0 |
| netsuite_transactions | 50 | 0 | 0 | 0 | 50 | 0 | 0 |
| netsuite_transaction_lines | 150 | 0 | 0 | 0 | 150 | 0 | 0 |
| **Total** | **223** | **0** | **0** | **0** | **223** | **0** | **0** |

The late rows did reach bronze: the new backdated snapshot tables hold memberships 13, certifications 10, transactions 50, transaction_lines 150 rows (`…__2026_06_20`). They did not reach silver because the silver flows failed (finding 2).

## 4. Reconciliation: source vs bronze vs silver

`Bronze` sums only the bronze tables the pipeline defines for the source's current watermark values (what `dq.py` and `bronze.py` resolve), so leftover objects are excluded (finding 1). `Merged away / absorbed` comes from the step B classification (duplicate pairs collapsed, NULL-key rows collapsed to one). `Not yet in silver` = bronze - rejected - silver - absorbed. `Unexplained` compares that with what the run should have left unprocessed; 0 means every gap is accounted for.

### Step B (full refresh)

| Table | Source | Bronze (tables the pipeline defines) | Rejected | Merged away / absorbed | Not yet in silver | Silver | Unexplained | Result |
|---|--:|--:|--:|--:|--:|--:|--:|---|
| customers | 2,000 | 2,000 | - | - | - | - | - | no silver table (FullLoad) |
| memberships | 2,524 | 2,524 | 25 | 48 | 0 | 2,451 | 0 | reconciles |
| certifications | 2,019 | 2,019 | 29 | 38 | 0 | 1,952 | 0 | reconciles |
| transactions | 10,098 | 10,098 | 0 | 196 | 0 | 9,902 | 0 | reconciles |
| transaction_lines | 30,288 | 30,288 | 0 | 578 | 0 | 29,710 | 0 | reconciles |

Gap explanation per table (rows removed between bronze and silver):

| Table | Bronze | - Rejected (DQ) | - Duplicate pairs merged | - NULL-key rows collapsed | = Silver | Matches |
|---|--:|--:|--:|--:|--:|---|
| memberships | 2,524 | 25 | 24 | 24 | 2,451 | yes |
| certifications | 2,019 | 29 | 19 | 19 | 1,952 | yes |
| transactions | 10,098 | 0 | 98 | 98 | 9,902 | yes |
| transaction_lines | 30,288 | 0 | 288 | 290 | 29,710 | yes |

### Step C (increment, normal run: silver flows failed)

| Table | Source | Bronze (tables the pipeline defines) | Rejected | Merged away / absorbed | Not yet in silver | Silver | Unexplained | Result |
|---|--:|--:|--:|--:|--:|--:|--:|---|
| customers | 2,100 | 2,100 | - | - | - | - | - | no silver table (FullLoad) |
| memberships | 2,896 | 2,896 | 25 | 48 | 372 | 2,451 | 0 | reconciles |
| certifications | 2,317 | 2,317 | 32 | 38 | 295 | 1,952 | 0 | reconciles |
| transactions | 11,588 | 11,588 | 0 | 196 | 1,490 | 9,902 | 0 | reconciles |
| transaction_lines | 34,758 | 34,758 | 0 | 578 | 4,470 | 29,710 | 0 | reconciles |

Silver is unchanged from step B, so every row emitted by the increment and not rejected is waiting in bronze: memberships 372 emitted, certifications 298 emitted, transactions 1,490 emitted, transaction_lines 4,470 emitted.

## 5. Findings

### Finding 1: `log_run_audit` over-counts bronze (`log_run_audit.py` and `dq.py` resolve bronze tables differently)

| Step | Table | True bronze rows | `run_audit` bronze rows | Over-count | `run_audit` silver rows_read | Actual silver rows |
|---|---|--:|--:|--:|--:|--:|
| A | memberships | 2,500 | 2,503 | 3 | 2,503 | 2,500 |
| A | certifications | 2,000 | 2,002 | 2 | 2,002 | 2,000 |
| A | transactions | 10,000 | 10,002 | 2 | 10,002 | 10,000 |
| A | transaction_lines | 30,000 | 30,003 | 3 | 30,003 | 30,000 |
| B | memberships | 2,524 | 2,527 | 3 | 2,502 | 2,451 |
| B | certifications | 2,019 | 2,021 | 2 | 1,992 | 1,952 |
| B | transactions | 10,098 | 10,100 | 2 | 10,100 | 9,902 |
| B | transaction_lines | 30,288 | 30,291 | 3 | 30,291 | 29,710 |

- `dq.py` / `bronze.py` call `bronze_table_names_for`, which asks the *source* for its distinct watermark values and builds exact names (`<table>__<YYYY_MM_DD>`).
- `log_run_audit.py:_bronze_row_count` runs `SHOW TABLES IN <catalog>.poc_bronze LIKE '<table>*'` and sums every match, including bronze tables the pipeline no longer defines.
- Leftover objects (present in `poc_bronze` in all three snapshots, registered to pipeline `d002bb31…`, holding 2 to 3 rows each (created_date 2026-08-01 confirmed for the memberships and lines views; the counts match the original data's 2026-08-01 snapshot for the other two), not read by silver): `poc_netsuite.poc_bronze.netsuite_certifications__2026_08_01`, `poc_netsuite.poc_bronze.netsuite_memberships__2026_08_01`, `poc_netsuite.poc_bronze.netsuite_transaction_lines__2026_08_01`, `poc_netsuite.poc_bronze.netsuite_transactions__2026_08_01`.
- Effect: bronze, dq and silver `rows_read` in `run_audit` are inflated by the leftover rows, so silver `rows_read` does not equal silver rows. The over-count is constant across runs A and B, so differences between them are still valid.
- Nothing was dropped. Why the pipeline keeps these views after the source lost that watermark was not investigated.

### Finding 2: the normal incremental run fails on all four silver flows (`STREAM_FAILED`, sources added)

- Job run 346077225702715: `run_pipeline` FAILED, `log_run_audit` and `refresh_credentials` SUCCESS. Job result `SUCCESS_WITH_FAILURES`.
- Pipeline update `342850e9-3647-438d-8430-12c45e5de2b0` FAILED. Flows named in the errors: `netsuite_certifications`, `netsuite_memberships`, `netsuite_transaction_lines`, `netsuite_transactions`.
- Error: *Flow ... had streaming sources added or removed. Please perform a full refresh in order to rebuild ... against the current set of sources.* (`assertion failed: There are [1] sources in the checkpoint offsets and now there are [3] sources requested by the query`).
- Cause: `dq.py` unions one streaming read per bronze snapshot table into `<table>_valid`. The increment introduced two new watermark values (2026-06-20 late rows, 2026-08-22 new snapshot), so the source count went from 1 to 3 and the silver checkpoints no longer match. By the same logic (inferred from the error and `dq.py`, not tested separately), any new watermark value will trigger this, so a plain incremental run cannot pick up new snapshots without a full refresh.
- State after the failure: bronze snapshot tables were created (see 3b), `rejected_rows` was recomputed (54 -> 57; the extra `Not in List` certification rows can only be new versions of already-invalid certifications, because the generator's updates keep `certification_type` and new or late rows are valid), silver is unchanged.
- `run_audit` logged 14 FAILED rows (every table and layer) with the generic message `Workload failed, see run output for details`; it does not say which flow failed or why.

### Finding 3: NULL business keys collapse to one silent row per table

- 435 rows were injected with a NULL business key. Silver holds 4 rows with a NULL key (one per table: memberships 1, certifications 1, transactions 1, transaction_lines 1); the other 431 were merged into them with no error.
- No DQ rule checks key presence, so nothing was rejected.

### Finding 4: duplicate business keys are silently merged away

- 429 duplicate rows were injected. All 429 were merged with their original by the AUTO CDC merge; silver has no duplicate keys and no error or reject is produced. Which of the two rows survived is not recorded in the pipeline outputs.

### Finding 5: only two of the enum rules catch anything, and one rejection is incidental

- `invalid_enum`: memberships.membership_status 25/25 rejected; transactions.type 0/100 rejected; certifications.certification_type 20/20 rejected. `transactions.type` has no DQ rule, so all its invalid values passed to silver.
- `end_before_start`: memberships 0/25 rejected; certifications 9/20 rejected. No rule compares end and start dates. The 9 rejected certifications were caught only because swapping the dates moved `certification_start_date` to or past 2027-01-01, which trips the existing `Date Range` rule (`certification_start_date < 2027-01-01`).

### Finding 6: content defects with no rule pass through untouched

- `negative_amount`: 300 injected, 300 reached silver, 0 rejected.
- `amount_mismatch`: 297 injected, 297 reached silver, 0 rejected.
- `orphan_lines`: 294 injected, 294 reached silver, 0 rejected.
- Customers defects (null `company_name`, malformed `email`, `updated_date` < `created_date`): 60 rows, all bronze only; customers has no silver table and no DQ rules.

### Finding 7: bronze drops `customers.created_date` and `updated_date` (pre-existing drift)

- Source `netsuite_customers` has 5 columns; bronze `netsuite_customers` has 3: `customer_internal_id`, `company_name`, `email`. `aidq_metadata.source_columns` lists only those three, and `bronze.py` selects `column_list` from it. Not fixed, as instructed.
- Consequence: the 20 `customer_updated_before_created` defects cannot be observed in bronze or anywhere downstream (`observable_in_bronze: false`). Before this work all 24 original customers rows also had NULL in both date columns.

### Finding 8: the schema-drift column was ignored and is not the cause of the failure

- `ALTER TABLE netsuite.netsuite_transactions ADD COLUMN custbody_region text` was applied in step C; source columns are now: `transaction_internal_id`, `tranid`, `customer_internal_id`, `type`, `date`, `created_date`, `updated_date`, `custbody_region`.
- No bronze transactions table contains `custbody_region` (4 tables checked) and silver has no such column, because the pipeline selects only the columns listed in `source_columns`.
- No error mentions the new column. The step C failure is the streaming-source change in finding 2.
- Nothing in the pipeline or `run_audit` records that the source schema changed.

### Finding 9: late-arriving rows

- 223 late rows (new keys, `created_date` 2026-06-20, `updated_date` before the table's watermark 2026-07-11) were injected. Classification: 223 caused failure, 0 silently absorbed, 0 passed to silver.
- Because the silver flows failed, this baseline cannot show whether the watermark/streaming logic would have skipped them. That needs a full-refresh run after the increment, which was not part of this baseline.

### Other observations

- Only three HARD rules exist (membership_status list, certification_type list, certification_start_date < 2027-01-01) and no SOFT rules, so the new SOFT-to-expectation wiring in `dq.py` was not exercised.
- In step B the run succeeded end to end with 2,005 defect rows; the only signal of bad data is the 54 rows in `rejected_rows`.

## 6. Things to know

- **Late-row behavior is unmeasured.** Because the silver flows failed in step C, this baseline cannot show whether the watermark and streaming logic would have skipped the late rows. A full-refresh run after the increment would show it. That was not part of this baseline.
- **Increment batch date.** Step C used `--batch-date 2026-08-22`. The default (2026-08-01, the latest snapshot plus 21 days) would have collided with, and overwritten, the four leftover `__2026_08_01` bronze views documented in finding 1.
- **Backup gate on `--increment`.** Only `--init` had a backup step originally. `--increment` now takes the same verified backup before changing data (with tests), because a fresh backup before each data change was required.
- **Backup step bug found and fixed.** The first `--init` attempt failed in the backup step (psycopg 3 cannot bind a tuple to `IN %s`). No data changed, and only the extra branch `pre-synthetic-backup-202609242052` was left behind. A regression test now covers it.
- **Prod deploy.** The prod bundle was redeployed so the baseline runs the current repo code (including the `is_active`, blank-watermark and SOFT-expectation edits). The job schedule was forced to PAUSED and verified after the runs.
- **Current state of the systems.** The `netsuite-sample` source holds the step C data, including its defects. `poc_netsuite` holds the failed step C state (bronze updated, `rejected_rows` recomputed, silver from step B). Nothing has been dropped or cleaned up.
- **What was not exercised.** No SOFT rules exist, so the SOFT-to-expectation wiring in `dq.py` never ran, and that code has not been run inside a pipeline.
- **Tests.** The unit suite passes (123 tests), including the generator, the writer against a fake connection, and the defect classifier.

## 7. Backups and restore

| Snapshot | Schema (copy) | Lakebase branch | Rows: customers / memberships / certifications / transactions / lines |
|---|---|---|---|
| before A (original 232 rows) | netsuite_backup_202609242053 | pre-synthetic-backup-202609242053 | 24 / 27 / 21 / 56 / 104 |
| before B (clean data) | netsuite_backup_202609251523 | pre-synthetic-backup-202609251523 | 2000 / 2500 / 2000 / 10000 / 30000 |
| before C (defect data) | netsuite_backup_202609251530 | pre-synthetic-backup-202609251530 | 2000 / 2524 / 2019 / 10098 / 30288 |

Also present: branch `pre-synthetic-backup-202609242052`, left by a failed first attempt (a snapshot of the untouched original data). Nothing was deleted. To restore a snapshot, copy the tables from its schema back into `netsuite` (or restore from the branch).

## 8. Files (`netsuite_ingestion/baseline/`)

`clean.json`, `defects_full.json`, `incremental.json` (metrics snapshots); `manifest_clean.json`, `manifest_defects.json`, `manifest_increment.json` (injected defects with exact keys); `classification_full.json`, `classification_incremental.json`; `defects_baseline.yaml`, `increment_config.yaml`; `collect_metrics.py`, `classify_defects.py`, `build_report.py`.
