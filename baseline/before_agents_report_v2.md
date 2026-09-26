# Before-agents baseline v2: redesigned bronze

Same source data and defect config as baseline v1 (`before_agents_report.md`), run through the redesigned pipeline in the **dev** target (catalog `workspace`, prod untouched): one append-only streaming bronze table per incremental source fed by `once` flows per `updated_date`, a key ledger with per-date fingerprints, and a full-refresh guard. No agents were running. Every number is computed from the saved snapshots in `baseline/v2/`.

## Summary

- **Step B (migration full refresh, defects, `bronze_rebuild=true`)** reproduces v1 exactly: silver memberships 2,451, certifications 1,952, transactions 9,902, transaction_lines 29,710, 54 rejected rows, and identical outcomes for all 2,005 injected defect rows.
- **Step C (increment 1, normal run) now succeeds.** In v1 the same run failed all four silver flows. All 223 late-arriving rows reached silver (v1: 223 caused failure).
- **Step D (increment 2, late rows onto already-loaded dates, normal run) succeeds.** All 223 late rows reached silver; bronze equals the source row-for-row in every table (no duplicates, no misses), loaded through top-up flows.
- `run_audit` counts are exact (v1 over-counted bronze by 2 to 3 rows per table because of leftover objects).
- The full-refresh guard blocked a full refresh without `bronze_rebuild=true` on the real dev pipeline and left bronze unchanged.
- Everything else v1 found still holds: rules only catch what they cover (see section 3).

## 1. What was run (dev target)

| Step | Data change and run | Backup taken before it | Job run id | Job result | Pipeline update |
|---|---|---|--:|---|---|
| B. Migration full refresh, defects | `--init --seed 42 --config defects_baseline.yaml`, deploy `bronze_rebuild=true`, job with `full_refresh` | netsuite_backup_202609252235 | 1107720384901464 | SUCCESS | COMPLETED |
| C. Increment 1, normal run | `--increment --seed 43 --batch-date 2026-08-22 --apply-schema-drift` (late_arriving 10%) | netsuite_backup_202609260101 | 860122969061603 | SUCCESS | COMPLETED |
| D. Increment 2, normal run | `--increment --seed 44` (batch 2026-09-14, late_arriving 10%) | netsuite_backup_202609260109 | 793341885130688 | SUCCESS | COMPLETED |

Each data change was preceded by a verified backup (schema copy plus Lakebase branch). Job tasks per run: `refresh_credentials`, `run_pipeline`, `sync_ledger`, `ledger_check`, `log_run_audit`.

## 2. Headline numbers

| Table | B source | B bronze | B silver | C source | C bronze | C silver | D source | D bronze | D silver |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| customers | 2,000 | 2,000 | - | 2,100 | 2,100 | - | 2,200 | 2,200 | - |
| memberships | 2,524 | 2,524 | 2,451 | 2,896 | 2,896 | 2,577 | 3,281 | 3,281 | 2,704 |
| certifications | 2,019 | 2,019 | 1,952 | 2,317 | 2,317 | 2,052 | 2,625 | 2,625 | 2,152 |
| transactions | 10,098 | 10,098 | 9,902 | 11,588 | 11,588 | 10,402 | 13,128 | 13,128 | 10,902 |
| transaction_lines | 30,288 | 30,288 | 29,710 | 34,758 | 34,758 | 31,210 | 39,378 | 39,378 | 32,710 |

Bronze equals the source in every table and every step, so nothing was dropped and nothing was loaded twice.

Rejects (`poc_reject.rejected_rows`):

| Step | Table | Reason | Rows |
|---|---|---|--:|
| B | certifications | Date Range | 9 |
| B | certifications | Not in List | 20 |
| B | memberships | Not in List | 25 |
| C | certifications | Date Range | 9 |
| C | certifications | Not in List | 23 |
| C | memberships | Not in List | 25 |
| D | certifications | Date Range | 9 |
| D | certifications | Not in List | 23 |
| D | memberships | Not in List | 25 |

## 3. Classification of every injected defect row

Definitions as in v1: **Rejected** (HARD DQ rule), **Passed to silver**, **Silently absorbed** (merged away by the key merge, no error), **Caused failure**, **Unresolved**, **Bronze only** (customers have no silver table).

### 3a. Step B: defects, migration full refresh (same as v1 step B)

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

Outcome per defect identical to v1 step B: **yes**.

### 3b. Step C: increment 1, late-arriving rows (v2 vs v1)

| Table | Late rows | v2 passed to silver | v2 caused failure | v1 outcome |
|---|--:|--:|--:|---|
| memberships | 13 | 13 | 0 | v1: caused_failure 13 |
| certifications | 10 | 10 | 0 | v1: caused_failure 10 |
| transactions | 50 | 50 | 0 | v1: caused_failure 50 |
| transaction_lines | 150 | 150 | 0 | v1: caused_failure 150 |

### 3c. Step D: increment 2, late rows onto already-loaded dates

| Table | Late rows | Rejected | Passed to silver | Silently absorbed | Caused failure | Unresolved | Bronze only (customers) |
|---|--:|--:|--:|--:|--:|--:|--:|
| netsuite_memberships | 13 | 0 | 13 | 0 | 0 | 0 | 0 |
| netsuite_certifications | 10 | 0 | 10 | 0 | 0 | 0 | 0 |
| netsuite_transactions | 50 | 0 | 50 | 0 | 0 | 0 | 0 |
| netsuite_transaction_lines | 150 | 0 | 150 | 0 | 0 | 0 | 0 |
| **Total** | **223** | **0** | **223** | **0** | **0** | **0** | **0** |

## 4. Reconciliation: source, bronze, silver

| Step | Table | Source rows | Bronze rows | Rejected (cumulative) | Silver rows | Bronze = source |
|---|---|--:|--:|--:|--:|---|
| B | memberships | 2,524 | 2,524 | 25 | 2,451 | yes |
| B | certifications | 2,019 | 2,019 | 29 | 1,952 | yes |
| B | transactions | 10,098 | 10,098 | 0 | 9,902 | yes |
| B | transaction_lines | 30,288 | 30,288 | 0 | 29,710 | yes |
| C | memberships | 2,896 | 2,896 | 25 | 2,577 | yes |
| C | certifications | 2,317 | 2,317 | 32 | 2,052 | yes |
| C | transactions | 11,588 | 11,588 | 0 | 10,402 | yes |
| C | transaction_lines | 34,758 | 34,758 | 0 | 31,210 | yes |
| D | memberships | 3,281 | 3,281 | 25 | 2,704 | yes |
| D | certifications | 2,625 | 2,625 | 32 | 2,152 | yes |
| D | transactions | 13,128 | 13,128 | 0 | 10,902 | yes |
| D | transaction_lines | 39,378 | 39,378 | 0 | 32,710 | yes |

Silver is smaller than bronze by the rejected rows plus rows merged by the key (duplicate pairs, NULL keys collapsed to one row, and older versions of updated keys); the per-step gaps for B are explained exactly in v1 section 4 and are unchanged. Step B silver-gap check: memberships 2,524 - 25 rejected - 48 merged = 2,451 (matches silver 2,451), certifications 2,019 - 29 rejected - 38 merged = 1,952 (matches silver 1,952), transactions 10,098 - 0 rejected - 196 merged = 9,902 (matches silver 9,902), transaction_lines 30,288 - 0 rejected - 578 merged = 29,710 (matches silver 29,710).

## 5. Findings

### Finding 1: v1's `run_audit` over-count is fixed

| Table | True bronze rows | `run_audit` bronze rows | Over-count | `run_audit` silver rows_read | Bronze minus rejects |
|---|--:|--:|--:|--:|--:|
| memberships | 2,524 | 2,524 | 0 | 2,499 | 2,499 |
| certifications | 2,019 | 2,019 | 0 | 1,990 | 1,990 |
| transactions | 10,098 | 10,098 | 0 | 10,098 | 10,098 |
| transaction_lines | 30,288 | 30,288 | 0 | 30,288 | 30,288 |

`log_run_audit` now counts the exact table `poc_bronze.<table>`; there are no per-date tables and therefore no leftover objects to match by prefix. Silver `rows_read` equals bronze minus rejected rows.

### Finding 2: the normal incremental run no longer fails

v1 step C: job `SUCCESS_WITH_FAILURES`, pipeline `FAILED` with `streaming sources added or removed` on all four silver flows. v2 steps C and D: job `SUCCESS` / `SUCCESS`, pipeline `COMPLETED` / `COMPLETED`, no error events. Silver reads one streaming table whose identity never changes, so new dates only add flows to bronze.

### Finding 3: late rows, both kinds

- Step C late rows (created 2026-06-20, older than the watermark 2026-07-11) formed **new dates**: loaded whole by snapshot flows, no top-up needed.
- Step D late rows landed on **already-loaded dates** (2026-06-20 to 06-24, 07-11 to 07-13, 08-22, 08-23): the source fingerprint of those dates differed from the ledger's, pairs were fetched only for them, and top-up flows named `<table>__<date>__topup_<hash>` extracted just the missing keys. Bronze equals the source afterwards, so nothing was loaded twice.

  - memberships: bronze dates after D = 13 (ledger dates 13, ledger pairs 3,233).
  - certifications: bronze dates after D = 12 (ledger dates 12, ledger pairs 2,587).
  - transactions: bronze dates after D = 14 (ledger dates 14, ledger pairs 12,932).
  - transaction_lines: bronze dates after D = 14 (ledger dates 14, ledger pairs 38,800).
### Finding 4: planning stays small

Update phase times from the pipeline event log (seconds): B (rebuild, one date per table) INITIALIZING 33, C 50, D 65 (about 40 flows including top-ups). Scratch measurements had shown about 12 minutes for 1,600 flows defined every run; `pending_only` avoids that.

### Finding 5: schema drift is still ignored, and still nothing records it

Source `netsuite_transactions` columns now end with `custbody_region`; bronze `netsuite_transactions` columns are `updated_date, _snapshot_date, _loaded_at` (no `custbody_region`). Neither the run nor `run_audit` mentions the change.

### Finding 6: what did not change from v1

- Rules only catch what they cover: 45 of 145 invalid enums rejected (no rule on `transactions.type`), 9 of 45 reversed dates rejected only through the 2027 cutoff rule; negative amounts, amount mismatches and orphan lines pass through untouched.
- NULL business keys collapse to one silent row per table and duplicate pairs merge away with no signal.
- Bronze still drops `customers.created_date` and `updated_date` (`source_columns` still lists three columns), so 20 customer date defects stay invisible.
- Source deletes are not handled (README).

### Finding 7: the full-refresh guard

On the real dev pipeline a full refresh with `bronze_rebuild=false` was started (update 6ee304cd-938b-4301-9f0b-58cd2c21585f: FAILED). The guard stopped it before any data changed; bronze row counts were identical before and after. The message the operator sees:

```
BLOCKED: update 6ee304cd-938b-4301-9f0b-58cd2c21585f of pipeline 98d66f50-6117-41f6-b26c-8f1a3708a563 was stopped before it changed any data. It is a full refresh of ALL tables with bronze_rebuild=false.
WHY: normal runs define once-flows only for dates missing from the key ledger (flow_scope=pending_only). A full refresh empties the bronze streaming tables first, so with only those flows bronze would be rebuilt from the pending dates and lose everything else (measured: every incremental bronze table ended with 0 rows).
TO RE-RUN with bronze_rebuild=true (defines a flow for every source date, ignores the ledger, no top-ups):
  1. databricks bundle deploy -t dev --var="bronze_rebuild=true" --profile <PROFILE>
  2. databricks bundle run netsuite_ingestion_daily -t dev --pipeline-params full_refresh=true --profile <PROFILE>
  3. databricks bundle deploy -t dev --var="bronze_rebuild=false" --profile <PROFILE>   # back to normal runs
```

An earlier attempt of the same test was also stopped, but with the message that the refresh mode could not be read from the event log (the guard fails closed). The `create_update` event existed with `full_refresh: true`, so the in-pipeline read returned nothing that time; the cause is not confirmed (suspected event-log write lag). The reader now retries 5 times, 10 s apart, and the message states why a read failed. The rerun above produced the specific message.

### Finding 8: the canary

`guard_canary_check` (weekly, schedule PAUSED) final run in dev: **SUCCESS**. normal_1: COMPLETED rows=1  | full_refresh: FAILED rows=1 Traceback (most recent call last): | selective_refresh: FAILED rows=1 Traceback (most recent call last): | normal_2: COMPLETED rows=1  | guard canary OK: normal updates run, full and selective refreshes are blocked, data untouched

How it got there, because the history matters:

1. First run: all four updates failed with `RESOURCE_EXHAUSTED` (free serverless compute limit). The canary task plus a pipeline update plus a still-running SQL warehouse exceeded it; the canary correctly reported these as failures, not as blocked refreshes. Stopping the warehouse fixed it.
2. Second run: the guard blocked both refreshes, but with the fail-closed message (the update's `create_update` event was not visible in the event log at graph build), so the canary failed. Serverless jobs retried the task and the retry passed.
3. Changes: the reader retries 5 times 10 s apart and reports why it failed, the message says the condition can be transient and to re-run, and the canary now treats a fail-closed block as a warning (data is safe, detection did not run) instead of a failure.
4. Final run: passed on the first attempt, with the specific messages (`a full refresh of ALL tables`, `a full refresh of bronze table(s) canary_bronze`).

Open item: the event is intermittently not visible at graph build during refreshes (seen in the canary and once on the real dev pipeline); the cause is not confirmed (suspected event-log write lag). The guard is safe in both cases; it just cannot always say which refresh it stopped.

## 6. Things to know

- **Dev, not prod.** The prod pipeline `netsuite_ingestion_poc` and its 22 tables in `poc_netsuite` were not touched. Old per-date bronze views are untouched there; they are dropped only after the prod migration succeeds and this baseline matches, and only with approval.
- **Dev catalog.** A catalog cannot be created through the API on Default Storage, so dev uses schemas in the `workspace` catalog (`poc_bronze`, `poc_silver`, `poc_reject`, `canary`, `ledger`). The earlier dev pipeline owned no tables and was deleted so the bundle could recreate it in the new catalog.
- **Shared source.** Dev and prod read the same `netsuite-sample` source and `aidq-metadata`. The metadata now says `updated_date` / `1900-01-01`, which the deployed prod code (old per-date design) would interpret differently; the prod schedule stays paused.
- **Backups accumulate.** Each generator run takes a schema copy and a Lakebase branch (`pre-synthetic-backup-*`); clean them up when no longer needed.
- **Not covered.** Flow counts above about 400 per table, JDBC-scale source sizes for the fingerprint query, and the guard under `flow_scope=all_dates`.

## 7. Files (`netsuite_ingestion/baseline/v2/`)

`B.json`, `C.json`, `D.json` (snapshots), `manifest_B/C/D.json` (injected defects), `classification_B/C/D.json`, `guard_negative_test.txt`, `canary_run.log`, `dev_resources.txt`; scripts `collect_metrics.py --layout v2`, `classify_defects.py`, `build_report_v2.py`.
