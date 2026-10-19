# Platform design notes (given to the monitor agent)

How the NetSuite ingestion platform works, as a person running it would know it. The monitor / RCA agent
(`src/aidq_agent`) receives this file with its instructions. Keep it to mechanisms and conventions; **never put
an incident's diagnosis here** (incident write-ups live in `evals/incidents/`, and the evals would stop measuring
anything). Update it when the design changes.

## Daily cycle (UTC)

* 05:00 `netsuite_daily_generator` appends one day of synthetic data to the Postgres source (`netsuite-sample`,
  schema `netsuite`); 05:30 dev `netsuite_ingestion_daily` runs the pipeline. Prod is paused.
* Every merge to `main` runs deploy-dev (GitHub Actions), which redeploys dev and starts one ingestion run
  (trigger ONE_TIME). Jobs and pipelines change only through these deploys unless someone edits them by hand.
* A run that never starts sends no failure email; only failed runs do.

## Source

* Five tables. `netsuite_customers` is a full load. The other four are incremental, with watermark column
  `updated_date` (a **date**, no time of day) and business keys `membership_internal_id`,
  `certification_internal_id`, `transaction_internal_id`, `transaction_line_id`.
* An update to a record is a new row with the same business key and a later `updated_date`. Rows are appended,
  never updated or deleted in place.
* The generator writes one batch per day; with `--allow-before-watermark` a batch may be dated on days the
  source already holds.

## Bronze and the key ledger

* Bronze (`poc_bronze.<table>`) has `_snapshot_date` = the row's `updated_date` day. A day not loaded before gets
  a snapshot flow that loads all of that day's rows.
* Every incremental bronze row has `_row_hash`: md5 of the table's **active** `source_columns` (not every raw
  source column), computed by Postgres when the row is extracted.
* Rows that arrive later for an already-loaded day are loaded by **top-up flows**. To find them, the pipeline
  keeps a key ledger (`ledger.bronze_keys`) and per-day fingerprints (`ledger.bronze_fingerprints`), both built
  from the **distinct (business_key, snapshot_date, row_hash) entries** in bronze. Each day's source fingerprint
  (entry count + hash sum, also over distinct entries) is compared with the ledger's; only days whose fingerprints
  differ are inspected, and a top-up loads the source rows whose entry the ledger does not have yet. That includes a
  second version of a key on a day already loaded (same key and day, different content). An exact copy of a row
  already loaded adds no entry and is not loaded again.
* The hashed column set is recorded per table (`ledger.row_hash_columns`) when bronze is rebuilt. If the active
  `source_columns` of an incremental table change, normal runs fail at graph build with an error naming the table
  and the column change until bronze is rebuilt (`bronze_rebuild=true`).
* Every error raised while the bronze notebook builds the graph (guards included) appears in the pipeline event log
  as a long traceback that starts at `bronze.py, cell 1, line 8` (where the pipeline runs the notebook): that line
  says nothing about the cause. The guards' messages start with `BLOCKED:` near the end of the text;
  `get_pipeline_errors` reports them as `markers` (e.g. `row_hash_guard`). A failed update is retried by the
  pipeline (`RETRY_ON_FAILURE`), so one blocked run shows several failed updates.
* `ledger_check` verifies that the ledger matches bronze; OK means the two agree, not that bronze matches the
  source.
* A full refresh of bronze is blocked unless `bronze_rebuild=true`.

## Silver, rejects, gold

* Silver keeps one version per key (AUTO CDC ordered by `updated_date`, then `_row_hash`). Two versions of a key
  with the same `updated_date` day are ordered by `_row_hash`: deterministic and the same in every environment,
  but not necessarily the newer version.
* HARD data-quality rules send rows to `poc_reject.rejected_rows` (reject rate vs `discard_threshold` in
  `v_table_health`); SOFT rules are expectations that only report.
* Gold: two materialized views over silver (customer revenue, customer status).

## Normal noise (not problems)

* Business keys with several versions on one `updated_date` day in the source (`compare_bronze_to_source`
  `same_day_versions`, the daily check's INFO line): all versions are in bronze; silver keeps one.
* Guard row WARN with 2 event-log reads: the bronze guard needs a retry to read its event; normal.
* Lakebase endpoints and the SQL warehouse on Free Edition can be disabled after inactivity; tools report this
  as `unavailable`.
* The audit log may be unreadable for the agent's identity; `config_changes` is then `unavailable`.
