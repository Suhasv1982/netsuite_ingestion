# netsuite_ingestion (POC)

Metadata-driven bronze/DQ/silver pipeline for the 5 NetSuite sample tables,
driven entirely by the `aidq_metadata` control tables (`source_table_def`,
`source_columns`, `data_quality_rules`).

## Layout

```
databricks.yml                                      bundle config (catalog / pg hosts / bronze_rebuild / flow_scope vars)
resources/netsuite_ingestion_poc.pipeline.yml       pipeline resource
resources/netsuite_ingestion_job.job.yml            job: refresh_credentials -> run_pipeline -> sync_ledger -> ledger_check (+ log_run_audit)
resources/guard_canary.{pipeline,job}.yml           weekly guard canary (schedule PAUSED)
src/netsuite_ingestion/metadata.py                  JDBC readers, pure builders, incremental-bronze planning (flows, ledger)
src/netsuite_ingestion/transformations/bronze.py    poc_bronze.<table>: streaming table + once-flows (incremental), batch table (FullLoad)
src/netsuite_ingestion/transformations/dq.py        HARD-rule validity split -> poc_reject.rejected_rows; SOFT rules as expectations
src/netsuite_ingestion/transformations/silver.py    poc_silver.<table>: AUTO CDC merge (business_key, watermark_col)
src/netsuite_ingestion/sync_ledger.py               job task: keep the bronze key ledger in step with bronze
src/netsuite_ingestion/ledger_check.py              job task: ledger + fingerprints vs bronze check, warns on mismatch
src/netsuite_ingestion/canary/                      guard canary: tiny pipeline + task that asserts the full-refresh guard still works
src/netsuite_ingestion/log_run_audit.py             job task: one aidq_metadata.run_audit row per (table, layer)
tools/netsuite_gen.py, tools/pg_writer.py            synthetic data generator for the netsuite-sample source
baseline/                                           before-agents baseline (v1) and once-flow experiment results
tests/                                              pytest (no Spark, no database needed)
```

## Load types (source_table_def)

`watermark_col` decides the load type: **blank = FullLoad, non-blank = Incremental**
(`load_mode` holds the matching label). Today: `netsuite_customers` is a
FullLoad (batch table, overwritten each run); the other four are Incremental
with `watermark_col = updated_date` and a start floor in `bronze_watermark`
(ISO date, `1900-01-01` = load everything on the first run).

## Incremental bronze (one streaming table per source)

`poc_bronze.<table>` is an append-only streaming table with two added columns,
`_snapshot_date` (the `updated_date` value as a date) and `_loaded_at`. It is fed
by `once=True` append flows planned at graph build (`metadata.plan_flows`):

* **snapshot flow** per distinct source date at or above the floor, named
  `<table>__<YYYYMMDD>`. A flow that already ran is not run again, so each
  increment loads once. A new date, including one older than the current
  maximum, loads on the next normal run.
* **top-up flow** for late rows on an already-loaded date. Detection is by key,
  never by row count (an update can move one row out of a date and a late row in
  while the count stays the same), and it is cheap: Postgres computes one
  **fingerprint per date** server-side (count of distinct `(business_key, date)`
  pairs plus the sum of a 60-bit md5-derived bigint per pair) and returns one row
  per date; the ledger stores the same numbers. Pairs are fetched only for dates whose
  fingerprints differ, anti-joined with the ledger's pairs, and the missing pairs are
  extracted by a flow named `<table>__<YYYYMMDD>__topup_<hash of the pending key set>`,
  so a stale ledger reproduces the same name and does not load twice.
  The fingerprint is defined once and computed three ways (Postgres SQL, Spark SQL,
  a Python reference in `metadata.py`); `tests/test_fingerprint_integration.py`
  (`NETSUITE_INTEGRATION=1`) proves the three agree and `tests/test_fingerprints.py`
  pins known values offline.

Silver reads that single table (`<table>_valid` view -> AUTO CDC keyed on
`business_key`, sequenced by `updated_date`), so its streaming source never changes.
Behavior of `once` flows on this runtime is documented in
`baseline/once_flow_experiments.md`.

### The key ledger

`<catalog>.ledger.bronze_keys (table_name, business_key, snapshot_date)` plus
`<catalog>.ledger.bronze_fingerprints (table_name, snapshot_date, row_count, fp_sum)` are plain
Delta tables **outside** the pipeline: a pipeline cannot read its own tables at graph
build or inside a flow. `sync_ledger` appends the pairs bronze holds after every run and refreshes the fingerprints of
the dates that changed;
`ledger_check` then compares pairs both ways and the stored fingerprints with fingerprints
recomputed from bronze, prints `WARN` and logs a `ledger` row
in `run_audit` on any mismatch (it never fails the job). Recovery: run
`sync_ledger` with `--bronze-rebuild true`.

## Operating rules

* **Normal runs are never a full refresh.**
* **A full refresh must set `bronze_rebuild=true`, and this is enforced.** At graph build
  `bronze.py` reads the update's `create_update` event from the pipeline's own event log and
  raises if it is a full refresh (or a selective refresh naming a bronze table) without
  `bronze_rebuild=true`. With the flag it ignores the ledger, defines a flow for every date and
  no top-ups. Without the guard, a full refresh in `pending_only` scope left every bronze table
  empty (measured), and one with top-ups enabled loaded late rows twice (bronze duplicates;
  silver unaffected because AUTO CDC merges them).
  Procedure: `databricks bundle deploy --var="bronze_rebuild=true"`, run the job with a full
  refresh, then redeploy with the default `bronze_rebuild=false`.
* `flow_scope` = `pending_only` (default: flows only for dates missing from the ledger, plus
  top-ups) or `all_dates` (a flow for every source date each run). Planning costs about 0.45 s
  per defined flow (1,600 flows = about 12 min per update, whether or not any flow runs), so
  `pending_only` is the default (about 1 min for the same tables).
* A streaming table with no flow fails the update, so `plan_flows` returns a constant-named
  empty `<table>__anchor` flow when nothing else would be defined.
* The guard fails closed: if the update's `create_update` event cannot be read it refuses to run in
  `pending_only` scope. This was seen to happen intermittently (the event log is written
  asynchronously and the event is sometimes not visible at graph build), so the read retries
  5 times 10 s apart, the message says why the read failed and shows the latest event-log rows, and
  the advice is to re-run the same update first.
* When the guard blocks a refresh the update fails before any data changes. The message states what
  was blocked, why, and the exact commands to re-run it:
  `bundle deploy --var bronze_rebuild=true`, `bundle run netsuite_ingestion_daily --pipeline-params
  full_refresh=true`, `bundle deploy --var bronze_rebuild=false` (prod adds
  `--var schedule_pause_status=PAUSED`).

## Guard canary (check for platform changes)

`guard_canary_check` (weekly, Mondays 07:00 UTC, **schedule PAUSED**: unpause deliberately) runs a tiny
pipeline (`guard_canary`, same guard code as `bronze.py`) through four updates: normal, full refresh,
selective refresh of its table, normal again. It fails unless normal updates complete, both refreshes are
stopped by the guard and the canary table is untouched. Stopped by the fail-closed path (event not
visible) counts as a warning, not a failure. A failure means the platform changed how a refresh is
reported in the pipeline event log (`create_update.full_refresh` / `full_refresh_selection`) or `event_log()`
access changed: fix the guard before the next full refresh. Run it by hand with
`databricks bundle run guard_canary_check -t <target>`.
Free Edition note: the task needs its own serverless compute plus one for each pipeline update; a running
SQL warehouse counts too, so stop the warehouse first or the updates die with `RESOURCE_EXHAUSTED`.
Serverless jobs retry a failed task, so a first-attempt failure can still end as a passing run.
* Never `DROP` a `poc_bronze` table outside Lakeflow: silver's streaming checkpoint would fail
  with `DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE`; only a full refresh recovers.
* No production deploy or run without the owner's approval.

## Known limitations

* **Source deletes are not handled.** A row deleted (or moved to another date) in the source
  stays in bronze, the ledger and silver; nothing removes it.
* Two source rows with the same `(business_key, updated_date)` cannot be told apart: a late
  duplicate of an already-loaded pair is never captured.
* Rows whose watermark is NULL, or older than the floor, are never extracted.
* Every normal run reads the distinct `(business_key, date)` pairs of each incremental source
  once to find late rows; this scales with source size. Late pairs are capped (200,000 per table);
  beyond that the run fails and asks for a `bronze_rebuild` full refresh.
* The ledger lags by one run and depends on `sync_ledger`; a failed sync is what `ledger_check`
  warns about.
* `netsuite_customers` stays a FullLoad (blank watermark). `source_columns` still omits its
  `created_date`/`updated_date`, which the source has; bronze therefore drops them.
* Database tokens are ~1 hour OAuth credentials kept in the `netsuite_ingestion_poc` secret scope and
  refreshed by the `refresh_credentials` task.

## Data access: direct JDBC, not Lakehouse Federation

Both source (`netsuite-sample`) and control (`aidq-metadata`) data live in Lakebase Postgres
projects. Reads go straight over JDBC (`spark.read.format("jdbc")`, see `metadata.PgConn` /
`read_jdbc_table`) rather than through a Unity Catalog foreign catalog: Postgres federation only
supports username/password auth, and native Postgres login is disabled on both projects.

## Dev environment

`dev` writes to schemas in the `workspace` catalog (`poc_bronze`, `poc_silver`, `poc_reject`, `canary`,
`ledger`) because the prod pipeline owns the tables in `poc_netsuite` and a catalog cannot be created through
the API on Default Storage. Dev and prod share the `netsuite-sample` source and the `aidq-metadata` tables.
The v2 baseline (`baseline/before_agents_report_v2.md`) was produced in dev.

## Migration from the per-date bronze tables

1. Metadata: `watermark_col = updated_date`, `bronze_watermark = 1900-01-01` (done, backup schema
   `aidq_metadata_backup_202609251923`).
2. Deploy with `bronze_rebuild=true`, run the job with a full refresh (silver's checkpoints move from
   a multi-source union to the single bronze table), then redeploy with `bronze_rebuild=false`.
   Rehearsed in dev: full refresh, then two normal increments, all green (`baseline/before_agents_report_v2.md`).
3. Confirm the v2 baseline matches (`baseline/`), then, and only then, drop the old
   `poc_bronze.<table>__<date>` views (including the four `__2026_08_01` leftovers).

## Running tests locally

```bash
cd netsuite_ingestion
pip install -e ".[dev,tools]"
pytest
```

## Deploying and running

```bash
databricks bundle validate --profile <PROFILE>
databricks bundle deploy -t <TARGET> --profile <PROFILE>          # prod needs the owner's approval
databricks bundle run netsuite_ingestion_daily -t <TARGET> --profile <PROFILE>
```
