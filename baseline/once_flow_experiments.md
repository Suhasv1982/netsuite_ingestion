# append_flow(once=True) experiments (scratch pipelines)

Question: can bronze be one streaming table per source, fed by one `once=True` append flow per snapshot value, with Silver reading that single table?
Run 2026-09-25 on serverless in `workspace.scratch_bronze_exp` (pipelines `scratch_bronze_exp` and `scratch_plan`). The source was small Delta tables plus one JDBC read of the real Lakebase `netsuite-sample` source. Nothing in `poc_netsuite`, the prod pipeline or the real source was touched. The scratch schema, both pipelines and their workspace folders were removed afterwards; the scratch secret scope was deleted right after use.

## Part 1: behavior of `once` flows

| # | Question | Result | Evidence |
|---|---|---|---|
| E1 | Does `once=True` accept a JDBC batch read? | **Yes** | 2,500 memberships rows loaded from Lakebase via JDBC |
| E2 | Does a `once` flow re-run on a normal update when nothing changed? | **No** | 20 rows stayed 20; JDBC 2,500 stayed 2,500 |
| E3 | Does a new snapshot value load on a normal update, including one older than the existing minimum? | **Yes, only its slice** | 2026-01-03 and 2025-12-30 appended; 35 rows, 0 duplicates |
| E4 | What happens when a flow disappears from the code, then comes back with the same name? | **Data kept, no failure, no re-run on re-add** | 35 rows throughout |
| E5 | Does a downstream AUTO CDC silver table survive new flows with no `skipChangeCommits` and no full refresh? | **Yes** | silver tracked bronze (35, 40, 42, 47, 57) with no full refresh |
| E6 | Does a full refresh re-run every flow? | **Yes, rebuilds bronze** | 35 rows rebuilt from source |
| E7a | Can graph-build code read the pipeline's own target to learn what is loaded? | **No** | `REFERENCE_DLT_DATASET_OUTSIDE_QUERY_DEFINITION` |
| E7b | Can a top-up flow read its own target inside the query definition? | **No** | `Failed to read dataset ... exp_bronze ... could not be resolved`; also blocks silver |
| E7c | Can a top-up flow anti-join against an external ledger table (not pipeline-owned)? | **Yes** | 5 late rows into 2 already-loaded dates appended, 40 rows, 0 duplicates |
| E7d | Are top-up flows re-run when nothing changed? | **No** | 40 rows stayed 40 |
| E7e | Hazard: full refresh while unloaded late rows exist and top-ups are enabled | **Duplicates in bronze** | bronze 44 rows / 42 distinct; silver 42 (AUTO CDC absorbed them) |
| E7f | Mitigation: full refresh with top-ups off | **Clean** | 42 rows / 42 distinct |
| E8 | A `once` flow fails at runtime, then the cause is fixed | **Atomic, retried once** | failed update left bronze at 52; next normal update loaded 5 rows, 57 total, 0 duplicates |

Two first attempts at forcing the E8 failure (`raise_error`, divide by zero) did not fail (constant-folded to NULL, ANSI off in pipelines); the Python UDF version failed as intended.

## Part 2: planning cost with ~400 once flows per table (4 tables = 1,600 flows)

Real `metadata.py` planning functions, Delta sources with 400 distinct dates each. Seconds per update phase from the pipeline event log.

| Update | INITIALIZING (graph build and planning) | SETTING_UP_TABLES | RUNNING |
|---|---|---|---|
| Small pipeline, 4 to 7 flows | about 10.5 | about 1.5 | 4 to 9 |
| P1: first load, 1,600 flows all run | 693 | 107 | 1,533 |
| P2: normal, `all_dates`, 1,600 flows defined, none run | **761** | 82 | 69 |
| P7: same as P2, instrumented | **693** | 77 | 71 |
| P3b: normal, `pending_only`, nothing pending (anchor flows only) | **53** | 0.9 | 3.5 |

* Cost is in defining flows, not running them: about 0.43 s per flow (P7: Python graph build 400 s of which about 0.22 s per flow is defining it; the rest is the platform's own analysis). `all_dates` is not viable at 400 dates per table; `pending_only` is 13x faster.
* In `pending_only` the remaining cost is about 10 s per table for the two Spark jobs that find source dates and pending pairs (`collect_plan_inputs`).
* P3 first failed with `No query found for dataset ...`: a streaming table with zero flows fails the update. `plan_flows` now returns a constant-named empty anchor flow when nothing else is defined.

## Part 3: can a full refresh be detected at graph build?

| Question | Result |
|---|---|
| Does any Spark configuration key reveal a full refresh? | **No.** 623 pipeline-related keys were dumped for a normal and a full-refresh update; the only difference is `spark.pipelines.updateId`. `pipelines.id` and `spark.pipelines.updateId` are readable. |
| Can graph-build code read the pipeline's own event log? | **Yes.** `SELECT details FROM event_log('<pipelines.id>') WHERE event_type='create_update' AND origin.update_id='<spark.pipelines.updateId>'` returns the current update's event. |
| What does that event say? | `full_refresh: true` for a full refresh. A selective refresh shows `full_refresh: false` plus `full_refresh_selection: ['<table>']`. |
| What happens to bronze on a full refresh with `pending_only` and no rebuild flag? | **Data loss:** every bronze table ended with 0 rows (P6b), because only the anchor flow was defined. |

So the hard guard is possible: `metadata.refresh_guard_error` raises when the update is a full refresh, or a selection naming a bronze table, unless `bronze_rebuild=true`, and fails closed if the event cannot be read while in `pending_only` scope.

## Not tested

* The guard inside the real pipeline (it was tested as a pure function and the event-log read was proven on the scratch pipeline, but `bronze.py` itself has not run on Databricks).
* Behavior when the event log is not readable (permissions).
* Flow counts above 400 per table, and JDBC-scale source sizes for the `(business_key, date)` pair scan.
