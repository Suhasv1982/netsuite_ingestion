# Guard check: 10 normal dev updates (2026-09-26)

Setup: dev target, dev metadata branch (`aidq-metadata/dev`), SQL warehouse STOPPED, no source or metadata change
between updates, `pipelines start-update` without `--full-refresh`, flow_scope `pending_only`.
(One extra update ran before these when the first runner script crashed on event parsing; it also completed.)

| # | Result | Wall s | INITIALIZING s | SETTING_UP_TABLES s | RUNNING s | Guard block |
|---|---|---|---|---|---|---|
| 1 | COMPLETED | 65 | 33.6 | 2.1 | 14.0 | no |
| 2 | COMPLETED | 76 | 54.4 | 2.0 | 13.4 | no |
| 3 | COMPLETED | 75 | 55.6 | 1.9 | 11.9 | no |
| 4 | COMPLETED | 43 | 20.7 | 2.2 | 10.8 | no |
| 5 | COMPLETED | 54 | 28.1 | 2.2 | 12.2 | no |
| 6 | COMPLETED | 43 | 20.8 | 2.5 | 13.2 | no |
| 7 | COMPLETED | 64 | 35.0 | 2.0 | 13.8 | no |
| 8 | COMPLETED | 65 | 45.2 | 2.0 | 11.8 | no |
| 9 | COMPLETED | 44 | 20.4 | 2.0 | 14.0 | no |
| 10 | COMPLETED | 54 | 29.0 | 2.0 | 10.6 | no |

* **Fail-closed guard blocks: 0 of 10.** Every update completed. Because there were no blocks, the test of reading
  `full_refresh` from the Pipelines REST API was not needed and was not run.
* INITIALIZING: min 20.4 s, median 31.3 s, mean 34.3 s, max 55.6 s. It is the largest and most variable phase
  (it includes graph build: the guard's event-log read with its retries, and the ledger/JDBC planning). WAITING_FOR_RESOURCES was not
  observed. Timings come from the pipeline event log (`update_progress` events).
* Earlier, the guard read nothing intermittently on other runs; 0 blocks in 10 is a small sample, not proof that it is gone.
