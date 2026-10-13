# Plan: the watchdog triggers missed dev runs (self-healing)

Status: plan approved with the owner's checklist (2026-10-13), built in the same PR. Dev only.

## Goal

The monitor runs on GitHub as the external watchdog. It keeps running when Databricks schedules stop firing (2026-10-06, 2026-10-11..13). When today's scheduled dev generator or dev ingestion run did not happen, it triggers that run through the Jobs API, waits for the result, and writes an incident labelled self-healed with the outcome.

## Rules (owner checklist 2026-10-13)

| # | Rule | How |
|---|---|---|
| 1 | Trigger only when there is **no run today and none active** | today = UTC date. A job is due when its cron's fire time today has passed by 30 min (grace), it has **no run started today** (any trigger: scheduled, manual, watchdog) and **no run active** now. It is also never triggered while its schedule is PAUSED (a deliberate pause is respected). |
| 2 | **Generator first, then dev after it succeeds** | Generator first: run-now, then wait. Dev ingestion is triggered only when today's generator run (existing or triggered) ended `SUCCESS`. If the generator failed or timed out, dev is not triggered, and the incident says why. |
| 3 | **At most one trigger per job per day, no retry loop** | Rule 1 already blocks a second trigger (the first one is "a run today"). Run-now also carries `idempotency_token = watchdog-<job>-<date>`. A failed or timed-out triggered run is reported in the incident and never re-triggered. |
| 4 | **Endpoint wake-up first** | `wake_endpoints()` (#42) runs before the watchdog. Each job's own first task also wakes them (dev job) or its connect does (generator). |
| 5 | **Hard-coded allowlist of the two dev jobs, never prod** | `ALLOWLIST = {"generator": "[dev ci_dev] netsuite_daily_generator", "ingestion": "[dev ci_dev] netsuite_ingestion_daily"}`: exact names, resolved to ids at run time. Any other id, and any name without the `[dev ci_dev] ` prefix, is refused in code (tested). Second layer: ci-dev has **no permission** on the prod job `netsuite_ingestion_daily` (owner ci-prod), so the API would refuse it too. |
| 6 | **Run permissions ci-dev needs** | **None new.** ci-dev is `IS_OWNER` of both dev jobs, which includes run (checked 2026-10-13). The generator runs as data-generator (job-level `run_as`); run-now keeps that identity and needs no "Service Principal User" (only setting `run_as` does). See below. |
| 7 | **Incident labelled self-healed, with the outcome** | One ORCHESTRATION incident per day. The summary starts `[self-healed]` (all triggered runs succeeded) or `[self-heal failed]`. `evidence.self_healed` lists per job: trigger time, run id, result, duration, or why it was not triggered. The model's RCA (why the scheduler did not fire) follows, with the schedule facts from #41. |
| 8 | **Dry-run mode** | `--write` off (the default locally; the workflow's `write` input): no trigger. The report lists what it *would* trigger. Kill switch: repository variable `WATCHDOG_TRIGGER=off`. |
| 9 | **Eval case from the 10-11 fixture** | New fixture `evals/incidents/2026-10-11_scheduler_stopped_three_days.yaml`. Cassette `2026-10-11_afternoon` is recorded as of 2026-10-11 13:00, when both jobs had missed today. A fake trigger client (no real runs) returns SUCCESS. Deterministic checks: generator triggered before ingestion, label `[self-healed]`, outcomes in the report. Judge: the fixture's must-identify / must-not-claim items. |
| 10 | **Timeout covers both jobs with margin** | Waits: generator up to 20 min (observed ~1 min), dev ingestion up to 45 min (observed 6-15 min). Plus the monitor itself (~5-10 min). The workflow `timeout-minutes` goes from 30 to **90**. A wait that times out is an outcome (incident), not a retry. |
| 11 | **`databricks-workspace` concurrency group** | Unchanged: the monitor job already runs in it (`cancel-in-progress: false`). While the watchdog waits for a triggered run, it holds the group, so a deploy-dev queues behind it rather than deploying in the middle of the run. |

Also included:

* **Workspace refuses runs** (`DENY_NEW_AND_EXISTING_RESOURCES` / `INACTIVE`): the incident says "could not trigger: workspace inactive; press Start in the console", and the run fails (GitHub email).
* **Bug fix: a multi-day outage was never reported.** `missed_schedules` counted every miss before the first scheduled (PERIODIC) run in the 3-day window as `unconfirmed`. On 10-13 (manual runs only) the monitor said "Healthy" despite 10-11 and 10-12. Now a miss is also confirmed when it comes after the time the current schedule is known to be in place: the later of the job's creation time and the last successful deploy-dev run (the schedule cannot have changed since). It stays unconfirmed only before that, or when the deploy history is unavailable and no PERIODIC run proves the schedule.
* **No backfill** (owner 2026-10-13): only today's fire times. The generator's batch date is its run's start date, so a late trigger makes today's batch.

## Run permissions (evidence, 2026-10-13; no grant needed)

| Job | Owner | ci-dev | data-generator |
|---|---|---|---|
| `[dev ci_dev] netsuite_daily_generator` (run_as data-generator) | ci-dev | IS_OWNER | CAN_VIEW |
| `[dev ci_dev] netsuite_ingestion_daily` (run_as ci-dev) | ci-dev | IS_OWNER | CAN_VIEW |
| `netsuite_ingestion_daily` (prod, run_as ci-prod) | ci-prod | **none** | none |

## Known limits

* GitHub starts the scheduled monitor hours late (12:30-15:30 UTC observed). A missed 05:00 run is self-healed in the afternoon of the same day.
* If GitHub itself does not run the monitor, nothing triggers. GitHub disables schedules after 60 days without repository activity.
* A dev job that is already running when the monitor starts is waited for by nobody. The next monitor run sees its result.

## Tests

* Unit, with a fake Jobs client: due or not (grace period, any run today, an active run, a paused schedule); generator first; dev only after a generator SUCCESS; no dev trigger after a failure or timeout; the idempotency token; allowlist refusals (another name, a prod-like name); dry run; kill switch; workspace inactive; the incident label and evidence; the missed-schedule confirmation rule (10-11..10-13 shape).
* Eval: the 10-11 case (rule 9).
* Dev: the next real skipped day. Or, with the owner's OK, pause the dev schedules for one cycle to simulate a skip, then unpause.
