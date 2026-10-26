# Plan: reduce Free Edition usage (one chained daily job, quota-aware watchdog)

Status: **plan approved by the owner on 2026-10-26** (decisions in section 1a; written 2026-10-19). Dev only; prod stays on hold. **Nothing is
built or deployed by this plan.** Owner request 2026-10-19, after the Free Edition fair-use quota was reached on
2026-10-18 (incident 32: no scheduled run on 10-18/10-19; run-now refused with "Triggering new runs for organization
<id> is currently disabled temporarily"; SQL warehouse and foundation model endpoints refused too).

Nothing in this plan may be deployed or merged while the quota is exhausted: every merge to `main` starts deploy-dev,
which runs one ingestion job.

## 0. Facts (checked 2026-10-19, read-only, from `databricks.yml` and `resources/`)

| Fact | Value |
|---|---|
| Scheduled dev jobs today | generator 05:00 (run_as data-generator); `netsuite_ingestion_daily` 05:30 (ci-dev, 5 tasks: refresh_credentials, run_pipeline, log_run_audit, sync_ledger, ledger_check); `knowledge_refresh` 07:15 (ci-dev); `schema_drift` 07:30 (dq-agent); `dq_validator` 07:45 **and** 08:45 (ci-dev); `dq_recommender` Sundays 08:00 (dq-agent) |
| Cron triggers per day | 6 (7 on Sundays); each job run starts its own serverless compute for each task |
| Run identities | three different ones (data-generator, ci-dev, dq-agent). A Databricks job has one `run_as`, so one job with all the tasks would run everything as one identity and break the least-privilege design (grants/*.yml) |
| Free Edition | at most 5 concurrent tasks; fair-use quota ("compute shut off for the rest of the day" in the limitations page; this time the refusal lasted more than a day) |
| Watchdog (GitHub monitor, 06:17) | triggers at most the generator and the ingestion job, once per job per day, never a retry; a refused run-now is recorded as REFUSED. Kill switch `WATCHDOG_TRIGGER=off` (set 2026-10-19) |
| Monitor on 10-18/10-19 | wrote 2 incidents a day (ORCHESTRATION "[self-heal failed]" and OTHER for the warehouse refusals), called the model each time (the call itself was refused on 10-19) |
| Monitor tools that use the SQL warehouse | `compare_bronze_to_source` (bronze counts), `get_recent_deploys` (system.access.audit) |

## 1. Decisions for the owner

1. **Chain shape: a parent job of `run_job_task`s** (recommended), `[dev ci_dev] netsuite_daily_chain`, cron 05:00
   UTC, run_as ci-dev. Each task runs the existing child job, which keeps its own `run_as`:
   `generator -> ingestion -> knowledge_refresh -> schema_drift -> dq_validator`. The children lose their own
   schedules (they become trigger-less jobs, still runnable by hand and by the watchdog through the parent).
   To verify first (step 1): that a run_job_task child runs as the child's `run_as`, not the parent's, and what
   permission the parent's identity needs on each child (expected: CAN_MANAGE_RUN; ci-dev deploys all of them).
   Alternative: one job with every task as ci-dev: fewer moving parts, but dq-agent and data-generator lose their
   separate identities. Not recommended.
2. **Failure semantics.** generator -> ingestion `ALL_SUCCESS` (no ingestion of a day that was not generated, as the
   watchdog does today). knowledge_refresh, schema_drift and dq_validator `run_if: ALL_DONE` (they read metadata
   and the source, not the day's bronze, so a failed ingestion does not have to stop them). Recommended.
3. **Validator once a day**, last in the chain (it validates the drift proposals of that run). The 08:45 run goes.
4. **The Sunday recommender:** (a) a `condition_task` "is it Sunday (UTC)" in the chain, then `dq_recommender`, then
   the validator, so the validator still runs once and sees both agents' proposals (recommended); or (b) keep its
   own Sunday 08:00 schedule and accept a second validator run on Sundays.
5. **Quota day = one deterministic incident, no model call.** When the watchdog's run-now or a tool is refused with a
   known quota message, the monitor writes **one COMPUTE_QUOTA incident per day** (fingerprint
   `compute_quota:<date>`), folds that day's other signals into it (missed schedules, tool errors from refused
   warehouse calls, the refused trigger) as evidence, and **does not call the model** that day. Alternative: still
   investigate non-quota signals with the model. Not recommended: on 10-19 the model was refused too.
6. **Quota markers** (exact substrings, matched anywhere in the text, like the error markers of #79):
   `is currently disabled temporarily` (run-now), `Cannot create or query foundation model endpoints`,
   `The request could not be processed by the warehouse`, `RESOURCE_EXHAUSTED`. New ones are added only from a real
   occurrence.
7. **No retries on a quota day:** after the first quota refusal of the day the watchdog stops (does not try the
   next job), and on the following days it tries **once** (one run-now of the parent), which also serves as the
   probe that the quota has reset. With `WATCHDOG_TRIGGER=off` it only reports.

## 1a. Owner decisions (2026-10-26)

| # | Decision |
|---|---|
| 1 | Parent job of `run_job_task`s; each child keeps its own `run_as` (as recommended) |
| 2 | generator -> ingestion `ALL_SUCCESS`; the three later jobs `ALL_DONE` (as recommended) |
| 3 | Validator once a day, last in the chain; the 08:45 run goes |
| 4 | Option (a): Sunday `condition_task` in the chain, then `dq_recommender`, then the validator |
| 5 | Quota day = one deterministic COMPUTE_QUOTA incident, no model call (as recommended) |
| 6 | The quota markers as listed, unchanged |
| 7 | No retries; one probe run-now of the parent per day (as recommended) |

* **Step 1 spike approved:** the throw-away parent job, one dev deploy after the quota reset, deleted afterwards.
* **Watchdog:** the GitHub workflow `monitor-dev.yml` was disabled on 2026-10-26 (owner). Turn it back on as soon as
  the quota resets, with `WATCHDOG_TRIGGER=off` (report only). Turn triggering on (`WATCHDOG_TRIGGER=on`) after this
  plan's build is merged (step 5).

## 2. Steps (one PR each, dev only)

1. **Spike (read-only plus one dev deploy after the quota reset, owner's OK):** a throw-away parent job with one
   run_job_task on a trivial child that prints `current_user`; confirms the child's run identity and the permission
   needed. Result written into this plan before step 2. Deleted afterwards (owner's OK).
2. **Chain in the bundle** (`databricks.yml`): the parent job with the five run_job_tasks (and decision 4), children
   without schedules, the validator's second cron removed. The watchdog's allowlist becomes the parent only
   (ingestion is no longer triggered on its own; the parent's own dependency does the "generator first" rule).
   Tests: `tests/test_bundle_targets.py` (dev has exactly one scheduled job, the chain order and `run_if` per task,
   no schedule on a child, prod unchanged and PAUSED). Monitor: `get_recent_job_runs` learns the parent job
   (`missed_schedules` on the parent; the children's runs are listed under it).
3. **Quota-aware watchdog and monitor** (decisions 5-7): `logic.QUOTA_MARKERS`, the watchdog stops on the first
   quota refusal and reports `QUOTA`; triage builds one COMPUTE_QUOTA group per day from the quota refusal and folds
   the day's other signals into it; the graph skips `investigate` for that group and writes a deterministic summary
   (what was refused, since when, what did not run). Builds on #79 (facts step); merged after it.
4. **Eval:** a case `quota_refused_10_18` with a cassette derived from a recorded healthy one by replacing the
   refused results with the exact 10-18 texts (a live recording is impossible after the reset); checks: exactly one
   incident, category COMPUTE_QUOTA, no model call, the trigger attempted once. The watchdog case
   `watchdog_self_healed_10_11` is re-pointed at the parent job. Run only these two cases (eval policy).
5. **STATUS** and `docs/watchdog_plan.md` updated; `WATCHDOG_TRIGGER` back to `on` **only with the owner**, after the
   quota has reset and step 3 is deployed.

## 3. Expected effect

| | Today | After |
|---|---|---|
| Scheduled triggers per day (dev) | 6 (7 on Sundays) | 1 |
| Validator runs per day | 2 | 1 |
| Monitor on a quota day | 2+ incidents, model calls | 1 incident, no model call |
| Watchdog on a quota day | one refused run-now per job per day | one refused run-now (the probe) per day |

The number of task runs (each with its own serverless start) barely changes: the chain adds one parent task per
child. The saving is in fewer separate runs, the validator's second run, and no model or warehouse calls on quota
days. Further cuts, not in this plan (owner's call): run `knowledge_refresh` only when `config_change_log` or the
docs changed; merge the ingestion job's three small post-pipeline tasks into one.

## 4. Not in this plan

Prod (on hold, owner 2026-10-19), migrations (COMPUTE_QUOTA already exists), grants (only the run-permission the
spike finds, shown first), the monitor's own schedule (GitHub, 06:17).
