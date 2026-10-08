# Plan: run the monitor agent as the final task of the dev daily job

Status: plan, not implemented (owner request 2026-10-08). Dev only. Until it ships, the GitHub workflow
`monitor-dev.yml` keeps a daily schedule, moved to `17 6 * * *` (06:17 UTC).

## Goal

The monitor + RCA agent (`src/aidq_agent`, docs/phase4_agent_plan.md) runs right after the dev pipeline, inside the
`[dev ci_dev] netsuite_ingestion_daily` job, instead of on a GitHub schedule that starts up to ~7 h late. The GitHub
workflow stays for manual runs (`workflow_dispatch`) only.

## Identity

| Option | Run identity | Rights it needs | Notes |
|---|---|---|---|
| **A (recommended first)**: a `monitor` task in the daily job | **ci-dev** (the dev job's `run_as`; Databricks has one run identity per job, not per task) | what the GitHub monitor uses today as ci-dev: job/pipeline reads, SQL warehouse, source + dev metadata reads, INSERT on `incidents`, the free model endpoint | no new principal or grant; same identity as the monitor today |
| B (later, least privilege) | a new **monitor-agent** SP, in its own `monitor_dev` job (`run_as` monitor-agent) that the daily job starts as its final `run_job_task` | read-only job/pipeline/warehouse access; Postgres group role `aidq_monitor` = SELECT + INSERT on `incidents` only; not in `aidq_owner` | follows the principle of design v2 section 3 (an LLM-driven agent does not run as ci-dev, which is in `aidq_owner`); new SP, grants and role script, asked first |

The monitor's model only calls the five read-only tools, and its single write is fixed code (INSERT into
`incidents`), so A keeps today's risk unchanged. B removes ci-dev's `aidq_owner` membership from the agent's process.
Recommendation: ship A, plan B with the DQ recommender's identities.

## What has to change

1. **Reads without CLIs.** `aidq_mcp/reads.py` shells out to the `databricks` CLI and `gh api`; serverless job compute has neither. Move the Databricks reads to `databricks-sdk` (jobs, pipelines events, warehouses, Lakebase credentials), which the job's identity authenticates without configuration. Keep the CLI path for local runs.
2. **GitHub deploys from inside Databricks.** `get_recent_deploys` lists deploy-dev runs with a GitHub token. In the job: a fine-grained read-only token (Actions: read, this repository only) in the dev secret scope, read by the task (**new secret: ask first**). Without it the tool reports `unavailable`, as it already does for the audit log.
3. **Task.** `monitor` (`spark_python_task`, `python -m aidq_agent --write`), `depends_on` the last task (ledger_check, log_run_audit), `run_if: ALL_DONE`, so a failed pipeline is investigated the same morning. Environment: the repo package (`langgraph`, `mcp`, `psycopg`, `databricks-sdk`) as a serverless environment dependency. Timeout 30 min.
4. **No feedback loop.** Today the GitHub job fails on purpose when it writes an incident (the failure email is the alert). Inside the daily job that would mark the daily run FAILED and make the next monitor run report it. Instead the `monitor` task exits 0 after writing, and alerts by a task value / job notification on a separate condition. Options to decide:
   * (a) the `monitor` task fails, and triage and `daily_check` ignore a run whose only failed task is `monitor`, or
   * (b) a separate `monitor_dev` job (option B's job) with its own `on_failure` email, started by `run_job_task` with the daily job not waiting on its result.
   Recommendation: (b) when B ships; (a) until then.
5. **No watchdog outside the job (owner decision 2026-10-08).** If the scheduler does not fire (incident 2026-10-06), the monitor inside the job does not run that day either. The next day's run still reports it: `get_recent_job_runs` looks back 3 days and lists the missed fire time (`missed_schedules`), so a skipped day is caught one day late. **Accepted risk:** while the scheduler stays down, nothing reports. Someone has to notice the missing daily email or reports, or run the workflow by hand.
6. **Rollout.** Deploy to dev through a PR (deploy-dev); first run dry (`--write` off) for one cycle; then `--write`; then remove the GitHub `schedule` (keep `workflow_dispatch` only).

## Tests and checks

* Unit: the SDK reads (fakes, like `tests/test_aidq_mcp_server.py`), the task's exit code rules (point 4), and triage ignoring the monitor task's own failure (if (a)).
* Dev: one dry cycle with the report in the task output; compare with the GitHub run of the same day (same signals).
* The eval (`evals/run_agent_eval.py`) is unchanged: it replays recorded tool outputs.
