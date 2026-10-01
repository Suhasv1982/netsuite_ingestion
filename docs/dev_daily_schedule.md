# Proposal: daily dev schedule (not deployed, not unpaused)

Goal: every day, append a small realistic increment to the source and run the dev job, so dev always has fresh
data flowing through bronze, DQ, silver and gold, and regressions show up without anyone starting a run.
**Nothing below is deployed.** Unpausing needs the owner's OK, and items 1 to 4 under "Blockers" must be settled
first.

## Shape

One dev-only job, `dev_daily_increment`, defined under `targets.dev.resources` so it never exists in prod:

```yaml
targets:
  dev:
    resources:
      jobs:
        dev_daily_increment:
          name: dev_daily_increment
          schedule:
            quartz_cron_expression: "0 0 9 * * ?"   # 09:00 UTC daily
            timezone_id: UTC
            pause_status: PAUSED                     # dev mode pauses it anyway; unpause only with the owner's OK
          queue: { enabled: true }                   # a CI smoke run in progress queues it instead of skipping it
          max_concurrent_runs: 1
          tasks:
            - task_key: generate_increment
              spark_python_task:
                python_file: ../tools/netsuite_gen.py
                parameters: ["--increment", "--seed", "{{job.run_id}}",
                             "--batch-date", "{{job.start_time.iso_date}}",
                             "--config", "../tools/increment_daily.yaml",
                             "--auth", "sdk", "--backup", "schema", "--backup-keep", "7"]
              environment_key: tools
            - task_key: run_ingestion
              depends_on: [{ task_key: generate_increment }]
              run_job_task: { job_id: "${resources.jobs.netsuite_ingestion_daily.id}" }
          environments:
            - environment_key: tools
              spec: { client: "3", dependencies: ["faker>=30", "pyyaml", "psycopg[binary]>=3.1", "databricks-sdk>=0.94.0"] }
```

* **Time: 09:00 UTC.** CI only touches the workspace on pull requests and merges (event-driven) and the canary is
  manual, so no fixed CI slot exists to collide with; 09:00 UTC is outside the prod job's 06:00 UTC slot (prod is
  paused, but this keeps the two apart if it is unpaused). A deploy-dev
  smoke run that overlaps is queued by `queue.enabled` rather than dropped.
* **Batch date = the run date; seed = the job run id** (`--seed` takes an integer, and the run id is unique and
  recorded, so any day's data can be regenerated). Without
  `--batch-date` each increment is placed 21 days after the latest date, so daily runs would move the synthetic
  dates into 2027 within days, where the HARD rule `certification_start_date < DATE '2027-01-01'` rejects every
  new certification.
* **Low defect rates** (`tools/increment_daily.yaml`): 5% late rows (top-ups), 0.5% each of invalid enums (HARD
  rejects), end-before-start and amount mismatches (the two SOFT rules in dev metadata).

## Blockers

1. **Backups per run: resolved (2026-10-01).** `--backup schema` takes only the verified schema copy, named
   `netsuite_backup_daily_<stamp>`, and no Lakebase branch; `--backup-keep 7` then drops daily copies beyond the
   newest 7. Only `netsuite_backup_daily_*` schemas are ever dropped: the historical `netsuite_backup_<stamp>` copies
   and the `pre-synthetic-backup-*` branches are never touched. Tested; a local `--dry-run` of the daily command works.
2. **CLI dependency: resolved (2026-10-01).** `--auth sdk` mints the database credential with `databricks-sdk`
   (`WorkspaceClient().postgres`) as the job's run-as identity. Not yet run inside a job (needs blocker 3).
3. **Identity and rights: owner decision.** The writer needs INSERT/UPDATE on `netsuite.*` and CREATE on the source
   database (backup schemas). That is a grant on the source prod also reads, so it is a prod-touching grant.
   Options: widen ci-dev (CI then writes prod's source), or a separate `data-generator` service principal with only
   those rights (preferred).
4. **The source is shared with prod: owner decision.** Prod's schedule is on hold; once unpaused it ingests whatever
   the generator wrote. Either accept synthetic daily data in prod, or keep the daily dev schedule paused while prod
   runs, or point dev at its own source branch (a `netsuite-sample` branch for dev; that also changes dev's
   `source_pg_host`).

## Once unblocked

Deploy with the schedule PAUSED, run it once by hand, check `run_audit`, the `guard` row and the SOFT check
(`tools/check_soft_expectations.py`), then ask the owner to unpause.
