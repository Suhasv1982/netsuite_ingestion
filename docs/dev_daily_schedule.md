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
                             "--config", "../tools/increment_daily.yaml", "--backup", "schema-only"]
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

## Blockers (to settle before it is deployed)

1. **Backups per run.** Every `--increment` creates a no-expiry Lakebase branch and a full schema copy of the five
   tables. `netsuite-sample` has 5 of 10 branches, so a daily run exhausts the branch quota in 5 days (the branch
   step then fails "best effort") and the schema copies grow without bound. Needed: a `--backup schema-only` mode
   and a retention rule (for example keep the last 7 daily schema copies). Deleting old backups is a deletion, so
   the retention rule itself needs the owner's approval.
2. **The generator shells out to the Databricks CLI** (`pg_writer.connect` mints the database token with
   `databricks postgres ...`). A job task has no CLI; it needs a `databricks-sdk` code path
   (`WorkspaceClient().postgres.generate_database_credential`).
3. **Identity and rights.** The job runs as the deploying identity (ci-dev once step D lands), which today has
   read-only access to the source. The generator needs INSERT/UPDATE on `netsuite.*` and CREATE on the database
   (backup schemas). Options: widen ci-dev on the source, or a separate `data-generator` service principal
   (preferred: CI keeps read-only on the source).
4. **The source is shared with prod.** The generator writes to `netsuite-sample/production`, which prod also
   reads. Prod is paused, but its first run (the bronze rebuild) will ingest everything generated until then, and
   the dev-vs-prod comparison of the first prod release needs a quiet source: the schedule must be paused for the
   release window.

## Once unblocked

Deploy with the schedule PAUSED, run it once by hand, check `run_audit`, the `guard` row and the SOFT check
(`tools/check_soft_expectations.py`), then ask the owner to unpause.
