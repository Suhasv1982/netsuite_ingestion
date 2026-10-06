# STATUS (updated 2026-10-06)

Read this first, then run `git status` and check open PRs. Updated after every Phase 2 step.

## 1. Current state

**Repo.** `Suhasv1982/netsuite_ingestion`, public. `main` is protected: pull request required (0 approvals),
required checks `gitleaks (full history)`, `pytest` and `bundle validate` (added 2026-09-30), administrators
included, no force push or deletion. Secret scanning and push protection on. Local pre-push hook: gitleaks + pytest.

**Pull requests.** Merged: #1-#20, #21 (daily schedules). Required checks on `main`: `gitleaks (full history)`,
`pytest`, `bundle validate`, `migrate plan (dev)`.

**Work in progress** (owner's instructions of 2026-09-30):
* Part 1, close out Phase 1 in one dev-only PR: gold layer, SOFT-rule expectation check, guard read-retry count,
  README, proposed daily dev schedule (shown, not unpaused).
* Part 2, Phase 2 CI/CD, one PR per step: A ci-dev spike, B bundle validate, C `tools/migrate.py`, D deploy-dev,
  E canary, F promote-prod. Stop after F and report.

**Pre-approved** (2026-09-30): ci-dev resources (SP, OAuth secret, Lakebase roles, grants, scope
`netsuite_ingestion_dev` with ci-dev WRITE); the three `DATABRICKS_*_DEV` GitHub secrets; workflows on feature
branches and PRs; merging a PR once all checks are green and it touches no prod resources.
**Ask first:** anything touching prod; deleting or destroying anything; branch protection; history rewrites.

**Schedules.** prod `netsuite_ingestion_daily`, dev `netsuite_ingestion_daily` and dev `guard_canary_check` are
all PAUSED (both the `[dev suhasv]` and the `[dev ci_dev]` sets).

**Lakebase endpoints.** Found disabled on 2026-09-30 (since 2026-09-27) and again at about 20:15 UTC the same day,
about 3.5 h after last use; most likely the Free Edition compute quota (not verified). Re-enabled: dev metadata and
source (owner's OK), prod metadata (by the owner, 20:53 UTC). Nothing runs on prod: its schedule is PAUSED.

**Metadata databases (Lakebase project `aidq-metadata`).**

| | Branch | Migrations | Owner of `aidq_metadata` |
|---|---|---|---|
| dev | `dev` | 001, 002 recorded (backfill, schema verified); 003 applied by the first deploy-dev run | `aidq_owner` (no-login; members: owner, ci-dev) |
| prod | `production` | **none applied**, no `schema_migrations` | the owner (no `aidq_owner`, no ci-dev role yet) |

**Source database (`netsuite-sample`):** production plus backups `pre-synthetic-backup-202609242053`,
`...202609252235`, `...202609260101`, `...202609260109` (5 of 10 branches).

## 0. First prod release (2026-10-01, steps 0-7 done; step 8 open)

| Step | Result |
|---|---|
| 0 bind | existing prod job `netsuite_ingestion_daily` and pipeline `netsuite_ingestion_poc` bound into ci-prod's bundle deployment (`bundle deployment bind`); plan showed in-place updates, 0 deletes |
| 1 backups | Lakebase branch `aidq-metadata/pre-release-202610010053`; schema copy `aidq_metadata_backup_202610010053` (5 tables, counts verified); JSON snapshot in `baseline/raw/` (local) |
| 2 migrations | 001, 002, 003 applied on prod metadata as `aidq_owner` (`--plan` first; PR #14 fixed the plan to dry-run files cumulatively) |
| 3 deploy | promote-prod (bb48fef, `bronze_rebuild=true`): job and pipeline run as ci-prod, scope `netsuite_ingestion_prod`, schedule PAUSED; guard canary created. First attempt failed (403 on bundle ACLs) -> PR #15 dropped bundle `permissions`; job owner moved to ci-prod; the pipeline stays owned by the owner (only a metastore admin, `System user`, can change a pipeline owner) |
| backup of prod tables | `poc_netsuite.backup_pre_release_202610010127`: 22 tables (17 per-date bronze views + customers, 4 silver, rejects) via CTAS (deep clone is refused for MVs/STs), counts verified. **Keep until step 8 plus a few good scheduled runs; ask before dropping.** |
| 4-5 rebuild and compare | dev run 648860584880106, then prod `full_refresh` run 210819933825138 (first attempt failed in setup: ci-prod lacked CREATE on `poc_netsuite.default`; granted). Source exact counts equal before and after. **Dev = prod exactly** for bronze (5), silver (4), rejects as (table, rule, key) sets (54), ledger keys (57,552) and fingerprints (53), gold (5,280 / 1,861); no extra dev `_snapshot_date`. Prod `ledger_check` OK |
| 6 normal mode | promote-prod (`bronze_rebuild=false`, one run 613901313133793): 0 new bronze rows, `ledger_check` OK, guard WARN (2 reads) |
| 7 old views | the 16 per-date materialized views (`<table>__2026_06_20/07_11/08_01/08_22`) dropped as ci-prod after confirming no current dataset defines them; silver, rejects, gold counts unchanged; copies remain in the backup schema |
| 8 schedule | **on hold (owner, 2026-10-01)**: prod schedule stays PAUSED |

Access changes made during the release: ci-prod CAN_MANAGE on project `aidq-metadata` (backup branches), ci-prod
CREATE TABLE/MV on `poc_netsuite.default`, owner USE SCHEMA + SELECT on the prod and dev pipeline schemas (tables are
owned by the run identities). PR #16 codifies the UC grants (additive step in CI; needs the `GRANT_OWNER_PRINCIPAL`
repository variable). `aidq-metadata` has 6 of 10 branches (4 `pre-release-*`, one per promote-prod run plus the
step-1 backup); promote-prod now prunes them (below).

## 0b. After the release (2026-10-01)

* UC grants are codified (PR #16): `grants/<target>.yml`, applied additively by `tools/apply_grants.py` in deploy-dev
  and promote-prod (repository variable `GRANT_OWNER_PRINCIPAL`). Today a no-op: every grant is present.
* Migration 004 (PR #18): no PUBLIC EXECUTE on `enforce_proposal_lifecycle()` / `log_config_change()`. Applied on
  dev by deploy-dev (audit trigger verified to still fire); prod gets it with the next approved promote-prod run.
* Backup-branch retention: promote-prod runs `tools/prune_backup_branches.py` after its backup: keeps the newest 3
  `pre-release-*` branches and every one younger than 14 days; production, dev and other names are never touched.
  Today it deletes nothing (all four are from 2026-10-01).
* Not done, on purpose: MANAGE for the CI identities on their schemas (the grant step can then restore a missing
  grant instead of failing; for prod that is a prod grant change: ask first); dropping
  `poc_netsuite.backup_pre_release_202610010127` (condition: step 8 plus a few good scheduled runs).

## 0c. Daily operation (owner decisions 2026-10-01)

* **The generator is the source system.** `netsuite_daily_generator` (dev-only job) appends one day of synthetic
  data to `netsuite-sample/production`, which dev and prod both read; prod accepts it. Defect rate stays low
  (`tools/increment_daily.yaml`). Heavy defect / schema-drift experiments go later to an on-demand dev branch of
  netsuite-sample, not to this source.
* **Schedules (UTC, staggered):** generator 05:00, dev `netsuite_ingestion_daily` 05:30 (both unpaused), prod
  `netsuite_ingestion_daily` 06:30 (**PAUSED until step 8**: after one good day of generator + dev, the owner approves
  a promote-prod run with `schedule_pause_status=UNPAUSED`).
* **data-generator** service principal: the generator job's run identity (job-level `run_as`), no OAuth secret.
  Source grants (`grants/source.yml`, applied 2026-10-01, exact match verified): USAGE on `netsuite`; SELECT +
  INSERT on the four incremental tables; SELECT + INSERT + UPDATE on `netsuite_customers`; CONNECT + CREATE on the
  database (its `netsuite_backup_daily_*` schemas; Postgres cannot restrict CREATE to a name prefix). No DELETE,
  TRUNCATE, REFERENCES, TRIGGER, ALTER, role memberships, or access to metadata, UC or secrets. deploy-dev runs
  `tools/apply_pg_grants.py --check` and fails on anything missing or extra. ci-dev has the "Service Principal User"
  role on data-generator (to deploy a job that runs as it); data-generator has CAN_VIEW on the dev bundle.
  Repository variable `DATA_GENERATOR_SP`.
* **First generator runs (2026-10-01):** run 1 aborted on import (`psycopg-binary` 3.3 wheel; PR #22 switched to plain
  `psycopg`), nothing written. Run 2 wrote one increment (+100 customers, +397 memberships, +318 certifications,
  +1,590 transactions, +4,770 lines; backup schema `netsuite_backup_daily_202610011947`) but (a) was reported FAILED
  only because of `sys.exit(0)` and (b) dated updated rows up to +2 days (source watermark now **2026-10-03**). Fixed:
  exit code, `--update-spread-days 0` and `--skip-if-not-after-watermark` for the daily job, so the 2026-10-02 and
  10-03 runs skip cleanly and real daily increments resume on **2026-10-04**. Dev and prod ingest the rows dated
  10-02/10-03 like any other dates.
* **Rows dated in the future, kept on purpose (owner decision 2026-10-01):** generator run 2 (2026-10-01) wrote
  updated versions with `updated_date` 2026-10-02 and 2026-10-03 (memberships 93 + 90, transactions 373 + 359, and
  the matching certifications and lines). They entered the source on 10-01. Keep them as a natural "future
  updated_date" defect for the Phase 4 rule recommender. They are not a pipeline bug: bronze loads them as their
  own snapshot dates like any other.
* **Owner offline until 2026-10-05.** Failure emails for the two daily dev jobs (generator, dev
  `netsuite_ingestion_daily`) go to the `NOTIFICATION_EMAIL` repository variable's address (bundle, `on_failure`).
  **On 2026-10-05: run `python tools/daily_check.py --date <d> --profile DEFAULT` for 2026-10-02, 10-03, 10-04 and
  10-05** (read-only; exit 1 on any FAIL). Expected (owner decision 2026-10-01: generate every day): the generator
  writes an increment **every day, 10-02 to 10-05** (`--allow-before-watermark`: the 10-02 and 10-03 batches land on
  dates the source already holds since run 2, so dev picks them up through top-up flows as late rows on loaded dates;
  `--skip-if-batch-exists` keeps a same-day re-run from writing twice); dev job SUCCESS each day with ledger OK; dev
  bronze equal to the source; no prod runs (paused). Note: on 10-02 and 10-03 some new versions of existing rows are
  dated before those keys' 10-03 versions from run 2; silver (AUTO CDC by updated_date) correctly keeps the later one,
  so silver grows less than bronze on those days.
  **Then decide on step 8** (promote-prod, `schedule_pause_status=UNPAUSED`, approved in the `prod` environment;
  prod cron 06:30 UTC; the first scheduled prod run catches up on run 2 and every increment since; also brings
  migration 004 to prod).
* Backups of the source by the daily job: schema copies `netsuite_backup_daily_<stamp>` only (no Lakebase branch),
  newest 7 kept; historical `netsuite_backup_<stamp>` copies and `pre-synthetic-backup-*` branches are never touched.
* **ci-dev MANAGE** on the six dev pipeline schemas (granted 2026-10-01), so the dev grant step can restore a missing
  grant. **Prod: no MANAGE for ci-prod by decision**; a missing prod grant fails the prod grant step.
* **Prod table backup** `poc_netsuite.backup_pre_release_202610010127`: keep until step 8 plus three good scheduled
  prod runs, then ask the owner before dropping.

## 0d. Daily checks 10-02 to 10-05 (run 2026-10-06)

* Generator and dev `netsuite_ingestion_daily` SUCCESS every day (scheduled), ledger OK, guard WARN (2 reads), no
  prod runs. The source and dev metadata endpoints were found disabled again (last active 10-05 about 05:35 UTC)
  and re-enabled with the owner's OK; prod metadata is still disabled.
* **Incident 1: same-day duplicate versions not loaded.** Dev bronze was short of the source by 287 rows
  (memberships 24, certifications 6, transactions 63, lines 194), all on snapshot dates 10-02 and 10-03. Cause:
  generator run 2 (10-01) wrote versions dated 10-02/10-03; the 10-02/10-03 batches (`--allow-before-watermark`)
  then wrote a second version of some of the same keys with the same `updated_date` (a plain date). The key ledger
  and the per-date fingerprints count distinct (business_key, date) pairs, so such a version changes no
  fingerprint and no top-up loads it; bronze and silver keep the 10-01 version. **Known pipeline limitation**: a
  source that updates a key twice in one day loses the second version. Owner decision (10-06): keep the 287 rows
  as a defect; the generator no longer writes a version on a (key, date) the source already holds;
  `daily_check.py` compares per table and snapshot date and reports a date short by exactly its same-day
  duplicates as a known-defect WARN (duplicates that arrive in one batch, e.g. 07-11, are loaded and stay OK).
* **Incident 2: no scheduled runs on 10-06.** Both dev schedules UNPAUSED, no job change in the audit log since
  10-04, no deploy-dev since 10-01, and no `runTriggered` on 10-06: the scheduler never fired. Recorded as a
  platform cause (Free Edition, idle workspace suspected, not confirmed).
* Prod job cron is `0 0 6 * * ?` (06:00 UTC), not 06:30 as planned: settle before step 8.

## 0e. Phase 3: read-only MCP server (2026-10-06)

* `src/aidq_mcp` (plan: `docs/phase3_mcp_plan.md`, PRs #29, #30): five read-only tools, dev only:
  `get_table_health`, `get_recent_job_runs` (with `missed_schedules` / `missed_schedules_unconfirmed`),
  `get_pipeline_errors`, `compare_bronze_to_source`, `get_recent_deploys`. Runs as the owner's CLI profile;
  read-only enforced in code (read-only Postgres sessions, fixed SELECT templates, get/list CLI calls only,
  tested). A disabled endpoint is reported `unavailable`, never enabled. `env="prod"` is refused.
* Run: `PYTHONPATH=src .venv/Scripts/python -m aidq_mcp --profile DEFAULT [--smoke]`. Register for Claude Code
  (local scope, not committed): `claude mcp add aidq-netsuite --scope local -e PYTHONPATH=<repo>/src --
  <repo>/.venv/Scripts/python.exe -m aidq_mcp --profile DEFAULT`.
* **Incident replay (step C):** fresh headless Claude Code sessions limited to these tools (`--strict-mcp-config`,
  all built-in tools disallowed), started outside the repo so no STATUS/memory context. Both answers met every
  `must_identify` and no `must_not_claim` of the fixtures. Incident 1: gap on 10-02/10-03 only, equal to the
  same-day duplicates, ledger never loads them; it guessed (and flagged as unverified) that PR #27 should have
  backfilled. Incident 2: no trigger on 10-06, schedules UNPAUSED and unchanged since 10-01, no deploy before
  15:28; cause not found, platform listed as a possibility only.
* Found by the replay and fixed: with a 7-day window `missed_schedules` flagged 09-30 and 10-01, before the
  schedules existed. Misses before the first scheduled run in the window are now `missed_schedules_unconfirmed`.
* Next: Phase 4 (LangGraph monitor + RCA after the dev daily job, these tools only, writes incidents). Needs:
  migration 005 for incident categories `ORCHESTRATION` / `DATA_COMPLETENESS`, and (ask first) a read-only SP
  `aidq-reader` with INSERT on `incidents`.

## 1a. First CI dev run (2026-09-30, deploy-dev run on the #11 merge: green)

* Rebuild mode (poc_bronze was empty after `[dev suhasv]` was removed): migrations (003 applied by the previous,
  failed run, as `aidq_owner`), deploy with `bronze_rebuild=true`, one run, redeploy with `bronze_rebuild=false`.
* Job run 449191401642789 (`[dev ci_dev]`, as ci-dev): bronze = source in all five tables, silver 2,152 / 2,704 /
  32,710 / 10,902, rejects 32 + 25, ledger rebuilt and `ledger_check` OK for all four, guard OK in 1 read. Same
  numbers as the hand-deployed run earlier that day.
* Dev metadata: `schema_migrations` 001, 002 (backfill), 003 (`applied_by` aidq_owner); `netsuite_customers` has
  `watermark_col` NULL. Deployed dev: `bronze_rebuild=false`, scope `netsuite_ingestion_dev`, schedules PAUSED.
* The dev tables are owned by ci-dev (the pipeline's identity); the owner reads them through USE SCHEMA + SELECT
  on `workspace.poc_bronze`, `poc_silver`, `poc_reject`, `poc_gold` and `canary` (granted 2026-09-30).

## 1b. Phase 1 close-out (Part 1, 2026-09-30)

* Gold: `poc_gold.gold_customer_revenue` and `poc_gold.gold_customer_status` (materialized views over silver;
  SQL in `gold_sql.py`, tested on DuckDB). Schema `workspace.poc_gold` created for dev; **prod needs
  `poc_netsuite.poc_gold` before the first prod deploy with gold** (ask).
* Dev run (job run 888453864069675, `[dev suhasv]`, source unchanged since 2026-09-26): bronze = source
  (certifications 2,625, customers 2,200, memberships 3,281, transaction_lines 39,378, transactions 13,128);
  silver 2,152 / 2,704 / 32,710 / 10,902 (same as v2 step D); rejects 32 certifications + 25 memberships;
  ledger OK for all four; gold_customer_revenue 5,280 rows (2,039 customers, months 2026-05 to 2026-09, revenue
  68,527,268); gold_customer_status 1,861 customers, 408 with an active membership.
* Guard read count: first real run needed **2** event-log reads (`guard` row WARN in `run_audit`).
* SOFT rules: two added to **dev** metadata (rules 4 and 5: `Amount Equals Qty x Rate` on transaction_lines,
  `End Not Before Start` on memberships). A normal run with no new source data emits no expectation metrics
  (nothing flows). After a selective refresh of the two silver tables (update 9ab9415e), the transaction_lines
  rule is in the event log (38,781 passed, 597 failed = the 297 amount mismatches + 300 negative amounts of the
  baseline). The memberships rule was missing from that update (its flow wrote no
  metrics event), but a memberships-only refresh (update acc79388) shows it: 3,227 passed, 29 failed (= the 3,256
  valid rows). **Both SOFT rules confirmed.** A single update can lack a flow's metrics event, so one MISSING
  from `tools/check_soft_expectations.py` is a reason to re-check another update, not proof the rule is unwired.
* Proposed daily dev schedule: `docs/dev_daily_schedule.md` (not deployed; 4 blockers listed).

**Compute outage 2026-09-30, about 20:13 to 20:55 UTC:** Lakebase endpoints disabled again and the SQL warehouse
refused to start (`Cannot create the resource`). Both worked again after the endpoints were re-enabled.

## 2. ci-dev (Phase 2, step A: done 2026-09-30)

* Service principal `ci-dev`, OAuth M2M. One OAuth secret (90 days, expires 2026-12-29), the one in GitHub; the
  first secret was deleted on 2026-09-30 with the owner's OK.
* GitHub secrets `DATABRICKS_HOST_DEV`, `DATABRICKS_CLIENT_ID_DEV`, `DATABRICKS_CLIENT_SECRET_DEV`.
* Lakebase roles `ci-dev` (no admin membership) on `aidq-metadata/dev` and `netsuite-sample/production`.
  Grants: dev metadata USAGE on `aidq_metadata`, SELECT on `source_table_def`, `source_columns`,
  `data_quality_rules`, SELECT + INSERT on `run_audit`; source USAGE + SELECT on schema `netsuite` (default
  privileges for new tables), nothing on the backup schemas. Migration (DDL) rights: step C/D.
* UC: USE CATALOG `workspace`; USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY on
  `workspace.poc_bronze`, `poc_silver`, `poc_reject`, `poc_gold` (new, created 2026-09-30), `ledger`, `canary`,
  `default` (USE SCHEMA only since 2026-09-30: it holds other projects' tables). SQL warehouse: CAN_MANAGE (canary).
* Grant inventory 2026-09-30: nothing beyond the plan. BROWSE on other catalogs comes from `account users` (all
  users), not from a ci-dev grant. Another user (`sid.v@...`) held ALL_PRIVILEGES on `poc_netsuite`; reduced to
  USE CATALOG + USE SCHEMA + SELECT (read-only) on 2026-09-30 with the owner's OK.
* Scope `netsuite_ingestion_dev`, ci-dev WRITE (owner MANAGE). ci-dev has no access to `netsuite_ingestion_poc`.
* Verified as ci-dev: Lakebase login and the grants (allowed and denied cases), scope WRITE, `bundle validate`
  dev + prod, and `run_as` (accepted with `mode: development`; job and both pipelines run as ci-dev).
* A test deploy as ci-dev created the `[dev ci_dev]` set (2 jobs, 2 pipelines, schedules PAUSED, never run).
  It becomes the canonical dev in step D.

## 2b. bundle validate in PR checks (Phase 2, step B: PR #4)

* New job `bundle validate` in `.github/workflows/pr-checks.yml`: validates dev and prod (schedule PAUSED) as
  ci-dev, OAuth M2M; CLI 1.16.0 installed from the release with its checksum verified; job-level concurrency group
  `databricks-workspace`, `cancel-in-progress: false`; fork PRs skip with a notice, a same-repo PR without the
  secret fails. Public logs mask the host and client id (checked: no occurrence in the job log).
* `tests/test_bundle_targets.py`: dev and prod never share catalog, metadata key, endpoint or host; the ledger
  follows `${var.catalog}`.
* Next (owner's OK): add `bundle validate` to the required status checks of `main`.

## 2c. Migration runner (Phase 2, step C)

* `tools/migrate.py --env dev|prod --plan | --apply | --backfill UPTO`. `--apply`: each file plus its
  `schema_migrations` row in one transaction; stops at the first failure; refuses if an applied file's checksum
  changed. `--plan`: read-only, dry-runs pending files in a rolled-back transaction. Files contain no
  `BEGIN`/`COMMIT` (removed from 001/002 in this step, the one permitted edit; the runner rejects such files).
* Dev backfill done 2026-09-30: replaying 001+002 changed nothing in a catalog snapshot (columns, constraints,
  indexes, views, functions, triggers), so both were recorded with the checksums of the edited files.
* `003_blank_watermark_to_null`: blank `watermark_col` -> NULL plus a CHECK against blanks. Dry run OK on dev;
  applied by deploy-dev (step D). The code already treats blank and NULL the same.
* Every migration transaction starts with `SET LOCAL ROLE aidq_owner` (dry runs and backfill too), so what a
  migration creates is owned by that no-login role; `migrate.py` refuses to run if the role is missing or the
  caller is not a member.
* `migrate plan (dev)` job in pr-checks (as ci-dev, concurrency `databricks-workspace`). The CLI install is now
  the local composite action `.github/actions/databricks-cli`.
* **Blocked: ci-dev has no migration rights on dev metadata.** Creating a no-login owner role and moving
  ownership of `aidq_metadata` objects to it (so ci-dev can run DDL) was refused by the tool's permission
  classifier; the owner runs it (SQL in the Phase 2 report). Until then `migrate plan (dev)` fails in CI with a
  permission error, and deploy-dev cannot apply migrations.

## 2d. deploy-dev (Phase 2, step D)

* `.github/workflows/deploy-dev.yml`: on push to `main` (and on demand) as ci-dev: `migrate --plan` + `--apply`
  on dev metadata, `bundle deploy -t dev`, one normal smoke run of `netsuite_ingestion_daily`. Job-level
  concurrency `databricks-workspace`; secrets only on the steps that call Databricks; run URLs stripped from the
  public log.
* Secret scope per target (plan 3a, code part): bundle variable `secret_scope` (dev `netsuite_ingestion_dev`,
  prod still `netsuite_ingestion_poc` until the prod scope exists), passed to every job task and to the pipeline
  configuration (`metadata.pg_conn_from_conf`). Tests: the scope differs per target and no resource file names
  a scope literally.
* Bronze rebuild mode: when `workspace.poc_bronze` holds no streaming table (fresh dev, e.g. after the
  `[dev suhasv]` set is removed) or on `workflow_dispatch` with `bronze_rebuild`, deploy-dev deploys with
  `bronze_rebuild=true`, runs (full refresh only if bronze exists), `sync_ledger` rebuilds `workspace.ledger.*`,
  and it always redeploys with `bronze_rebuild=false`. So the first CI dev run is a rebuild (owner decision).
* No `run_as` in the dev target: CI deploys dev as ci-dev, which is then also the run identity; `run_as` (proven
  in step A) is for prod, where a human deploy must still run as ci-prod (step F).
* **First deploy-dev run will fail until:** (1) ci-dev has migration rights on dev metadata (owner SQL);
  (2) the hand-deployed `[dev suhasv]` pipelines are removed, because they own the dev tables the CI pipeline
  writes; (3) compute is available again.

**Hand-deployed `[dev suhasv]` set (removal needs the owner's OK; nothing removed):**

| Kind | Name | Goes with it |
|---|---|---|
| job | `[dev suhasv] netsuite_ingestion_daily` | run history |
| job | `[dev suhasv] guard_canary_check` | run history |
| pipeline | `[dev suhasv] netsuite_ingestion_poc` | the tables it owns: `workspace.poc_bronze` (5 source tables + `guard_reads`), `workspace.poc_silver` (4), `workspace.poc_reject.rejected_rows`, `workspace.poc_gold` (2) |
| pipeline | `[dev suhasv] guard_canary` | `workspace.canary.canary_bronze` |
| folder | the owner's `.bundle/netsuite_ingestion/dev` | deployed files and bundle state |

Not affected: the owner's `.bundle/netsuite_ingestion/prod` folder and every prod resource; `workspace.ledger.*`
(not pipeline-owned; the first CI run with `bronze_rebuild=true` rebuilds it); the dev metadata branch; other
projects' tables in `workspace.default`. Data lost: only dev copies that the next CI run rebuilds from the source
(the v2 baseline numbers are in `baseline/v2/`). Method: `bundle destroy -t dev` as the owner from the working
copy that holds the owner's bundle state.

## 2e. Guard canary workflow (Phase 2, step E)

* `.github/workflows/canary.yml`: `workflow_dispatch` only (no GitHub schedule until the owner decides), as
  ci-dev, job-level concurrency `databricks-workspace`. Stops every running SQL warehouse first and waits until
  all are stopped, then `bundle run guard_canary_check -t dev`; URLs stripped from the public log.
* **Blocked: ci-dev cannot stop the warehouse.** Granting it CAN_MANAGE on the SQL warehouse was refused by the
  tool's permission classifier (owner action). Until then the workflow fails at the stop step on purpose instead
  of running the canary with a warehouse up. The `[dev ci_dev] guard_canary` pipeline also needs
  `workspace.canary.canary_bronze`, which the `[dev suhasv] guard_canary` pipeline owns (section 2d).

## 2f. promote-prod (Phase 2, step F)

* `.github/workflows/promote-prod.yml`, `workflow_dispatch` with `release_sha` (full SHA), `run_after_deploy`,
  `bronze_rebuild` (both default false).
  * Job `release gate` (no secrets) fails unless `release_sha` is on main (`git merge-base --is-ancestor` against
    `origin/main`), has a successful deploy-dev run (`gh run list --status success`), and the `prod` environment
    exists **with required reviewers** (GitHub would otherwise create it on first use without protection).
    Dry-run locally: a malformed SHA, a SHA not on main, and a missing deploy-dev workflow all fail closed.
  * Job `promote to prod` (environment `prod`, concurrency `databricks-workspace`): backup branch of prod
    metadata, `migrate --plan`/`--apply` (prod), `bundle deploy -t prod` with the schedule PAUSED, optionally one
    run (never with `bronze_rebuild`; the rebuild's full refresh is started by hand, plan section 5).
* **Not created (ask first):** ci-prod, its OAuth secret and Lakebase role, scope `netsuite_ingestion_prod`, the
  `prod` environment and the `DATABRICKS_*_PROD` environment secrets. Also needed then: `run_as` ci-prod and
  ci-prod in the prod target's `permissions`, and `poc_netsuite.poc_gold` (the prod metadata endpoint is
  enabled again since 2026-09-30).

## 2g. Step F prod setup (approved and done 2026-09-30)

* Service principal **ci-prod** with one OAuth secret (90 days, expires 2026-12-29), stored only as `prod`
  environment secrets `DATABRICKS_HOST_PROD`, `DATABRICKS_CLIENT_ID_PROD`, `DATABRICKS_CLIENT_SECRET_PROD`.
* GitHub environment **`prod`**: required reviewer = owner (self-review allowed: solo owner), deployment branches
  `main` only. Repository-level secrets hold only the dev ones.
* Lakebase roles `ci-prod` (no admin membership) on `aidq-metadata/production` and `netsuite-sample/production`.
  Grants like dev: prod metadata USAGE on `aidq_metadata`, SELECT on the three config tables, SELECT + INSERT on
  `run_audit`; source USAGE + SELECT on `netsuite` (default privileges for new tables).
* Scope **`netsuite_ingestion_prod`**: ci-prod WRITE, owner MANAGE. ci-prod has no access to the dev or `poc` scope.
* UC: USE CATALOG `poc_netsuite`; USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY on
  `poc_bronze`, `poc_silver`, `poc_reject`, `poc_gold`, `ledger`, `canary` (the last three created for prod);
  USE SCHEMA on `default`.
* Release step 0 access (granted 2026-09-30, owner's OK): ci-prod CAN_MANAGE on the existing prod job
  `netsuite_ingestion_daily` and pipeline `netsuite_ingestion_poc` (owner stays IS_OWNER). Not yet done: the
  `bundle deployment bind` itself (first release).
* Verified as ci-prod: identity, Lakebase login and grants (allowed and denied), prod scope WRITE, no access to
  other scopes, `bundle validate -t prod` from a clean checkout (no warnings).
* PR (prod target): `run_as` and CAN_MANAGE = the deploying service principal (ci-prod via promote-prod),
  `secret_scope: netsuite_ingestion_prod`; tests.
* **Owner-run script pending:** `aidq_owner` on prod metadata with ci-prod as member (`grant_ci_prod.py`, same as
  the dev script); promote-prod's `migrate --apply` needs it.

## 3. Decisions waiting for the owner

1. Step 8 (unpause prod): on hold until the daily checks are clean (section 0d); also settle the prod cron (06:00 vs 06:30).
2. After step 8 plus three good scheduled prod runs: drop `poc_netsuite.backup_pre_release_202610010127` (ask).

## 4. Gotchas

* **Deploy identity and local state.** A bundle deploy reads the local `.databricks/` state cache. Deploying as
  another identity from a working copy that has the owner's cache tries to update the owner's resources (ci-dev
  gets 403). Deploy as ci-dev only from a clean checkout (CI always is).
* **Dev-mode prefix is the deployer.** Only the CI-deployed `[dev ci_dev]` set exists now. Do not deploy dev by
  hand: a second set would write the same `workspace.poc_*` tables, which the CI pipeline owns.
* **Per-target secret keys.** Scope `netsuite_ingestion_poc` holds `meta_pg_token` (prod) and `meta_pg_token_dev`
  (dev); the per-environment scopes of plan section 3a replace this in step D. Do not point dev at the prod key.
* **Migrations before code.** Migration files stay byte-stable once recorded; add a new migration instead.
* **Guard read retries.** The bronze guard retries reading its `create_update` event 5 times, 10 s apart, and fails
  closed for `pending_only`.
* **Development mode and schedules.** An explicit `pause_status: UNPAUSED` on a job is honored in dev mode; only
  `presets.trigger_pause_status: UNPAUSED` is refused there.
* **Free Edition limits.** Stop the SQL warehouse before the canary or several updates (`RESOURCE_EXHAUSTED`). At
  most 5 concurrent job tasks. No account console or account APIs (no OIDC). One Lakebase project per account
  (owner's note; this account already has two, so no new project can be added). Lakebase endpoints can end up
  disabled after inactivity.
* **Full refresh is blocked unless `bronze_rebuild=true`.**
* **Windows.** PowerShell 5.1 strips quotes inside JSON arguments to native commands: use Git Bash for
  `--json '...'`. `MSYS_NO_PATHCONV=1` for workspace paths starting with `/Users/...`.
* **GitHub.** Protected `main`: every change through a PR. Never force-push; never `pull_request_target`.
