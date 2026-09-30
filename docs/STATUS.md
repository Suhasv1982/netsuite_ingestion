# STATUS (updated 2026-09-30)

Read this first, then run `git status` and check open PRs. Updated after every Phase 2 step.

## 1. Current state

**Repo.** `Suhasv1982/netsuite_ingestion`, public. `main` is protected: pull request required (0 approvals),
required checks `gitleaks (full history)` and `pytest`, administrators included, no force push or deletion. Secret
scanning and push protection on. Local pre-push hook runs gitleaks and pytest.

**Pull requests.** Merged: #1 repo hygiene, #2 dev metadata branch + migrations, #3 Phase 2 plan revisions.
Open: see `gh pr list`.

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

**Lakebase endpoints.** All three were found **disabled** on 2026-09-30 (disabled around 2026-09-27 01:10-01:50
UTC, cause unknown, likely a Free Edition inactivity policy). Re-enabled with the owner's OK:
`aidq-metadata/dev` and `netsuite-sample/production`. `aidq-metadata/production` (prod metadata) **stays disabled**:
prod cannot run until it is re-enabled (ask).

**Metadata databases (Lakebase project `aidq-metadata`).**

| | Branch | Migrations 001 / 002 | Secret key |
|---|---|---|---|
| dev | `dev` | 001, 002 applied and recorded (backfill, schema verified 2026-09-30); 003 pending | `meta_pg_token_dev` in `netsuite_ingestion_poc` |
| prod | `production` | **not applied** | `meta_pg_token` in `netsuite_ingestion_poc` |

**Source database (`netsuite-sample`):** production plus backups `pre-synthetic-backup-202609242053`,
`...202609252235`, `...202609260101`, `...202609260109` (5 of 10 branches).

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
  baseline). **The memberships rule is not**: that flow emitted no metrics event at all in that update, although
  silver memberships holds 24 rows with end_date < start_date. A memberships-only refresh (update acc79388)
  completed but could not be checked: compute became unavailable (see below). Open.
* Proposed daily dev schedule: `docs/dev_daily_schedule.md` (not deployed; 4 blockers listed).

**Compute unavailable since about 20:13 UTC 2026-09-30.** Both re-enabled Lakebase endpoints were disabled
again (dev metadata 20:13, source 20:16, about 3.5 h after their last activity) and the SQL warehouse refuses to
start (`Cannot create the resource, please try again later`). Most likely a Free Edition compute quota; not
verified (the audit-log query needs the warehouse).

## 2. ci-dev (Phase 2, step A: done 2026-09-30)

* Service principal `ci-dev`, OAuth M2M. Two OAuth secrets are active (90 days, both expire 2026-12-29): the first
  one's local copy was deleted before it was needed again, so a second one was created and is the one in GitHub.
  **Deleting the first secret needs the owner's OK.**
* GitHub secrets `DATABRICKS_HOST_DEV`, `DATABRICKS_CLIENT_ID_DEV`, `DATABRICKS_CLIENT_SECRET_DEV`.
* Lakebase roles `ci-dev` (no admin membership) on `aidq-metadata/dev` and `netsuite-sample/production`.
  Grants: dev metadata USAGE on `aidq_metadata`, SELECT on `source_table_def`, `source_columns`,
  `data_quality_rules`, SELECT + INSERT on `run_audit`; source USAGE + SELECT on schema `netsuite` (default
  privileges for new tables), nothing on the backup schemas. Migration (DDL) rights: step C/D.
* UC: USE CATALOG `workspace`; USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY on
  `workspace.poc_bronze`, `poc_silver`, `poc_reject`, `poc_gold` (new, created 2026-09-30), `ledger`, `canary`,
  `default`. **`default` is too broad** (it holds other projects' tables): reduce to USE SCHEMA (owner SQL).
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

## 3. Decisions waiting for the owner

1. Every prod step (plan section 5), including re-enabling the prod metadata endpoint.
2. Deleting the first ci-dev OAuth secret.
3. After step D: removing the hand-deployed `[dev suhasv]` set (a list will be shown).
4. After step B: adding `bundle validate` to the required checks.
5. Unpausing the proposed daily dev schedule (Part 1).

## 4. Gotchas

* **Deploy identity and local state.** A bundle deploy reads the local `.databricks/` state cache. Deploying as
  another identity from a working copy that has the owner's cache tries to update the owner's resources (ci-dev
  gets 403). Deploy as ci-dev only from a clean checkout (CI always is).
* **Dev-mode prefix is the deployer.** `[dev suhasv]` vs `[dev ci_dev]`. Both sets write the same
  `workspace.poc_*` tables; a pipeline owns the tables it created, so only one set can run.
* **Per-target secret keys.** Scope `netsuite_ingestion_poc` holds `meta_pg_token` (prod) and `meta_pg_token_dev`
  (dev); the per-environment scopes of plan section 3a replace this in step D. Do not point dev at the prod key.
* **Migrations before code.** Migration files stay byte-stable once recorded; add a new migration instead.
* **Guard read retries.** The bronze guard retries reading its `create_update` event 5 times, 10 s apart, and fails
  closed for `pending_only`.
* **Free Edition limits.** Stop the SQL warehouse before the canary or several updates (`RESOURCE_EXHAUSTED`). At
  most 5 concurrent job tasks. No account console or account APIs (no OIDC). One Lakebase project per account
  (owner's note; this account already has two, so no new project can be added). Lakebase endpoints can end up
  disabled after inactivity.
* **Full refresh is blocked unless `bronze_rebuild=true`.**
* **Windows.** PowerShell 5.1 strips quotes inside JSON arguments to native commands: use Git Bash for
  `--json '...'`. `MSYS_NO_PATHCONV=1` for workspace paths starting with `/Users/...`.
* **GitHub.** Protected `main`: every change through a PR. Never force-push; never `pull_request_target`.
