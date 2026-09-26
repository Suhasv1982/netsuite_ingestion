# STATUS (paused 2026-09-26)

Work is paused for a few days. Nothing is running. Read this first, then run `git status` and check open PRs.

## 1. Current state

**Repo.** `Suhasv1982/netsuite_ingestion`, **public** since 2026-09-26. `main` is protected: pull request required
(0 approvals), required checks `gitleaks (full history)` and `pytest`, administrators included, force pushes and
deletions blocked. Secret scanning and push protection are on. Local pre-push hook (`.githooks/pre-push`) runs gitleaks
and pytest.

**Pull requests.** Merged: #1 (repo hygiene), #2 (dev metadata branch, migrations, `is_active` filter, Phase 2 plan).
Open when this file was written: none. This branch (`chore/status-handoff`) holds only this file and one line in
`CLAUDE.md`; no PR is opened for it yet.

**Nothing is running** (checked 2026-09-26): no active job runs, all pipelines IDLE, the SQL warehouse is STOPPED.
Schedules: prod `netsuite_ingestion_daily`, dev `netsuite_ingestion_daily` and dev `guard_canary_check` are all
PAUSED.

**Pipelines.**
* dev `[dev suhasv] netsuite_ingestion_poc`: last two updates COMPLETED (guard check, 10 of 10 completed).
* prod `netsuite_ingestion_poc`: its last two updates (2026-09-25) FAILED. They are from the baseline v1 work
  (step C failed by design). Prod has not been touched since; its bronze is still the old per-date-view design.
* dev `guard_canary`: the FAILED updates in its history are the blocked full/selective refreshes, by design.

**Metadata databases (Lakebase project `aidq-metadata`, 2 of 10 branches).**

| | Branch | Endpoint host | Migrations 001 / 002 | Bundle secret key |
|---|---|---|---|---|
| dev | `dev` (copy of production, created 2026-09-26) | `ep-aged-cell-d8guvji9...` | applied | `meta_pg_token_dev` |
| prod | `production` | `ep-withered-pond-d8augt33...` | **not applied** | `meta_pg_token` |

**Source database (`netsuite-sample`, 5 of 10 branches):** production plus backups `pre-synthetic-backup-` `202609242053`
(original 232 rows), `202609252235` (v2 step B), `202609260101` (v2 C), `202609260109` (v2 D). Matching Postgres
schemas `netsuite_backup_<same ids>` exist. The other backups were deleted on 2026-09-26 with your approval.

**Migrations: how they were applied.** By a one-off script (dry run in a rolled-back transaction, then commit) against
the dev branch only, on 2026-09-26: 001, then 002 (002 requires 001's `incidents` table). Re-running 001 as a dry run
succeeds. There is no `schema_migrations` table yet, so nothing in the database records that they ran.
`tools/migrate.py` (Phase 2) is meant to backfill those two rows with the file checksums.

## 2. Decisions waiting for your approval

1. Review `docs/phase2_cicd_plan.md` before anything in it is implemented.
2. CI auth to Databricks: workspace service principal (needs a spike; creation on Free Edition is unverified) or a
   personal access token (works today). The reviewer role grants PR waits on this.
3. Every prod step: migrations 001/002 on prod metadata, `bundle deploy -t prod` with `bronze_rebuild=true`, the full
   refresh, dropping the old `poc_bronze.<table>__<date>` views (including the four `__2026_08_01` leftovers), and
   unpausing the prod schedule. Each needs its own explicit OK.
4. Creating GitHub workflow files, environments (`prod`, required reviewer) or secrets: show first, per CLAUDE.md.
5. Whether to open a PR for this branch (`chore/status-handoff`) and merge it.

## 3. Next steps, in order

1. You review the Phase 2 plan and decide the CI auth option (item 2 above).
2. Open a PR for this branch if you want STATUS.md on `main`.
3. Spike: can a workspace service principal be created and used from CI on Free Edition (else use a PAT)?
4. Implement `tools/migrate.py` (`--plan`, `--apply`, `schema_migrations`, checksum check); backfill the dev rows.
5. Implement the GitHub Actions workflows in stages (PR checks with bundle validate, deploy-dev on merge, canary,
   promote-prod behind the `prod` environment). Each workflow file is shown to you before it is committed.
6. Role-grants PR: reviewer role grants; revoke PUBLIC EXECUTE on `enforce_proposal_lifecycle()` and
   `log_config_change()`; record the guard-read retry count in `run_audit` or the job output so degradation is visible.
7. First prod release (section 5 of the plan): backups, then migrations 001/002 on prod metadata, then the bronze
   migration, compare with the dev v2 baseline, then decide on dropping old views and the schedule.
8. Phase 4: rollback function, separate PR.

## 4. Gotchas

* **Per-target secret keys.** Scope `netsuite_ingestion_poc` holds `meta_pg_token` (prod) and `meta_pg_token_dev`
  (dev). Bundle variable `meta_pg_token_key` selects it and the pipeline reads it through `meta_pg_token_key` in its
  configuration. Do not point dev at the prod key: `refresh_credentials` would overwrite prod's credential. The source
  database key `source_pg_token` is shared.
* **Dev vs prod metadata.** The dev target points at `aidq-metadata/dev` through `meta_pg_endpoint` and `meta_pg_host`.
  The dev branch has migrations 001/002; prod metadata does not. `filter_active_rules` treats a missing `is_active` as
  active, so the same code runs on both.
* **Migrations before code.** In each environment apply migrations first, then deploy code. Migration files must stay
  byte-stable (LF endings are enforced for `migrations/*.sql`): do not edit an applied file, add a new migration.
* **Guard read retries.** The bronze guard reads its own update's `create_update` event from the event log; it is
  sometimes not visible yet, so it retries 5 times, 10 s apart, and fails closed for `pending_only`. In the 10-update
  check there were 0 blocks, but earlier runs saw it read nothing intermittently. The retry count is not recorded
  anywhere yet (planned in the role-grants PR). The canary treats a fail-closed block as a warning.
* **Free Edition compute limits.** Stop the SQL warehouse before running the canary or several updates, or serverless
  quota errors (`RESOURCE_EXHAUSTED`) appear. Serverless jobs auto-retry failed tasks. At most 5 concurrent job tasks.
  Free Edition has no account console or account APIs, so no OIDC federation for CI. A catalog cannot be created
  through the API on Default Storage: dev uses schemas in catalog `workspace`.
* **Full refresh is blocked unless `bronze_rebuild=true`.** Use the commands printed in the guard message. A normal run
  must never be a full refresh.
* **Windows / Git Bash.** Set `MSYS_NO_PATHCONV=1` for workspace paths starting with `/Users/...`; Python text-mode
  writes turn LF into CRLF (write with `newline=""`); do not use heredocs with nested triple quotes.
* **GitHub.** Protected `main`: every change goes through a PR. The gh token needed the `workflow` scope to push a
  workflow file. Never force-push; ask before touching secrets or workflow files.
