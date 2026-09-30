# Phase 2 plan: CI/CD for netsuite_ingestion

Approved 2026-09-30 with the revisions recorded in section 7. Implementation is in progress: nothing here is
live until it is merged, and every workflow file is shown to the owner before it is committed.

## 0. Facts this plan rests on (checked 2026-09-26, ledger rows 2026-09-30)

| Fact | Evidence |
|---|---|
| The repo is **public** since 2026-09-26 (it was private on the Free plan, where branch protection and rulesets returned `403 Upgrade to GitHub Pro or make this repository public`). Both are available now. | `gh repo view`; decision recorded in section 2. |
| Environments with required reviewers need Pro/Team for private repos; Free plans get them for public repos, which this repo now is. | GitHub docs: "Users with GitHub Free plans can configure environments for public repositories." |
| Secret scanning and push protection are free on public repos (they were off while the repo was private). To be switched on with the branch-protection change. | GitHub API: `Secret scanning is disabled on this repository` (checked while private). |
| Databricks Free Edition has no account console and no account-level APIs, so OIDC federation for GitHub Actions (which needs `account service-principal-federation-policy`) is not available. | Docs: "No access to the account console or account-level APIs"; `databricks account ...` returns Not Found here. |
| Personal access tokens work. One exists ("earthquake_analytics project", created 2026-04-08). | `databricks tokens list`. |
| Workspace-level service principals and OAuth M2M secrets **work on Free Edition** (verified 2026-09-30 with `ci-dev`). | Spike results, section 3. |
| Free Edition allows at most 5 concurrent job tasks per account, and the free serverless quota was hit when the canary ran with a SQL warehouse also running. | Docs; canary run history in `baseline/before_agents_report_v2.md`. |
| A Lakebase project allows 10 unarchived branches. After the 2026-09-26 cleanup and the new `dev` branch, `netsuite-sample` has 5 (production + 4 backups) and `aidq-metadata` has 2. | `databricks postgres list-branches`. |
| The key ledger and its fingerprints are **per target, not shared**: every reader and writer builds the name from `${var.catalog}` (`dev` = `workspace`, `prod` = `poc_netsuite`), so dev uses `workspace.ledger.bronze_keys` / `bronze_fingerprints` and prod uses `poc_netsuite.ledger.*`. Only the dev tables exist today; prod's are created by `ensure_ledger_table` on the first prod run with the new code. | `resources/netsuite_ingestion_poc.pipeline.yml` (`ledger_table`, `ledger_fingerprint_table`), `sync_ledger` / `ledger_check` `--catalog ${var.catalog}`, `metadata.ledger_table_name`; `system.information_schema.tables WHERE table_schema = 'ledger'` on 2026-09-30. |

## 1. Pipeline stages

```
pull request ─► pr-checks: gitleaks, pytest, bundle validate (dev+prod), migrate --plan (dev branch)
merge to main ─► deploy-dev: migrate --apply (dev branch), bundle deploy -t dev, smoke run (one normal job run)
weekly / manual ─► canary: stop warehouse, bundle run guard_canary_check -t dev
manual, gated ─► promote-prod: ancestry + deploy-dev check, backup, migrate --apply (prod), bundle deploy -t prod,
                 rebuild run, dev-vs-prod comparison
```

* **pr-checks** (exists: gitleaks + pytest). Add `databricks bundle validate -t dev` and
  `-t prod --var schedule_pause_status=PAUSED`. `bundle validate` calls the workspace API, so this job needs a
  Databricks credential; give it the low-privilege **dev** identity only, and skip it (with a visible notice) when
  secrets are unavailable, for example on fork PRs. Once the job exists and has passed on `main`, add it to the
  required status checks of `main` (a branch-protection change: shown to the owner first).
* **Ledger isolation test** (pytest, no workspace needed): asserts that the dev and prod targets resolve different
  catalogs and that the pipeline and every ledger job task take the ledger location from `${var.catalog}`, so a
  change that pins it to one catalog fails CI.
* **migrate --plan** (read-only) runs against the dev Lakebase branch and posts the plan (already present,
  conflicting, to apply) to the job summary.
* **deploy-dev** runs sequentially, never in parallel with the canary: Free Edition serverless limits.
* **canary** first stops the SQL warehouse, then runs `bundle run guard_canary_check -t dev`.
  The Databricks-side job schedule stays PAUSED; the GitHub `schedule:` trigger is added only when you decide to.

### Concurrency: one group for everything that touches Databricks

Every job that calls the workspace (bundle validate, migrate, deploy-dev, canary, promote-prod) declares

```yaml
concurrency:
  group: databricks-workspace
  cancel-in-progress: false
```

at **job** level, so gitleaks and pytest stay parallel and only the Databricks jobs queue behind each other.
pr-checks keeps its per-PR workflow group (`cancel-in-progress: true`) as well; that can only cancel a read-only
`bundle validate`, never a deploy.

Caveat (GitHub semantics): a group holds at most **one running and one pending** job. A new pending job replaces
the older pending one, which is cancelled even with `cancel-in-progress: false`. Consequences:

* deploy-dev replaced by a newer deploy-dev: harmless, the newer `main` supersedes it.
* A pending promote-prod (after approval) could be displaced by a canary or a PR validate queued behind it. So
  promote-prod is started only when nothing else is queued, and its first step re-checks the run status of the
  group; a displaced run shows as cancelled and is simply re-dispatched. Nothing runs half-way, because the gate
  step comes before any change.

## 2. The prod gate: variant (a), decided 2026-09-26

**Decision: variant (a).** The repo is public, so the `prod` environment with required reviewers and branch protection on
`main` are available on the Free plan. Variant (b) below is kept for reference only; it is not the plan.

### (a) Public repo: environment with required reviewers (chosen)

* Environment `prod`: required reviewer = you, deployment branches = `main` only, optional wait timer. The prod
  credential is an **environment secret**, visible only to jobs that target `prod` after approval.
* Branch protection on `main`: pull request required, status checks `gitleaks (full history)` and `pytest`
  required (plus `bundle validate` once it exists, section 1), no force push, no deletion, admins included.
* `CODEOWNERS` for `.github/workflows/` and `migrations/` is **advisory only**: it requests the owner's review but
  "Require review from Code Owners" stays **off**. With a solo owner and 0 required approvals, turning it on would
  block every PR that touches those paths, since an author cannot approve their own PR.
* Secret scanning and push protection on (free for public repos).
* `promote-prod.yml` triggers on `workflow_dispatch` or a `v*` tag and has a `prod` environment job, so it pauses
  for approval before any prod step runs.
* **First step of promote-prod (the release gate), before any credential is used.** It fails unless both hold:
  1. `release_sha` is an ancestor of (or equal to) `origin/main`: `git fetch origin main` then
     `git merge-base --is-ancestor "$RELEASE_SHA" origin/main`. Checkout uses `fetch-depth: 0`.
  2. A **successful** `deploy-dev` run exists for exactly that commit:
     `gh run list --workflow deploy-dev.yml --commit "$RELEASE_SHA" --status success --json databaseId`
     is non-empty. The job needs `permissions: actions: read` for this.
* Cost of this variant, accepted: the whole history is public, including the first commit (email, two Lakebase
  hostnames, some object ids; no credentials; gitleaks reports no findings). Removing that would need a history
  rewrite, which CLAUDE.md forbids without approval.
* Solo developer: the reviewer is you, so the environment gate is a deliberate second click (and a `main`-only
  deployment restriction), not a second pair of eyes. Branch protection therefore requires 0 approvals.

```yaml
# promote-prod.yml (variant a), sketch
on: { workflow_dispatch: { inputs: { release_sha: { required: true } } } }
permissions: { contents: read, actions: read }
jobs:
  prod:
    runs-on: ubuntu-latest
    environment: prod                # required reviewer approves here
    concurrency: { group: databricks-workspace, cancel-in-progress: false }
    steps:
      - uses: actions/checkout@<sha>
        with: { ref: ${{ inputs.release_sha }}, fetch-depth: 0, persist-credentials: false }
      - name: Release gate (ancestor of main + green deploy-dev)
        env: { RELEASE_SHA: ${{ inputs.release_sha }}, GH_TOKEN: ${{ github.token }} }
        run: |
          set -euo pipefail
          git fetch --no-tags origin main
          git merge-base --is-ancestor "$RELEASE_SHA" origin/main \
            || { echo "::error::$RELEASE_SHA is not on main"; exit 1; }
          n=$(gh run list --workflow deploy-dev.yml --commit "$RELEASE_SHA" --status success --json databaseId --jq length)
          [ "$n" -gt 0 ] || { echo "::error::no successful deploy-dev run for $RELEASE_SHA"; exit 1; }
      - run: python tools/migrate.py --env prod --plan
      # ... backup, migrate --apply, bundle deploy -t prod, rebuild run, dev-vs-prod comparison (section 5)
```

### (b) Private repo on Free: manual `workflow_dispatch`, no enforced reviewer (not chosen)

Free private repos cannot enforce reviewers or branch protection, so the gate is a set of checks the workflow makes
itself plus your own click:

* Trigger only `workflow_dispatch`, with inputs `release_sha` (must equal the current `main` HEAD) and
  `confirm` (must equal the string `deploy-prod`).
* First step fails unless `github.ref == 'refs/heads/main'` and `github.actor == 'Suhasv1982'`.
* Verify that the `pr-checks` run for `release_sha` is green (`gh run list --commit`).
* Fail if the release range (last release tag to `release_sha`) touches `.github/workflows/` unless an input
  `workflows_reviewed=yes` is set: a soft stand-in for CODEOWNERS.
* Secrets can only be repository-level, so **any branch's workflow file can read them** when it runs. Mitigations:
  the prod credential gets its own secret name used by `promote-prod.yml` only; the rule in CLAUDE.md that workflow
  files change only with your approval; and prod credentials with the smallest possible rights. Residual risk: an
  unreviewed workflow change pushed to a branch could read the secret. If that is not acceptable, keep the prod credential
  out of GitHub entirely and run `promote-prod` locally from a script (variant b2), using CI only for dev.

### Choosing

Done: (a). The comparison that led here: (a) enforces a reviewer, keeps the prod secret in an environment that only
approved `main` jobs can read, and allows branch protection, at the cost of a public history; (b) enforces nothing and
leaves the prod credential readable by any branch's workflow.

### Workflow rules (also in CLAUDE.md)

* Never use `pull_request_target`: it runs with the base repo's secrets and write token on fork PRs.
* Scheduled workflows are disabled by GitHub after 60 days without repository activity; the canary's `schedule:`
  trigger (if you enable it) must be re-enabled then, and a disabled canary is a silent gap, not a pass.

## 3. How CI authenticates to Databricks

Ranked by preference (secrets are created by you or with your approval only):

1. **Service principal with OAuth M2M.** Two workspace-level service principals, `ci-dev` and `ci-prod`
   (`databricks service-principals create`), each with an OAuth secret
   (`databricks service-principal-secrets-proxy create <id>`), stored as `DATABRICKS_CLIENT_ID` /
   `DATABRICKS_CLIENT_SECRET` (plus `DATABRICKS_HOST` as a variable) with `DATABRICKS_AUTH_TYPE=oauth-m2m`.
   Rights: `ci-dev` gets the dev schemas in `workspace` and the dev jobs; `ci-prod` gets `poc_netsuite` and the prod
   jobs. Rotate the secrets every 90 days. **Chosen: the 2026-09-30 spike showed it works on Free Edition.**
   Host and client id are stored as GitHub **secrets**, not variables, so they are masked in the public Actions logs
   (`bundle validate` prints the identity and workspace path).
2. **OIDC federation** (no long-lived secret): not possible on Free Edition (needs account-level APIs). It becomes
   the preferred option if the workspace ever moves to a paid account.
3. **Personal access token** as the fallback: a new dedicated token (comment `ci-netsuite-ingestion`, 90-day
   lifetime) stored as `DATABRICKS_TOKEN`. Drawbacks: tied to your identity, carries your admin rights, and jobs
   would run as you. Do not reuse the existing "earthquake_analytics" token.

### SP spike: results (2026-09-30, `ci-dev` only)

`ci-dev` was created (display name `ci-dev`) with one OAuth secret (90-day lifetime, expires 2026-12-29).
`ci-prod` is **not** created; ask first.

| # | Check | Result |
|---|---|---|
| 1 | Create a workspace SP and an OAuth secret on Free Edition | **Works.** `current-user me` as the SP returns it. |
| 2 | `bundle validate -t dev` and `-t prod --var schedule_pause_status=PAUSED` as the SP | **Both OK.** Prod prints one warning: its bundle `permissions` name only the owner, not the deploying SP. Harmless for validate; for `ci-prod` deploys, the prod target's permissions must list `ci-prod`. |
| 3 | `generate-database-credential` for `aidq-metadata/dev` as the SP | **Works without any grant.** Logging in with it fails (`password authentication failed`): the SP has **no Postgres role** yet. |
| 4 | Secret scope WRITE on a dev-only scope (`netsuite_ingestion_dev`, created with approval) | **Works:** put, list, get and delete a test key. The SP **cannot** get, put or list in `netsuite_ingestion_poc`, and cannot change ACLs of the dev scope. Test key removed. |
| 5 | Can the SP create scopes? (a probe expected to be denied) | **Yes.** The workspace lets any user or SP create secret scopes; the creator gets MANAGE. The probe left an empty scope `ci_dev_probe`, which stays until the owner approves deleting it. |
| 6 | `run_as` with `mode: development`, and for the pipeline | **Not tested yet:** needs a deploy as the SP, which needs items 3/7 below first. |

Still needed before CI can **deploy and run** dev (each shown to the owner before it is done):

* Postgres role for the SP (application id) on `aidq-metadata/dev` (read/write on `aidq_metadata`) and on
  `netsuite-sample/production` (read-only).
* UC grants on catalog `workspace`: USE CATALOG, and USE SCHEMA / CREATE TABLE / MODIFY / SELECT on the dev schemas
  (`poc_bronze`, `poc_silver`, `poc_reject`, `ledger`, `canary`, `default`), or CREATE SCHEMA where they do not exist.
* The code change of section 3a (scope per target).

### 3a. Secret scopes per environment (decided 2026-09-30)

Scopes are split per environment regardless of the spike: `netsuite_ingestion_dev` and `netsuite_ingestion_prod`.
`ci-dev` gets WRITE on the dev scope only; `ci-prod` later gets WRITE on the prod scope only. The owner keeps MANAGE
on both. Creating or deleting a scope is always asked first.

Today everything is in `netsuite_ingestion_poc`: `source_pg_token` (shared), `meta_pg_token` (prod) and
`meta_pg_token_dev` (dev). All three are **short-lived OAuth tokens minted by `refresh_credentials` at the start of
every run**, so nothing needs copying: the first run in each environment writes its own keys.

Migration, in order:

1. **Code (one PR):** new bundle variable `secret_scope` (dev `netsuite_ingestion_dev`, prod
   `netsuite_ingestion_prod`) passed to every job task (`--secret-scope ${var.secret_scope}`, replacing the four
   hard-coded `netsuite_ingestion_poc`) and to the pipeline configuration (`secret_scope`), read by
   `metadata.pg_conn_from_conf` instead of its hard-coded default. Key names become the same in both scopes
   (`source_pg_token`, `meta_pg_token`), so `meta_pg_token_key` is dropped. `test_bundle_targets.py` asserts the scope
   differs per target and that no resource file names a scope literally.
2. **Dev:** `netsuite_ingestion_dev` exists (created in the spike, `ci-dev` WRITE). The first dev run after the code
   PR writes the dev keys.
3. **Prod (with the first prod release, section 5):** create `netsuite_ingestion_prod` (ask), grant `ci-prod` WRITE
   (ask), deploy; the first prod run writes the prod keys.
4. **Cleanup:** after both environments have run on their own scope, delete `netsuite_ingestion_poc` (ask). Until
   then prod keeps using it, so nothing breaks between steps.

Residual risk: any workspace user or SP can **create** scopes (spike item 5). That does not give access to existing
scopes, and CI never creates scopes.

### 3b. Dev resources: CI-deployed dev becomes canonical (decided 2026-09-30)

Dev-mode names and the bundle root depend on the deploying identity, so CI's dev deploy creates a second set
(`[dev <ci-dev>] ...`, root under the SP's workspace folder) next to the hand-deployed `[dev suhasv]` set. The CI set
becomes the canonical dev.

**Expected conflict, to verify at the first CI deploy:** both sets write the same tables, because dev mode prefixes
resource names but **not** catalog or schema: both pipelines target `workspace.poc_bronze.*` / `poc_silver.*` /
`poc_reject.*`, both jobs write `workspace.ledger.*`, and both canaries use `workspace.canary`. A Unity Catalog table
created by a pipeline is owned by that pipeline, so the CI pipeline's first update is expected to fail on the tables
the hand-deployed pipeline owns. The deploy itself should succeed; the **run** is where it breaks.

Sequence:

1. CI deploys dev (`bundle deploy -t dev` as `ci-dev`) and `bundle validate` passes; no run yet.
2. Proposal to the owner, before anything is removed: the list of hand-deployed resources (jobs
   `[dev suhasv] netsuite_ingestion_daily` and `[dev suhasv] guard_canary_check`, pipelines
   `[dev suhasv] netsuite_ingestion_poc` and `[dev suhasv] guard_canary`, the bundle folder
   `.bundle/netsuite_ingestion/dev` under the owner's workspace folder), and the dev data each takes with it.
   Deleting a pipeline drops the tables it owns (`workspace.poc_bronze/poc_silver/poc_reject` tables and
   `workspace.canary`). Data check before asking: everything worth keeping from dev is already in the repo
   (`baseline/v2/*`, stripped) or in the `aidq-metadata/dev` Lakebase branch, which is not touched; the dev
   ledger (`workspace.ledger.*`) is not pipeline-owned and is rebuilt by the first CI run anyway.
3. With the owner's OK only: `bundle destroy -t dev` as the owner (removes exactly the hand-deployed set).
   Resources of other bundles (for example `[dev suhasv] earthquake_analytics_etl_job`) are not touched.
4. First CI dev run with `bronze_rebuild=true` (bronze is rebuilt from the source; `sync_ledger` rebuilds the
   ledger), then one normal run; `ledger_check` must report OK. This is also where `run_as` (spike item 6) is tested.

## 4. Migrations per environment

* Files: `migrations/NNN_name.sql`, forward-only, guarded with `IF NOT EXISTS` where possible. **Files contain no
  `BEGIN` / `COMMIT`**: the runner owns the transaction.
* Runner `tools/migrate.py` with tracking table `aidq_metadata.schema_migrations(version, name, checksum, applied_at,
  applied_by, environment)`:
  * `--plan`: compares each pending file with the live schema and reports what is already present or conflicting. It
    changes nothing.
  * `--apply`: per file, **one transaction** that runs the file and inserts its `schema_migrations` row, then
    commits; a failure rolls back both. Refuses to continue if an already-applied file's checksum changed.
  * The runner rejects a file that contains a top-level `BEGIN` / `COMMIT` / `ROLLBACK` (those would end the
    runner's transaction early and let the file commit without its tracking row).
* **One-time change to 001 and 002 (approved 2026-09-30):** remove their `BEGIN;` / `COMMIT;` lines in the same PR
  as `tools/migrate.py`. This is the only permitted edit to an applied file: 001/002 are applied on dev only and not
  yet recorded anywhere, so no stored checksum changes. After this PR the byte-stable rule applies to them as usual.
* **Backfill on dev (first run):** before recording 001/002 as applied, `migrate.py --backfill` verifies that the
  dev schema matches what the files create (tables, columns and types, constraints including the
  `incidents_category_check` values, functions, triggers). Any difference stops the backfill and is reported;
  only an exact match inserts the two rows with the checksums of the edited files.
* Environment mapping: **dev** = the `dev` branch of `aidq-metadata` (host and endpoint from bundle variables);
  **prod** = its `production` branch. `refresh_credentials` takes the endpoint as a variable (`meta_pg_endpoint`); the secret key is per-target
  (`meta_pg_token_dev` for dev, `meta_pg_token` for prod, variable `meta_pg_token_key`) so a dev run cannot overwrite
  the token a prod run reads. Both are implemented in the dev-metadata PR.
* **Order in every environment: migrations are applied before the code deploys.** `--plan`, backup (prod only),
  `--apply`, and only then `bundle deploy`. A failed migration stops the release before any code changes.
* Because of that order, code must work on the schema both before and after its migration for the length of one
  release: new columns are optional to the code. Example: `filter_active_rules` treats a missing `is_active` as
  active, so the code deploys cleanly whether or not migration 001 has run.
* State today: 001 and 002 were applied to the **dev** metadata branch on 2026-09-26 with a one-off script (dry run
  first, then commit), so `schema_migrations` does not exist there yet. Prod has neither migration.
* Rollback is roll-forward with a new migration; for prod, restore from the pre-release Lakebase branch or schema copy.

## 5. First prod release: bronze migration plus the metadata migrations

Every step below runs behind the prod gate (including the release-gate step of section 2) and, apart from 1, only
after your approval.

1. Backups: Lakebase branch `pre-release-<version>` of `aidq-metadata` production, schema copy of `aidq_metadata`,
   and the current metadata state; confirm the branch quota is not exhausted.
2. `migrate --plan` then `--apply` for the metadata migrations (`001_agent_governance`, `002_incident_compute_quota`)
   on **prod** metadata.
3. `bundle deploy -t prod --var bronze_rebuild=true --var schedule_pause_status=PAUSED`.
4. `bundle run netsuite_ingestion_daily -t prod --pipeline-params full_refresh=true`; `sync_ledger` rebuilds the
   ledger; `ledger_check` must report OK.
5. **Dev vs prod on the same source state** (replaces the comparison with the v2 baseline):
   * Right before step 4, run dev once (normal run) so dev and prod read the same `netsuite-sample/production` state.
     No writer (`tools/netsuite_gen.py`, `tools/pg_writer.py`) may run between the dev run and the prod run; record
     the source row counts per table before the dev run and again after the prod run, and they must be equal.
   * Dev must run the same code as prod: dev's deployed commit must equal `release_sha` (redeploy dev at that
     commit first if `main` has moved on).
   * The active rules in dev and prod metadata are diffed first; a difference is reported, since it explains any
     reject difference.
   * Compare per table and snapshot date: bronze row counts, distinct key pairs and ledger fingerprints, silver row
     counts, reject counts per rule, and the defect classification. Everything must match before going on.
6. `bundle deploy -t prod --var bronze_rebuild=false`; one normal run must succeed.
7. With your approval only: drop the old `poc_bronze.<table>__<date>` views, including the four `__2026_08_01`
   leftovers.
8. Separately: unpause the prod schedule.

## 6. Open questions

* Does `run_as` work with `mode: development` and for the pipeline (section 3, spike item 6)? Tested at the first
  CI dev run.
* Is the pipeline table-ownership conflict between the two dev sets real (section 3b)? Seen at the first CI dev run.
* Migration content: `001_agent_governance.sql` (you will provide it) and the `COMPUTE_QUOTA` category change.

## 7. Revisions approved 2026-09-30

1. Ledger location confirmed per target (section 0); guarded by a pytest (section 1). No new variable was needed.
2. promote-prod release gate: ancestor of `origin/main` and a green `deploy-dev` run (section 2).
3. No `BEGIN`/`COMMIT` in migration files; file plus tracking row in one transaction; backfill verifies the dev
   schema first (section 4).
4. SP spike includes scope WRITE and `run_as`; only `ci-dev` is created, then report and ask (section 3).
5. First prod release compares dev and prod on the same source state (section 5, step 5).
6. One `databricks-workspace` concurrency group, `cancel-in-progress: false` (section 1).
7. CLAUDE.md: no `pull_request_target`; CODEOWNERS advisory only; scheduled workflows auto-disable after 60 days.
8. `bundle validate` becomes a required status check once it exists (sections 1, 2).

Later on 2026-09-30:

9. Secret scopes split per environment, `netsuite_ingestion_dev` / `netsuite_ingestion_prod`, WRITE for the matching
   CI identity only (section 3a).
10. CI-deployed dev resources become the canonical dev; the hand-deployed set is removed only after the owner
    approves a list of what goes (section 3b).
11. SP spike run: OAuth M2M works on Free Edition and is the CI login method (section 3).
