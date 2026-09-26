# Phase 2 plan: CI/CD for netsuite_ingestion (plan only, nothing here is implemented)

## 0. Facts this plan rests on (checked 2026-09-26)

| Fact | Evidence |
|---|---|
| The repo is **public** since 2026-09-26 (it was private on the Free plan, where branch protection and rulesets returned `403 Upgrade to GitHub Pro or make this repository public`). Both are available now. | `gh repo view`; decision recorded in section 2. |
| Environments with required reviewers need Pro/Team for private repos; Free plans get them for public repos, which this repo now is. | GitHub docs: "Users with GitHub Free plans can configure environments for public repositories." |
| Secret scanning and push protection are free on public repos (they were off while the repo was private). To be switched on with the branch-protection change. | GitHub API: `Secret scanning is disabled on this repository` (checked while private). |
| Databricks Free Edition has no account console and no account-level APIs, so OIDC federation for GitHub Actions (which needs `account service-principal-federation-policy`) is not available. | Docs: "No access to the account console or account-level APIs"; `databricks account ...` returns Not Found here. |
| Personal access tokens work. One exists ("earthquake_analytics project", created 2026-04-08). | `databricks tokens list`. |
| Workspace-level service principals and OAuth secrets exist in the CLI (`service-principals`, `service-principal-secrets-proxy`). Creating one on Free is **not yet verified**. | CLI help; only app-owned SPs exist today. |
| Free Edition allows at most 5 concurrent job tasks per account, and the free serverless quota was hit when the canary ran with a SQL warehouse also running. | Docs; canary run history in `baseline/before_agents_report_v2.md`. |
| A Lakebase project allows 10 unarchived branches. After the 2026-09-26 cleanup and the new `dev` branch, `netsuite-sample` has 5 (production + 4 backups) and `aidq-metadata` has 2. | `databricks postgres list-branches`. |

## 1. Pipeline stages

```
pull request ─► pr-checks: gitleaks, pytest, bundle validate (dev+prod), migrate --plan (dev branch)
merge to main ─► deploy-dev: migrate --apply (dev branch), bundle deploy -t dev, smoke run (one normal job run)
weekly / manual ─► canary: stop warehouse, bundle run guard_canary_check -t dev
manual, gated ─► promote-prod: backup, migrate --apply (prod), bundle deploy -t prod, rebuild run, verify
```

* **pr-checks** (exists in the cleanup PR: gitleaks + pytest). Add `databricks bundle validate -t dev` and
  `-t prod --var schedule_pause_status=PAUSED`. `bundle validate` calls the workspace API, so this job needs a
  Databricks credential; give it the low-privilege **dev** identity only, and skip it (with a visible notice) when
  secrets are unavailable, for example on fork PRs.
* **migrate --plan** (read-only) runs against the dev Lakebase branch and posts the plan (already present,
  conflicting, to apply) to the job summary.
* **deploy-dev** runs sequentially, never in parallel with the canary: Free Edition serverless limits.
* **canary** first stops the SQL warehouse, then runs `bundle run guard_canary_check -t dev`.
  The Databricks-side job schedule stays PAUSED; the GitHub `schedule:` trigger is added only when you decide to.

## 2. The prod gate: variant (a), decided 2026-09-26

**Decision: variant (a).** The repo is public, so the `prod` environment with required reviewers and branch protection on
`main` are available on the Free plan. Variant (b) below is kept for reference only; it is not the plan.

### (a) Public repo: environment with required reviewers (chosen)

* Environment `prod`: required reviewer = you, deployment branches = `main` only, optional wait timer. The prod
  credential is an **environment secret**, visible only to jobs that target `prod` after approval.
* Branch protection on `main`: pull request required, status checks `gitleaks` and `pytest` required, no force push,
  no deletion, admins included. `CODEOWNERS` for `.github/workflows/` and `migrations/`.
* Secret scanning and push protection on (free for public repos).
* `promote-prod.yml` triggers on `workflow_dispatch` or a `v*` tag and has a `prod` environment job, so it pauses
  for approval before any prod step runs.
* Cost of this variant, accepted: the whole history is public, including the first commit (email, two Lakebase
  hostnames, some object ids; no credentials; gitleaks reports no findings). Removing that would need a history
  rewrite, which CLAUDE.md forbids without approval.
* Solo developer: the reviewer is you, so the environment gate is a deliberate second click (and a `main`-only
  deployment restriction), not a second pair of eyes. Branch protection therefore requires 0 approvals.

```yaml
# promote-prod.yml (variant a), sketch
on: { workflow_dispatch: { inputs: { release_sha: { required: true } } } }
permissions: { contents: read }
jobs:
  prod:
    runs-on: ubuntu-latest
    environment: prod                # required reviewer approves here
    steps:
      - uses: actions/checkout@<sha>
        with: { ref: ${{ inputs.release_sha }} }
      - run: python tools/migrate.py --env prod --plan
      # ... backup, migrate --apply, bundle deploy -t prod, rebuild run, verify (section 5)
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

## 3. How CI authenticates to Databricks

Ranked by preference (secrets are created by you or with your approval only):

1. **Service principal with OAuth M2M.** Create two workspace-level service principals, `ci-dev` and `ci-prod`
   (`databricks service-principals create`), create an OAuth secret for each
   (`databricks service-principal-secrets-proxy create <id>`), and store `DATABRICKS_CLIENT_ID` /
   `DATABRICKS_CLIENT_SECRET` (plus `DATABRICKS_HOST` as a variable) with `DATABRICKS_AUTH_TYPE=oauth-m2m`.
   Rights: `ci-dev` gets the dev schemas in `workspace` and the dev jobs; `ci-prod` gets `poc_netsuite` and the prod
   jobs. Jobs then run as the service principal, which needs read access to the secret scope and a Lakebase Postgres
   role on both projects. **This is unverified on Free Edition: a 15-minute spike (create SP, create secret, run
   `bundle validate`, generate a Lakebase credential) decides it.** Rotate the secrets every 90 days.
2. **OIDC federation** (no long-lived secret): not possible on Free Edition (needs account-level APIs). It becomes
   the preferred option if the workspace ever moves to a paid account.
3. **Personal access token** as the fallback: a new dedicated token (comment `ci-netsuite-ingestion`, 90-day
   lifetime) stored as `DATABRICKS_TOKEN`. Drawbacks: tied to your identity, carries your admin rights, and jobs
   would run as you. Do not reuse the existing "earthquake_analytics" token.

Lakebase databases: CI never holds a database password. Migration and job code mint a short-lived credential with
`databricks postgres generate-database-credential` under the CI identity.

## 4. Migrations per environment

* Files: `migrations/NNN_name.sql`, forward-only, guarded with `IF NOT EXISTS` where possible.
* Runner `tools/migrate.py` with tracking table `aidq_metadata.schema_migrations(version, name, checksum, applied_at,
  applied_by, environment)`:
  * `--plan`: compares each pending file with the live schema and reports what is already present or conflicting. It
    changes nothing.
  * `--apply`: one transaction per file; refuses to continue if an already-applied file's checksum changed.
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
  first, then commit), so `schema_migrations` does not exist there yet. `tools/migrate.py` will backfill the two
  rows, with the file checksums, the first time it runs. Prod has neither migration.
* Rollback is roll-forward with a new migration; for prod, restore from the pre-release Lakebase branch or schema copy.

## 5. First prod release: bronze migration plus the metadata migrations

Every step below runs behind the prod gate and, apart from 1, only after your approval.

1. Backups: Lakebase branch `pre-release-<version>` of `aidq-metadata` production, schema copy of `aidq_metadata`,
   and the current metadata state; confirm the branch quota is not exhausted.
2. `migrate --plan` then `--apply` for the metadata migrations (`001_agent_governance`, `002_incident_compute_quota`)
   on **prod** metadata.
3. `bundle deploy -t prod --var bronze_rebuild=true --var schedule_pause_status=PAUSED`.
4. `bundle run netsuite_ingestion_daily -t prod --pipeline-params full_refresh=true`; `sync_ledger` rebuilds the
   ledger; `ledger_check` must report OK.
5. Compare with the dev v2 baseline (row counts, rejects, classification): they must match before going on.
6. `bundle deploy -t prod --var bronze_rebuild=false`; one normal run must succeed.
7. With your approval only: drop the old `poc_bronze.<table>__<date>` views, including the four `__2026_08_01`
   leftovers.
8. Separately: unpause the prod schedule.

## 6. Open questions

* Does workspace-level SP creation work on Free Edition (section 3, spike)?
* Migration content: `001_agent_governance.sql` (you will provide it) and the `COMPUTE_QUOTA` category change.
