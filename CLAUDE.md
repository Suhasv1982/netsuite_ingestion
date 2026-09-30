# CLAUDE.md: rules for working in this repo

Instructions for Claude Code and any other agent. They apply on top of the README.

## Git

* Work on a branch and open a pull request. **Never push directly to `main`.** Do not merge a pull request unless
  the owner asks you to.
* **Never force-push or rewrite history.** That means no `git push --force` / `--force-with-lease`, no
  `git filter-repo` / BFG, no `git commit --amend` or `git rebase` on commits that are already pushed, and no deleting
  and recreating branches or tags to hide something. If a secret was committed, stop and tell the owner: the
  credential must be rotated first, and the owner decides about any history cleanup.
* **Ask before creating or changing GitHub secrets or workflow files** (`.github/workflows/`), and also before
  changing branch protection, repository visibility or any other repository setting.
* Never bypass the pre-push hook (`git push --no-verify`). If the hook fails, fix the cause.
* Commits made with Claude end with the `Co-Authored-By` line the tool provides.

## GitHub Actions

* **Never use `pull_request_target`.** It runs with the base repository's secrets and a write token on pull requests
  from forks. Use `pull_request`, which gets no secrets on fork PRs.
* Every job that calls the Databricks workspace uses the job-level concurrency group `databricks-workspace` with
  `cancel-in-progress: false` (see `docs/phase2_cicd_plan.md`, section 1).
* `CODEOWNERS` is **advisory only**. Do not enable "Require review from Code Owners": with a solo owner and 0
  required approvals it would block every PR that touches the owned paths.
* GitHub disables scheduled workflows after 60 days without repository activity. A disabled canary is a silent gap,
  not a pass: re-enable it when that happens.

## What must not be committed

* Credentials of any kind: tokens, keys, passwords, connection strings with secrets, `.env` files.
* Raw row dumps: the `row_state` block of baseline snapshots and everything in `baseline/raw/`. Commit the stripped
  snapshots instead (`python baseline/strip_row_state.py`).
* Files that carry environment identifiers: workspace hosts or URLs, Lakebase hosts, emails, run-page URLs, job logs,
  raw Jobs API responses. See `baseline/README.md` for the per-file list.
* `.databricks/`, `.venv/`, build output, caches, and the session transcripts (`SESSION_LOG*`, `prompts.pdf`).
* Prefer bundle variables over hard-coded hosts, emails and ids in new code and config.

## Before every push

The `.githooks/pre-push` hook runs **gitleaks over the commits being pushed, then pytest**, and blocks the push if
either fails. One-time setup per clone:

```bash
python -m venv .venv
.venv/Scripts/python -m pip install ".[dev,tools]"     # .venv/bin/python on Linux/macOS
git config core.hooksPath .githooks
winget install --id Gitleaks.Gitleaks                  # or any gitleaks 8.x on PATH
```

The same two checks run on every pull request (`.github/workflows/pr-checks.yml`).

## Databricks

* **No production deploy or run without the owner's approval.** The prod job schedule stays PAUSED; deploy prod with
  `--var="schedule_pause_status=PAUSED"`.
* Develop and rehearse in the `dev` target (catalog `workspace`).
* Ask before dropping the old per-date bronze views (`poc_bronze.<table>__<date>`) or touching anything in
  `poc_netsuite`.
* A full refresh of bronze is only allowed with `bronze_rebuild=true` (the guard in `bronze.py` enforces it).
* Stop the SQL warehouse before running the guard canary on Free Edition.
