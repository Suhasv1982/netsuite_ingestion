# Phase 3 plan: read-only MCP server (minimal scope)

Status: **approved 2026-10-06** (identity: the owner's DEFAULT profile for the local test; plan ships in PR A).

## 1. Goal and scope

A local MCP server (Python, stdio) that exposes five **read-only** tools over the NetSuite ingestion platform, so
an agent (Claude Code now, the Phase 4 LangGraph monitor later) can diagnose a daily run the way the 10-06 session
did by hand. Success test: with Claude Code as the client and only these tools, the evidence in both incident
fixtures (`evals/incidents/`) can be reproduced.

In scope: the five tools, dev environment, local stdio transport, tests, Claude Code registration.
Out of scope: any write (incidents, proposals: Phase 4), prod reads (section 6), hosting / remote transport,
free-form SQL, enabling endpoints or starting jobs.

## 2. Layout

```
src/aidq_mcp/
  __init__.py
  __main__.py      # python -m aidq_mcp  -> stdio server
  server.py        # MCPServer app: the 5 tool definitions (readOnlyHint), argument validation, output shaping
  reads.py         # I/O: Databricks CLI/SDK calls, Postgres (psycopg), SQL warehouse statements
  logic.py         # pure functions (gap classification, missed-schedule detection, redaction); unit-tested
  config.py        # env -> job names, endpoints, catalog, tables (dev only for now)
tests/test_aidq_mcp_logic.py   # pure logic, no workspace
tests/test_aidq_mcp_server.py  # tool registration + schemas, with reads.py faked
```

* SDK: official `mcp` Python package, 2.x (`mcp.server.mcpserver.MCPServer`, formerly FastMCP), pinned
  `mcp>=2.3,<3`. Added to the `dev` extra in `pyproject.toml` so the existing
  `pytest` CI job installs it; **no workflow file change**.
* `daily_check.py` keeps working unchanged. Its pure helpers (`explain_count_gaps`, `audit_findings`,
  `runs_on_date`) move to `logic.py` later only if both copies start to drift (not in this phase).

## 3. The five tools

All take `env` (only `"dev"` accepted in Phase 3). All return JSON objects with a `status` of `ok`,
`unavailable` (e.g. a disabled Lakebase endpoint, with the reason) or `error`, plus bounded lists (default 50
items, hard cap 200). Table arguments are checked against the allowlist from `config.py`.

| Tool | Arguments | Reads | Returns |
|---|---|---|---|
| `get_table_health` | `env`, `table?` | metadata Postgres `aidq_metadata.v_table_health`, latest `guard` row of `run_audit` | per table and layer: status, error, rows read/written/rejected, reject rate vs threshold, active rules, open proposals, timestamps |
| `get_recent_job_runs` | `env`, `days=3` (max 14), `job?` (`generator`, `ingestion`, `canary`) | Jobs API `list-runs`, `jobs get` (schedule) | runs (start, end, trigger, result, state message); schedule (cron, pause status); **`missed_schedules`**: cron fire times in the window with no run started within 15 min |
| `get_pipeline_errors` | `env`, `hours=24` (max 168) | pipeline event log (`list-pipeline-events`, level ERROR/WARN), `run_audit` rows with status FAILED | events (time, level, flow, message truncated to 500 chars), failed audit rows |
| `compare_bronze_to_source` | `env`, `table?`, `since?` (date) | source Postgres counts per `updated_date` day and distinct keys; dev bronze counts per `_snapshot_date` (SQL warehouse) | per table: totals, and per date only where counts differ: source, bronze, same-day duplicate excess, classification `known_defect_same_day_duplicates` / `unexplained` (same rule as `daily_check.py`) |
| `get_recent_deploys` | `env`, `days=7` (max 30) | GitHub Actions runs of `deploy-dev.yml` (`gh api`, read-only), audit log `system.access.audit` job/pipeline config actions (create, update, reset, delete, changeJobAcl) on the env's resources | deploys (time, sha, conclusion), config changes (time, action, resource, actor redacted to a role: owner / ci-dev / system / other) |

`missed_schedules` and the deploy/config-change view are what incident 2 needed; per-date gaps with the
duplicate classification are what incident 1 needed.

## 4. Read-only by construction

* Postgres: every connection sets `default_transaction_read_only = on`; queries are fixed templates with bound
  parameters; no DDL/DML strings exist in the package. A test greps `src/aidq_mcp` for `INSERT|UPDATE|DELETE|
  CREATE|ALTER|DROP|GRANT|TRUNCATE` outside comments.
* SQL warehouse: fixed `SELECT` templates only; table names come from the allowlist, never from the caller.
  The warehouse may auto-start on a query (accepted cost; the tool never calls `warehouses start`).
* Databricks REST: only `get`/`list` calls. Never enables an endpoint, starts a run or deploys. A disabled
  endpoint returns `status: unavailable` with `"endpoint disabled (this server never enables it)"`.
* No tool takes free-form SQL or arbitrary API paths.

## 5. Identity and secrets

* **Phase 3 (local test):** the owner's CLI profile, passed as `--profile` / `AIDQ_PROFILE` in the local MCP
  registration. Read-only is enforced by the code above, not by the identity.
* **Phase 4 (the monitor runs unattended):** a dedicated service principal `aidq-reader` with SELECT-only grants
  (metadata, source, dev UC schemas, `system.access.audit`) plus INSERT on `incidents` for the agent's one write.
  Creating it is a permission change: proposed here, done only with the owner's OK when Phase 4 starts.
* Output redaction (in `logic.py`, tested): no hosts, URLs, emails or tokens in any tool result; actors become
  roles. Run and job ids are returned (the agent needs them) but are never committed (fixtures use placeholders).

## 6. Environments

Dev only. `env="prod"` returns `error: prod not enabled in phase 3`. Adding prod later is a config entry plus the
owner's OK (prod metadata endpoint is currently disabled and step 8 is on hold).

## 7. Testing

1. Unit (CI, no workspace): gap classification, missed-schedule detection (cron fire times vs runs, incl. the
   10-06 case), redaction, argument limits, tool schemas, the read-only grep.
2. Live smoke (local, read-only): `python -m aidq_mcp --smoke` calls each tool once against dev and prints
   sizes and statuses.
3. Claude Code as client:
   `claude mcp add aidq-netsuite --scope local -- .venv/Scripts/python -m aidq_mcp --profile DEFAULT`
   (local scope: not committed). Then, in a fresh session, ask the two incident questions ("why is dev bronze
   short of the source?", "why did nothing run on 10-06?") and check the answers against each fixture's
   `grading.must_identify` / `must_not_claim`. Results recorded in STATUS.

## 8. Steps (one PR each)

| Step | PR | Done when |
|---|---|---|
| A | package skeleton, `config.py`, `logic.py` + unit tests, `mcp` in `dev` extra | CI green |
| B | `reads.py` + the five tools + server tests (faked I/O) | CI green; live smoke OK |
| C | Claude Code registration (local), incident replay, STATUS update | both fixtures reproduced |

## 9. Risks

* Free Edition: endpoints get disabled and the warehouse may refuse to start; tools must degrade to
  `unavailable`, not fail the session.
* `system.access.audit` lags by minutes; `get_recent_deploys` says so in its result.
* Incident categories in `aidq_metadata.incidents` lack `ORCHESTRATION` and `DATA_COMPLETENESS` (both fixtures
  need them): a migration 005 in Phase 4, not here.
