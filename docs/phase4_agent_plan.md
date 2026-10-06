# Phase 4 plan: first agent, daily monitor + root-cause analysis (dev)

Status: **approved 2026-10-06** (decisions in section 9). The agent runs as **ci-dev** (owner choice over a new
read-only service principal).

## 1. Goal and scope

After the dev daily job, a LangGraph agent checks the day's run using **only the five Phase 3 MCP tools**,
decides whether anything is wrong, investigates, and writes an incident to `aidq_metadata.incidents` (dev).
Success test: on the incident fixtures it reproduces each `grading.must_identify`, claims nothing in
`must_not_claim`, and on a healthy day writes nothing.

Out of scope: prod, fixing anything, proposals (`config_proposals`), Slack/email beyond the run summary, more
than one agent.

## 2. Where it runs

**Recommended: a GitHub Actions workflow `monitor-dev.yml`**, cron 06:15 UTC (45 min after the dev job starts)
plus `workflow_dispatch`, as ci-dev with the existing `DATABRICKS_*_DEV` secrets, job-level concurrency
`databricks-workspace`, `cancel-in-progress: false`, `permissions: actions: read, contents: read`.
Why there: the MCP server needs the Databricks CLI (the repo's composite action installs it) and `gh` (present on
runners, `GITHUB_TOKEN` reads Actions runs). A Databricks job task has neither, so the reads would need a rewrite
to the SDK first.
Caveats: GitHub cron can start late (minutes) and is disabled after 60 days without repo activity (CLAUDE.md:
a disabled monitor is a silent gap). The agent checks its own last-run time and reports a gap.

## 3. Graph (LangGraph)

```
collect ──> triage ──(no signals)──────────────────────────────> report ──> END
                 └──(signals)──> dedupe ──(all already OPEN)──> report
                                      └──> investigate ──> write ──> report
```

| Node | LLM? | Does |
|---|---|---|
| `collect` | no | calls all five tools once (env dev, default windows) through an MCP client session over stdio |
| `triage` | no | deterministic signals with a fingerprint each: confirmed `missed_schedules`; a run whose result is not SUCCESS; pipeline ERROR events; FAILED `run_audit` rows; `threshold_breached`; `compare_bronze_to_source` verdict `unexplained_gaps`; a missing daily run of the monitor itself. Known defects (`known_defects_only`, guard WARN with 2 reads) are context, not signals |
| `investigate` | yes | one bounded tool-use loop per signal group: the model may call the five MCP tools (max 8 calls, arguments validated by the server) and returns a structured RCA: `category`, `summary`, `root_cause`, `evidence[]`, `not_the_cause[]`, `confidence` (low/medium/high), `suggested_fix`. It is told to separate verified facts from hypotheses |
| `dedupe` | no | drops a signal group whose fingerprint already has an OPEN incident, before the model runs (no cost to see a known problem again) |
| `write` | no | INSERT into `aidq_metadata.incidents` (only write in the agent; skipped in `--dry-run`, the default for evals) |
| `report` | no | GitHub job summary (markdown): signals, RCAs, incident ids; exit 1 when a new incident was written, so GitHub emails the failure |

The LLM never gets a write tool: writing is a fixed node after the model has finished.

## 4. Migration 005 (dev; approved)

`migrations/005_incident_categories_fingerprint.sql`: add `ORCHESTRATION` and `DATA_COMPLETENESS` to
`incidents_category_check`; columns `fingerprint text` (not blank) and `evidence jsonb`; a partial unique index
on `fingerprint` where `status = 'OPEN'`, so dedupe is enforced by the database, not only by the agent. Applied on dev by deploy-dev (migrations before code); prod only with an
approved promote-prod run.

## 5. Identity: ci-dev (decided) and what it needs

* Already has: dev jobs and pipelines (owner), dev UC schemas (SELECT), source SELECT, dev metadata SELECT on
  config tables and `run_audit`, and membership in `aidq_owner` (for migrations).
* Dev metadata: **no grant needed.** ci-dev is an `aidq_owner` member with INHERIT, and `aidq_owner` owns
  `incidents` and `v_table_health` (checked 2026-10-06: `has_table_privilege` true for SELECT and INSERT).
* `system.access.audit`: **cannot be granted** (the owner has no MANAGE on catalog `system`; Databricks owns it).
  Owner decision: degrade gracefully. As ci-dev, `get_recent_deploys` returns the GitHub deploys and
  `config_changes: {"unavailable": ...}`; changes made outside CI (UI edits) are not visible to the monitor.
* Trade-off accepted by choosing ci-dev: the identity can deploy and write (it is an `aidq_owner` member), so
  read-only rests on the MCP server's code and on the agent having no write tool, not on the identity.

## 6. Model

The RCA quality in the Phase 3 replay came from Claude. Options in section 9: the Anthropic API
(`claude-opus-5-5`, configurable; needs a GitHub secret `ANTHROPIC_API_KEY`) or a Databricks-hosted open model
on this workspace (no new secret; ci-dev needs CAN_QUERY; no Claude endpoint exists here). The graph takes a
model client interface, so the choice is one config value plus its credentials.

## 7. Evaluation

* **Recorded tool outputs** per fixture: `evals/incidents/<id>.tools.json`, captured by calling the real tools
  with the clock fixed at the detection time (`Tools(now=...)`), ids scrubbed to stable placeholders (the fixture
  test checks no ids or hosts are committed). A replay MCP server serves them, so evals are deterministic in
  their inputs and need no workspace.
* Fixtures: the two incidents plus a **healthy day** (2026-10-05 12:00: both runs SUCCESS, only known defects).
* Grading per run: deterministic (incident written or not; category; the tools in `tools_expected` were called;
  nothing written on the healthy day) and an LLM judge for each `must_identify` / `must_not_claim` item
  (yes/no with a quoted span from the agent's report). 3 runs per fixture; report pass rates in
  `evals/results/<date>.md`.
* Target before scheduling: healthy day 3/3 no incident; each incident 3/3 on deterministic checks and on
  every `must_not_claim`, at least 2/3 on each `must_identify`.

## 8. Steps (one PR each)

| Step | PR | Done when |
|---|---|---|
| A | migration 005, `get_recent_deploys` degrades without audit access, this plan | 005 applied on dev by deploy-dev |
| B | `src/aidq_agent/`: graph, triage rules, RCA prompt and schema, writer; unit tests with a fake model and fake tools | CI green; local `--dry-run` against live dev |
| C | recorded tool outputs, replay server, eval harness, first results | targets in section 7 met |
| D | `monitor-dev.yml` (workflow file: ask first) and the model secret | first scheduled run reported |

## 9. Decisions (owner, 2026-10-06)

Results: `evals/results/2026-10-06_databricks-gpt-oss-120b.md` (all targets met).


1. Runtime: GitHub Actions workflow `monitor-dev.yml` (the workflow file itself is still shown before it is added, step D).
2. Model: ~~Anthropic API~~ changed 2026-10-06: **a free model served on the workspace,
   `databricks-gpt-oss-120b`**, for evals and production (no API key, no GitHub secret). `--model claude-opus-5-5`
   still selects the Anthropic API. The judge in evals is a different free model (`databricks-qwen35-122b-a10b`).
5. The agent gets `docs/platform_design_notes.md` (mechanisms only, never an incident's diagnosis) with its
   instructions, the knowledge a person running the platform has; the five tools alone cannot show the ledger's
   design (first eval: 0/3 on that item without the notes, 2/3 with them).
3. Migration 005 with the `fingerprint` / `evidence` columns and the unique index on open fingerprints.
4. ci-dev access: approved; none needed for metadata; the audit log degrades gracefully (section 5).
