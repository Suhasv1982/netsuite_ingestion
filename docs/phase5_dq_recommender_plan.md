# Phase 5 plan: DQ rule recommender

Status: **plan only** (owner, 2026-10-08). Design: `docs/dq_recommender_design.md` (v2; its decision log is
section 13). Dev only. Every step below is a separate PR. The owner merges them, outside 05:00-07:00 UTC.

## 0. Prerequisites

| # | Item | State |
|---|---|---|
| 0.1 | PR #35 (row hash) merged | done 2026-10-08 (`d63c249`); deploy-dev on main green |
| 0.2 | One clean dev cycle on main | 10-09: `tools/daily_check.py --date 2026-10-09` with no FAIL; then the cassettes are re-recorded |
| 0.3 | Manifests persisted; batches 10-02..10-08 recomputed | PR #36. All six batches reproduce the rows they wrote exactly |
| 0.4 | Migration 006, `agent` grants, `metadata_roles` | PR #37 (stacked on #36) |
| 0.5 | Owner runs `tools/setup_dq_roles.sh DEFAULT` | after #37 is merged (deploy-dev applies 006 first) |

## 1. Conflicts between design v2 and the current code

Found while preparing #36 and #37; each has a resolution below. Owner decisions of 2026-10-08 are marked
**decided**.

1. **NULL predicates in `dq.py` (confirmed).** The valid view keeps `WHERE pred` and the rejects keep `WHERE NOT pred`, so a row whose predicate is NULL goes **nowhere**: not to silver, not to `rejected_rows`, and with no reason (the reason expression is `CASE WHEN NOT pred`). Today's three HARD rules give 0 NULL rows on dev bronze (checked 2026-10-08), so nothing is lost now. v2 handles it in the validator (step 6 fails NULL-returning rules). That doesn't protect rules written by hand. **Decided: both.** `dq.py` is NULL-safe (PR #40: each HARD rule is `coalesce((expr), false)`, so a NULL result is a reject with its reason). The validator still fails NULL-returning rules. No silver refresh is needed: 0 rows are affected today. Not yet verified: how a SOFT expectation counts a NULL result in the event log. Step 6 checks this on dev with a test rule before the validator relies on it.
2. **`CAN_QUERY` on `databricks-gpt-oss-120b` cannot be granted.** It is a pay-per-token foundation-model endpoint with no endpoint id or permissions object, and it is open to every workspace user (checked 2026-10-08). #37 records this in `grants/dev.yml` instead of a grant.
3. **The validator's role is enforced by our code, not by the database.** It runs as ci-dev and does `SET ROLE aidq_validator`, but ci-dev is also in `aidq_owner`. v2 accepts this for dev. For prod the validator would need its own identity.
4. **Running the agent as dq-agent needs no OAuth secret if it runs as a Databricks job** with `run_as: dq-agent` (as the generator runs as data-generator). That needs ci-dev to hold "Service Principal User" on dq-agent so it can deploy the job (owner action, like data-generator). A GitHub-hosted agent would need an OAuth secret and GitHub secrets (ask first). **Decided: a Databricks job with `run_as: dq-agent`**, consistent with moving the monitor into the dev job (PR #38). No OAuth secret or GitHub secret for dq-agent.
5. **Prompt location.** v2 says `agents/prompts/dq_recommender_v1.md`; the repo keeps agents under `src/` (`src/aidq_agent`). Planned: `src/aidq_dq_agent/prompts/dq_recommender_v1.md`, so the job deploys it with the code.
6. **Ground truth covers only some defect types.**
   * The daily batches inject 4 types: `late_arriving`, `invalid_enum`, `end_before_start`, `amount_mismatch`.
   * The other row-expressible types (`null_business_key`, `negative_amount`, the three customers defects) exist only in `baseline/manifest_defects.json` (the 07-11 init), together with `duplicate_business_key` and `orphan_lines`. The 08-22 increment has a manifest; the 09-14 increment and the 10-01 run do not.
   * Step 2 verifies each baseline manifest against today's source (its keys must still carry the defect). Recall targets apply only to the types whose ground truth verifies.
   * The future-date defect the 10-01 run left (rows dated 10-02/10-03) has no manifest and is in the past now; v2 already excludes future dates from the targets.
7. **Same-day versions are not duplicates.** Since #35 bronze holds every version of a key on a day (`same_day_versions` is info). The profiler must count duplicate keys per (key, `_row_hash`), or exclude same-day versions; otherwise the agent reports normal versions as `DQ_NOT_EXPRESSIBLE` duplicates.
8. **Bronze metadata columns.** `_row_hash`, `_snapshot_date` and `_loaded_at` are not source columns. The profiler skips them, and the validator rejects rules that reference them (they are not in `source_columns`).
9. **Customers rejects.** `dq.py` builds rejects for customers, but customers has no silver table, and its `run_audit` reject rows need checking (step 6) before the impact check (v2 section 6.5) can use a customers reject rate.
10. **`ADD_COLUMN` (later).** The row-hash guard (#35) blocks normal runs after an incremental table's active columns change. An `ADD_COLUMN` proposal must say that approval requires a bronze rebuild (STATUS 0g). Out of scope here (v2 section 11).

## 2. Steps

1. **Ground truth in place** (after 0.5).
   - Load the six recomputed manifests (#36 rollout).
   - Verify `baseline/manifest_defects.json` and `manifest_increment.json` against today's source, load the ones that verify as `source = baseline`, and document the rest as unknown ground truth.
   - From then on the daily job writes its own batch.
2. **Read-only MCP tools** in `src/aidq_mcp`: `get_dq_rules`, `get_open_and_rejected_proposals` (30 days), `profile_table`, `get_rejects_summary`. Fixed SQL templates, like the five existing tools; the read-only test stays. `profile_table` runs on bronze through the warehouse:
   - per column: type, null rate, distinct count, min/max, top values (low cardinality)
   - cross-column candidates: end vs start dates, amount vs quantity × rate, dates vs `current_date`
   - duplicates per (key, `_row_hash`); never raw rows
3. **Write toolbox** (`src/aidq_dq_agent/write_tools.py`, runs as dq-agent): `submit_proposal` (INSERT one PENDING `ADD_DQ_RULE`, `proposed_by = dq_agent@<version>`, `agent_trace_id`) and `report_finding` (INSERT an incident, category `DQ_NOT_EXPRESSIBLE`, with a fingerprint per (table, finding), so the open-fingerprint index stops repeats).
4. **Agent** (`src/aidq_dq_agent`, LangGraph, `databricks-gpt-oss-120b`): collect (rules, proposals, rejects summary, profiles) → recommend (model, read tools only) → submit (fixed code, schema-checked). The design's no-nagging rule is applied in code before submit. Prompt `prompts/dq_recommender_v1.md`, version recorded in each trace.
5. **Validator** (`tools/validate_proposals.py`, ci-dev + `SET ROLE aidq_validator`):
   - Parse with sqlglot (Databricks dialect). Exactly one boolean expression. No comments, `;`, subqueries, DDL/DML, or non-deterministic functions except `current_date` / `current_timestamp`.
   - Columns must be active `source_columns`, with reserved names quoted.
   - Dry run on bronze through the warehouse: count TRUE / FALSE / NULL.
   - Fail on NULL (conflict 1). Flag (not fail) an impact above `discard_threshold`.
   - Write `validation_result` and the status (migration 006 enforces the transition).
6. **NULL checks on dev** (before the validator is trusted): a test SOFT and a test HARD rule that return NULL on some rows. Record where the rows go and how the expectation counts them. Then the `dq.py` decision (conflict 1).
7. **Review CLI** (`tools/review.py`, the owner, `SET ROLE aidq_reviewer`): `list`, `show`, `approve [--severity]` (sets `approved_severity` with VALIDATED → APPROVED, then `apply_proposal()`), `reject --reason`.
8. **Tracing.** An MLflow experiment in the workspace. One trace per agent run, with the trace id on every proposal and incident. Token usage and latency per run.
9. **Evals** (`evals/run_dq_eval.py`): 3 live runs on dev bronze, as dry runs (proposals captured, not inserted), scored against `workspace.generator.manifests`:
   - recall per verified row-expressible type
   - precision
   - false rejects: rows the validator's dry run rejects whose key is not in the manifest for that defect type
   - unsafe rules reaching VALIDATED: must be 0
   - `DQ_NOT_EXPRESSIBLE` coverage (duplicates, orphans, late rows)
   - repeatability across runs

   The report goes to `evals/results/`.
10. **Trigger.** A Databricks job `dq_recommender` (`run_as` dq-agent; validator task in a job run as ci-dev), `workflow_dispatch`-equivalent manual runs plus weekly. Deployed by deploy-dev.
11. **Done when** (v2 section 12):
    - The agent submits proposals and incidents.
    - The validator marks them.
    - Three rules are approved, one with a severity override.
    - The next daily run rejects new violating rows.
    - A silver-only refresh (allowed: the bronze guard blocks only bronze) shows history in rejects and absent from silver.
    - The eval meets the targets and is committed.

## 3. Asks before implementation

* "Service Principal User" on dq-agent for ci-dev (owner action): needed for the decided job route. It can be added to
  `tools/setup_dq_roles.sh` (#37) on request.
* Any new secret (none planned with the job route).
