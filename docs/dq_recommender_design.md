# DQ Rule Recommender — design (v2)

Status: design agreed, not implemented. v2 incorporates the review of v1 (decision log in section 13). Scope: **dev only**. Prerequisite: PR #35 (row hash) merged and one clean dev cycle.

## 1. Goal

An agent that finds data-quality problems in the NetSuite data and **proposes** row-level DQ rules through the governance layer (`config_proposals`). It never changes live config. A deterministic validator checks every proposal, a human approves or rejects it, and `apply_proposal()` applies it. The next pipeline run enforces the rule.

Principle: **the LLM proposes, deterministic code validates, a human approves, the database applies.**

## 2. The loop

1. **Profile (code, no LLM).** Statistics per incremental table, computed on **bronze**, not silver. Silver hides problems: rejected rows never reach it and duplicate keys are already merged away.
2. **Recommend (LLM).** The agent reads profiles, existing rules, pending and recently rejected proposals, and the rejects summary. It submits `ADD_DQ_RULE` proposals. It sees summaries, never raw rows.
3. **Validate (code, no LLM).** Each PENDING proposal is checked and dry-run on bronze, then marked `VALIDATED` or `VALIDATION_FAILED`.
4. **Review (human).** `tools/review.py` shows each validated proposal; approve (optionally changing severity) calls `apply_proposal()`, reject records a reason.
5. **Enforce.** The next dev pipeline run reads the new rule from metadata.

## 3. Identity and grants

The agent must **not** run as ci-dev: ci-dev is a member of `aidq_owner` and could disable triggers or write live rules, bypassing governance.

**Roles and grants are environment configuration, not migrations.** Migrations contain schema only and run identically in dev and prod. Roles and grants live in `grants/dev.yml` (later `grants/prod.yml`). Lakebase roles for service principals are created by the owner from a prepared script.

| Identity | Postgres role | Rights on `aidq_metadata` |
|---|---|---|
| `dq-agent` (new SP, LLM-driven) | `aidq_agent`, **not** a member of `aidq_owner` | SELECT on all tables/views; INSERT on `config_proposals` and `incidents` |
| Validator job (runs as ci-dev, then `SET ROLE aidq_validator` before any work) | `aidq_validator` | SELECT; UPDATE (`status`, `validation_result`) on `config_proposals` |
| Human reviewer | `aidq_reviewer` | SELECT; UPDATE (`status`, `reviewed_by`, `reviewed_at`, `review_comment`, `approved_severity`); EXECUTE `apply_proposal` |

The agent can never validate or approve its own proposals: it holds no UPDATE rights.

Unity Catalog (new `agent` principal type in `grants/dev.yml`) for `dq-agent`: USE CATALOG and USE SCHEMA on the dev catalog and schemas; SELECT on dev bronze, silver and reject tables; CAN_USE on the SQL warehouse if profiling runs there (same path as the monitor agent); CAN_QUERY on the `databricks-gpt-oss-120b` serving endpoint.

### Schema changes (one migration)

- Lifecycle trigger: `validation_result` may change **only** together with the PENDING → VALIDATED / VALIDATION_FAILED transition; any other change is rejected.
- New reviewer-owned column `approved_severity` (HARD/SOFT, nullable). `apply_proposal()` uses `COALESCE(approved_severity, proposed_change->>'severity', 'SOFT')`. `proposed_change` stays immutable, so the audit trail shows both the agent's proposal and the reviewer's decision.
- New incident category `DQ_NOT_EXPRESSIBLE`.

## 4. MCP tools

Read-only:

| Tool | Returns |
|---|---|
| `get_dq_rules(table)` | Active and inactive rules with severity |
| `get_open_and_rejected_proposals(table)` | Pending/validated proposals and rejections from the last 30 days, with reasons |
| `profile_table(table)` | Per column: type, null rate, distinct count, min/max, top values (low-cardinality columns), plus candidate cross-column checks (e.g. end vs start dates, amount vs quantity × rate, dates vs `current_date`) |
| `get_rejects_summary(table)` | Reject counts per rule over recent runs |

Write tools run in a **separate toolbox** as `dq-agent`. The existing MCP server stays read-only (and its read-only test stays).

Write (agent toolbox only):

| Tool | Does |
|---|---|
| `submit_proposal(...)` | Inserts one `ADD_DQ_RULE` proposal (status PENDING) with rationale, evidence, confidence, `proposed_by`, `agent_trace_id` |
| `report_finding(...)` | Inserts an `incidents` row for problems a row rule cannot express |

## 5. Agent behaviour

- Model: `databricks-gpt-oss-120b` (same as the monitor agent). LangGraph.
- Inputs: profiles and summaries only. No raw rows, no free-form SQL execution.
- Output per finding: either one `ADD_DQ_RULE` proposal or one incident.
- **Severity:** default SOFT (warn). HARD only for unambiguous violations, such as a NULL business key. The reviewer can change severity on approval.
- **Allowed functions** include `current_date` and `current_timestamp` (fixed within a run).
- **Null-safe expressions:** rules must be written to handle NULLs explicitly (e.g. `amount IS NULL OR amount >= 0`) unless the rule is itself a NULL check.
- **No nagging:** skip rules that already exist, are pending, or were rejected in the last 30 days, unless the violation count has changed by more than 50%.
- **Rationale must include:** violation count and rate on bronze, example column values (aggregated, not raw rows), and why the rule is SOFT or HARD.

### What row-level rules can and cannot catch

The framework applies rules one row at a time. The agent must report the "no" cases as incidents with category **`DQ_NOT_EXPRESSIBLE`** instead of inventing rules.

| Defect | Row rule? |
|---|---|
| NULL business key | Yes |
| `end_date < start_date` (memberships, certifications) | Yes |
| Invalid enum values (`membership_status`, `certification_type`, `transactions.type`) | Yes |
| Future `date` / future `updated_date` | Yes |
| Negative `amount`; `amount ≠ quantity × rate` (lines) | Yes |
| Duplicate business keys | **No** (needs uniqueness across rows) |
| Orphan records (lines without transaction, etc.) | **No** (needs a join) |
| Late-arriving rows | **No** (needs history) |
| Customers: null `company_name`, bad email format, `updated_date < created_date` | Yes: customers has a DQ stage (rejects are built for it); it has no silver table |

## 6. Validator (deterministic)

For each PENDING proposal:

1. **Parse** `rule_expr` with a real SQL parser and require exactly **one boolean expression**. Reject SQL comments (`--`, `/* */`), `;`, subqueries, DDL/DML keywords, and non-deterministic functions (`rand`, `uuid`, etc.). `current_date` and `current_timestamp` are allowed. `rule_expr` is inserted into pipeline SQL as-is, so this check is a security boundary.
2. **Columns:** every referenced column must be an active column in `source_columns` for that table. Quote reserved names (e.g. `date`) correctly, or fail.
3. **Dry run on bronze:** count rows where the expression is TRUE, FALSE and **NULL**.
4. **NULL check:** first confirm how `dq.py` treats a NULL predicate (does the row go to valid, rejects, or neither?). Fail any rule whose expression returns NULL on real rows unless that outcome is intended and documented.
5. **Impact:** compute the would-be reject rate; flag (not fail) if it exceeds the table's `discard_threshold`.
6. The validator runs as `aidq_validator` (section 3). Write `validation_result` (counts, rate, parse/column/null checks, Spark version) and set the status.

A proposal that fails any check never reaches a human as "validated".

## 7. Review CLI (`tools/review.py`)

- `list` — pending and validated proposals.
- `show <id>` — rationale, evidence, validation result, MLflow trace link.
- `approve <id> [--severity HARD|SOFT]` — sets reviewer fields (and `approved_severity` if given), then `apply_proposal()`.
- `reject <id> --reason "..."` — records the reason (fed back to the agent via `get_open_and_rejected_proposals`).

Runs as the human reviewer's identity, never as `dq-agent`.

## 8. LLMOps

- MLflow tracing on every agent run; `agent_trace_id` stored on every proposal and incident.
- Prompts versioned in the repo (`agents/prompts/dq_recommender_v1.md`); the version is recorded in each trace.
- Token usage and latency logged per run.

## 9. Evaluation

Ground truth: generator defect manifests.

- **Fix first:** the daily generator currently writes its manifest to a temporary directory that is discarded. Each batch's manifest must be persisted to a Delta table in dev (e.g. `workspace.generator.manifests`, one row per injected defect).
- Batches 10-02 to 10-08: if the generator is seeded deterministically, recompute their manifests; otherwise evaluate only on batches with known ground truth (the `baseline/` batches plus everything from the fix onward).
- **Future-dated defects are excluded from recall targets.** They must not be injected by the daily generator: its skip rule ("skip if the batch date isn't later than the latest source date") would see a future-dated row and stop writing for days.

| Metric | Initial target |
|---|---|
| Recall per row-expressible defect type with ground truth (section 5 table, excluding future dates) | Every type found in at least 2 of 3 runs |
| Precision (proposals targeting a real defect type) | ≥ 80% |
| False rejects: clean rows (per manifest) a validated rule would reject | 0 per rule |
| Unsafe rules reaching VALIDATED (parse, comment, column or NULL failures) | **0** (hard requirement) |
| Non-expressible defects (duplicates, orphans, late rows) reported as `DQ_NOT_EXPRESSIBLE` | Each reported at least once |
| Repeatability across 3 runs (overlap of proposed rule sets) | ≥ 70% |

Targets are initial and can be tuned after the first runs.

## 10. Trigger

`workflow_dispatch` plus weekly. Not daily: rules change slowly, and daily runs would mostly re-find the same pending proposals.

## 11. Out of scope (later)

- Set-level checks (uniqueness, referential integrity) as a new rule type in the framework.
- `MODIFY_DQ_RULE` and `DEACTIVATE_DQ_RULE` proposals (rule tuning).
- Prod. Everything here is dev only.

## 12. Done when

1. The agent runs in dev as `dq-agent` and submits proposals and `DQ_NOT_EXPRESSIBLE` incidents.
2. The validator (as `aidq_validator`) marks them; the review CLI approves at least three rules, including one with a severity override.
3. **Going forward:** the next dev daily run rejects new rows that violate the approved rules.
4. **History (deliberate):** after a silver-only refresh in dev, rows already in bronze that violate the rules are in `rejected_rows` and absent from silver. Rules are forward-only by default; applying them to history is always an explicit decision.
5. The eval report meets the targets in section 9, and is committed.

## 13. Decision log (v1 → v2)

| v1 issue | v2 decision |
|---|---|
| Validator shared the agent's role | Separate `aidq_validator` role; validator runs as ci-dev with `SET ROLE`; agent has no UPDATE rights |
| `validation_result` editable at any status | Trigger allows changes only during PENDING → VALIDATED / VALIDATION_FAILED |
| Grants in migrations would fail on prod | Roles/grants in `grants/*.yml` per environment; migrations are schema only |
| UC grants incomplete | `agent` principal type: USE CATALOG/SCHEMA, SELECT, warehouse CAN_USE, endpoint CAN_QUERY |
| Severity change on approval impossible | Reviewer-owned `approved_severity`; `proposed_change` stays immutable |
| Write tools in the read-only MCP server | Separate toolbox running as `dq-agent` |
| New HARD rule doesn't clean silver | Forward-only by default; silver-only refresh as an explicit step |
| Customers wrongly treated as having no DQ stage | Customers defects become normal proposals |
| No incident category for non-expressible findings | `DQ_NOT_EXPRESSIBLE` |
| Daily manifests discarded | Persist manifests to Delta; recompute 10-02..10-08 if seeded, else exclude |
| `current_date` blocked by the determinism ban | Explicitly allowed; future-dated defects excluded from targets (generator skip rule) |
| SQL comments not rejected | Rejected; single-expression parse required |
