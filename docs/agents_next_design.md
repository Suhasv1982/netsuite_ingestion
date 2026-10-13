# Next agents — design (v1)

Covers: **model routing**, the **schema drift agent**, **RAG over metadata** (with a lineage graph), and the **source onboarding agent**.
Status: design only, not implemented. Scope: **dev only**. Builds on `docs/dq_recommender_design.md` (v2); where they overlap, the shared foundation in section 1 applies to all agents.

Principle, unchanged: **the LLM proposes, deterministic code validates, a human approves, the database applies.**

---

## 1. Shared foundation (generalize what the DQ recommender builds)

Build these once, during or right after the DQ recommender, instead of per agent.

| Piece | Generalization |
|---|---|
| Agent identity | The `dq-agent` service principal and `aidq_agent` role serve **all proposal-writing agents**. `proposed_by` records the agent name and version (e.g. `schema_drift@0.1`); `agent_trace_id` links each proposal to its MLflow trace. One SP keeps Free Edition setup manageable; per-agent SPs are a later hardening step. |
| Write toolbox | `submit_proposal(type, table_id, change, rationale, evidence, confidence)` accepts every proposal type; `report_finding(category, ...)` writes incidents. Still a separate toolbox from the read-only MCP server. |
| Validator | One framework with a validator per proposal type (`ADD_DQ_RULE`, `ADD_COLUMN`, `DEACTIVATE_COLUMN`, `ONBOARD_TABLE`). Runs as `aidq_validator`. |
| Review CLI | `tools/review.py` shows every type. It displays a **"requires bronze rebuild"** flag when approval changes a hashed column set, and prints the exact rebuild command after approval. |
| LLM calls | All go through the router (section 2). |
| Tracing | MLflow traces for every agent run; versioned prompts in `agents/prompts/`. |

### Schema changes (one migration, schema only)

- `config_proposals.proposal_type`: add `DEACTIVATE_COLUMN` and `ONBOARD_TABLE`.
- `apply_proposal()`: new branches for both (section 3.5 and 5.5). `ONBOARD_TABLE` inserts the table definition and all its columns in one transaction.
- `config_proposals`: add `requires_rebuild boolean` (set by the validator, shown in review).
- `incidents.category`: add `SCHEMA_TYPE_CHANGE` (type changes are reported, not auto-proposed).

Roles and grants stay in `grants/dev.yml` and the owner-run role script, not in migrations.

---

## 2. Model routing

**Goal:** use the smallest model that meets each task's quality bar, escalate when it doesn't, and record cost and quality per route.

### 2.1 Design

- `agents/common/llm.py`: one `complete(task, messages, schema=None)` function. Every agent calls this; no agent names a model directly.
- `agents/routing.yml` maps **task → primary model → fallback model**, e.g.:

| Task | Primary | Fallback |
|---|---|---|
| `triage_summary` (monitor report text) | small | large |
| `drift_rationale` | small | large |
| `rule_generation` (DQ) | large | — |
| `root_cause` | large | — |
| `onboarding_design` | large | — |
| `rag_answer` | medium | large |

  Model names are resolved from the endpoints available in the workspace (e.g. small = `databricks-meta-llama-3-1-8b-instruct` or `databricks-gemma-3-12b`, medium = `databricks-gpt-oss-20b`, large = `databricks-gpt-oss-120b`). Confirm each is queryable on Free Edition before use.
- **Escalation rules** (deterministic): on timeout, endpoint error, rate limit, or **structured output that fails schema validation**, retry once on the fallback model. Log the escalation reason.
- **Per-call log** (Delta table `workspace.agents.llm_calls`): timestamp, agent, task, model, escalated (y/n) and reason, input/output tokens, latency, estimated cost (from a price table in `routing.yml`; Free Edition is free, so this is an illustrative list-price estimate), trace id.

### 2.2 How routes are chosen: by evaluation, not guesswork

- For each task, run that agent's eval suite with each candidate model.
- A task routes to the smaller model **only if it meets the same eval targets** as the large model. Record the comparison in `evals/routing_report.md` (quality vs tokens vs latency per model).
- Re-run when prompts or models change.

### 2.3 Grants

CAN_QUERY on each routed endpoint for the identities that call it (ci-dev for the monitor, the agent SP for proposal writers).

### 2.4 Done when

All agents call through the router; `llm_calls` is populated; the routing report shows at least one task safely moved to a smaller model, with eval evidence; an injected endpoint failure triggers a logged escalation.

---

## 3. Schema drift agent

**Goal:** detect differences between the source tables and the metadata, and propose the metadata change, with its cost (a bronze rebuild) made explicit.

### 3.1 Detection (deterministic)

For each onboarded table, compare the source `information_schema.columns` (netsuite-sample) with active `source_columns`:

| Difference | Handling |
|---|---|
| Column in source, not in metadata | `ADD_COLUMN` proposal |
| Active column in metadata, missing in source | `DEACTIVATE_COLUMN` proposal (urgent: the next run will fail) |
| Type changed | `SCHEMA_TYPE_CHANGE` incident (needs a human/code decision) |
| Table in source, not in `source_table_def` | Hand-off to the onboarding agent (section 5) |

Known real cases to use as test data: `netsuite_customers.created_date` / `updated_date` (in source, not in metadata) and `custbody_region` (if present from the earlier drift injection).

### 3.2 Agent (LLM, small model via routing)

The LLM is only called when the diff is non-empty. It:
- maps the source type to the metadata `data_type`;
- proposes an `ordinal`;
- judges business relevance (e.g. `custbody_*` custom fields) and writes the rationale;
- groups related changes on the same table into one review item where sensible.

Every `ADD_COLUMN` / `DEACTIVATE_COLUMN` rationale **must state that approval requires a bronze rebuild** for incremental tables (the hashed column set changes and the guard fails closed). Evidence: null rate and distinct count of the new column, rows affected, and the impact analysis from RAG/lineage (section 4) for removed columns.

### 3.3 Tools

Read-only: `get_source_columns(table)`, `get_metadata_columns(table)`, `get_impact(table.column)` (section 4). Write: the shared toolbox.

### 3.4 Validator

`ADD_COLUMN`: column exists in source; not already active; type mappable; ordinal free. `DEACTIVATE_COLUMN`: column active in metadata; absent from source (or explicitly requested); not the business key or watermark column. Sets `requires_rebuild` for incremental tables.

### 3.5 `apply_proposal` branch

`DEACTIVATE_COLUMN` sets `is_active = false` (never deletes the row, so history stays). `ADD_COLUMN` already exists.

### 3.6 Trigger

Daily after the dev pipeline (the diff is cheap and deterministic; the LLM runs only when something changed). No duplicate proposals: skip if an equivalent proposal is pending or was rejected in the last 30 days.

### 3.7 Evaluation

Inject drift on a **dev source branch** (never the shared production source): add a column, drop an unused column, change a type. Targets: every injected change detected (deterministic, so 100%); correct proposal type or incident category; rebuild flag correct; no proposals when there is no drift (run on an unchanged source).

### 3.8 Done when

Both real cases produce `ADD_COLUMN` proposals; one is approved, the guard fails closed on the next run as designed, the dev rebuild runs, and the column appears in bronze and silver.

---

## 4. RAG over metadata, with a lineage graph

**Goal:** answer engineers' and agents' questions about the platform ("what breaks if I change `transactions.type`?", "why was this rule added?", "which tables have no DQ rules?") with grounded, cited answers.

### 4.1 Two retrieval paths

- **Lineage graph (deterministic) for impact questions.** Facts like "which gold columns depend on this column" must come from a graph traversal, never from vector similarity.
- **Vector search for explanations and history.** Documents, rationales, incidents and change history are retrieved semantically.

### 4.2 Lineage graph (also covers the JD's "Knowledge Graph")

- Delta tables `workspace.knowledge.nodes` (source table/column, bronze/silver/gold table/column, DQ rule, proposal, incident) and `workspace.knowledge.edges` (`feeds`, `derived_from`, `validated_by`, `proposed_by`, `changed_by`, `caused`).
- Built from: metadata (`source_table_def`, `source_columns`, `data_quality_rules`); the pipeline's known structure (bronze → silver is 1:1 per column); **gold column lineage parsed from the gold SQL** with a SQL parser; proposals, change log and incidents.
- If Unity Catalog lineage system tables are available on Free Edition, cross-check against them; do not depend on them.
- Tool: `get_impact(object)` returns all downstream nodes with paths. `get_upstream(object)` for root-cause use.

### 4.3 Corpus for vector search

One document per: table (definition, columns, rules, load mode), column, DQ rule (with the proposal rationale that created it), incident, applied change, and each repo doc (README, design docs, STATUS). No environment identifiers, no raw data rows.

- Chunks in a Delta table `workspace.knowledge.docs` (id, type, object, text, source link, updated_at).
- Embeddings via an available embedding endpoint (e.g. `databricks-gte-large-en` or `databricks-bge-large-en`).
- **Free Edition limit:** one Vector Search endpoint. Reuse the existing endpoint; the old demo indexes may need removing to free capacity (ask before deleting). Delta-sync index on `knowledge.docs`.

### 4.4 Tools (read-only MCP server)

`search_metadata(question, k)`, `get_impact(object)`, `get_upstream(object)`, `explain_history(object)` (proposals, changes and incidents for an object, in order).

Answers must cite the documents or graph paths they used. If retrieval finds nothing relevant, the answer says so.

### 4.5 Consumers

The drift agent (impact of removed columns), the DQ recommender (history of rejected proposals), the monitor (upstream causes), and humans through Claude Code as an MCP client.

### 4.6 Refresh

Rebuild the graph and corpus after each deploy-dev and after the daily dev pipeline (small and cheap). Delta sync updates the index.

### 4.7 Evaluation

A fixed set of ~25 questions with known answers: impact questions checked exactly against the graph (100% required), history questions (retrieval recall@5 ≥ 0.9), and "no answer" questions (must not invent one). Citation present on every answer.

### 4.8 Done when

The four tools work from Claude Code; the eval meets its targets; the drift agent uses `get_impact` in at least one proposal's evidence.

---

## 5. Source onboarding agent

**Goal:** given a new source table, propose its complete metadata so the metadata-driven pipeline ingests it **with no code change**.

### 5.1 Test table

Add `netsuite_items` to the source (item master: `item_id`, `item_name`, `item_type`, `base_price`, `is_inactive`, `created_date`, `updated_date`). Realistic: `transaction_lines` already references `item_id` / `item_name`. Extend the generator to write items, with a few injected defects. Creating the table is a DDL change on the shared source; it is ignored by prod until onboarded in prod metadata. Owner approval required.

### 5.2 Profiling (deterministic)

Per column: type, null rate, distinct count, uniqueness; candidate **business keys** (non-null and unique, alone or as a small combination); candidate **watermark columns** (date/timestamp columns that grow with updates); row count.

### 5.3 Agent (LLM, large model)

Proposes one `ONBOARD_TABLE` with:
- `source_table_def`: business key, watermark column, load mode (`INCREMENTAL` if a watermark exists, else full), `scd_type`, destination schema, business area, `discard_threshold`;
- `source_columns`: every column with `data_type` and `ordinal`;
- rationale explaining each choice, with the profiling evidence (e.g. "`item_id`: 0 nulls, 100% unique").

It does **not** propose DQ rules. After onboarding, the DQ recommender handles that table on its next run.

### 5.4 Validator

Table exists in source and is not already onboarded; business key non-null and unique **on the actual source data**; watermark column exists and is a date/timestamp; every column exists; destination schema exists; load mode consistent with the watermark.

### 5.5 `apply_proposal` branch

`ONBOARD_TABLE` inserts the `source_table_def` row and all `source_columns` rows in one transaction, linked to the proposal.

### 5.6 Trigger

On demand (`workflow_dispatch` with a table name), and when the drift agent reports a source table missing from metadata.

### 5.7 Evaluation

Ground truth: the known correct design for `netsuite_items` (and, as a replay test, the existing five tables with their metadata hidden). Targets: correct business key and watermark for every table; correct load mode; column list exact; validator rejects a deliberately wrong key.

### 5.8 Done when

The agent onboards `netsuite_items`; after approval, the next dev run creates its bronze and silver tables **without any code change**; the daily check covers it.

---

## 6. Build order

| Order | Item | Depends on | Why this order |
|---|---|---|---|
| 0 | DQ recommender (in progress) + shared foundation | — | Builds the identity, toolbox, validator and review CLI everyone reuses |
| 1 | Model routing | — (can start now) | Small, cross-cutting; every later agent uses it from day one |
| 2 | Schema drift agent | Foundation, migration | Real cases already exist; mostly deterministic |
| 3 | RAG + lineage graph | — (read-only; can run in parallel with 2) | Gives drift and DQ agents impact evidence |
| 4 | Onboarding agent | Foundation, migration, new source table | Largest change; best demo ("new table, no code change") |

One PR per item, plan in the PR description first, dev only, same rules as before.

## 7. Out of scope

Prod for any of this; per-agent service principals; auto-approval of any proposal type; type-change proposals (incidents only); a web UI for review (the CLI is enough for now).
