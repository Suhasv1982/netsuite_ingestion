# Phase 7 plan: RAG over metadata, with a lineage graph

Status: **plan only** (2026-10-15). Design: `docs/agents_next_design.md` section 4. Dev only, read-only for every
consumer. Can run in parallel with the schema drift agent (phase 6). Every step below is a separate PR unless noted.

## 0. Facts (checked 2026-10-15, read-only)

| Fact | Value |
|---|---|
| Vector Search endpoints | **two**, both ONLINE: `vs_endpoint_1` (1 index: `agentic_catalog.agentic_schema.product_docs_combined_index`) and `dbdemos_vs_endpoint` (1 index: `main.dbdemos_ai_agent.knowledge_base_vs_index`). The design assumed one endpoint and possibly removing demo indexes: **not needed**, a second index fits on an existing endpoint |
| Embedding endpoints | `databricks-gte-large-en`, `databricks-bge-large-en`, `databricks-qwen3-embedding-0-6b` |
| UC lineage system table | `system.access.column_lineage` is readable (24k rows in 7 days): usable as a cross-check, not a dependency |
| Gold SQL | `src/netsuite_ingestion/gold_sql.py`: parseable with sqlglot (already a dev dependency) |
| Metadata | `source_table_def`, `source_columns`, `data_quality_rules`, `config_proposals`, `config_change_log`, `incidents` |

## 1. Open decisions (owner)

1. **A new dev schema `workspace.knowledge`** for `nodes`, `edges` and `docs` (Delta). The owner creates it (as with
   `workspace.generator` and `workspace.aidq_traces`) and grants (`grants/dev.yml`, owner-applied):
   - ci-dev: `USE SCHEMA`, `CREATE TABLE`, `SELECT`, `MODIFY` (the refresh job writes the graph and the corpus);
   - dq-agent: `USE SCHEMA`, `SELECT` (the drift and DQ agents read impact and history);
   - the owner: `SELECT` (Claude Code as an MCP client).
2. **Which Vector Search endpoint** gets the index: **`vs_endpoint_1`** (recommended; generic name) or
   `dbdemos_vs_endpoint`. Either way **no demo index is removed**. The index is a Delta-sync index on
   `workspace.knowledge.docs`, created by ci-dev; ci-dev needs `CAN_USE` on the endpoint (endpoint permission: a grant
   change), and dq-agent / the owner need `SELECT` on the index (UC).
3. **Embedding model:** `databricks-gte-large-en` (recommended: the design's first choice, English docs).

## 2. Steps

1. **Lineage graph (deterministic)** `src/aidq_knowledge/graph.py`, written to `workspace.knowledge.nodes` / `edges`:
   - nodes: source table/column, bronze/silver/gold table/column, DQ rule, proposal, incident;
   - edges: `feeds` (source column -> bronze -> silver, 1:1 per active column), `derived_from` (gold columns, parsed
     from the gold SQL with sqlglot), `validated_by` (column -> DQ rule, from `rule_expr` columns), `proposed_by`
     (rule -> proposal), `changed_by` (object -> change-log entry), `caused` (incident -> object);
   - a cross-check against `system.access.column_lineage` reported, never required.
   Pure builders + one writer; unit-tested on fixtures.
2. **Corpus** `src/aidq_knowledge/corpus.py`, written to `workspace.knowledge.docs` (id, type, object, text,
   source link, updated_at): one document per table, column, DQ rule (with its proposal's rationale), incident,
   applied change, and each repo doc (README, design docs, STATUS), chunked. No environment identifiers, no data rows
   (the same redaction as the MCP tools).
3. **Vector index**: Delta-sync index on `knowledge.docs` (decision 2, 3), created idempotently by the refresh job.
4. **Read-only MCP tools** (added to `aidq_mcp`, its read-only test stays): `search_metadata(question, k)`,
   `get_impact(object)`, `get_upstream(object)`, `explain_history(object)`. Answers cite the documents or graph paths
   they used; nothing relevant -> "no answer".
5. **Refresh job** `knowledge_refresh` (dev, as ci-dev): rebuild graph and corpus after deploy-dev and after the daily
   dev pipeline (small and cheap); Delta sync updates the index.
6. **Eval** (`evals/run_rag_eval.py`): ~25 fixed questions with known answers: impact questions checked exactly
   against the graph (100%), history questions (recall@5 >= 0.9), "no answer" questions (must not invent one);
   a citation on every answer. Report in `evals/results/`.
7. **Consumers**: the drift agent's `get_impact` evidence for removed columns (phase 6), the DQ recommender's history
   of rejected proposals, the monitor's upstream causes.
8. **Done when** (design 4.8): the four tools work from Claude Code; the eval meets its targets; the drift agent uses
   `get_impact` in at least one proposal's evidence.

## 3. Not in this plan

Prod; write tools of any kind; removing Vector Search demo indexes (not needed: two endpoints exist); a UI.
