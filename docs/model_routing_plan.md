# Plan: model routing (agents_next_design.md section 2, build order item 1)

Status: plan approved with the review corrections and **built** (owner, 2026-10-13); not merged until the owner's OK. Dev only. Design: `docs/agents_next_design.md` (committed in this PR). Section 1 below lists where that design conflicts with the current code, for every part of it. Only model routing is planned here; each other agent gets its own plan PR, in the section 6 order.

## 1. Review of agents_next_design.md against the code

### Model routing (planned in section 2 below)

1. **Package paths.** The design uses `agents/common/llm.py`, `agents/routing.yml` and `agents/prompts/`. The repo keeps agent code under `src/` (`src/aidq_agent`, `src/aidq_mcp`), so the CI and evals `PYTHONPATH=src` and packaging work unchanged. The DQ plan already moved prompts to `src/`. Planned: `src/aidq_llm/` (router, `routing.yml`, call log).
2. **`complete(task, messages, schema)` cannot carry the monitor's calls.** The only LLM work today is the monitor's RCA (`aidq_agent/dbx.py`): a multi-turn **tool-calling loop** that ends with a `submit_rca` tool, plus the eval judge. The router needs `tools` too. Escalation for a tool loop is per investigation: restart the investigation on the fallback model, not per message.
3. **`triage_summary` is not an LLM task.** The monitor's triage and report text are deterministic code (`triage.py`, `graph.report`). The monitor's model task is `root_cause`.
4. **Two retry layers.** `DatabricksChat` already retries HTTP 429/5xx up to 4 times with backoff. The design adds "retry once on the fallback". Plan: transient errors are retried in the client as now; after that, or on a schema failure, the router escalates once to the fallback. Each step is logged.
5. **`CAN_QUERY` grants (2.3) cannot be made.** The `databricks-*` foundation-model endpoints are pay-per-token, have no endpoint id or permissions object, and are open to every workspace user (checked 2026-10-08 and again 2026-10-13). Nothing to grant; the plan says so instead of adding grants.
6. **Model availability: confirmed** (2026-10-13). All candidates are listed READY, and each answered a one-tool call with a tool call: llama-3.1-8b (5.3 s), gemma-3-12b (1.0 s), gpt-oss-20b (0.8 s), gpt-oss-120b (1.0 s), qwen3-next-80b (1.1 s), llama-3.3-70b (0.8 s). Also available: `databricks-qwen35-122b-a10b` (the current judge), `databricks-llama-4-maverick`, `databricks-deepseek-v4-flash-0731`, and the embeddings `bge-large-en`, `gte-large-en`, `qwen3-embedding-0-6b`. One basic call says nothing about quality: section 2.4 measures it.
7. **The call log as a Delta table (`workspace.agents.llm_calls`).** The monitor runs on GitHub (no Spark), so writing Delta means a SQL warehouse statement per run. That starts the warehouse, which counts against the Free Edition compute quota. **Decide:**
   - (a) Delta as designed: one batched INSERT per run through the warehouse; schema `workspace.agents` and grants are owner-applied.
   - (b) Postgres `aidq_metadata.llm_calls` (migration 007): the monitor already writes incidents there, no warehouse is needed, `INSERT` goes to ci-dev and `aidq_agent`.

   Recommended: (b), exposed to Spark/SQL readers later if needed.

   **Decided (owner 2026-10-13): (b)** Postgres `aidq_metadata.llm_calls`, migration 007, grants in `grants/dev.yml`.
   - **Conditions:** logging failures never crash an agent (the call details go to the run summary instead), and a **90-day retention rule**.
   - **Built:**
     - `prune_llm_calls(keep_days)` (SECURITY DEFINER, not executable by PUBLIC), called after each run's insert; `retention_days: 90` in `routing.yml`.
     - Role `aidq_llm_logger` (members ci-dev and dq-agent): INSERT on `llm_calls` and EXECUTE on the prune function, nothing else.
8. **The trace id.** MLflow tracing does not exist yet (DQ plan step 8). Until it does, `llm_calls.trace_id` = the run's `agent_trace_id` (the monitor already sets one per run).
9. **Eval-based routing needs more runs.** The 2026-10-13 evals swung on items whose input did not change: 3 runs per case is too noisy to show that a smaller model "meets the same targets".
   **Decided (owner 2026-10-13):** enough runs, or paired comparisons on the same inputs. Built as both, in `src/aidq_llm/compare.py`. A task moves to a candidate only if all four rules hold:
   - at least 10 paired runs per case;
   - every check and every must-not-claim item passes N/N;
   - every must-identify item meets ceil(2N/3);
   - no item loses more paired runs to the baseline than it wins back, beyond 1 (b - c <= 1).

   The report gives the exact sign test per item.
10. **The judge is not routed.** The eval judge stays fixed (`databricks-qwen35-122b-a10b`), so model comparisons share one yardstick.

### Shared foundation, schema drift, RAG, onboarding (for their later plan PRs)

11. **One SP for all proposal writers (section 1).** Consistent with `grants/dev.yml` (`agent` principal, `aidq_agent`). New proposal types need migration 007+ (`DEACTIVATE_COLUMN`, `ONBOARD_TABLE`, `requires_rebuild`, `SCHEMA_TYPE_CHANGE`). Migration 006 already added `approved_severity` and `DQ_NOT_EXPRESSIBLE`.
12. **Drift test cases (3.1).** `netsuite_customers.created_date` / `updated_date` are on a **full-load** table: no row hash and no rebuild needed, but bronze currently drops them by design ("pre-existing drift we do not fix"). `custbody_region` exists in the source today (`netsuite_transactions`, outside `source_columns`); an `ADD_COLUMN` there needs a bronze rebuild (row-hash guard, #35).
13. **The drift trigger "daily after the dev pipeline" (3.6).** That would be a task in the dev job (ci-dev) or the GitHub monitor. Its LLM writer must run as dq-agent (section 1), and Databricks has one run identity per job. So a separate `schema_drift` job with `run_as: dq-agent` is needed, like the DQ recommender (decided 2026-10-13).
14. **Drift injection on a dev source branch (3.7).** `netsuite-sample` already has 5 of its 10 branches. A new branch is possible; the source project's branch count needs watching.
15. **RAG: one Vector Search endpoint on Free Edition (4.3).** Old demo indexes may have to go: ask before deleting (as the design says).
16. **Onboarding `load_mode` values (5.3).** The metadata uses `Incremental` / `FullLoad` (the code lower-cases them), not `INCREMENTAL`. Proposals must use the existing values.
17. **Onboarding DDL on the shared source (5.1)**: owner approval, as the design says. `netsuite_items` is invisible to prod until onboarded in prod metadata.

## 2. The model routing plan

### 2.1 Router

`src/aidq_llm/router.py`:
- `complete(task, messages, tools=None, schema=None)` returns the response.
- `investigate(task, ...)` wraps a whole tool loop with one escalation.
- No agent names a model. The monitor's `DatabricksInvestigator` and the eval judge call the router.
- `src/aidq_llm/routing.yml`: task → primary, fallback, max_tokens, and an illustrative list price per model.

| Task | Used by | Primary (to start) | Fallback |
|---|---|---|---|
| `root_cause` | monitor investigation (tool loop) | `databricks-gpt-oss-120b` (today's model) | `databricks-qwen3-next-80b-a3b-instruct` |
| `eval_judge` | `evals/run_agent_eval.py` | `databricks-qwen35-122b-a10b` (fixed, not routed) | none |
| `rule_generation`, `drift_rationale`, `onboarding_design`, `rag_answer` | later agents | set in their plan PRs, by eval | |

Moving `root_cause` to a smaller primary happens only through 2.4.

### 2.2 Escalation (deterministic, once per call or investigation)

- Escalate on: a timeout, an endpoint error after the client's own retries, a rate limit after retries, or structured output that fails schema validation (for `root_cause`: no valid `submit_rca` within the tool budget).
- Retry once on the fallback. Never a loop: if the fallback also fails, the error is returned. For the monitor that means "RCA failed", while the signal is still recorded (as today).
- Every escalation is logged with its reason.

### 2.3 Call log

One row per model call: time, agent, task, model, escalated and reason, input/output tokens, latency, estimated cost (list price from `routing.yml`; Free Edition is free, so this is illustrative), trace id, outcome. Storage per decision 1.7. Rows are buffered in memory and written once per run, so a failed write never breaks the agent: the error goes into the report (as with incidents, #41).

### 2.4 Choosing routes by eval

- `evals/run_agent_eval.py --model M` already exists. A new `evals/routing_compare.py` runs the monitor eval suite with each candidate for `root_cause`, 5 runs per case.
- It writes `evals/routing_report.md`: per model, every target (pass/fail), tokens, latency and estimated cost.
- A candidate becomes the primary only if it meets every target the current primary meets, with must-not-claim items at 5/5.
- Candidates: gpt-oss-20b, qwen3-next-80b, llama-3.3-70b, gemma-3-12b.

### 2.5 Tests

- Unit, with a fake chat client: route lookup; an unknown task is an error; escalation on each trigger, exactly once; no escalation on success; tool-loop escalation restarts on the fallback; log rows (fields, buffering, a failed write); no agent module names a model (static check over `src/`).
- Integration (opt-in, like the fingerprint test): one tiny call per routed model.
- Injected failure: a primary that always errors → one logged escalation, and the fallback answer is used (design 2.4 "done when").

### 2.6 Done when (design 2.4, adjusted)

- The monitor and the eval judge call through the router.
- The call log has rows from scheduled monitor runs.
- `evals/routing_report.md` compares at least four models for `root_cause`, with eval evidence. A task moves to a smaller model only if it passes; if none passes, the report says so and nothing moves.
- An injected endpoint failure produces a logged escalation.

### 2.7 Asks before the build

- Decision 1.7: call-log storage. Recommended: Postgres; migration 007 + an INSERT grant in `grants/dev.yml` for ci-dev and `aidq_agent`.
- Nothing else: no new identity, no grant on model endpoints (not possible, 1.5), no workflow change.
