# Phase 6 plan: schema drift agent

Status: **decided, building** (owner decisions 2026-10-15). Design: `docs/agents_next_design.md` section 3. Dev only. Builds on the shared
foundation (migration 008, merged 2026-10-15) and on the DQ recommender's identity, toolbox, validator, review CLI,
routing and tracing. Every step below is a separate PR unless noted.

## 0. Facts (checked 2026-10-15, read-only)

| Fact | Value |
|---|---|
| Source columns not in metadata | `netsuite_customers.created_date`, `netsuite_customers.updated_date`, `netsuite_transactions.custbody_region` |
| Active metadata columns missing in source | none |
| Type differences | none |
| Source tables not onboarded | none |
| `netsuite_customers` | FullLoad: adding a column needs **no** bronze rebuild (bronze is rewritten every run) |
| `netsuite_transactions` | Incremental: adding `custbody_region` changes the hashed column set, so the row-hash guard fails closed until a dev bronze rebuild |
| Generator | `BRONZE_DROPS` says customers' `created_date` / `updated_date` are "pre-existing drift we do NOT fix" (a deliberate earlier choice) |
| Migration 008 | `DEACTIVATE_COLUMN`, `requires_rebuild`, `SCHEMA_TYPE_CHANGE` exist on dev |

## 1. Decisions (owner, 2026-10-15)

1. **dq-agent reads the source schema:** USAGE on schema `netsuite` and SELECT on the five tables (`grants/source.yml`
   `dq_agent`, owner-applied, PR #71), plus its Lakebase login role on netsuite-sample / production. The onboarding
   plan must note that a newly onboarded table (e.g. `netsuite_items`) needs the same SELECT.
2. **`aidq_validator` gets UPDATE on `requires_rebuild`** (PR #71). The validator sets it **only for incremental
   tables**: customers is full load, has no row hash, and needs no rebuild.
3. **The customers date columns:** the agent proposes them; the owner decides at review.
4. **Trigger:** daily at **07:30 UTC**.

## 2. Steps

1. **Detection (deterministic)** `src/aidq_drift/detect.py`: per onboarded table, source columns vs active
   `source_columns`: added, removed, type changed (via a type map Postgres -> metadata `data_type`, refusing unknown
   types), and source tables not in `source_table_def` (a hand-off note for the onboarding agent). Pure diff + reads.
2. **Read tools** in `aidq_mcp` (read-only, like the DQ tools): `get_source_columns(table)`,
   `get_metadata_columns(table)`, `profile_column(table, column)` (null rate, distinct count; aggregates only).
   `get_impact` comes from the RAG plan (phase 7); until it exists the evidence says "impact: not available".
3. **Agent** `src/aidq_drift/` (LangGraph, like the DQ agent): collect the diff, model step only when non-empty
   (routing task `drift_rationale`; primary `databricks-gpt-oss-120b` until an eval shows a smaller model qualifies),
   submit through the shared write toolbox, extended to `ADD_COLUMN` / `DEACTIVATE_COLUMN` and
   `report_finding(SCHEMA_TYPE_CHANGE)`. Every incremental-table rationale states that approval requires a bronze
   rebuild. No-nagging as in the DQ agent. Traced (MLflow, same experiment store, own experiment
   `/Shared/aidq/schema_drift`).
4. **Validator** (`tools/validate_proposals.py`, per type): `ADD_COLUMN`: column exists in source, not already active,
   type mappable, ordinal free; `DEACTIVATE_COLUMN`: active in metadata, absent from source, not the business key or
   watermark. Sets `requires_rebuild` for incremental tables (decision 2).
5. **Review CLI**: `review.py` already shows "requires bronze rebuild" and prints the rebuild hint; it will read
   `requires_rebuild` from the column instead of inferring it.
6. **Job** `schema_drift` (dev, run_as dq-agent, daily 07:30 UTC; validator task after it as ci-dev, like the DQ
   jobs), with a failure email.
7. **Eval** on a **dev branch of netsuite-sample** (owner pre-approval 2026-10-15): inject an added column, a dropped
   unused column and a type change; targets: every injected change detected (100%), correct proposal type or
   incident category, rebuild flag correct, nothing proposed on an unchanged source. The branch is created for the
   eval and listed in STATUS (deleting it later is asked first).
8. **Done when** (design 3.8): the real cases produce `ADD_COLUMN` proposals; one is approved; the guard fails closed
   on the next run as designed; the dev rebuild runs (`deploy-dev` with `bronze_rebuild=true`, owner's OK); the
   column appears in bronze and silver.

## 3. Not in this plan

Prod; type-change proposals (incidents only); auto-approval; onboarding new tables (phase 8, after this is built).
