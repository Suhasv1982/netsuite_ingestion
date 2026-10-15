# Phase 8 plan: source onboarding agent

Status: **plan, waiting for the owner's review** (written 2026-10-15). Design: `docs/agents_next_design.md` section 5.
Dev only. Builds on the shared foundation (migration 008: `ONBOARD_TABLE`, nullable `table_id`, the `apply_proposal`
branch), the DQ recommender's identity, toolbox, validator, review CLI, routing and tracing, the schema drift agent
(which reports a source table missing from metadata) and the lineage graph. **Nothing in this plan has been applied:
the DDL and the grants below are shown for review only.** Every step below is a separate PR unless noted.

## 0. Facts (checked 2026-10-15, read-only)

| Fact | Value |
|---|---|
| Item ids in `netsuite_transaction_lines` | 23 distinct (101-103, 3001-3020), no NULL; each id has exactly **one** `item_name` and **one** `rate` |
| Lines per item | about 4,400-4,800 each; first lines dated 2026-06-20 |
| `netsuite.netsuite_items` / `netsuite_products` | neither exists |
| Source table owner | the workspace owner, for all five tables (CI fails if a service principal owns one) |
| Source constraints | only `netsuite_customers` has a primary key (the generator's upsert needs it) |
| Pipeline type handling | the pipeline does not read `source_columns.data_type` (JDBC infers types); the drift agent's type map covers `numeric` -> `DECIMAL` |
| `apply_proposal(ONBOARD_TABLE)` | inserts `source_table_def` without `bronze_watermark` (NULL = no floor, 1900-01-01: same as the existing tables), `ingest_path` defaults to `MANAGED`, `max_parallel` to 1 |
| Business areas | 1 Membership, 2 Certification, 3 Transactions |
| Business-key uniqueness in the existing source | customers: exact. The four incremental tables are append-only with versions: e.g. transactions 35,508 rows, 16,901 keys, 99 NULL keys (injected defects), 35,249 distinct (key, `updated_date`) |
| Hard-coded table lists | `tools/daily_check.py` (`TABLES`, `BUSINESS_KEY`) and the `silver_refresh` job's `--tables` in `databricks.yml`. The pipeline, `ledger_check` and `sync_ledger` read `source_table_def` |
| Pipeline source identity (dev) | `pg_user` = the deploying identity (ci-dev); it already reads the five tables through its Lakebase project role, not through `grants/source.yml` |
| `review.py` / `validate_proposals.py` | `JOIN source_table_def USING (table_id)`: a proposal with `table_id` NULL is **silently missing** from `list`, `show` and validation |

## 1. Decisions for the owner

1. **Table name: `netsuite_items`** (NetSuite's own record name is "item"; the lines already say `item_id` /
   `item_name`). Recommended.
2. **`base_price` type: `numeric(12,2)`** (a price; exercises the `DECIMAL` mapping). Alternative: `integer`, like the
   lines' `rate`. Recommended: `numeric(12,2)`.
3. **Primary key on `item_id`.** Needed for the generator's upsert (as in customers). Consequence: no duplicate-key
   defect on this table (not in your defect list anyway).
4. **Load mode (what the agent should find):** `Incremental`, watermark `updated_date` (a DATE), `scd_type` `Merge`,
   business area 3 (Transactions), destination `netsuite.netsuite`, `discard_threshold` 0.05. Updates are made **in
   place** (price changes, inactivation) with `updated_date` moved to the batch date, so the watermark picks them up
   and silver merges on `item_id`. This is the ground truth for the eval; the agent must arrive at it from profiling.
5. **data-generator needs SELECT too**, not only INSERT/UPDATE: the upsert (`ON CONFLICT DO UPDATE`) and the daily
   backup (`CREATE TABLE ... AS TABLE`) both read the table. Same as customers today.
6. **Validator uniqueness rule for versioned tables** (needed for the replay eval, 5.7): the design says "business key
   non-null and unique on the actual source data", which only holds for customers and for items. Proposed rule:
   FullLoad: exactly unique and non-null. Incremental: (key, watermark) unique on at least 98% of rows, and NULL keys
   at most `discard_threshold`. Today: memberships 99.2%, certifications 99.4%, transactions 99.3%, lines 99.3%,
   NULL keys 0.3% or less.
7. **Trigger without a workflow change:** a Databricks job `source_onboarding` (run on demand with a `table`
   parameter, as dq-agent; validator task after it as ci-dev), plus the drift agent's daily hand-off note. A
   `workflow_dispatch` wrapper (design 5.6) is a workflow file: only if you want it, in its own PR.

## 2. DDL on the shared source (for review; NOT applied)

Run by the owner on `netsuite-sample` / production, database `databricks_postgres` (the owner owns the table, like the
other five). Prod ignores the table until it is onboarded in prod metadata.

```sql
CREATE TABLE netsuite.netsuite_items (
  item_id       integer PRIMARY KEY,     -- business key: the distinct item_ids already in transaction_lines
  item_name     text,                    -- the name the lines carry for that id
  item_type     text,                    -- Inventory / Non-inventory / Service / Kit
  category      text,
  base_price    numeric(12,2),
  is_inactive   boolean,
  created_date  date,
  updated_date  date                     -- a DATE, like the other tables' watermarks
);
```

Seeding (by the generator, step 2, not by hand): one row per distinct `item_id` in the lines, `item_name` and
`base_price` taken from the lines (each id has a single name and rate today), `item_type` and `category` from a fixed
table in the generator, `is_inactive` false, `created_date` = `updated_date` = the item's first line date.

| item_id | item_name | item_type | category |
|---|---|---|---|
| 101 | Membership Dues Product | Service | Membership |
| 102 | Certification Exam Product | Service | Certification |
| 103 | Training Voucher Product | Non-inventory | Education |
| 3001 | Annual Membership Fee | Service | Membership |
| 3002 | Certification Exam Fee | Service | Certification |
| 3003 | Study Guide | Inventory | Publications |
| 3004 | Webinar Pass | Non-inventory | Education |
| 3005 | Conference Ticket | Non-inventory | Events |
| 3006 | Training Workshop | Service | Education |
| 3007 | Practice Test Bundle | Kit | Certification |
| 3008 | Renewal Fee | Service | Membership |
| 3009 | Late Fee | Service | Fees |
| 3010 | Chapter Dues | Service | Membership |
| 3011 | Exam Retake | Service | Certification |
| 3012 | Digital Badge | Non-inventory | Certification |
| 3013 | Mentorship Program | Service | Career |
| 3014 | Journal Subscription | Non-inventory | Publications |
| 3015 | Career Coaching | Service | Career |
| 3016 | Group Discount Pack | Kit | Membership |
| 3017 | Sponsorship Package | Kit | Events |
| 3018 | Lab Kit | Inventory | Education |
| 3019 | Handbook | Inventory | Publications |
| 3020 | Recertification Fee | Service | Certification |

Undo (only with the owner's OK; a deletion): `DROP TABLE netsuite.netsuite_items;` before it is onboarded anywhere.

## 3. Grants (for review; NOT applied)

Added to `grants/source.yml` in the same PR that the owner applies with `tools/apply_pg_grants.py --apply` **after** the
DDL (the CI `--check` would fail on a table that does not exist yet, so the file is not changed in this plan PR):

```yaml
  data_generator:
    tables:
      netsuite.netsuite_items: [SELECT, INSERT, UPDATE]   # decision 5: SELECT for the upsert and the backup
  dq_agent:
    tables:
      netsuite.netsuite_items: [SELECT]                   # drift detection, profiling, onboarding (plan #69 decision 1)
```

Equivalent SQL that `apply_pg_grants.py` would run (role names are the service principals' ids, not written here):

```sql
GRANT SELECT, INSERT, UPDATE ON netsuite.netsuite_items TO "<data-generator>";
GRANT SELECT ON netsuite.netsuite_items TO "<dq-agent>";
```

Not changed: schema USAGE and database CONNECT (both roles have them); no DELETE / TRUNCATE; no ownership.
**To verify before onboarding** (read-only): that the dev pipeline identity (ci-dev) can read the new table through its
project role, as it reads the other five. If it cannot, that is one more grant, shown to the owner first.

## 4. Steps

1. **Review CLI and validator accept new-table proposals** (small PR, first, independent of the DDL):
   `review.py` `LIST_SQL` / `SHOW_SQL` and `validate_proposals.py` use `LEFT JOIN source_table_def`, and show
   `COALESCE(d.source_table, p.proposed_change->'table'->>'source_table')` (and the proposed load mode) for
   `ONBOARD_TABLE`. `show` prints the proposed table definition and the column list. `needs_rebuild` is false for
   `ONBOARD_TABLE` (a new table has no hashed column set yet). Tests: an `ONBOARD_TABLE` row appears in `list`, `show`
   and the validator's queue; the other types are unchanged.
2. **Generator** (`tools/netsuite_gen.py`, `tools/pg_writer.py`): `T_ITEMS` with its columns and the fixed type /
   category table; `--init` and the first increment after the DDL seed the 23 rows from the lines (idempotent: only
   missing ids); each increment updates a few items in place (price change, inactivation) through an upsert on
   `item_id`, moving `updated_date` to the batch date; new lines use the item's current valid price and normally only
   active items; the backup copies the table. Low-rate defects, recorded in the manifest like the others:
   `item_null_category`, `item_negative_price`, `item_invalid_type` (e.g. `Bundle`, `service`, `""`),
   `line_inactive_item` (a new line referencing an inactive item). All rates 0 by default; the daily config sets
   them like the existing defects. Tests: seeding matches the lines exactly, each defect lands at its rate, and the
   generator still works when the table does not exist (it skips items, so it can merge before the DDL).
3. **Owner applies the DDL and the grants** (sections 2 and 3), then the generator's next run seeds the table. The
   drift agent's next run (07:30) reports `netsuite_items` as a source table missing from metadata (the hand-off).
4. **Profiler** (deterministic, `aidq_onboard/profile.py`): per column type, null rate, distinct count, uniqueness;
   business-key candidates (alone or a pair; rule of decision 6); watermark candidates (date / timestamp columns whose
   maximum moves with updates: compares `max(col)` with the latest batch and the share of rows where it differs from
   `created_date`); row count. Aggregates only, no raw rows.
5. **Agent** `src/aidq_onboard/` (LangGraph, like the drift agent): profile, model step (new routing task
   `onboarding_design` in `routing.yml`, a large model as in design section 2; only `routing.yml` names models), one `ONBOARD_TABLE` through the write toolbox
   (`submit_onboard_table`, `table_id` NULL, no-nagging keyed on the table name), rationale per choice with the
   profiling numbers, `get_impact` / `search_metadata` evidence where useful. No DQ rules (the DQ recommender does that
   on its next run). Traced to its own experiment `/Shared/aidq/source_onboarding` in the same UC trace store.
6. **Validator** (`ONBOARD_TABLE` branch, as `aidq_validator`): the table exists in the source and is not in
   `source_table_def`; every proposed column exists with a mappable type and the list is exactly the source's; the
   business key passes decision 6 on the live data; the watermark exists and is a date / timestamp; load mode
   consistent with the watermark (Incremental needs one, FullLoad has none); destination schema exists;
   `business_area_id` exists. `requires_rebuild` false.
7. **Job** `source_onboarding` (decision 7), dev only, failure email.
8. **Daily check covers metadata tables**: `tools/daily_check.py` reads the table list and business keys from
   `source_table_def` instead of its constants (and the `silver_refresh` job's `--tables` is derived the same way, or
   documented as manual). Needed for "the daily check covers it" without editing a list per table.
9. **Eval** (`evals/run_onboarding_eval.py`, read-only on the source; proposals go to a fake toolbox): ground truth for
   `netsuite_items` (decision 4) and a replay of the five existing tables with their metadata hidden. Targets: correct
   business key and watermark for every table (6/6), correct load mode (6/6), column list exact (6/6), and the
   validator rejects a deliberately wrong key (e.g. `item_name`) and a wrong watermark (`created_date` on a table
   with in-place updates). Report in `evals/results/`.
10. **Done when** (design 5.8): the agent onboards `netsuite_items`; the owner approves it with `review.py`; the next
    dev run creates `poc_bronze.netsuite_items` and `poc_silver.netsuite_items` **with no code change**; the ledger
    check and the daily check cover it; the DQ recommender's next run looks at it.

## 5. Order and approvals

| Step | Needs the owner |
|---|---|
| 1 review / validator fix | merge |
| 2 generator | merge (rates stay 0 until the table exists) |
| 3 DDL + grants | **you run them** (shown above; DDL on the shared source, grant changes) |
| 4-7 profiler, agent, validator, job | merge each |
| 8 daily check | merge |
| 9 eval | review the report |
| 10 onboarding | approve the proposal in `review.py` |

## 6. Not in this plan

Prod (the prod metadata does not get `netsuite_items`; the prod pipeline ignores the table); auto-approval; DQ rules
for the new table (the DQ recommender's job); a `workflow_dispatch` trigger unless decision 7 says so; gold changes.
