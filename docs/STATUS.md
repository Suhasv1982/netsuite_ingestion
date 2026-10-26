# STATUS (updated 2026-10-26)

Read this first, then run `git status` and check open PRs. Updated after every Phase 2 step.

## 1. Current state

**Repo.** `Suhasv1982/netsuite_ingestion`, public. `main` is protected: pull request required (0 approvals),
required checks `gitleaks (full history)`, `pytest` and `bundle validate` (added 2026-09-30), administrators
included, no force push or deletion. Secret scanning and push protection on. Local pre-push hook: gitleaks + pytest.

**Pull requests** (2026-10-13). Merged: #1-#42 (#36-#42 on 2026-10-13, in the owner's order), and #43 (this
update). Open: the watchdog plan (section 0h). Required checks on `main`: `gitleaks (full history)`, `pytest`,
`bundle validate`, `migrate plan (dev)`.

**Work in progress** (owner's instructions of 2026-09-30):
* Part 1, close out Phase 1 in one dev-only PR: gold layer, SOFT-rule expectation check, guard read-retry count,
  README, proposed daily dev schedule (shown, not unpaused).
* Part 2, Phase 2 CI/CD, one PR per step: A ci-dev spike, B bundle validate, C `tools/migrate.py`, D deploy-dev,
  E canary, F promote-prod. Stop after F and report.

**Pre-approved** (2026-09-30): ci-dev resources (SP, OAuth secret, Lakebase roles, grants, scope
`netsuite_ingestion_dev` with ci-dev WRITE); the three `DATABRICKS_*_DEV` GitHub secrets; workflows on feature
branches and PRs; merging a PR once all checks are green and it touches no prod resources.
**Ask first:** anything touching prod; deleting or destroying anything; branch protection; history rewrites.

**Schedules** (2026-10-15). Dev: generator 05:00 and `[dev ci_dev] netsuite_ingestion_daily` 05:30 UTC, UNPAUSED (they
do not always fire on Free Edition: section 0h); `guard_canary_check` PAUSED. Prod `netsuite_ingestion_daily` PAUSED.
Monitor (GitHub `monitor-dev.yml`): daily ~06:17 UTC, the external watchdog. `knowledge_refresh` daily 07:15 (ci-dev);
`schema_drift` daily 07:30 (dq-agent); `dq_validator` daily 07:45 and 08:45 (ci-dev); `dq_recommender` Sundays 08:00
UTC as dq-agent, writing proposals (write=true); the owner reviews with `tools/review.py`. `silver_refresh`: manual
only, each run after the owner's OK. Full list: section 0p.

**Lakebase endpoints.** Found disabled on 2026-09-30 (since 2026-09-27) and again at about 20:15 UTC the same day,
about 3.5 h after last use; most likely the Free Edition compute quota (not verified). Re-enabled: dev metadata and
source (owner's OK), prod metadata (by the owner, 20:53 UTC). Nothing runs on prod: its schedule is PAUSED.

**Metadata databases (Lakebase project `aidq-metadata`).**

| | Branch | Migrations | Owner of `aidq_metadata` |
|---|---|---|---|
| dev | `dev` | 001, 002 recorded (backfill, schema verified); 003 applied by the first deploy-dev run | `aidq_owner` (no-login; members: owner, ci-dev) |
| prod | `production` | **none applied**, no `schema_migrations` | the owner (no `aidq_owner`, no ci-dev role yet) |

**Source database (`netsuite-sample`):** production plus backups `pre-synthetic-backup-202609242053`,
`...202609252235`, `...202609260101`, `...202609260109` (5 of 10 branches).

## 0t. 2026-10-26: quota again, incidents merged, monitor off, #82 decided, source duplicates

**Quota timeline.** Available **10-20..10-23**: every scheduled dev job fired and ended SUCCESS (generator,
ingestion, knowledge_refresh, schema_drift, dq_validator x2); nothing was merged. **Restricted again since 10-24**:
no scheduled run on 10-24, 10-25 (incl. the Sunday dq_recommender) or 10-26; warehouse refused on 10-24.
`daily_check` for 10-20..10-23 not run (needs the warehouse).

**Incidents (owner).** 37, 39, 42, 43, 44, 45 merged into **32** (RESOLVED, `evidence.resolution.merged_into = 32`;
32 lists them and the three episodes in `evidence.merge`). 32 is the only OPEN quota incident. 36
(`netsuite_items` not onboarded) stays OPEN: it closes through onboarding.

**Watchdog.** GitHub workflow `monitor-dev.yml` **disabled** (owner, 10-26; `gh workflow enable monitor-dev.yml`),
on top of `WATCHDOG_TRIGGER=off`. No daily report, no incidents, no endpoint wakes; nothing reports the reset.
**Turn the workflow back on as soon as the quota resets, with `WATCHDOG_TRIGGER=off`; triggering on after #82's
build merges.**

**#82 usage reduction: all 7 decisions taken** (plan section 1a: 1 rec., 2 rec., 3 once a day, 4 (a), 5 rec.,
6 as listed, 7 rec.); the spike's throw-away dev deploy approved (after the reset).

**Source duplicates (incidents 7/9/11/13), read-only analysis.** 716 true duplicates (same key, same
`updated_date` day): 429 from the 07-11 base load's `duplicate_business_key` defect (= its manifest, = the
incidents), 287 from incident 1's 10-02/10-03 collisions (21 exact copies). Nothing after 10-03; the daily config
never had the defect. **PR #86**: the generator refuses the defect and a guard refuses any second row of a key on
one day; the cleanup plan (`docs/source_duplicate_cleanup_plan.md`, backup branch, self-checking SQL for the owner
to run) was **approved by the owner on 10-26**; it runs after the reset. **Nothing deleted yet.**

**Orphans (incident 14):** all 584 orphan line ids come from the 07-11 base load: 294 `orphan_lines` (= manifest)
and 290 lines of the 99 transactions whose key `null_business_key` nulled. Known defects, not a bug; the row count
grows because daily versions copy those lines (1,739 rows now; 1,340 through 10-14 = the incident). Stays OPEN.

**Set-level checks** (uniqueness, referential integrity) for real sources: added to the phase 8 plan (#77, section 7)
as the next work after onboarding (owner, 10-26).

**Late rows (8/10/12/15): RESOLVED** (owner, 10-26) as handled by design (top-up flows); the last `daily_check`
(10-16) PASSED, bronze = source per table and day.

**PR #87:** DQ findings carry counts by snapshot date and up to 5 example keys (from the profile, not the model).

**After the reset (owner order):**
1. Re-enable `monitor-dev.yml` (`WATCHDOG_TRIGGER` stays off); `daily_check` for the first day back (and 10-20..23).
2. Merge one at a time, outside 05:00-07:00 UTC, each after the previous deploy-dev: **#83, #84, #85, #81, #80,
   #77** (owner confirmed #83 first, 10-26). #79 after the owner has seen its eval.
3. #86 (then section 4 of its plan: backup branch, cleanup by the owner, dev bronze rebuild, `daily_check`), #87
   (after the DQ eval), the #82 build (spike first).

**Handoff 2026-10-26 ~20:40 UTC (owner offline).**
* **State:** no Databricks run active, no GitHub run in progress, nothing uncommitted, only `main` checked out (no
  worktree). Quota still restricted (no scheduled run since 10-23). `monitor-dev.yml` disabled, `WATCHDOG_TRIGGER=off`.
* **Open PRs (10):** #77 (onboarding plan + section 7 set-level checks), #79 (monitor facts; eval pending), #80
  (this STATUS), #81 (eval artifacts), #82 (usage reduction; decisions recorded), #83 (items grants), #84, #85
  (onboarding steps 1-2), #86 (generator duplicate guard + approved cleanup plan), #87 (finding evidence detail).
  None may merge until the reset (a merge starts deploy-dev, which runs a job).
* **OPEN incidents:** 7, 9, 11, 13 (duplicates; close after the cleanup and a clean `daily_check`), 14 (orphans,
  known defects), 32 (quota), 36 (`netsuite_items` not onboarded).
* **Reset date unknown:** Databricks publishes none. The first restriction lasted 10-18..10-19 (back on 10-20); this
  one since 10-24. **How to tell:** the scheduled generator (05:00 UTC) and dev ingestion (05:30) show new PERIODIC
  runs (`databricks jobs list-runs --profile DEFAULT`); start nothing to probe. Earliest: 2026-10-27 05:00 UTC.
* **First steps after the reset:** the list above ("After the reset"), starting with re-enabling the monitor.

**Prod:** on hold.

## 0s. Handoff 2026-10-19 ~16:10 UTC (owner offline): quota restriction, open PRs, first steps after the reset

**Free Edition fair-use quota restriction (incident 32, the only OPEN COMPUTE_QUOTA incident).** Started
**2026-10-18** (last scheduled runs: 10-17, all SUCCESS; nothing fired on 10-18 or 10-19). Refused since: job
run-now (`Triggering new runs for organization <id> is currently disabled temporarily`), the SQL warehouse, the
foundation model endpoints. The Lakebase endpoints still work (re-enabled on use). **Not reset** as of 10-19 16:10 UTC.

**At handoff:** `WATCHDOG_TRIGGER=off` (repository variable; the monitor runs and reports, triggers nothing). No
Databricks run active. Three PR-check runs queued on GitHub for #83/#84/#85 (API and Lakebase calls only, no job).
Nothing uncommitted; no worktree. No live eval, model call or job was started after the restriction began.

**DQ proposals:** **none open.** The 7 from 10-14/10-15 were reviewed on 10-16 (0q: 17, 19, 22 rejected; 12, 13,
18, 25 applied); the recommender has not run since (Sunday 10-18 did not fire), and no drift proposal was written.
Still OPEN: incidents **7-15** (DQ_NOT_EXPRESSIBLE, 10-14: duplicate keys, late arrivals, orphan lines; for the
owner's review) and **32** (quota).

**Onboarding (#77):** the owner created `netsuite.netsuite_items` (10-19; 8 columns as planned, owner-owned;
SELECT for ci-dev and ci-prod from the default privileges, accepted). Grants applied by this session: data-generator
SELECT/INSERT/UPDATE, dq-agent SELECT; `apply_pg_grants.py --check` exact for both. The table is empty until the
generator's first run with #85.

**Open PRs (none may merge while the restriction lasts: a merge starts deploy-dev, which runs a job):**

| PR | What | State |
|---|---|---|
| #83 | `grants/source.yml` for netsuite_items (+ least-privilege test: UPDATE on customers and items only) | applied already; **merge FIRST**: deploy-dev's `apply_pg_grants.py --check` fails on the live items grants until this file is on main |
| #80 | this STATUS (0q, 0r, 0s) | docs |
| #77 | onboarding plan | decisions approved 10-16; docs |
| #81 | eval results as GitHub Actions artifacts (`agent-eval.yml`, dq-eval upload) | needed by #79's eval |
| #79 | monitor deterministic facts (incident 30) | eval incomplete: see step 4 below |
| #84 | onboarding step 1: review CLI / validator see ONBOARD_TABLE | unit tests only |
| #85 | onboarding step 2: generator fills netsuite_items (item defects off) | unit tests only |
| #82 | usage-reduction plan (chained daily job, quota-aware watchdog) | **7 owner decisions** (section 1) |

**When the quota resets, first:**
1. **Confirm the reset without starting anything:** the next scheduled 05:00 generator and 05:30 dev runs fire on
   their own (schedules UNPAUSED). If they do not fire, the owner decides how to probe (watchdog stays off).
2. `python tools/daily_check.py --date <that day> --profile DEFAULT`. 10-18 and 10-19 have no batch; no backfill
   unless the owner says so (precedent 10-13: none).
3. Merge **#83 first**, alone; wait for its deploy-dev (grant check exact). Then one PR at a time, each after the
   previous deploy-dev: #80, #77, #81, #84, #85 (#85: the next generator run seeds the 23 items).
4. **#79:** merge main into the branch (for `agent-eval.yml`), then run only `missed_runs_metadata_unavailable` and
   `guard_blocked_after_add_column_10_15` (`gh workflow run agent-eval.yml --ref fix/monitor-deterministic-facts
   -f cases="missed_runs_metadata_unavailable guard_blocked_after_add_column_10_15"`), explain the 10-16 run-3
   failure, then the full suite once, then merge.
5. Owner: the 7 decisions in #82; `WATCHDOG_TRIGGER` back to `on` only with the owner (#82 step 5).
6. Optional, once: the live rolled-back review test for #84 (`tests/test_review_cli.py::test_review_on_dev_rolled_back`).

**Eval policy (owner 2026-10-19):** the affected cases while developing, the full suite only before a merge.
**Prod:** all activities on hold.

## 0r. 2026-10-17..19: platform restriction (no scheduled runs since 10-18); open items

* **10-17:** generator, dev job, knowledge_refresh, schema_drift, dq_validator x2 all scheduled and SUCCESS.
* **10-18 and 10-19: no scheduled run fired** (no generator, dev job, knowledge_refresh, schema_drift,
  dq_validator; the Sunday 10-18 `dq_recommender` did not run either). The watchdog's run-now was refused:
  `Triggering new runs for organization <id> is currently disabled temporarily`. Also refused since 10-18: the SQL
  warehouse (`The request could not be processed by the warehouse`) and the foundation model endpoints
  (`Cannot create or query foundation model endpoints, please try again later`). The monitor re-enabled both
  Lakebase endpoints each day. Free Edition platform restriction, cause not confirmed; nothing was changed on our
  side. Nothing done about it (owner decision pending).
* **New OPEN incidents:** 31 (10-17, OTHER): the same misdiagnosis as incident 30 (the 10-15 20:01 run is still in
  the 3-day window; new fingerprint because 30 was resolved); #79 fixes the diagnosis. 32 and 34 (ORCHESTRATION,
  "[self-heal failed]": missed runs 10-18, 10-19, run-now refused). 33 and 35 (OTHER: warehouse refused; 35 has no
  RCA, the model call was refused).
* **PR #79** (monitor deterministic facts, incident 30): built, pytest green, the new eval case met its targets on
  10-16. The full regression eval is incomplete (one failed run in `missed_runs_metadata_unavailable`, not yet
  examined; the last case lost the network); a re-run on 10-19 was refused by the model endpoints. Re-run before
  merging.
* **Owner decisions 2026-10-19:**
  * The restriction is the **Free Edition fair-use quota** (same message on the Databricks community). **Trigger
    nothing until it resets.** Watchdog kill switch set: repository variable **`WATCHDOG_TRIGGER=off`** (the monitor
    still runs and reports; back to `on` only with the owner). **Hold every merge** too: a merge starts deploy-dev,
    which runs an ingestion job.
  * Incidents: **32 is the single OPEN COMPUTE_QUOTA incident** (33, 34, 35 merged into it and RESOLVED, each with
    `evidence.resolution.merged_into = 32`; 32 lists them in `evidence.merge`). **31 RESOLVED as a duplicate of 30**
    (SCHEMA_DRIFT, proposal 48).
  * **Usage reduction: plan PR #82** (one chained daily job of run_job_tasks, validator once a day, a quota refusal =
    one COMPUTE_QUOTA incident per day with no model call and no retries).
  * **#79:** when the model endpoints work again, re-run only `missed_runs_metadata_unavailable` and
    `guard_blocked_after_add_column_10_15` first and explain the run-3 failure before merging.
  * **Eval results as GitHub Actions artifacts: PR #81** (new manual workflow `agent-eval.yml`; `dq-eval.yml` uploads
    too; timestamped result files).
  * **Eval policy:** the affected cases while developing; the full suite only before a merge.
  * **Prod activities on hold.**
  * Worktree `../wt_status` removed.
* **Waiting for the owner:** see 0s (the DDL and the grants are done).

## 0q. 2026-10-16: daily cycle, incident 30, proposals reviewed, onboarding default privileges

**Daily cycle 10-16 (all scheduled, all SUCCESS):** generator 05:00, dev `netsuite_ingestion_daily` 05:30-05:42,
`knowledge_refresh` 07:15, `schema_drift` 07:30 (no new proposals), `dq_validator` 07:45 and 08:45.
`daily_check --date 2026-10-16`: **PASSED** (bronze = source per table and date, ledger OK; WARN only for no prod
run). No Lakebase endpoint had to be re-enabled.

**Incident 30 (monitor 11:32 UTC): misdiagnosis, corrected and RESOLVED (owner 2026-10-16).** The monitor wrote
ORCHESTRATION, "a code bug in bronze.py line 8", and recommended a code rollback. Real cause: the row-hash guard
blocked the 10-15 20:01 dev run and its 5 retries (`BLOCKED: the hashed column set of netsuite_transactions changed
in source_columns (added custbody_region)`) after proposal 48 was applied at 19:27, by design (0o); the rebuild at
20:20 fixed it. The error text is ~14k characters and the BLOCKED line is near its end; the tool returned the first
500 (same pattern as incident 6). Re-categorized SCHEMA_DRIFT, linked to proposal 48, the monitor's original
diagnosis kept in `evidence.resolution`. Fix: PR #79 (0r).

**DQ proposals reviewed (owner 2026-10-16):** rejected 17, 19, 22 (HARD "ID not null" on certifications,
transactions, lines) with the reason "NULL IDs are injected generator defects, not real source behaviour; do not
enforce". Approved and applied as SOFT: 12 (Company Name Not Null), 13 (Valid Email Format), 18 (Certification Start
Not In Future), 25 (Transaction Date Not In Future): rules 18-21, enforced from the next dev run. No proposal open.
Rule 11 (memberships ID not null, HARD) unchanged. Incidents 7-15 (DQ_NOT_EXPRESSIBLE, 10-14) are still OPEN.

**aidq MCP server registered in Claude Code** on the owner's machine (user scope, all projects; not committed):
`claude mcp add aidq --scope user -e PYTHONPATH=<repo>/src -e PYTHONIOENCODING=utf-8 -- <repo>/.venv/Scripts/python.exe
-m aidq_mcp --profile DEFAULT`. Connected. (The package is not installed in the venv, so `PYTHONPATH` is needed;
the bare command in 0p fails with `No module named 'aidq_mcp'`.) Remove: `claude mcp remove aidq -s user`.

**Onboarding (#77): plan decisions approved (owner 2026-10-16); default privileges checked (read-only).** On
`netsuite-sample`/production the owner's role has default privileges in schema `netsuite`: every table the owner
creates there gets **SELECT for ci-dev and ci-prod** automatically (`pg_default_acl`, objtype r), nothing else.
So `netsuite.netsuite_items` will be readable by **ci-prod** as soon as it exists. **Accepted by the owner
(2026-10-16)**: read-only, and prod ignores the table until it is onboarded in prod metadata. It also means ci-dev
(the dev pipeline identity) needs no extra grant. dq-agent and data-generator get theirs explicitly (plan section 3)
after the owner runs the DDL.

## 0p. End of day 2026-10-15: RAG done-when, what is merged, waiting, scheduled

**RAG + lineage agent against design 4.8** (checked 21:10 UTC):

| Done-when item | Status |
|---|---|
| The four tools work from Claude Code | **Met at the MCP level, not registered.** Over MCP stdio (`python -m aidq_mcp --profile DEFAULT`, the transport Claude Code uses) the server lists 13 tools; `get_impact`, `get_upstream`, `explain_history` and `search_metadata` all return `ok` on dev (search: proposal 48 at 0.70). The server is not registered in this machine's Claude Code (`claude mcp list`); registering it is the owner's local choice: `claude mcp add aidq -- .venv/Scripts/python -m aidq_mcp --profile DEFAULT` from the repo |
| The eval meets its targets | **Met.** `evals/results/rag_eval_2026-10-15.md`: PASS (impact / upstream 10/10, recall@5 90%, no-answer 5/5, citations) |
| The drift agent uses `get_impact` in a proposal's evidence | **Met in the drift eval only.** The eval's removed-column proposal carries the impact; the only real drift proposals (46-48) were written before the graph existed and say "not available". The next real drift proposal writes it |

**Cleanup (owner-approved 2026-10-15):** worktrees `wt_drift` and `wt_rag` removed (clean; their branches merged);
Lakebase branch `netsuite-sample/drift-eval` deleted (production and the four archived backups untouched).

**Merged today:** #66-#76 (DQ recommender weekly schedule, migration 008, drift and RAG plans, their grants, the
drift agent, RAG + lineage, the RAG eval and `get_impact` evidence, the drift done-when). Last deploy-dev (after #76):
success at 21:14 UTC.

**Waiting for the owner:**
* **#77, phase 8 onboarding plan** (`docs/phase8_onboarding_plan.md`): 7 decisions (section 1), the `netsuite_items`
  DDL and the grants (data-generator SELECT/INSERT/UPDATE: SELECT added, see decision 5; dq-agent SELECT). **Nothing
  applied.**
* Optional: register the `aidq_mcp` server in Claude Code (above).
* Open item from before: the prod access gap (0k), no action while dev only.

**At end of day:** no active job runs, no GitHub run in progress except #77's PR checks, nothing uncommitted; only
`main` checked out (no worktrees).

**Scheduled, will keep running (UTC):**

| Time | Job | Identity |
|---|---|---|
| 05:00 daily | `[dev ci_dev] netsuite_daily_generator` | data-generator |
| 05:30 daily | `[dev ci_dev] netsuite_ingestion_daily` | ci-dev |
| ~06:17 daily | GitHub `monitor-dev.yml` (the watchdog; GitHub may start it late) | ci-dev |
| 07:15 daily | `[dev ci_dev] knowledge_refresh` | ci-dev |
| 07:30 daily | `[dev ci_dev] schema_drift` | dq-agent |
| 07:45 and 08:45 daily | `[dev ci_dev] dq_validator` | ci-dev |
| Sundays 08:00 | `[dev ci_dev] dq_recommender` (writes proposals) | dq-agent |

PAUSED: prod `netsuite_ingestion_daily`, both `guard_canary_check`. Manual only: `silver_refresh`. No merges
05:00-07:00 UTC.

## 0o. Schema drift agent done-when completed (2026-10-15)

* **Owner approved** proposals 46, 47 (`netsuite_customers.created_date` / `updated_date`, full load) and 48
  (`netsuite_transactions.custbody_region`, incremental); applied 19:5x UTC.
* **Guard fails closed as designed:** a dev job run at 20:01 UTC failed in `run_pipeline` with `BLOCKED: the hashed
  column set of netsuite_transactions changed in source_columns (added custbody_region)` (4 pipeline retries); the
  other tasks completed.
* **Dev bronze rebuild** (owner's OK): deploy-dev with `bronze_rebuild=true` 20:20-20:36 UTC, success; dev job
  SUCCESS; ledger check exact on all four tables; redeployed with `bronze_rebuild=false`.
* **Columns landed:** `custbody_region` in bronze (25,410 of 35,508 filled, 4 values) and silver (14,664 of 16,902,
  4 values); the customers dates in bronze on all 3,400 rows. Bronze lines = 106,518 = the source; the ledger's
  106,497 counts exact duplicate rows once (21 in the source), by design.
* All items of design 3.8 are met.

## 0n. RAG + lineage built; done-when evidence ready for review (2026-10-15)

* **Knowledge store live** (#74; job `knowledge_refresh` as ci-dev, daily 07:15 UTC): `workspace.knowledge.nodes`
  (165) / `edges` (241) / `docs` (295, CDF on) and the Delta-sync index `docs_index` on `vs_endpoint_1`
  (databricks-gte-large-en), READY. No demo index touched.
* **Owner read grant** on `workspace.knowledge` (USE SCHEMA, SELECT, as approved in #71) applied by hand 2026-10-15:
  `apply_grants.py` had skipped it because it treated the schema owner as covered, but owning a schema does not give
  SELECT on tables ci-dev created there. Fixed: no owner exemption on schemas.
* **Tools** (aidq_mcp, read-only): `get_impact`, `get_upstream`, `explain_history`, `search_metadata` (REST query:
  the CLI's `query-index` fails to parse the response; hits below 0.55 dropped).
* **Eval** (`evals/results/rag_eval_2026-10-15.md`): PASS. Impact / upstream 10/10 exact; history recall@5 90% (the
  miss: "loaded in full every run" vs the table doc's "FullLoad"); 5/5 off-topic questions with no hit above 0.55
  (relevant hits 0.62-0.76, best off-topic 0.45-0.50); a citation on every hit; explain_history checks pass.
* **Drift agent uses get_impact** in its evidence (removed / retyped column: the column's downstream; added column:
  the table's). Drift eval re-run (repeatable now): PASS, the removed column's proposal carries the impact. The open
  real proposals 46-48 predate this; the next real drift writes it.
* Using the tools from Claude Code: the existing `aidq_mcp` server (`python -m aidq_mcp --profile DEFAULT`) now lists
  the four tools.

## 0m. Schema drift agent built; done-when evidence ready for review (2026-10-15)

* **Grants applied by the owner** (#71, `setup_dq_roles.sh` 17:0x UTC, all checks exact): dq-agent reads the source
  (Lakebase role on netsuite-sample/production, USAGE on `netsuite`, SELECT on the 5 tables); `aidq_validator` UPDATE
  on `requires_rebuild`; `workspace.knowledge` created with ci-dev / dq-agent grants; ci-dev CAN_USE on
  `vs_endpoint_1` (no demo index touched).
* **Drift agent merged** (#72): job `schema_drift` daily 07:30 UTC as dq-agent; `dq_validator` daily 07:45 and 08:45.
  Eval on the dev branch `drift-eval` of netsuite-sample (7-day TTL; deleting it early is asked first): PASS.
* **Real run (17:24 UTC, write, traced):** proposals 46, 47 (`netsuite_customers.created_date` / `updated_date`,
  full load, `requires_rebuild` false) and 48 (`netsuite_transactions.custbody_region`, incremental,
  `requires_rebuild` true), all VALIDATED. Waiting for the owner's review; then (if 48 is approved) the guard fails
  closed on the next run and the dev rebuild runs with the owner's OK (design 3.8).
* `review.py` prints UTF-8 (a rationale held a narrow no-break space and crashed `show` on the Windows console).

## 0l. Next agents started (2026-10-15)

* **Migration 008** (shared foundation, schema only, owner-approved): merged (#68) and applied on dev by deploy-dev.
  `DEACTIVATE_COLUMN`, `ONBOARD_TABLE` (table_id NULL only for it), `requires_rebuild` (set only with validation),
  incident category `SCHEMA_TYPE_CHANGE`, the two `apply_proposal` branches, change log on `source_table_def`
  inserts, and the table_id sequence moved past the existing ids (it collided with table 1 on dev). Not applied in prod.
* **Plans open, waiting for owner decisions (nothing built):** schema drift agent #69 (dq-agent read access to the
  source schema; `aidq_validator` UPDATE on `requires_rebuild`; the customers columns; 07:30 UTC trigger) and
  RAG + lineage #70 (schema `workspace.knowledge` and its grants; `vs_endpoint_1` CAN_USE for ci-dev; embedding
  model). Two Vector Search endpoints exist, so no demo index has to be removed.
* **Drift today (read-only):** source has `netsuite_customers.created_date` / `updated_date` and
  `netsuite_transactions.custbody_region` that the metadata lacks; nothing else.
* Onboarding agent: plan after the drift agent is built (owner order).

## 0k. DQ recommender done-when items (2026-10-15)

* **Enforcement (item 3):** the scheduled 05:30 dev run on 10-15 (all tasks SUCCESS; generator 05:00 and monitor
  11:38 normal) enforced the approved rules: rule 12 rejects 80 -> 84 with the day's 4 new violating rows, none in
  silver; rule 11 rejects 25 (no new NULL keys that day).
* **Silver-only refresh (item 4), owner's OK 2026-10-15:** job `silver_refresh` (refresh_credentials, then one update:
  full refresh of the 4 silver tables, rejects and gold recomputed; bronze not in the selection), 15:13-15:17 UTC,
  nothing else running. Bronze unchanged (rows and content checksums); silver violations of rule 11: 1 -> 0, rule 12:
  17 -> 0 (silver certifications -13: 4 keys keep an earlier valid version). Report: the "Silver-only refresh" section
  of `evals/results/dq_eval_2026-10-14_v2.md`; numbers in `evals/results/silver_refresh_2026-10-15.json`.
* All five done-when items of design v2 section 12 are met (eval report committed with #56/#65).
* **Traced write run (2026-10-15 15:36 UTC):** proposal 25 (the quoted `date` rule, re-proposed after #21's
  rejection reason) links to its MLflow trace (`review.py show 25` prints the link); everything else was skipped by
  no-nagging (open or recently rejected equivalents, open incidents). Validated: VALIDATED.
* **Prod access gap (recorded, no action while dev only; owner 2026-10-15):** ci-dev and data-generator have
  project-level manage on the shared Lakebase projects, which covers prod (section 0h). dq-agent has no prod access.
  Resolve before prod goes live.

## 0j. DQ recommender: first write run, review, tracing (2026-10-14)

* **First write run as dq-agent** (job `dq_recommender`, write=true, 18:43 UTC): 11 PENDING proposals (12-22) and
  9 DQ_NOT_EXPRESSIBLE incidents (7-15). dq-agent needed no Lakebase project permission (CAN_USE not granted).
  `dq_validator` (ci-dev as aidq_validator): 10 VALIDATED, 1 VALIDATION_FAILED (#21, unquoted `date`).
* **Owner review:** applied #20 (transactions type, proposed HARD, approved SOFT: rule 10), #14 (memberships key
  not NULL, HARD: rule 11), #16 (certifications end before start, proposed SOFT, approved HARD); rejected #15
  (reason recorded as "...", to be corrected with `review.py correct-reason`) and #21. Done-when item 2 met.
  Open: the next daily run enforcing them, then the silver-only refresh demo (asks first).
* **Trace storage: Unity Catalog.** Experiment `/Shared/aidq/dq_recommender` is bound to the trace location
  `workspace.aidq_traces` (owner choice; `tools/setup_dq_traces.py`, run by the owner 2026-10-14:
  `STORAGE: unity-catalog workspace.aidq_traces`). UC trace storage works on Free Edition, so the standard-storage
  fallback (owner decision: automatic if UC fails) was not needed. dq-agent: USE SCHEMA, SELECT, MODIFY on the
  schema and CAN_EDIT on the experiment. Verified with a traced dry run (20:15 UTC): one trace, 25 spans (8 tool
  calls, recommend, 3 model calls with tokens and latency, 12 writes); the trace id is the run's agent_trace_id.

## 0i. Incident 6 and the manual-run rule (2026-10-14)

* **Rule (owner, 2026-10-14): manual pipeline runs go through the job, never directly on the pipeline.** The dev
  job's first task, `refresh_credentials`, writes a fresh Lakebase database credential (they last about 60 min) to
  the secret scope; a pipeline update started on the pipeline itself reads whatever token is stored there. To run
  part of the job, use the job with `only` (for example `["refresh_credentials"]` first), not a pipeline update.
* **Incident 6.** A manual pipeline update at 01:11 UTC (cause `API_CALL`, started directly on the pipeline during
  the #46..#51 merges) failed in bronze with `OAuth: User is not authorized`: the stored credential was 4 h 27 min
  old. The monitor (12:01 UTC run) wrote it as OTHER, "transient execution-environment error": the tool's error
  text was truncated before the PSQLException. Owner: re-categorized CREDENTIAL_EXPIRED with the real cause and
  RESOLVED (16:40 UTC); the monitor's original diagnosis is kept in `evidence.resolution`.
* **Monitor fact.** `get_pipeline_errors` now returns, for each FAILED ingestion pipeline update, who started it
  (`JOB_TASK` or not) and the stored credential's age then (the last successful `refresh_credentials` task before
  it); `triage.credential_facts` turns that into a deterministic fact for the model. Eval case
  `credential_expired_manual_run_10_14` (cassette `2026-10-14_credential_expired`): PASS, 3/3 CREDENTIAL_EXPIRED,
  never "transient error" (`evals/results/2026-10-14_credential_expired_databricks-gpt-oss-120b.md`).
* **DQ recommender, same day.** Steps 4 and 9 merged (#52, #54; roles #53). gpt-oss-20b does not qualify for
  `root_cause` (#55; routing unchanged). Prompt v2 + candidate checklist + profiler signals + eval scoring on known
  ground truth: #56. Step 10, `dq_recommender` (run_as dq-agent) and `dq_validator` jobs: #57. ci-dev got USE SCHEMA
  + SELECT on `workspace.generator` (owner OK, 2026-10-14) for the DQ eval on GitHub (`dq-eval.yml`).

## 0h. Outage 10-11..10-13, clean cycle, follow-ups (2026-10-13)

**Dev only** (owner, 2026-10-08) still applies.

* **What happened.**
  * 10-09 and 10-10: generator and dev ingestion ran on schedule; the monitor found no gaps.
  * 10-11, 10-12, 10-13: the scheduler fired neither dev job. There was no `runTriggered` event, the schedules were UNPAUSED and unchanged since the 10-08 deploy, and nothing was deployed.
  * The source and dev metadata endpoints were disabled on 10-11 (about 15:00-16:00 UTC, no audit event), and workspace audit activity fell from about 60-90k to about 3.7k events a day.
  * The GitHub monitor still ran every day, but crashed in `dedupe` on the disabled metadata endpoint, so the missed runs were never reported (failure emails only).
  * `daily_check` could not run either (it refused to enable endpoints).
* **Recovery (owner decisions 2026-10-13).**
  * Endpoints re-enabled at 14:3x UTC.
  * No backfill of 10-11/10-12: the generator ran once for 10-13 (run 518214397439106), then the dev job (run 602469849426968).
  * `daily_check --date 2026-10-13` PASSED: bronze = source per table and date, ledger OK; WARN only for the guard (2 reads) and no prod run. **This is the clean cycle after #35.**
* **Why the schedules stop (investigation 2026-10-13).**
  * The scheduler skipped all runs on 10-06 and 10-11..10-13 and fired normally on every other day since 10-02.
  * Only the two dev jobs have schedules in this workspace, so there is no other job to compare against.
  * On 10-06 runs resumed after an interactive login; on 10-13 only manual runs worked.
  * This matches community reports that an idle Free Edition workspace is deactivated (`denyReason: INACTIVE`, "only a manual Start in the console reactivates it") and that Lakebase endpoints are disabled after idle days.
  * The official limitations page documents only fair-usage quotas ("compute shut off for the rest of the day") and that inactive accounts may be deleted.
  * Not confirmed. **Schedules do not reliably come back on their own**: recorded in `evals/incidents/2026-10-06_scheduler_did_not_fire.yaml` (`recurrence`).
* **Follow-ups (PRs, owner decisions 2026-10-13).**
  * The monitor stays on GitHub as the external watchdog. #38 now only moves its cron to 06:17 UTC.
  * #41: the monitor never loses its report. A store failure keeps the full diagnosis in the run summary, exit 2, annotation `metadata unavailable`; new eval case.
  * #42: the generator, the dev job's first task (dev only, bundle var `wake_lakebase_endpoints`), the monitor and `daily_check` re-enable a disabled endpoint before use and log `ENABLED Lakebase endpoint ...`.
* **Eval** (2026-10-13, cassettes re-recorded: `2026-10-06_afternoon`, new healthy `2026-10-09_after_daily_run`). **FAIL in both runs, on model-judged items only.** Owner decision: it does not gate merges, because the clean cycle passed.
  * Run 1: `missed_runs` "UNPAUSED and unchanged" 1/3.
  * #41 then added deterministic schedule facts (pause state, cron, last config change) and the must-not-claim "the schedules were or may have been paused".
  * Run 2: that item reached 2/3, but "outside the pipeline" was 1/3, "paused" was still claimed once (must-not-claim 2/3), and `duplicate_versions` "same key, same date twice" was 1/3 although that case did not change. Model variance at 3 runs.
  * The new `missed_runs_metadata_unavailable` case passed every check in both runs.
  * Results: `evals/results/2026-10-13_*` (run 1) and `2026-10-13b_*` (run 2).
* **Incident 1 resolved** (2026-10-13 15:28 UTC). The real finding (scheduler skipped; likely Free Edition workspace inactivity, unconfirmed) and the agent's two errors are in `evidence.resolution`. No incident is open.
* **Lakebase permission applied: CAN_MANAGE.**
  * Test first: CAN_USE (granted to ci-dev and data-generator on both projects) is not enough. ci-dev's PATCH was refused: "assign the user 'Can Manage' for Database project".
  * Then CAN_MANAGE as in #42: ci-dev on `netsuite-sample` and `aidq-metadata`, data-generator on `netsuite-sample`.
  * Verified: the monitor (dry run from the #42 branch, as ci-dev) re-enabled the disabled dev metadata endpoint and logged it.
  * The leftover CAN_USE for data-generator on `aidq-metadata` was removed 2026-10-13 (owner OK).
  * Final, direct: `netsuite-sample`: ci-dev CAN_MANAGE, data-generator CAN_MANAGE. `aidq-metadata`: ci-dev CAN_MANAGE, data-generator none (ci-prod CAN_MANAGE and the owner CAN_MANAGE, unchanged).
  * Known gaps: project-level manage covers prod (below). Every workspace user, both SPs included, inherits CAN_CREATE on both projects through the `users` group (workspace default, not ours).
  * **ci-dev and data-generator have project-level manage on shared projects, which covers prod; resolve before prod goes live.**
* **Merged in the owner's order:** #36, #37, then the owner ran `tools/setup_dq_roles.sh DEFAULT` (dq-agent `47b5b3c3-62ad-425d-9322-1d7907438113`, its Lakebase role, the three group roles checked exact, `workspace.generator`, the UC and warehouse grants), then #38, #39, #40, #41, #42.
  * The deploy-dev runs for #38-#41 were **cancelled**: only one job can wait in the `databricks-workspace` concurrency group, so each newer queued job (finally #42's PR checks) cancelled the waiting deploy.
  * The deploy for #42's merge (run 38425185821, all of #36-#42) was green: migrations through 006 applied, smoke run SUCCESS, ledger_check OK (transaction_lines 93,807 entries, 0 mismatches).
  * Merge one PR at a time and let its deploy-dev finish before the next push.
* **Found:** a multi-day outage is never reported. `missed_schedules` treats misses before the first scheduled run in the 3-day window as `unconfirmed`. After 10-11..10-13 (manual runs only) the monitor said "Healthy". Fix planned in the watchdog PR.
* **Watchdog PR #44**: plan approved with the owner's checklist and **built** (not merged). It also fixes the outage bug above. Eval case `watchdog_self_healed_10_11` (new fixture 2026-10-11): see `evals/results/2026-10-13c_watchdog_*`.
* **#44 merged** 2026-10-13 19:53 UTC (`663e09b`), alone. Its deploy-dev was green (smoke run SUCCESS, ledger OK).
* **Model routing, PR #45**: plan approved with the review corrections, **built**, waiting for the owner's merge OK.
  * `src/aidq_llm`: the router (task -> primary/fallback, one escalation, no loops), `routing.yml` (the only place that names models), and the call log in Postgres `aidq_metadata.llm_calls` (migration 007, 90-day retention via `prune_llm_calls`). A logging failure never crashes an agent: the calls go to the run summary.
  * The monitor routes `root_cause`. The eval judge model comes from `routing.yml`.
  * Paired comparison `evals/routing_compare.py` (rules in `src/aidq_llm/compare.py`). No task has moved to a smaller model.
  * **After merge (owner):** re-run `! bash tools/setup_dq_roles.sh DEFAULT`. It creates `aidq_llm_logger` and extends the SELECT grants to the new table, after which `dq_roles.py --check` is exact again.
* `docs/agents_next_design.md` (owner): no work until everything above is merged (owner, 2026-10-13).

## 0. First prod release (2026-10-01, steps 0-7 done; step 8 open)

| Step | Result |
|---|---|
| 0 bind | existing prod job `netsuite_ingestion_daily` and pipeline `netsuite_ingestion_poc` bound into ci-prod's bundle deployment (`bundle deployment bind`); plan showed in-place updates, 0 deletes |
| 1 backups | Lakebase branch `aidq-metadata/pre-release-202610010053`; schema copy `aidq_metadata_backup_202610010053` (5 tables, counts verified); JSON snapshot in `baseline/raw/` (local) |
| 2 migrations | 001, 002, 003 applied on prod metadata as `aidq_owner` (`--plan` first; PR #14 fixed the plan to dry-run files cumulatively) |
| 3 deploy | promote-prod (bb48fef, `bronze_rebuild=true`): job and pipeline run as ci-prod, scope `netsuite_ingestion_prod`, schedule PAUSED; guard canary created. First attempt failed (403 on bundle ACLs) -> PR #15 dropped bundle `permissions`; job owner moved to ci-prod; the pipeline stays owned by the owner (only a metastore admin, `System user`, can change a pipeline owner) |
| backup of prod tables | `poc_netsuite.backup_pre_release_202610010127`: 22 tables (17 per-date bronze views + customers, 4 silver, rejects) via CTAS (deep clone is refused for MVs/STs), counts verified. **Keep until step 8 plus a few good scheduled runs; ask before dropping.** |
| 4-5 rebuild and compare | dev run 648860584880106, then prod `full_refresh` run 210819933825138 (first attempt failed in setup: ci-prod lacked CREATE on `poc_netsuite.default`; granted). Source exact counts equal before and after. **Dev = prod exactly** for bronze (5), silver (4), rejects as (table, rule, key) sets (54), ledger keys (57,552) and fingerprints (53), gold (5,280 / 1,861); no extra dev `_snapshot_date`. Prod `ledger_check` OK |
| 6 normal mode | promote-prod (`bronze_rebuild=false`, one run 613901313133793): 0 new bronze rows, `ledger_check` OK, guard WARN (2 reads) |
| 7 old views | the 16 per-date materialized views (`<table>__2026_06_20/07_11/08_01/08_22`) dropped as ci-prod after confirming no current dataset defines them; silver, rejects, gold counts unchanged; copies remain in the backup schema |
| 8 schedule | **on hold (owner, 2026-10-01)**: prod schedule stays PAUSED |

Access changes made during the release: ci-prod CAN_MANAGE on project `aidq-metadata` (backup branches), ci-prod
CREATE TABLE/MV on `poc_netsuite.default`, owner USE SCHEMA + SELECT on the prod and dev pipeline schemas (tables are
owned by the run identities). PR #16 codifies the UC grants (additive step in CI; needs the `GRANT_OWNER_PRINCIPAL`
repository variable). `aidq-metadata` has 6 of 10 branches (4 `pre-release-*`, one per promote-prod run plus the
step-1 backup); promote-prod now prunes them (below).

## 0b. After the release (2026-10-01)

* UC grants are codified (PR #16): `grants/<target>.yml`, applied additively by `tools/apply_grants.py` in deploy-dev
  and promote-prod (repository variable `GRANT_OWNER_PRINCIPAL`). Today a no-op: every grant is present.
* Migration 004 (PR #18): no PUBLIC EXECUTE on `enforce_proposal_lifecycle()` / `log_config_change()`. Applied on
  dev by deploy-dev (audit trigger verified to still fire); prod gets it with the next approved promote-prod run.
* Backup-branch retention: promote-prod runs `tools/prune_backup_branches.py` after its backup: keeps the newest 3
  `pre-release-*` branches and every one younger than 14 days; production, dev and other names are never touched.
  Today it deletes nothing (all four are from 2026-10-01).
* Not done, on purpose: MANAGE for the CI identities on their schemas (the grant step can then restore a missing
  grant instead of failing; for prod that is a prod grant change: ask first); dropping
  `poc_netsuite.backup_pre_release_202610010127` (condition: step 8 plus a few good scheduled runs).

## 0c. Daily operation (owner decisions 2026-10-01)

* **The generator is the source system.** `netsuite_daily_generator` (dev-only job) appends one day of synthetic
  data to `netsuite-sample/production`, which dev and prod both read; prod accepts it. Defect rate stays low
  (`tools/increment_daily.yaml`). Heavy defect / schema-drift experiments go later to an on-demand dev branch of
  netsuite-sample, not to this source.
* **Schedules (UTC, staggered):** generator 05:00, dev `netsuite_ingestion_daily` 05:30 (both unpaused), prod
  `netsuite_ingestion_daily` 06:30 (**PAUSED until step 8**: after one good day of generator + dev, the owner approves
  a promote-prod run with `schedule_pause_status=UNPAUSED`).
* **data-generator** service principal: the generator job's run identity (job-level `run_as`), no OAuth secret.
  Source grants (`grants/source.yml`, applied 2026-10-01, exact match verified): USAGE on `netsuite`; SELECT +
  INSERT on the four incremental tables; SELECT + INSERT + UPDATE on `netsuite_customers`; CONNECT + CREATE on the
  database (its `netsuite_backup_daily_*` schemas; Postgres cannot restrict CREATE to a name prefix). No DELETE,
  TRUNCATE, REFERENCES, TRIGGER, ALTER, role memberships, or access to metadata, UC or secrets. deploy-dev runs
  `tools/apply_pg_grants.py --check` and fails on anything missing or extra. ci-dev has the "Service Principal User"
  role on data-generator (to deploy a job that runs as it); data-generator has CAN_VIEW on the dev bundle.
  Repository variable `DATA_GENERATOR_SP`.
* **First generator runs (2026-10-01):** run 1 aborted on import (`psycopg-binary` 3.3 wheel; PR #22 switched to plain
  `psycopg`), nothing written. Run 2 wrote one increment (+100 customers, +397 memberships, +318 certifications,
  +1,590 transactions, +4,770 lines; backup schema `netsuite_backup_daily_202610011947`) but (a) was reported FAILED
  only because of `sys.exit(0)` and (b) dated updated rows up to +2 days (source watermark now **2026-10-03**). Fixed:
  exit code, `--update-spread-days 0` and `--skip-if-not-after-watermark` for the daily job, so the 2026-10-02 and
  10-03 runs skip cleanly and real daily increments resume on **2026-10-04**. Dev and prod ingest the rows dated
  10-02/10-03 like any other dates.
* **Rows dated in the future, kept on purpose (owner decision 2026-10-01):** generator run 2 (2026-10-01) wrote
  updated versions with `updated_date` 2026-10-02 and 2026-10-03 (memberships 93 + 90, transactions 373 + 359, and
  the matching certifications and lines). They entered the source on 10-01. Keep them as a natural "future
  updated_date" defect for the Phase 4 rule recommender. They are not a pipeline bug: bronze loads them as their
  own snapshot dates like any other.
* **Owner offline until 2026-10-05.** Failure emails for the two daily dev jobs (generator, dev
  `netsuite_ingestion_daily`) go to the `NOTIFICATION_EMAIL` repository variable's address (bundle, `on_failure`).
  **On 2026-10-05: run `python tools/daily_check.py --date <d> --profile DEFAULT` for 2026-10-02, 10-03, 10-04 and
  10-05** (read-only; exit 1 on any FAIL). Expected (owner decision 2026-10-01: generate every day): the generator
  writes an increment **every day, 10-02 to 10-05** (`--allow-before-watermark`: the 10-02 and 10-03 batches land on
  dates the source already holds since run 2, so dev picks them up through top-up flows as late rows on loaded dates;
  `--skip-if-batch-exists` keeps a same-day re-run from writing twice); dev job SUCCESS each day with ledger OK; dev
  bronze equal to the source; no prod runs (paused). Note: on 10-02 and 10-03 some new versions of existing rows are
  dated before those keys' 10-03 versions from run 2; silver (AUTO CDC by updated_date) correctly keeps the later one,
  so silver grows less than bronze on those days.
  **Then decide on step 8** (promote-prod, `schedule_pause_status=UNPAUSED`, approved in the `prod` environment;
  prod cron 06:30 UTC; the first scheduled prod run catches up on run 2 and every increment since; also brings
  migration 004 to prod).
* Backups of the source by the daily job: schema copies `netsuite_backup_daily_<stamp>` only (no Lakebase branch),
  newest 7 kept; historical `netsuite_backup_<stamp>` copies and `pre-synthetic-backup-*` branches are never touched.
* **ci-dev MANAGE** on the six dev pipeline schemas (granted 2026-10-01), so the dev grant step can restore a missing
  grant. **Prod: no MANAGE for ci-prod by decision**; a missing prod grant fails the prod grant step.
* **Prod table backup** `poc_netsuite.backup_pre_release_202610010127`: keep until step 8 plus three good scheduled
  prod runs, then ask the owner before dropping.

## 0d. Daily checks 10-02 to 10-05 (run 2026-10-06)

* Generator and dev `netsuite_ingestion_daily` SUCCESS every day (scheduled), ledger OK, guard WARN (2 reads), no
  prod runs. The source and dev metadata endpoints were found disabled again (last active 10-05 about 05:35 UTC)
  and re-enabled with the owner's OK; prod metadata is still disabled.
* **Incident 1: same-day duplicate versions not loaded.** Dev bronze was short of the source by 287 rows
  (memberships 24, certifications 6, transactions 63, lines 194), all on snapshot dates 10-02 and 10-03. Cause:
  generator run 2 (10-01) wrote versions dated 10-02/10-03; the 10-02/10-03 batches (`--allow-before-watermark`)
  then wrote a second version of some of the same keys with the same `updated_date` (a plain date). The key ledger
  and the per-date fingerprints count distinct (business_key, date) pairs, so such a version changes no
  fingerprint and no top-up loads it; bronze and silver keep the 10-01 version. **Known pipeline limitation**: a
  source that updates a key twice in one day loses the second version. Owner decision (10-06): keep the 287 rows
  as a defect; the generator no longer writes a version on a (key, date) the source already holds;
  `daily_check.py` compares per table and snapshot date and reports a date short by exactly its same-day
  duplicates as a known-defect WARN (duplicates that arrive in one batch, e.g. 07-11, are loaded and stay OK).
* **Incident 2: no scheduled runs on 10-06.** Both dev schedules UNPAUSED, no job change in the audit log since
  10-04, no deploy-dev since 10-01, and no `runTriggered` on 10-06: the scheduler never fired. Recorded as a
  platform cause (Free Edition, idle workspace suspected, not confirmed).
* Prod job cron is `0 0 6 * * ?` (06:00 UTC), not 06:30 as planned: settle before step 8.

## 0e. Phase 3: read-only MCP server (2026-10-06)

* `src/aidq_mcp` (plan: `docs/phase3_mcp_plan.md`, PRs #29, #30): five read-only tools, dev only:
  `get_table_health`, `get_recent_job_runs` (with `missed_schedules` / `missed_schedules_unconfirmed`),
  `get_pipeline_errors`, `compare_bronze_to_source`, `get_recent_deploys`. Runs as the owner's CLI profile;
  read-only enforced in code (read-only Postgres sessions, fixed SELECT templates, get/list CLI calls only,
  tested). A disabled endpoint is reported `unavailable`, never enabled. `env="prod"` is refused.
* Run: `PYTHONPATH=src .venv/Scripts/python -m aidq_mcp --profile DEFAULT [--smoke]`. Register for Claude Code
  (local scope, not committed): `claude mcp add aidq-netsuite --scope local -e PYTHONPATH=<repo>/src --
  <repo>/.venv/Scripts/python.exe -m aidq_mcp --profile DEFAULT`.
* **Incident replay (step C):** fresh headless Claude Code sessions limited to these tools (`--strict-mcp-config`,
  all built-in tools disallowed), started outside the repo so no STATUS/memory context. Both answers met every
  `must_identify` and no `must_not_claim` of the fixtures. Incident 1: gap on 10-02/10-03 only, equal to the
  same-day duplicates, ledger never loads them; it guessed (and flagged as unverified) that PR #27 should have
  backfilled. Incident 2: no trigger on 10-06, schedules UNPAUSED and unchanged since 10-01, no deploy before
  15:28; cause not found, platform listed as a possibility only.
* Found by the replay and fixed: with a 7-day window `missed_schedules` flagged 09-30 and 10-01, before the
  schedules existed. Misses before the first scheduled run in the window are now `missed_schedules_unconfirmed`.
* Next: Phase 4 (LangGraph monitor + RCA after the dev daily job, these tools only, writes incidents). Needs:
  migration 005 for incident categories `ORCHESTRATION` / `DATA_COMPLETENESS`, and (ask first) a read-only SP
  `aidq-reader` with INSERT on `incidents`.

## 0f. Phase 4: monitor + RCA agent (2026-10-06)

* Plan `docs/phase4_agent_plan.md` (approved). Code `src/aidq_agent` (PRs #31-#34): LangGraph `collect -> triage ->
  dedupe -> investigate -> write -> report`; only `investigate` uses a model, only through the five aidq_mcp tools;
  writing is a fixed node (INSERT into dev `aidq_metadata.incidents`, one OPEN incident per fingerprint,
  migration 005 applied on dev).
* Model (owner decision): free `databricks-gpt-oss-120b` served on the workspace, for evals and production (no
  API key). `--model claude-opus-5-5` selects the Anthropic API. The agent's prompt includes
  `docs/platform_design_notes.md` (mechanisms only, never an incident's diagnosis).
* Evals (`evals/run_agent_eval.py`, recorded tool outputs, judge `databricks-qwen35-122b-a10b`): all targets met
  (`evals/results/2026-10-06_databricks-gpt-oss-120b.md`).
* Runs as ci-dev. ci-dev cannot read `system.access.audit` (not grantable on Free Edition): `config_changes` is
  `unavailable` there; GitHub deploys are still reported.
* **Scheduled:** `.github/workflows/monitor-dev.yml`, 06:15 UTC, writes incidents; fails (GitHub email) only when a
  new incident was written. The first run is expected to write the 10-06 ORCHESTRATION incident (missed runs still
  in its 3-day window). Manual run: Actions -> Monitor dev, `write` off for a dry run.
* Local: `PYTHONPATH=src .venv/Scripts/python -m aidq_agent --profile DEFAULT [--write]`.

## 0g. Same-day versions fix (2026-10-08, branch `fix/same-day-versions`)

**Dev only until the owner says otherwise (owner, 2026-10-08):** no promote-prod, no prod grants, no prod endpoint
changes; the prod schedule stays PAUSED.

* **What changed.** Every incremental bronze row has `_row_hash` = md5 of the table's **active** `source_columns`
  (in `source_columns` order), computed by Postgres only (`metadata.row_hash_sql`, `json_build_array(...)::text`:
  ISO dates whatever the session DateStyle/TimeZone, verified by `tests/test_fingerprint_integration.py`). Raw
  source columns outside `source_columns` (e.g. `netsuite_transactions.custbody_region`) are neither read nor
  hashed. The ledger (`ledger.bronze_keys`) and the per-date fingerprints are keyed on (key, date, row_hash), so a
  second version of a key on a loaded date changes the fingerprint and a top-up loads exactly that row
  (`row_hash_filter`). An exact copy of a loaded row adds no entry and is not loaded again.
* **Silver tiebreaker (owner decision):** AUTO CDC `sequence_by = struct(updated_date, _row_hash)`: content only,
  so every environment keeps the same version of a key whatever the load order; the kept version is deterministic,
  not necessarily the newer one (the source has no finer timestamp).
* **Fail closed on a column-set change.** `sync_ledger --bronze-rebuild true` records the hashed columns per table
  in `ledger.row_hash_columns` (only when bronze's columns equal the active `source_columns`; a normal run never
  writes it). At graph build, `bronze.py` refuses a normal run when the record is missing or differs, with an error
  that names the table and the change (added / removed / reordered) and the rebuild commands
  (`metadata.row_hash_guard_error`). A `bronze_rebuild` run is always allowed.
* **The 287 rows** (incident 1, 10-02/10-03) are no longer a known defect: a rebuild loads them. `daily_check.py`
  FAILs on any per-date count difference again and prints an INFO line with the business keys that have several
  same-day versions; `compare_bronze_to_source` has verdict `identical` / `gaps` plus `same_day_versions` (info);
  triage turns every gap into a signal and passes the same-day versions as a context note. The incident fixture
  `evals/incidents/2026-10-02_same_day_duplicate_versions.yaml` is kept; its eval case replays with the design
  notes of the time (`evals/design_notes/2026-10-06.md`).
* **Prod cron** pinned to 06:30 UTC in the prod target (`daily_cron`, test in `test_bundle_targets.py`). Config
  only: the deployed prod job keeps 06:00 and stays PAUSED until an approved promote-prod.
* **Rollout on dev (owner: merge when green and the next dev daily check is clean):**
  1. PR checks green.
  2. deploy-dev from the branch with `bronze_rebuild=true` (full refresh of the whole pipeline: bronze re-extracted
     with `_row_hash`, silver rebuilt with the new `sequence_by`, ledger and `row_hash_columns` rebuilt).
  3. Check: bronze = source per table and date (the 287 loaded), `ledger_check` OK, hashed columns recorded.
  4. Next scheduled cycle (generator 05:00, dev 05:30) on the branch deployment, then
     `python tools/daily_check.py --date <that day> --profile DEFAULT` with no FAIL.
  5. Re-record the eval cassettes `2026-10-06_afternoon` and `2026-10-09_after_daily_run` (post-fix state), point
     the healthy eval case at the latter, re-run the eval; then merge (deploy-dev on main runs a normal smoke run).
* **Note for Phase 4/5 (rule recommender):** an `ADD_COLUMN` proposal on an incremental table must say that
  approving it requires a bronze rebuild (the row hash covers the active columns; the guard blocks normal runs).

### Prod rebuild: deferred (preconditions for the future prod release)

Prod bronze has no `_row_hash`, so the first prod run with this code is blocked by the guard until prod bronze is
rebuilt. That rebuild is part of a future, owner-approved prod release, not of this change. Preconditions:

1. **A clean dev cycle** with this code: rebuild run, then at least one scheduled generator + dev run with a clean
   `daily_check` (no FAIL).
2. **A fresh prod backup** before the rebuild: a Lakebase backup branch of prod metadata (promote-prod does this)
   and a copy of the prod tables like `poc_netsuite.backup_pre_release_202610010127` (CTAS, counts verified).
3. **Dev-vs-prod comparison on the same source state:** after the prod rebuild (promote-prod with
   `bronze_rebuild=true`, then the full refresh by hand, schedule PAUSED), compare dev and prod with no generator
   run in between: bronze per table and date, ledger entries and fingerprints, silver (same kept version per key),
   rejects, gold.
4. **Then a normal prod run that loads 0 rows** (promote-prod with `bronze_rebuild=false`, one run): no new bronze
   rows, `ledger_check` OK, hashed columns recorded for all four tables.

Only after that: step 8 (unpause, cron 06:30) as its own decision.

## 1a. First CI dev run (2026-09-30, deploy-dev run on the #11 merge: green)

* Rebuild mode (poc_bronze was empty after `[dev suhasv]` was removed): migrations (003 applied by the previous,
  failed run, as `aidq_owner`), deploy with `bronze_rebuild=true`, one run, redeploy with `bronze_rebuild=false`.
* Job run 449191401642789 (`[dev ci_dev]`, as ci-dev): bronze = source in all five tables, silver 2,152 / 2,704 /
  32,710 / 10,902, rejects 32 + 25, ledger rebuilt and `ledger_check` OK for all four, guard OK in 1 read. Same
  numbers as the hand-deployed run earlier that day.
* Dev metadata: `schema_migrations` 001, 002 (backfill), 003 (`applied_by` aidq_owner); `netsuite_customers` has
  `watermark_col` NULL. Deployed dev: `bronze_rebuild=false`, scope `netsuite_ingestion_dev`, schedules PAUSED.
* The dev tables are owned by ci-dev (the pipeline's identity); the owner reads them through USE SCHEMA + SELECT
  on `workspace.poc_bronze`, `poc_silver`, `poc_reject`, `poc_gold` and `canary` (granted 2026-09-30).

## 1b. Phase 1 close-out (Part 1, 2026-09-30)

* Gold: `poc_gold.gold_customer_revenue` and `poc_gold.gold_customer_status` (materialized views over silver;
  SQL in `gold_sql.py`, tested on DuckDB). Schema `workspace.poc_gold` created for dev; **prod needs
  `poc_netsuite.poc_gold` before the first prod deploy with gold** (ask).
* Dev run (job run 888453864069675, `[dev suhasv]`, source unchanged since 2026-09-26): bronze = source
  (certifications 2,625, customers 2,200, memberships 3,281, transaction_lines 39,378, transactions 13,128);
  silver 2,152 / 2,704 / 32,710 / 10,902 (same as v2 step D); rejects 32 certifications + 25 memberships;
  ledger OK for all four; gold_customer_revenue 5,280 rows (2,039 customers, months 2026-05 to 2026-09, revenue
  68,527,268); gold_customer_status 1,861 customers, 408 with an active membership.
* Guard read count: first real run needed **2** event-log reads (`guard` row WARN in `run_audit`).
* SOFT rules: two added to **dev** metadata (rules 4 and 5: `Amount Equals Qty x Rate` on transaction_lines,
  `End Not Before Start` on memberships). A normal run with no new source data emits no expectation metrics
  (nothing flows). After a selective refresh of the two silver tables (update 9ab9415e), the transaction_lines
  rule is in the event log (38,781 passed, 597 failed = the 297 amount mismatches + 300 negative amounts of the
  baseline). The memberships rule was missing from that update (its flow wrote no
  metrics event), but a memberships-only refresh (update acc79388) shows it: 3,227 passed, 29 failed (= the 3,256
  valid rows). **Both SOFT rules confirmed.** A single update can lack a flow's metrics event, so one MISSING
  from `tools/check_soft_expectations.py` is a reason to re-check another update, not proof the rule is unwired.
* Proposed daily dev schedule: `docs/dev_daily_schedule.md` (not deployed; 4 blockers listed).

**Compute outage 2026-09-30, about 20:13 to 20:55 UTC:** Lakebase endpoints disabled again and the SQL warehouse
refused to start (`Cannot create the resource`). Both worked again after the endpoints were re-enabled.

## 2. ci-dev (Phase 2, step A: done 2026-09-30)

* Service principal `ci-dev`, OAuth M2M. One OAuth secret (90 days, expires 2026-12-29), the one in GitHub; the
  first secret was deleted on 2026-09-30 with the owner's OK.
* GitHub secrets `DATABRICKS_HOST_DEV`, `DATABRICKS_CLIENT_ID_DEV`, `DATABRICKS_CLIENT_SECRET_DEV`.
* Lakebase roles `ci-dev` (no admin membership) on `aidq-metadata/dev` and `netsuite-sample/production`.
  Grants: dev metadata USAGE on `aidq_metadata`, SELECT on `source_table_def`, `source_columns`,
  `data_quality_rules`, SELECT + INSERT on `run_audit`; source USAGE + SELECT on schema `netsuite` (default
  privileges for new tables), nothing on the backup schemas. Migration (DDL) rights: step C/D.
* UC: USE CATALOG `workspace`; USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY on
  `workspace.poc_bronze`, `poc_silver`, `poc_reject`, `poc_gold` (new, created 2026-09-30), `ledger`, `canary`,
  `default` (USE SCHEMA only since 2026-09-30: it holds other projects' tables). SQL warehouse: CAN_MANAGE (canary).
* Grant inventory 2026-09-30: nothing beyond the plan. BROWSE on other catalogs comes from `account users` (all
  users), not from a ci-dev grant. Another user (`sid.v@...`) held ALL_PRIVILEGES on `poc_netsuite`; reduced to
  USE CATALOG + USE SCHEMA + SELECT (read-only) on 2026-09-30 with the owner's OK.
* Scope `netsuite_ingestion_dev`, ci-dev WRITE (owner MANAGE). ci-dev has no access to `netsuite_ingestion_poc`.
* Verified as ci-dev: Lakebase login and the grants (allowed and denied cases), scope WRITE, `bundle validate`
  dev + prod, and `run_as` (accepted with `mode: development`; job and both pipelines run as ci-dev).
* A test deploy as ci-dev created the `[dev ci_dev]` set (2 jobs, 2 pipelines, schedules PAUSED, never run).
  It becomes the canonical dev in step D.

## 2b. bundle validate in PR checks (Phase 2, step B: PR #4)

* New job `bundle validate` in `.github/workflows/pr-checks.yml`: validates dev and prod (schedule PAUSED) as
  ci-dev, OAuth M2M; CLI 1.16.0 installed from the release with its checksum verified; job-level concurrency group
  `databricks-workspace`, `cancel-in-progress: false`; fork PRs skip with a notice, a same-repo PR without the
  secret fails. Public logs mask the host and client id (checked: no occurrence in the job log).
* `tests/test_bundle_targets.py`: dev and prod never share catalog, metadata key, endpoint or host; the ledger
  follows `${var.catalog}`.
* Next (owner's OK): add `bundle validate` to the required status checks of `main`.

## 2c. Migration runner (Phase 2, step C)

* `tools/migrate.py --env dev|prod --plan | --apply | --backfill UPTO`. `--apply`: each file plus its
  `schema_migrations` row in one transaction; stops at the first failure; refuses if an applied file's checksum
  changed. `--plan`: read-only, dry-runs pending files in a rolled-back transaction. Files contain no
  `BEGIN`/`COMMIT` (removed from 001/002 in this step, the one permitted edit; the runner rejects such files).
* Dev backfill done 2026-09-30: replaying 001+002 changed nothing in a catalog snapshot (columns, constraints,
  indexes, views, functions, triggers), so both were recorded with the checksums of the edited files.
* `003_blank_watermark_to_null`: blank `watermark_col` -> NULL plus a CHECK against blanks. Dry run OK on dev;
  applied by deploy-dev (step D). The code already treats blank and NULL the same.
* Every migration transaction starts with `SET LOCAL ROLE aidq_owner` (dry runs and backfill too), so what a
  migration creates is owned by that no-login role; `migrate.py` refuses to run if the role is missing or the
  caller is not a member.
* `migrate plan (dev)` job in pr-checks (as ci-dev, concurrency `databricks-workspace`). The CLI install is now
  the local composite action `.github/actions/databricks-cli`.
* **Blocked: ci-dev has no migration rights on dev metadata.** Creating a no-login owner role and moving
  ownership of `aidq_metadata` objects to it (so ci-dev can run DDL) was refused by the tool's permission
  classifier; the owner runs it (SQL in the Phase 2 report). Until then `migrate plan (dev)` fails in CI with a
  permission error, and deploy-dev cannot apply migrations.

## 2d. deploy-dev (Phase 2, step D)

* `.github/workflows/deploy-dev.yml`: on push to `main` (and on demand) as ci-dev: `migrate --plan` + `--apply`
  on dev metadata, `bundle deploy -t dev`, one normal smoke run of `netsuite_ingestion_daily`. Job-level
  concurrency `databricks-workspace`; secrets only on the steps that call Databricks; run URLs stripped from the
  public log.
* Secret scope per target (plan 3a, code part): bundle variable `secret_scope` (dev `netsuite_ingestion_dev`,
  prod still `netsuite_ingestion_poc` until the prod scope exists), passed to every job task and to the pipeline
  configuration (`metadata.pg_conn_from_conf`). Tests: the scope differs per target and no resource file names
  a scope literally.
* Bronze rebuild mode: when `workspace.poc_bronze` holds no streaming table (fresh dev, e.g. after the
  `[dev suhasv]` set is removed) or on `workflow_dispatch` with `bronze_rebuild`, deploy-dev deploys with
  `bronze_rebuild=true`, runs (full refresh only if bronze exists), `sync_ledger` rebuilds `workspace.ledger.*`,
  and it always redeploys with `bronze_rebuild=false`. So the first CI dev run is a rebuild (owner decision).
* No `run_as` in the dev target: CI deploys dev as ci-dev, which is then also the run identity; `run_as` (proven
  in step A) is for prod, where a human deploy must still run as ci-prod (step F).
* **First deploy-dev run will fail until:** (1) ci-dev has migration rights on dev metadata (owner SQL);
  (2) the hand-deployed `[dev suhasv]` pipelines are removed, because they own the dev tables the CI pipeline
  writes; (3) compute is available again.

**Hand-deployed `[dev suhasv]` set (removal needs the owner's OK; nothing removed):**

| Kind | Name | Goes with it |
|---|---|---|
| job | `[dev suhasv] netsuite_ingestion_daily` | run history |
| job | `[dev suhasv] guard_canary_check` | run history |
| pipeline | `[dev suhasv] netsuite_ingestion_poc` | the tables it owns: `workspace.poc_bronze` (5 source tables + `guard_reads`), `workspace.poc_silver` (4), `workspace.poc_reject.rejected_rows`, `workspace.poc_gold` (2) |
| pipeline | `[dev suhasv] guard_canary` | `workspace.canary.canary_bronze` |
| folder | the owner's `.bundle/netsuite_ingestion/dev` | deployed files and bundle state |

Not affected: the owner's `.bundle/netsuite_ingestion/prod` folder and every prod resource; `workspace.ledger.*`
(not pipeline-owned; the first CI run with `bronze_rebuild=true` rebuilds it); the dev metadata branch; other
projects' tables in `workspace.default`. Data lost: only dev copies that the next CI run rebuilds from the source
(the v2 baseline numbers are in `baseline/v2/`). Method: `bundle destroy -t dev` as the owner from the working
copy that holds the owner's bundle state.

## 2e. Guard canary workflow (Phase 2, step E)

* `.github/workflows/canary.yml`: `workflow_dispatch` only (no GitHub schedule until the owner decides), as
  ci-dev, job-level concurrency `databricks-workspace`. Stops every running SQL warehouse first and waits until
  all are stopped, then `bundle run guard_canary_check -t dev`; URLs stripped from the public log.
* **Blocked: ci-dev cannot stop the warehouse.** Granting it CAN_MANAGE on the SQL warehouse was refused by the
  tool's permission classifier (owner action). Until then the workflow fails at the stop step on purpose instead
  of running the canary with a warehouse up. The `[dev ci_dev] guard_canary` pipeline also needs
  `workspace.canary.canary_bronze`, which the `[dev suhasv] guard_canary` pipeline owns (section 2d).

## 2f. promote-prod (Phase 2, step F)

* `.github/workflows/promote-prod.yml`, `workflow_dispatch` with `release_sha` (full SHA), `run_after_deploy`,
  `bronze_rebuild` (both default false).
  * Job `release gate` (no secrets) fails unless `release_sha` is on main (`git merge-base --is-ancestor` against
    `origin/main`), has a successful deploy-dev run (`gh run list --status success`), and the `prod` environment
    exists **with required reviewers** (GitHub would otherwise create it on first use without protection).
    Dry-run locally: a malformed SHA, a SHA not on main, and a missing deploy-dev workflow all fail closed.
  * Job `promote to prod` (environment `prod`, concurrency `databricks-workspace`): backup branch of prod
    metadata, `migrate --plan`/`--apply` (prod), `bundle deploy -t prod` with the schedule PAUSED, optionally one
    run (never with `bronze_rebuild`; the rebuild's full refresh is started by hand, plan section 5).
* **Not created (ask first):** ci-prod, its OAuth secret and Lakebase role, scope `netsuite_ingestion_prod`, the
  `prod` environment and the `DATABRICKS_*_PROD` environment secrets. Also needed then: `run_as` ci-prod and
  ci-prod in the prod target's `permissions`, and `poc_netsuite.poc_gold` (the prod metadata endpoint is
  enabled again since 2026-09-30).

## 2g. Step F prod setup (approved and done 2026-09-30)

* Service principal **ci-prod** with one OAuth secret (90 days, expires 2026-12-29), stored only as `prod`
  environment secrets `DATABRICKS_HOST_PROD`, `DATABRICKS_CLIENT_ID_PROD`, `DATABRICKS_CLIENT_SECRET_PROD`.
* GitHub environment **`prod`**: required reviewer = owner (self-review allowed: solo owner), deployment branches
  `main` only. Repository-level secrets hold only the dev ones.
* Lakebase roles `ci-prod` (no admin membership) on `aidq-metadata/production` and `netsuite-sample/production`.
  Grants like dev: prod metadata USAGE on `aidq_metadata`, SELECT on the three config tables, SELECT + INSERT on
  `run_audit`; source USAGE + SELECT on `netsuite` (default privileges for new tables).
* Scope **`netsuite_ingestion_prod`**: ci-prod WRITE, owner MANAGE. ci-prod has no access to the dev or `poc` scope.
* UC: USE CATALOG `poc_netsuite`; USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY on
  `poc_bronze`, `poc_silver`, `poc_reject`, `poc_gold`, `ledger`, `canary` (the last three created for prod);
  USE SCHEMA on `default`.
* Release step 0 access (granted 2026-09-30, owner's OK): ci-prod CAN_MANAGE on the existing prod job
  `netsuite_ingestion_daily` and pipeline `netsuite_ingestion_poc` (owner stays IS_OWNER). Not yet done: the
  `bundle deployment bind` itself (first release).
* Verified as ci-prod: identity, Lakebase login and grants (allowed and denied), prod scope WRITE, no access to
  other scopes, `bundle validate -t prod` from a clean checkout (no warnings).
* PR (prod target): `run_as` and CAN_MANAGE = the deploying service principal (ci-prod via promote-prod),
  `secret_scope: netsuite_ingestion_prod`; tests.
* **Owner-run script pending:** `aidq_owner` on prod metadata with ci-prod as member (`grant_ci_prod.py`, same as
  the dev script); promote-prod's `migrate --apply` needs it.

## 3. Decisions waiting for the owner

0. **Dev only until the owner says otherwise** (2026-10-08): no promote-prod, prod grants or prod endpoint changes.
   Exception, owner decision 2026-10-13: ci-dev and data-generator have project-level CAN_MANAGE on the shared
   Lakebase projects (it covers prod metadata). **Resolve before prod goes live.**
1. Step 8 (unpause prod): after the same-day versions fix and the deferred prod rebuild (section 0g); prod cron is
   06:30 UTC in config (settled 2026-10-08).
2. After step 8 plus three good scheduled prod runs: drop `poc_netsuite.backup_pre_release_202610010127` (ask).

## 4. Gotchas

* **Deploy identity and local state.** A bundle deploy reads the local `.databricks/` state cache. Deploying as
  another identity from a working copy that has the owner's cache tries to update the owner's resources (ci-dev
  gets 403). Deploy as ci-dev only from a clean checkout (CI always is).
* **Dev-mode prefix is the deployer.** Only the CI-deployed `[dev ci_dev]` set exists now. Do not deploy dev by
  hand: a second set would write the same `workspace.poc_*` tables, which the CI pipeline owns.
* **Per-target secret keys.** Scope `netsuite_ingestion_poc` holds `meta_pg_token` (prod) and `meta_pg_token_dev`
  (dev); the per-environment scopes of plan section 3a replace this in step D. Do not point dev at the prod key.
* **Migrations before code.** Migration files stay byte-stable once recorded; add a new migration instead.
* **Guard read retries.** The bronze guard retries reading its `create_update` event 5 times, 10 s apart, and fails
  closed for `pending_only`.
* **Development mode and schedules.** An explicit `pause_status: UNPAUSED` on a job is honored in dev mode; only
  `presets.trigger_pause_status: UNPAUSED` is refused there.
* **Free Edition limits.** Stop the SQL warehouse before the canary or several updates (`RESOURCE_EXHAUSTED`). At
  most 5 concurrent job tasks. No account console or account APIs (no OIDC). One Lakebase project per account
  (owner's note; this account already has two, so no new project can be added). Lakebase endpoints can end up
  disabled after inactivity.
* **Full refresh is blocked unless `bronze_rebuild=true`.**
* **Manual runs go through the job** (owner, 2026-10-14): never start a pipeline update directly. The job refreshes
  the stored Lakebase credential first; a direct update uses a possibly expired token (incident 6, section 0i).
* **Windows.** PowerShell 5.1 strips quotes inside JSON arguments to native commands: use Git Bash for
  `--json '...'`. `MSYS_NO_PATHCONV=1` for workspace paths starting with `/Users/...`.
* **GitHub.** Protected `main`: every change through a PR. Never force-push; never `pull_request_target`.
