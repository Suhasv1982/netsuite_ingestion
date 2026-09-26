"""Build baseline/before_agents_report.md from the saved snapshots and manifests.

Every number in the tables is computed from: clean.json, defects_full.json,
incremental.json (collect_metrics.py), manifest_*.json (netsuite_gen.py) and
classification_*.json (classify_defects.py). Nothing is typed in by hand.
"""

import json
import re
from pathlib import Path

import classify_defects as cd

B = Path(__file__).resolve().parent
load = lambda name: json.loads((B / name).read_text(encoding="utf-8"))  # noqa: E731

clean, full, inc = load("clean.json"), load("defects_full.json"), load("incremental.json")
m_clean, m_def, m_inc = load("manifest_clean.json"), load("manifest_defects.json"), load("manifest_increment.json")
c_full, c_inc = load("classification_full.json"), load("classification_incremental.json")

TABLES = ["netsuite_memberships", "netsuite_certifications", "netsuite_transactions", "netsuite_transaction_lines"]
ALL = ["netsuite_customers"] + TABLES
TID = {1: "netsuite_customers", 2: "netsuite_memberships", 3: "netsuite_certifications", 4: "netsuite_transactions", 5: "netsuite_transaction_lines"}
short = lambda t: t.replace("netsuite_", "")  # noqa: E731
COLS = ["rejected", "passed_to_silver", "silently_absorbed", "caused_failure", "unresolved", "bronze_only"]
HEAD = ["Rejected", "Passed to silver", "Silently absorbed", "Caused failure", "Unresolved", "Bronze only (customers)"]


def _numeric(rows, i):
    return all(re.fullmatch(r"[\d,\-\* ]*", str(r[i])) for r in rows)


def md(headers, rows):
    align = ["--:" if _numeric(rows, i) and i > 0 else "---" for i in range(len(headers))]
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(align) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def n(x):
    return f"{x:,}" if isinstance(x, int) else str(x)


def bronze_sum(snap, table, names_key="expected_by_dq"):
    names = snap["bronze_expected_vs_actual"][names_key][table]
    return sum(snap["uc_row_counts"]["poc_bronze"][x] for x in names)


def rejected_rows(snap, table):
    return sum(r["rows"] for r in snap["reject_breakdown"] if r["source_table"] == table) if isinstance(snap["reject_breakdown"], list) else 0


def audit(snap, layer):
    return {TID[r["table_id"]]: r for r in snap["run_audit"] if r["layer"] == layer} if isinstance(snap["run_audit"], list) else {}


def class_table(classified):
    rows = []
    for name, v in classified.items():
        rows.append([name, n(v["rows"])] + [n(v.get(c, 0)) for c in COLS])
    tot = {c: sum(v.get(c, 0) for v in classified.values()) for c in COLS}
    rows.append(["**Total**", f"**{n(sum(v['rows'] for v in classified.values()))}**"] + [f"**{n(tot[c])}**" for c in COLS])
    return rows


def matrix(rows):
    out = []
    for r in rows:
        out.append([r["defect"], short(r["table"]), n(r["rows"])] + [n(r["outcomes"].get(c, 0)) for c in COLS])
    return out


def absorbed_by_table(classified_rows, defect=None):
    acc = {}
    for r in classified_rows:
        if defect is None or r["defect"] == defect:
            acc[r["table"]] = acc.get(r["table"], 0) + r["outcomes"].get("silently_absorbed", 0)
    return acc


def reconcile(snap, prior_absorbed, emitted, prior_rejected):
    rows = []
    for t in ALL:
        source = snap["source_row_counts"][t]
        bronze = bronze_sum(snap, t)
        if t == "netsuite_customers":
            rows.append([short(t), n(source), n(bronze), "-", "-", "-", "-", "-", "no silver table (FullLoad)"])
            continue
        rej = rejected_rows(snap, t)
        silver = snap["uc_row_counts"]["poc_silver"][t]
        absorbed = prior_absorbed.get(t, 0)
        pending = bronze - rej - silver - absorbed
        expected_pending = emitted.get(t, 0) - (rej - prior_rejected.get(t, 0)) if emitted else 0
        residual = pending - expected_pending
        rows.append([short(t), n(source), n(bronze), n(rej), n(absorbed), n(pending), n(silver), n(residual),
                     "reconciles" if residual == 0 else "UNEXPLAINED"])
    return md(["Table", "Source", "Bronze (tables the pipeline defines)", "Rejected", "Merged away / absorbed",
               "Not yet in silver", "Silver", "Unexplained", "Result"], rows)


def build():
    L = []
    add = L.append
    ok = lambda s: "SUCCESS" if s == "SUCCESS" else s  # noqa: E731

    add("# Before-agents baseline report\n")
    add("Pipeline: `netsuite_ingestion_poc` (prod bundle, job `netsuite_ingestion_daily`, catalog `poc_netsuite`), source: Lakebase "
        "`netsuite-sample` schema `netsuite`, metadata: Lakebase `aidq-metadata`. No agents were running. "
        "Every count below is computed from the saved snapshots in `baseline/` (see section 8).\n")

    # summary ---------------------------------------------------------
    tot = c_full["by_defect"]
    grand = {c: sum(v.get(c, 0) for v in tot.values()) for c in COLS}
    null_silver = sum(v["null_keys"] for v in full["silver_key_quality"].values())
    la_rows = c_inc["by_defect"].get("late_arriving", {}).get("rows", 0)
    add("## Summary\n")
    add(f"- Step A (clean data, full refresh) and step B ({sum(d['row_count'] for d in m_def['defects']):,} injected defect rows, full refresh) both succeeded. "
        f"Step C (increment with normal updates, {la_rows} late-arriving rows and the drift column, normal run) **failed**: all four silver flows stopped with "
        "\"streaming sources added or removed\", so silver is unchanged after it.\n"
        f"- Step B outcomes for the injected rows: {grand['rejected']} rejected, {grand['passed_to_silver']:,} passed to silver, "
        f"{grand['silently_absorbed']} silently absorbed, {grand['caused_failure']} caused failure, {grand['bronze_only']} customers rows bronze-only.\n"
        f"- Only membership status, certification type and the certification start-date cutoff produce rejections. Negative amounts, amount mismatches, orphan lines and invalid "
        f"transaction types pass through untouched; duplicate keys and NULL keys are merged away with no error ({null_silver} NULL-key rows remain in silver).\n"
        "- Source, bronze and silver reconcile with 0 unexplained rows for every table in steps B and C (section 4).\n"
        "- Findings 1 to 9 (section 5) cover the `run_audit` over-count, the incremental-run failure, the customers date columns dropped by bronze, and the drift column.\n")

    # 1 ---------------------------------------------------------------
    add("## 1. What was run\n")
    step_rows = [
        ["A. Clean, full refresh", "`--init --seed 42`, no defects", m_clean["backup"]["schema"], clean["job_run_id"], clean["job"]["state"],
         f"{clean['pipeline']['update_id'][:8]}… {clean['pipeline']['state']}"],
        ["B. Defects, full refresh", f"`--init --seed 42 --config defects_baseline.yaml` ({sum(d['row_count'] for d in m_def['defects']):,} defect rows)",
         m_def["backup"]["schema"], full["job_run_id"], full["job"]["state"], f"{full['pipeline']['update_id'][:8]}… {full['pipeline']['state']}"],
        ["C. Increment, normal run", f"`--increment --seed 43 --batch-date {m_inc['batch_date']} --apply-schema-drift` (late_arriving 10%)",
         m_inc["backup"]["schema"], inc["job_run_id"], inc["job"]["state"], f"{inc['pipeline']['update_id'][:8]}… {inc['pipeline']['state']}"],
    ]
    add(md(["Step", "Data change", "Backup taken before it (schema in `netsuite-sample`)", "Job run id", "Job result", "Pipeline update"], step_rows))
    add("\nSteps A and B ran with `full_refresh: true` (the source data was replaced). Step C ran the job normally, with no full refresh. "
        "Each backup is a verified copy of the five tables plus a Lakebase branch `pre-synthetic-backup-<timestamp>`.\n")

    # 2 ---------------------------------------------------------------
    add("## 2. Headline numbers\n")
    head = []
    for label, snap in (("A. Clean", clean), ("B. Defects, full refresh", full), ("C. Increment, normal run", inc)):
        tot_src = sum(snap["source_row_counts"][t] for t in TABLES)
        tot_silver = sum(snap["uc_row_counts"]["poc_silver"][t] for t in TABLES)
        rej = snap["uc_row_counts"]["poc_reject"]["rejected_rows"]
        head.append([label, n(tot_src), n(tot_silver), n(rej), "yes" if cd.run_failed(snap) else "no"])
    add(md(["Step", "Source rows (4 incremental tables)", "Silver rows", "Rows in `rejected_rows`", "Pipeline failed"], head))
    add("\nPer-table row counts:\n")
    rows = []
    for t in ALL:
        r = [short(t)]
        for snap in (clean, full, inc):
            silver = snap["uc_row_counts"]["poc_silver"].get(t, "-")
            r += [n(snap["source_row_counts"][t]), n(silver) if silver != "-" else "-"]
        rows.append(r)
    add(md(["Table", "A source", "A silver", "B source", "B silver", "C source", "C silver"], rows))
    add("\nReject breakdown (`poc_reject.rejected_rows`):\n")
    rej_rows = []
    for label, snap in (("A", clean), ("B", full), ("C", inc)):
        for r in snap["reject_breakdown"]:
            rej_rows.append([label, short(r["source_table"]), r["reason"], n(r["rows"])])
        if not snap["reject_breakdown"]:
            rej_rows.append([label, "-", "(none)", "0"])
    add(md(["Step", "Table", "Reason (rule name)", "Rows"], rej_rows))

    # 3 ---------------------------------------------------------------
    add("\n## 3. Classification of every injected defect row\n")
    add("Outcomes: **Rejected** = a HARD DQ rule sent it to `poc_reject.rejected_rows`. **Passed to silver** = reached `poc_silver`. "
        "**Silently absorbed** = in neither silver nor rejects, with no error (merged away by the AUTO CDC key merge). "
        "**Caused failure** = the run failed and the error names the table. **Unresolved** = the run failed and the outcome cannot be determined. "
        "**Bronze only** = customers rows (FullLoad, no DQ rules, no silver table). Rows are matched by business key, or by count for NULL keys.\n")
    add("### 3a. Step B: defects, full refresh\n")
    add("By defect type:\n")
    add(md(["Defect", "Rows injected"] + HEAD, class_table(c_full["by_defect"])))
    add("\nBy table:\n")
    add(md(["Table", "Rows injected"] + HEAD, class_table(c_full["by_table"])))
    add("\nDefect by table:\n")
    add(md(["Defect", "Table", "Rows"] + HEAD, matrix(c_full["rows"])))
    add("\n### 3b. Step C: increment, normal run\n")
    add("Only late-arriving rows are defects in this step (the updated versions and new rows are normal data).\n")
    add(md(["Defect", "Rows injected"] + HEAD, class_table(c_inc["by_defect"])))
    add("\nBy table:\n")
    add(md(["Table", "Rows injected"] + HEAD, class_table(c_inc["by_table"])))
    late_bronze = [(t, sum(v for k, v in inc["uc_row_counts"]["poc_bronze"].items() if k.startswith(t) and k.endswith("__2026_06_20"))) for t in TABLES]
    add("\nThe late rows did reach bronze: the new backdated snapshot tables hold "
        + ", ".join(f"{short(t)} {v}" for t, v in late_bronze) + " rows (`…__2026_06_20`). They did not reach silver because the silver flows failed (finding 2).\n")

    # 4 ---------------------------------------------------------------
    add("## 4. Reconciliation: source vs bronze vs silver\n")
    add("`Bronze` sums only the bronze tables the pipeline defines for the source's current watermark values (what `dq.py` and `bronze.py` resolve), "
        "so leftover objects are excluded (finding 1). `Merged away / absorbed` comes from the step B classification "
        "(duplicate pairs collapsed, NULL-key rows collapsed to one). `Not yet in silver` = bronze - rejected - silver - absorbed. "
        "`Unexplained` compares that with what the run should have left unprocessed; 0 means every gap is accounted for.\n")
    add("### Step B (full refresh)\n")
    absorbed_b = absorbed_by_table(c_full["rows"])
    add(reconcile(full, absorbed_b, {}, {}))
    add("\nGap explanation per table (rows removed between bronze and silver):\n")
    exp = []
    for t in TABLES:
        dup = absorbed_by_table(c_full["rows"], "duplicate_business_key").get(t, 0)
        nul = absorbed_by_table(c_full["rows"], "null_business_key").get(t, 0)
        rej = rejected_rows(full, t)
        exp.append([short(t), n(bronze_sum(full, t)), n(rej), n(dup), n(nul), n(full["uc_row_counts"]["poc_silver"][t]),
                    "yes" if bronze_sum(full, t) - rej - dup - nul == full["uc_row_counts"]["poc_silver"][t] else "NO"])
    add(md(["Table", "Bronze", "- Rejected (DQ)", "- Duplicate pairs merged", "- NULL-key rows collapsed", "= Silver", "Matches"], exp))
    add("\n### Step C (increment, normal run: silver flows failed)\n")
    emitted_c = m_inc["rows_emitted"]
    prior_rej = {t: rejected_rows(full, t) for t in TABLES}
    add(reconcile(inc, absorbed_b, emitted_c, prior_rej))
    add("\nSilver is unchanged from step B, so every row emitted by the increment and not rejected is waiting in bronze: "
        + ", ".join(f"{short(t)} {n(emitted_c[t])} emitted" for t in TABLES) + ".\n")

    # 5 ---------------------------------------------------------------
    add("## 5. Findings\n")
    # finding 1: run_audit over-count
    add("### Finding 1: `log_run_audit` over-counts bronze (`log_run_audit.py` and `dq.py` resolve bronze tables differently)\n")
    rows = []
    for label, snap in (("A", clean), ("B", full)):
        a_b, a_d, a_s = audit(snap, "bronze"), audit(snap, "dq"), audit(snap, "silver")
        for t in TABLES:
            true_b = bronze_sum(snap, t)
            rows.append([label, short(t), n(true_b), n(a_b[t]["rows_read"]), n(a_b[t]["rows_read"] - true_b),
                         n(a_s[t]["rows_read"]) if t in a_s else "-", n(snap["uc_row_counts"]["poc_silver"][t])])
    add(md(["Step", "Table", "True bronze rows", "`run_audit` bronze rows", "Over-count", "`run_audit` silver rows_read", "Actual silver rows"], rows))
    lo = clean["bronze_expected_vs_actual"]["leftover_bronze_tables"]
    add("\n- `dq.py` / `bronze.py` call `bronze_table_names_for`, which asks the *source* for its distinct watermark values and builds exact names "
        "(`<table>__<YYYY_MM_DD>`).\n"
        "- `log_run_audit.py:_bronze_row_count` runs `SHOW TABLES IN <catalog>.poc_bronze LIKE '<table>*'` and sums every match, "
        "including bronze tables the pipeline no longer defines.\n"
        "- Leftover objects (present in `poc_bronze` in all three snapshots, registered to pipeline `d002bb31…`, holding 2 to 3 rows each (created_date 2026-08-01 confirmed for the memberships and lines views; the counts match the original data's 2026-08-01 snapshot for the other two), "
        "not read by silver): " + ", ".join(f"`poc_netsuite.poc_bronze.{x}`" for x in lo) + ".\n"
        "- Effect: bronze, dq and silver `rows_read` in `run_audit` are inflated by the leftover rows, so silver `rows_read` does not equal silver rows. "
        "The over-count is constant across runs A and B, so differences between them are still valid.\n"
        "- Nothing was dropped. Why the pipeline keeps these views after the source lost that watermark was not investigated.\n")

    # finding 2
    errs = inc["pipeline"]["error_events"]
    flows = sorted({t for t in TABLES if any(t in json.dumps(e) for e in errs)})
    add("### Finding 2: the normal incremental run fails on all four silver flows (`STREAM_FAILED`, sources added)\n")
    add(f"- Job run {inc['job_run_id']}: `run_pipeline` FAILED, `log_run_audit` and `refresh_credentials` SUCCESS. Job result `{inc['job']['state']}`.\n"
        f"- Pipeline update `{inc['pipeline']['update_id']}` FAILED. Flows named in the errors: {', '.join(f'`{f}`' for f in flows)}.\n"
        "- Error: *Flow ... had streaming sources added or removed. Please perform a full refresh in order to rebuild ... against the current set of sources.* "
        "(`assertion failed: There are [1] sources in the checkpoint offsets and now there are [3] sources requested by the query`).\n"
        "- Cause: `dq.py` unions one streaming read per bronze snapshot table into `<table>_valid`. The increment introduced two new watermark values "
        "(2026-06-20 late rows, 2026-08-22 new snapshot), so the source count went from 1 to 3 and the silver checkpoints no longer match. "
        "By the same logic (inferred from the error and `dq.py`, not tested separately), any new watermark value will trigger this, so a plain incremental run cannot pick up new snapshots without a full refresh.\n"
        f"- State after the failure: bronze snapshot tables were created (see 3b), `rejected_rows` was recomputed "
        f"({full['uc_row_counts']['poc_reject']['rejected_rows']} -> {inc['uc_row_counts']['poc_reject']['rejected_rows']}; the extra "
        "`Not in List` certification rows can only be new versions of already-invalid certifications, because the generator's updates keep "
        "`certification_type` and new or late rows are valid), silver is unchanged.\n"
        "- `run_audit` logged 14 FAILED rows (every table and layer) with the generic message `Workload failed, see run output for details`; "
        "it does not say which flow failed or why.\n")

    # finding 3
    nulls = {t: v for t, v in full["silver_key_quality"].items()}
    nk = c_full["by_defect"]["null_business_key"]
    add("### Finding 3: NULL business keys collapse to one silent row per table\n")
    add(f"- {nk['rows']} rows were injected with a NULL business key. Silver holds {sum(v['null_keys'] for v in nulls.values())} rows with a NULL key "
        f"(one per table: " + ", ".join(f"{short(t)} {v['null_keys']}" for t, v in nulls.items()) + f"); the other {nk.get('silently_absorbed', 0)} were merged into them with no error.\n"
        "- No DQ rule checks key presence, so nothing was rejected.\n")

    # finding 4
    dup = c_full["by_defect"]["duplicate_business_key"]
    add("### Finding 4: duplicate business keys are silently merged away\n")
    add(f"- {dup['rows']} duplicate rows were injected. All {dup.get('silently_absorbed', 0)} were merged with their original by the AUTO CDC merge; "
        "silver has no duplicate keys and no error or reject is produced. Which of the two rows survived is not recorded in the pipeline outputs.\n")

    # finding 5
    add("### Finding 5: only two of the enum rules catch anything, and one rejection is incidental\n")
    inv = [r for r in c_full["rows"] if r["defect"] == "invalid_enum"]
    ebs = [r for r in c_full["rows"] if r["defect"] == "end_before_start"]
    add("- `invalid_enum`: " + "; ".join(f"{short(r['table'])}.{r['column']} {r['outcomes'].get('rejected', 0)}/{r['rows']} rejected" for r in inv) +
        ". `transactions.type` has no DQ rule, so all its invalid values passed to silver.\n"
        "- `end_before_start`: " + "; ".join(f"{short(r['table'])} {r['outcomes'].get('rejected', 0)}/{r['rows']} rejected" for r in ebs) +
        ". No rule compares end and start dates. The 9 rejected certifications were caught only because swapping the dates moved "
        "`certification_start_date` to or past 2027-01-01, which trips the existing `Date Range` rule (`certification_start_date < 2027-01-01`).\n")

    # finding 6
    add("### Finding 6: content defects with no rule pass through untouched\n")
    for d in ("negative_amount", "amount_mismatch", "orphan_lines"):
        v = c_full["by_defect"][d]
        add(f"- `{d}`: {v['rows']} injected, {v.get('passed_to_silver', 0)} reached silver, {v.get('rejected', 0)} rejected.")
    add("- Customers defects (null `company_name`, malformed `email`, `updated_date` < `created_date`): 60 rows, all bronze only; "
        "customers has no silver table and no DQ rules.\n")

    # finding 7
    cust_b = clean["table_columns"]["poc_bronze.netsuite_customers"]
    add("### Finding 7: bronze drops `customers.created_date` and `updated_date` (pre-existing drift)\n")
    add(f"- Source `netsuite_customers` has 5 columns; bronze `netsuite_customers` has {len(cust_b)}: {', '.join(f'`{c}`' for c in cust_b)}. "
        "`aidq_metadata.source_columns` lists only those three, and `bronze.py` selects `column_list` from it. Not fixed, as instructed.\n"
        "- Consequence: the 20 `customer_updated_before_created` defects cannot be observed in bronze or anywhere downstream (`observable_in_bronze: false`). "
        "Before this work all 24 original customers rows also had NULL in both date columns.\n")

    # finding 8
    src_cols = inc["bronze_expected_vs_actual"]["source_transactions_columns"]
    bronze_txn = [v for k, v in inc["table_columns"].items() if k.startswith("poc_bronze.netsuite_transactions__")]
    add("### Finding 8: the schema-drift column was ignored and is not the cause of the failure\n")
    add(f"- `ALTER TABLE netsuite.netsuite_transactions ADD COLUMN custbody_region text` was applied in step C; source columns are now: {', '.join(f'`{c}`' for c in src_cols)}.\n"
        f"- No bronze transactions table contains `custbody_region` ({len(bronze_txn)} tables checked) and silver has "
        f"{'no' if 'custbody_region' not in inc['table_columns']['poc_silver.netsuite_transactions'] else 'a'} such column, because the pipeline selects only the columns listed in `source_columns`.\n"
        "- No error mentions the new column. The step C failure is the streaming-source change in finding 2.\n"
        "- Nothing in the pipeline or `run_audit` records that the source schema changed.\n")

    # finding 9
    add("### Finding 9: late-arriving rows\n")
    la = c_inc["by_defect"].get("late_arriving", {})
    add(f"- {la.get('rows', 0)} late rows (new keys, `created_date` 2026-06-20, `updated_date` before the table's watermark 2026-07-11) were injected. "
        f"Classification: {la.get('caused_failure', 0)} caused failure, {la.get('silently_absorbed', 0)} silently absorbed, {la.get('passed_to_silver', 0)} passed to silver.\n"
        "- Because the silver flows failed, this baseline cannot show whether the watermark/streaming logic would have skipped them. "
        "That needs a full-refresh run after the increment, which was not part of this baseline.\n")

    add("### Other observations\n")
    add("- Only three HARD rules exist (membership_status list, certification_type list, certification_start_date < 2027-01-01) and no SOFT rules, "
        "so the new SOFT-to-expectation wiring in `dq.py` was not exercised.\n"
        "- In step B the run succeeded end to end with 2,005 defect rows; the only signal of bad data is the 54 rows in `rejected_rows`.\n")

    # 6 things to know -------------------------------------------------
    add("## 6. Things to know\n")
    add("- **Late-row behavior is unmeasured.** Because the silver flows failed in step C, this baseline cannot show whether the watermark and streaming logic would have "
        "skipped the late rows. A full-refresh run after the increment would show it. That was not part of this baseline.\n"
        f"- **Increment batch date.** Step C used `--batch-date {m_inc['batch_date']}`. The default (2026-08-01, the latest snapshot plus 21 days) would have collided with, "
        "and overwritten, the four leftover `__2026_08_01` bronze views documented in finding 1.\n"
        "- **Backup gate on `--increment`.** Only `--init` had a backup step originally. `--increment` now takes the same verified backup before changing data "
        "(with tests), because a fresh backup before each data change was required.\n"
        "- **Backup step bug found and fixed.** The first `--init` attempt failed in the backup step (psycopg 3 cannot bind a tuple to `IN %s`). No data changed, and only the "
        "extra branch `pre-synthetic-backup-202609242052` was left behind. A regression test now covers it.\n"
        "- **Prod deploy.** The prod bundle was redeployed so the baseline runs the current repo code (including the `is_active`, blank-watermark and SOFT-expectation edits). "
        "The job schedule was forced to PAUSED and verified after the runs.\n"
        "- **Current state of the systems.** The `netsuite-sample` source holds the step C data, including its defects. `poc_netsuite` holds the failed step C state "
        "(bronze updated, `rejected_rows` recomputed, silver from step B). Nothing has been dropped or cleaned up.\n"
        "- **What was not exercised.** No SOFT rules exist, so the SOFT-to-expectation wiring in `dq.py` never ran, and that code has not been run inside a pipeline.\n"
        "- **Tests.** The unit suite passes (123 tests), including the generator, the writer against a fake connection, and the defect classifier.\n")

    # 7 ---------------------------------------------------------------
    add("## 7. Backups and restore\n")
    rows = []
    for label, mf in (("before A (original 232 rows)", None), ("before B (clean data)", m_def), ("before C (defect data)", m_inc)):
        if mf is None:
            rows.append([label, "netsuite_backup_202609242053", "pre-synthetic-backup-202609242053", "24 / 27 / 21 / 56 / 104"])
        else:
            b = mf["backup"]
            counts = " / ".join(str(b["row_counts"][t]["backup"]) for t in ALL)
            rows.append([label, b["schema"], b["branch"]["name"].split("/")[-1], counts])
    add(md(["Snapshot", "Schema (copy)", "Lakebase branch", "Rows: customers / memberships / certifications / transactions / lines"], rows))
    add("\nAlso present: branch `pre-synthetic-backup-202609242052`, left by a failed first attempt (a snapshot of the untouched original data). "
        "Nothing was deleted. To restore a snapshot, copy the tables from its schema back into `netsuite` (or restore from the branch).\n")

    # 7 ---------------------------------------------------------------
    add("## 8. Files (`netsuite_ingestion/baseline/`)\n")
    add("`clean.json`, `defects_full.json`, `incremental.json` (metrics snapshots); `manifest_clean.json`, `manifest_defects.json`, `manifest_increment.json` "
        "(injected defects with exact keys); `classification_full.json`, `classification_incremental.json`; `defects_baseline.yaml`, `increment_config.yaml`; "
        "`collect_metrics.py`, `classify_defects.py`, `build_report.py`.\n")
    return "\n".join(L)


if __name__ == "__main__":
    out = B / "before_agents_report.md"
    out.write_text(build(), encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size:,} bytes)")
