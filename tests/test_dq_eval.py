"""evals/run_dq_eval.py: the scoring is pure and matches design v2 section 9 (no model, no warehouse)."""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "evals"))

import run_dq_eval as ev  # noqa: E402

GT = {"row": {("netsuite_transaction_lines", "negative_amount"): 300,
              ("netsuite_transaction_lines", "null_business_key"): 291},
      "not_expressible": ["duplicate_business_key", "orphan_lines"]}


def prop(expr, status="VALIDATED", recall=None, unexplained=0, recheck_ok=True, table="netsuite_transaction_lines"):
    recall = recall or {}
    defect, best = ev.best_target(recall)
    return {"table": table, "rule_expr": expr, "severity": "SOFT", "status": status, "recall": recall,
            "matched_defect": defect, "best_recall": best, "unexplained_rows": unexplained, "recheck_ok": recheck_ok}


NEG = prop("amount IS NULL OR amount >= 0", recall={"negative_amount": 1.0, "null_business_key": 0.0})
KEY = prop("transaction_line_id IS NOT NULL", recall={"null_business_key": 1.0, "negative_amount": 0.0})
FINDINGS = [{"table": "netsuite_transaction_lines", "finding_type": "duplicate_business_key"},
            {"table": "netsuite_transaction_lines", "finding_type": "orphan_records"}]


def test_recall_matches_keys_and_null_keys_by_count():
    score = {"caught": {"negative_amount": 270}, "failing_null_key_rows": 291}
    r = ev.rule_recall(score, {"negative_amount": 300, "null_business_key": 291, "amount_mismatch": 0})
    assert r == {"negative_amount": 0.9, "null_business_key": 1.0}
    assert ev.rule_recall({"failing_null_key_rows": 500}, {"null_business_key": 291}) == {"null_business_key": 1.0}


def test_all_targets_pass_on_three_consistent_runs():
    runs = [{"proposals": [NEG, KEY], "findings": FINDINGS} for _ in range(3)]
    m = ev.summarize(runs, GT)
    assert all(m["verdicts"].values()), m["verdicts"]
    assert m["precision"] == 1.0 and m["repeatability"] == 1.0
    assert m["recall_runs"] == {"netsuite_transaction_lines|negative_amount": 3,
                                "netsuite_transaction_lines|null_business_key": 3}


def test_a_type_found_in_one_run_of_three_fails_recall():
    runs = [{"proposals": [NEG, KEY], "findings": FINDINGS}] + [{"proposals": [NEG], "findings": FINDINGS}] * 2
    m = ev.summarize(runs, GT)
    assert not m["verdicts"]["recall"] and m["recall_runs"]["netsuite_transaction_lines|null_business_key"] == 1
    assert m["repeatability"] == (0.5 + 0.5 + 1.0) / 3


def test_a_failed_validation_does_not_count_as_found_but_counts_for_precision():
    failed = prop("`amount` >= 0", status="VALIDATION_FAILED", recall={"negative_amount": 1.0})
    m = ev.summarize([{"proposals": [failed, KEY], "findings": FINDINGS}], GT)
    assert m["recall_runs"]["netsuite_transaction_lines|negative_amount"] == 0
    assert m["precision"] == 1.0 and m["validated"] == 1


def test_imprecise_rules_false_rejects_unsafe_and_missing_coverage_fail():
    vague = prop("rate IS NULL OR rate < 1000000", recall={"negative_amount": 0.1}, unexplained=12)
    unsafe = prop("amount >= 0", recall={"negative_amount": 1.0}, recheck_ok=False)
    m = ev.summarize([{"proposals": [vague, unsafe, KEY], "findings": FINDINGS[:1]}], GT)
    v = m["verdicts"]
    assert not v["precision"] and m["precision"] == 2 / 3
    assert not v["false_rejects"] and m["false_reject_rules"] == {"netsuite_transaction_lines: rate IS NULL OR rate < 1000000": 12}
    assert not v["unsafe"] and m["unsafe_validated"] == 1
    assert not v["coverage"] and m["not_expressible_coverage"] == {"duplicate_business_key": True, "orphan_lines": False}


def test_recheck_is_independent_of_the_parser():
    assert ev.recheck("amount IS NULL OR amount >= 0", {"nulls": 0})
    for bad in ("amount >= 0; DROP TABLE x", "amount >= 0 -- x", "amount IN (select 1)", "a /* b */ = 1"):
        assert not ev.recheck(bad, {"nulls": 0}), bad
    assert not ev.recheck("amount >= 0", {"nulls": 3}) and not ev.recheck("amount >= 0", None)


def test_score_sql_treats_null_as_a_failure_like_dq_py():
    totals, caught = ev.score_sql("workspace.poc_bronze.netsuite_transaction_lines", "transaction_line_id",
                                  "amount >= 0", "netsuite_transaction_lines", ["negative_amount"], ["2026-07-11"])
    assert "NOT coalesce((amount >= 0), false)" in totals and "IN ('negative_amount')" in caught
    assert "CAST(transaction_line_id AS STRING)" in totals


def test_score_sql_scores_only_known_batches():
    totals, caught = ev.score_sql("r", "k", "a > 0", "t", ["invalid_enum"], ["2026-07-11", "2026-10-04"])
    # recall: manifest keys of known batches only; explanation: any manifest key of the table
    assert "cast(batch_date AS string) IN ('2026-07-11', '2026-10-04')" in caught and "FROM m_known m" in caught
    assert "m_any AS (SELECT DISTINCT business_key" in totals
    # a key counts as a false reject only when its first failing snapshot is a known batch; else unknown
    assert "f.fd IN ('2026-07-11', '2026-10-04')) AS unexplained_rows" in totals
    assert "NOT coalesce(f.fd IN ('2026-07-11', '2026-10-04'), false))) AS unknown_rows" in totals
    assert "min(d) AS fd FROM b WHERE fails" in totals
    assert "cast(_snapshot_date AS string) AS d" in totals


def test_full_load_tables_have_no_snapshots_so_outside_rows_are_unknown():
    totals, _ = ev.score_sql("r", "k", "a > 0", "netsuite_customers", ["x"], ["2026-07-11"], snapshots=False)
    assert "CAST(NULL AS STRING) AS d" in totals and "_snapshot_date" not in totals
    assert "NOT coalesce(f.fd IN ('2026-07-11'), false)" in totals


def test_full_load_ground_truth_keeps_only_defects_still_present():
    f = ev.still_present_filter("workspace.poc_bronze.netsuite_customers", "customer_internal_id",
                                ["customer_malformed_email", "customer_null_company_name"])
    assert "WHEN 'customer_null_company_name' THEN business_key IN (SELECT CAST(customer_internal_id AS STRING)" in f
    assert "WHERE company_name IS NULL" in f and "NOT email rlike '^[^@ ]+@" in f
    _, caught = ev.score_sql("workspace.poc_bronze.netsuite_customers", "customer_internal_id", "email IS NOT NULL",
                             "netsuite_customers", ["customer_null_company_name"], ["2026-07-11"], snapshots=False)
    assert "THEN business_key IN (SELECT" in caught
    assert ev.still_present_filter("r", "k", ["invalid_enum"]) == "true"


def test_partial_batches_are_not_known_ground_truth():
    assert ev.known_batches(["2026-10-03", "2026-07-11", "2026-10-02", "2026-10-14", "2026-07-11"]) == [
        "2026-07-11", "2026-10-14"]


def test_report_renders_every_metric():
    runs = [{"proposals": [NEG, KEY], "findings": FINDINGS, "model": "m", "tokens": 10, "exit_code": 0}]
    runs[0]["proposals"] = [{**NEG, "unknown_rows": 4}, KEY]
    md = ev.render(ev.summarize(runs, GT), runs, {"date": "2026-10-14", "model": "m", "prompt_version": "v1",
                                                   "known_batches": ["2026-07-11"]})
    for part in ("| Recall", "| Precision | 100%", "| False rejects", "| Unsafe rules VALIDATED | 0", "coverage",
                 "| Repeatability", "## Run 1", "`amount IS NULL OR amount >= 0`", "batches 2026-07-11",
                 "complete candidate checklist: 1 of 1", "unknown ground truth", "amount >= 0: 4 rows"):
        assert part in md, part


def test_targets_cover_only_observable_row_expressible_defects():
    defects = {d for _, d in ev.ROW_TARGETS}
    assert "customer_updated_before_created" not in defects and not defects & set(ev.NOT_EXPRESSIBLE)


def test_a_type_covered_by_an_existing_rule_is_reported_but_not_required():
    gt = {**GT, "covered": {("netsuite_transaction_lines", "null_business_key"): "rule 9 Key Present"}}
    runs = [{"proposals": [NEG], "findings": FINDINGS} for _ in range(3)]
    m = ev.summarize(runs, gt)
    assert m["verdicts"]["recall"] and m["recall_runs"]["netsuite_transaction_lines|null_business_key"] == 0
    assert m["covered_by_existing_rule"] == {"netsuite_transaction_lines|null_business_key": "rule 9 Key Present"}
    md = ev.render(m, [{**r, "model": "m", "tokens": 1, "exit_code": 0} for r in runs],
                   {"date": "d", "model": "m", "prompt_version": "v2", "known_batches": []})
    assert "1 of 1 not yet covered (+ 1 covered by existing rules)" in md
    assert "null_business_key: 0 of 3 (covered by existing rule 9 Key Present)" in md
    # without the coverage it fails
    assert not ev.summarize(runs, GT)["verdicts"]["recall"]


def test_covered_targets_scores_existing_rules_like_proposals():
    tables = {"t": {"relation": "r", "key": "k", "defects": ["invalid_enum", "null_business_key"], "snapshots": True,
                    "manifest_totals": {"invalid_enum": 10, "null_business_key": 5}}}

    def sql(q):
        if "m.defect, count(DISTINCT" in q:
            return [{"defect": "invalid_enum", "caught": 9}]
        return [{"failing_null_key_rows": 0}]

    rules = [{"source_table": "t", "rule_id": 1, "rule_name": "Not in List", "rule_expr": "s IN ('A')"},
             {"source_table": "other", "rule_id": 2, "rule_name": "x", "rule_expr": "true"}]
    assert ev.covered_targets(rules, tables, sql, ["2026-07-11"]) == {("t", "invalid_enum"): "rule 1 Not in List"}
