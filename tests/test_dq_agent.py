"""aidq_dq_agent: the model step (schema, loop, routing) and the graph (collect -> recommend -> submit -> report),
with fakes only: no model, no database."""

import asyncio
import json

from aidq_dq_agent import graph, recommend
from aidq_dq_agent.recommend import SUBMIT, SUBMIT_SCHEMA, DatabricksRecommender, Recommendation, schema_problems
from aidq_llm.router import Router

PROPOSAL = {"table": "netsuite_transaction_lines", "rule_name": "Amount Not Negative",
            "rule_expr": "amount IS NULL OR amount >= 0", "severity": "SOFT", "defect_type": "negative_amount",
            "candidate_id": "netsuite_transaction_lines:amount_negative",
            "rationale": "300 of 93,828 bronze rows (0.32%) have a negative amount; min -4500. SOFT: refunds may exist.",
            "evidence": {"violations": 300, "rows": 93828, "profile_fields": ["columns.amount.negative"]},
            "confidence": 0.8}
FINDING = {"table": "netsuite_transactions", "finding_type": "duplicate_business_key",
           "candidate_id": "netsuite_transactions:duplicate_business_key",
           "summary": "12 keys have conflicting created_date on the same day.",
           "evidence": {"keys_with_conflicting_created_date_same_day": 12}}
SUBMISSION = {"proposals": [PROPOSAL], "findings": [FINDING],
              "dismissed": [{"candidate_id": "netsuite_transactions:date_future",
                             "reason": "one row dated tomorrow from the 10-01 run; not a pattern"}]}
CANDIDATES = {"netsuite_transaction_lines": ["netsuite_transaction_lines:amount_negative"],
              "netsuite_transactions": ["netsuite_transactions:duplicate_business_key",
                                        "netsuite_transactions:date_future"]}
CONTEXT = {"today": "2026-10-14", "tables": ["netsuite_transaction_lines", "netsuite_transactions"],
           "profiles": {t: {"status": "ok", "candidates": [{"id": i, "count": 1, "rate": 0.1, "kind": "row_rule"}
                                                           for i in ids]} for t, ids in CANDIDATES.items()}}
TABLES = ["netsuite_transaction_lines", "netsuite_transactions"]


def call(name, args, i):
    return {"id": f"c{i}", "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}


def reply(tool_calls=None, content=""):
    return {"choices": [{"message": {"content": content, **({"tool_calls": tool_calls} if tool_calls else {})},
                         "finish_reason": "tool_calls" if tool_calls else "stop"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5}}


class FakeToolBox:
    def __init__(self, unavailable=()):
        self.unavailable, self.calls = set(unavailable), []

    async def list_tools(self):
        return [{"name": "profile_table", "description": "profile", "input_schema": {"type": "object"}}]

    async def call(self, name, args):
        self.calls.append((name, args))
        if name in self.unavailable or args.get("table") in self.unavailable:
            return {"status": "unavailable", "error": "endpoint disabled"}
        out = {"status": "ok", "tool": name, **({"table": args["table"]} if "table" in args else {})}
        if name == "profile_table":
            out["candidates"] = [{"id": i, "count": 1, "rate": 0.1, "kind": "row_rule"} for i in CANDIDATES[args["table"]]]
        return out


class FakeWriteTools:
    def __init__(self, dry_run=False, fail=False):
        self.dry_run, self.fail, self.proposals, self.findings = dry_run, fail, [], []

    def submit_proposal(self, args):
        if self.fail:
            raise ConnectionError("endpoint disabled")
        self.proposals.append(args)
        return {"status": "ok", "proposal_id": None if self.dry_run else 41}

    def report_finding(self, args):
        if self.fail:
            raise ConnectionError("endpoint disabled")
        self.findings.append(args)
        return {"status": "skipped", "reason": "an OPEN incident dq:netsuite_transactions:duplicate_business_key already exists"}


class FakeRecommender:
    def __init__(self, result=SUBMISSION, error=None):
        self.result, self.error, self.context = result, error, None

    async def recommend(self, context, toolbox):
        self.context = context
        return Recommendation(self.result, [], self.error, "model-under-test", {"input_tokens": 1, "output_tokens": 2})


def run_graph(recommender, write_tools, toolbox=None):
    return asyncio.run(graph.run(toolbox or FakeToolBox(), recommender, write_tools, TABLES, trace_id="t-1"))


# -- schema ----------------------------------------------------------------------------------------------------


def test_the_submission_schema_requires_a_defect_type_and_checks_nested_values():
    assert schema_problems(SUBMISSION, SUBMIT_SCHEMA) == []
    bad = {"proposals": [{**PROPOSAL, "severity": "MEDIUM", "confidence": 2, "extra": 1,
                          "evidence": {"violations": "300", "rows": 93828}}], "findings": []}
    problems = schema_problems(bad, SUBMIT_SCHEMA)
    for part in ("submission.dismissed is required", "severity must be one of", "confidence must be <= 1",
                 "extra is not allowed", "violations must be an integer"):
        assert any(part in p for p in problems), part
    no_type = {k: v for k, v in PROPOSAL.items() if k != "defect_type"}
    assert any("defect_type is required" in p for p in schema_problems({**SUBMISSION, "proposals": [no_type]}, SUBMIT_SCHEMA))
    assert schema_problems({"proposals": [], "findings": [], "dismissed": []}, SUBMIT_SCHEMA) == []


def test_booleans_are_not_numbers():
    assert schema_problems(True, {"type": "integer"}) == ["submission must be an integer"]


def test_prompt_is_versioned_and_states_the_core_rules():
    text = recommend.system_prompt()
    for part in ("TRUE for a valid row", "IS NULL OR amount >= 0", "SOFT", "DQ_NOT_EXPRESSIBLE",
                 "answer every candidate", "_rare_values", "_bad_format", "late_rows", "_row_hash", SUBMIT):
        assert part in text
    assert recommend.PROMPT_FILE.name == f"{recommend.PROMPT_VERSION}.md"


# -- the model loop --------------------------------------------------------------------------------------------


class ScriptChat:
    def __init__(self, replies):
        self.replies, self.messages = list(replies), []

    async def complete(self, model, messages, tools=None, max_tokens=4000):
        self.messages.append(list(messages))
        r = self.replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def test_loop_calls_read_tools_then_returns_the_submission():
    chat = ScriptChat([reply([call("profile_table", {"env": "dev", "table": "netsuite_transactions"}, 1)]),
                       reply([call(SUBMIT, SUBMISSION, 2)])])
    tb = FakeToolBox()
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend(CONTEXT, tb))
    assert rec.result == SUBMISSION and [c["tool"] for c in rec.tool_calls] == ["profile_table"]
    assert rec.usage == {"input_tokens": 20, "output_tokens": 10}
    # the model saw the submit tool, never a write tool
    names = {t["function"]["name"] for t in recommend.openai_tools(asyncio.run(tb.list_tools()))}
    assert names == {"profile_table", SUBMIT}


def test_an_invalid_submission_is_sent_back_and_a_corrected_one_accepted():
    bad = {**SUBMISSION, "proposals": [{**PROPOSAL, "severity": "MEDIUM"}]}
    chat = ScriptChat([reply([call(SUBMIT, bad, 1)]), reply([call(SUBMIT, SUBMISSION, 2)])])
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend(CONTEXT, FakeToolBox()))
    assert rec.result == SUBMISSION
    sent_back = chat.messages[1][-1]
    assert sent_back["role"] == "tool" and "severity must be one of" in sent_back["content"]


def test_tool_budget_is_enforced():
    many = [reply([call("profile_table", {"env": "dev", "table": "netsuite_transactions"}, i)])
            for i in range(recommend.MAX_TOOL_CALLS + 1)]
    chat = ScriptChat(many + [reply([call(SUBMIT, SUBMISSION, 99)])])
    tb = FakeToolBox()
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend(CONTEXT, tb))
    assert rec.result == SUBMISSION and len(tb.calls) == recommend.MAX_TOOL_CALLS


def test_a_json_answer_in_text_is_accepted_and_prose_is_nudged_then_given_up():
    chat = ScriptChat([reply(content="```json\n" + json.dumps(SUBMISSION) + "\n```")])
    assert asyncio.run(DatabricksRecommender(chat, "m").recommend(CONTEXT, FakeToolBox())).result == SUBMISSION
    chat = ScriptChat([reply(content="I think amounts look fine.")] * 3)
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend(CONTEXT, FakeToolBox()))
    assert rec.result is None and rec.error.startswith(f"no {SUBMIT} call")


def test_the_router_escalates_once_when_the_primary_gives_no_valid_submission():
    routes = {"tasks": {"rule_generation": {"primary": "big", "fallback": "other", "max_tokens": 100}}}
    chat = ScriptChat([RuntimeError("HTTP 503: down"), reply([call(SUBMIT, SUBMISSION, 1)])])
    router = Router(chat, "aidq_dq_recommender", routes=routes, trace_id="t-1")
    rec = asyncio.run(recommend.RoutedRecommender(router).recommend(CONTEXT, FakeToolBox()))
    assert rec.result == SUBMISSION and rec.model == "other"
    rows = router.log.rows
    assert [(r.model, r.escalated, r.outcome) for r in rows] == [("big", False, "error"), ("other", True, "ok")]
    assert rows[1].escalation_reason.startswith("error: model call failed")


# -- the graph -------------------------------------------------------------------------------------------------


def test_collect_recommend_submit_report():
    tb, wt, rec = FakeToolBox(), FakeWriteTools(), FakeRecommender()
    state = run_graph(rec, wt, tb)
    assert [c[0] for c in tb.calls] == ["get_dq_rules", "get_open_and_rejected_proposals", "get_rejects_summary",
                                        "profile_table", "profile_table"]
    assert set(rec.context["profiles"]) == set(TABLES) and rec.context["tables"] == TABLES
    # defect_type and the prompt version travel in the evidence; the write tool gets its own argument set
    [p] = wt.proposals
    assert "defect_type" not in p and "candidate_id" not in p and p["evidence"]["defect_type"] == "negative_amount"
    assert p["evidence"]["candidate_id"] == "netsuite_transaction_lines:amount_negative"
    assert p["evidence"]["prompt_version"] == recommend.PROMPT_VERSION and p["evidence"]["violations"] == 300
    [f] = wt.findings
    assert "candidate_id" not in f and f["evidence"]["candidate_id"] == FINDING["candidate_id"]
    assert state["exit_code"] == 0
    report = state["report"]
    for part in ("submitted (proposal 41)", "`amount IS NULL OR amount >= 0`", "skipped: an OPEN incident",
                 "## Dismissed candidates", "netsuite_transactions:date_future: one row dated", "trace t-1"):
        assert part in report, part
    assert "Dry run" not in report


def test_dry_run_says_so():
    state = run_graph(FakeRecommender(), FakeWriteTools(dry_run=True))
    assert "Dry run: nothing written." in state["report"] and "dry run: not written" in state["report"]
    assert state["exit_code"] == 0


def test_a_table_whose_profile_failed_is_left_out_and_reported():
    rec = FakeRecommender()
    state = run_graph(rec, FakeWriteTools(), FakeToolBox(unavailable={"netsuite_transactions"}))
    assert rec.context["tables"] == ["netsuite_transaction_lines"]
    assert "input unavailable: profile_table netsuite_transactions: unavailable" in state["report"]


def test_no_profile_means_no_model_call():
    rec = FakeRecommender()
    state = run_graph(rec, FakeWriteTools(), FakeToolBox(unavailable=set(TABLES)))
    assert rec.context is None and state["exit_code"] == graph.EXIT_METADATA_UNAVAILABLE
    assert "the model was not called" in state["report"]


def test_no_valid_recommendation_fails_the_run_and_writes_nothing():
    wt = FakeWriteTools()
    state = run_graph(FakeRecommender(result=None, error="no submit_recommendations call"), wt)
    assert state["exit_code"] == graph.EXIT_NO_RECOMMENDATION and not wt.proposals
    assert "No valid recommendation" in state["report"]


def test_a_store_failure_keeps_the_recommendations_in_the_report():
    state = run_graph(FakeRecommender(), FakeWriteTools(fail=True))
    assert state["exit_code"] == graph.EXIT_METADATA_UNAVAILABLE
    report = state["report"]
    assert "METADATA UNAVAILABLE" in report and report.count("NOT WRITTEN: ConnectionError") == 2
    assert "amount IS NULL OR amount >= 0" in report


def test_an_empty_submission_is_a_clean_run():
    state = run_graph(FakeRecommender(result={"proposals": [], "findings": [], "dismissed": []}), FakeWriteTools())
    assert state["exit_code"] == 0 and "Nothing to recommend." in state["report"]


# -- the candidate checklist -----------------------------------------------------------------------------------


def test_checklist_needs_every_candidate_answered_once_with_reasons():
    ids = recommend.candidate_ids(CONTEXT)
    assert len(ids) == 3
    assert recommend.checklist_problems(SUBMISSION, ids) == ([], [])
    partial = {**SUBMISSION, "dismissed": []}
    assert recommend.checklist_problems(partial, ids) == ([], ["netsuite_transactions:date_future"])
    bad = {"proposals": [{**PROPOSAL, "candidate_id": "made:up"}], "findings": [FINDING],
           "dismissed": [{"candidate_id": "netsuite_transactions:date_future", "reason": "fine"},
                         {"candidate_id": FINDING["candidate_id"], "reason": "duplicates are expected here"}]}
    problems, unanswered = recommend.checklist_problems(bad, ids)
    assert "unknown candidate_id 'made:up'" in problems and unanswered == ["netsuite_transaction_lines:amount_negative"]
    assert any("dismissal of netsuite_transactions:date_future needs a reason" in p for p in problems)
    assert any("is dismissed and also proposed or reported" in p for p in problems)
    # 'other' is allowed for a problem the checklist does not list
    extra = {**SUBMISSION, "proposals": SUBMISSION["proposals"] + [{**PROPOSAL, "candidate_id": "other"}]}
    assert recommend.checklist_problems(extra, ids) == ([], [])


def test_an_incomplete_checklist_is_sent_back_and_a_complete_one_accepted():
    partial = {**SUBMISSION, "dismissed": []}
    chat = ScriptChat([reply([call(SUBMIT, partial, 1)]), reply([call(SUBMIT, SUBMISSION, 2)])])
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend(CONTEXT, FakeToolBox()))
    assert rec.result == SUBMISSION
    assert "1 candidate(s) unanswered: netsuite_transactions:date_future" in chat.messages[1][-1]["content"]


def test_a_checklist_never_completed_fails_the_run_and_writes_nothing():
    partial = {**SUBMISSION, "dismissed": []}
    chat = ScriptChat([reply([call(SUBMIT, partial, i)]) for i in range(recommend.MAX_TOOL_CALLS + 6)])
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend(CONTEXT, FakeToolBox()))
    assert rec.result is None and rec.unanswered == ["netsuite_transactions:date_future"]
    assert "still unanswered" in rec.error

    class Fixed(FakeRecommender):
        async def recommend(self, context, toolbox):
            return rec

    wt = FakeWriteTools()
    state = run_graph(Fixed(), wt)
    assert state["exit_code"] == graph.EXIT_UNANSWERED and not wt.proposals and not wt.findings
    assert "Checklist incomplete:** 1 candidate(s) unanswered" in state["report"]
    assert "- netsuite_transactions:date_future" in state["report"]


def test_a_text_answer_must_also_complete_the_checklist():
    partial = {**SUBMISSION, "dismissed": []}
    chat = ScriptChat([reply(content=json.dumps(partial))] * 3)
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend(CONTEXT, FakeToolBox()))
    assert rec.result is None and rec.unanswered == ["netsuite_transactions:date_future"]


def test_the_user_prompt_lists_the_checklist():
    text = recommend.user_prompt(CONTEXT)
    assert "Candidate checklist (3; answer every one)" in text and "netsuite_transactions:date_future" in text
