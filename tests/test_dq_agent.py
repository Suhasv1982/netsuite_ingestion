"""aidq_dq_agent: the model step (schema, loop, routing) and the graph (collect -> recommend -> submit -> report),
with fakes only: no model, no database."""

import asyncio
import json

from aidq_dq_agent import graph, recommend
from aidq_dq_agent.recommend import SUBMIT, SUBMIT_SCHEMA, DatabricksRecommender, Recommendation, schema_problems
from aidq_llm.router import Router

PROPOSAL = {"table": "netsuite_transaction_lines", "rule_name": "Amount Not Negative",
            "rule_expr": "amount IS NULL OR amount >= 0", "severity": "SOFT", "defect_type": "negative_amount",
            "rationale": "300 of 93,828 bronze rows (0.32%) have a negative amount; min -4500. SOFT: refunds may exist.",
            "evidence": {"violations": 300, "rows": 93828, "profile_fields": ["columns.amount.negative"]},
            "confidence": 0.8}
FINDING = {"table": "netsuite_transactions", "finding_type": "duplicate_business_key",
           "summary": "12 keys have conflicting created_date on the same day.",
           "evidence": {"keys_with_conflicting_created_date_same_day": 12}}
SUBMISSION = {"proposals": [PROPOSAL], "findings": [FINDING],
              "not_recommended": [{"table": "netsuite_memberships", "what": "same-day versions",
                                   "reason": "normal in bronze"}]}
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
        return {"status": "ok", "tool": name, **({"table": args["table"]} if "table" in args else {})}


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
    for part in ("submission.not_recommended is required", "severity must be one of", "confidence must be <= 1",
                 "extra is not allowed", "violations must be an integer"):
        assert any(part in p for p in problems), part
    no_type = {k: v for k, v in PROPOSAL.items() if k != "defect_type"}
    assert any("defect_type is required" in p for p in schema_problems({**SUBMISSION, "proposals": [no_type]}, SUBMIT_SCHEMA))
    assert schema_problems({"proposals": [], "findings": [], "not_recommended": []}, SUBMIT_SCHEMA) == []


def test_booleans_are_not_numbers():
    assert schema_problems(True, {"type": "integer"}) == ["submission must be an integer"]


def test_prompt_is_versioned_and_states_the_core_rules():
    text = recommend.system_prompt()
    for part in ("TRUE for a valid row", "IS NULL OR amount >= 0", "SOFT", "DQ_NOT_EXPRESSIBLE",
                 "info_same_day_versions", "_row_hash", SUBMIT):
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
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend({"today": "2026-10-14", "tables": TABLES}, tb))
    assert rec.result == SUBMISSION and [c["tool"] for c in rec.tool_calls] == ["profile_table"]
    assert rec.usage == {"input_tokens": 20, "output_tokens": 10}
    # the model saw the submit tool, never a write tool
    names = {t["function"]["name"] for t in recommend.openai_tools(asyncio.run(tb.list_tools()))}
    assert names == {"profile_table", SUBMIT}


def test_an_invalid_submission_is_sent_back_and_a_corrected_one_accepted():
    bad = {**SUBMISSION, "proposals": [{**PROPOSAL, "severity": "MEDIUM"}]}
    chat = ScriptChat([reply([call(SUBMIT, bad, 1)]), reply([call(SUBMIT, SUBMISSION, 2)])])
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend({"tables": TABLES}, FakeToolBox()))
    assert rec.result == SUBMISSION
    sent_back = chat.messages[1][-1]
    assert sent_back["role"] == "tool" and "severity must be one of" in sent_back["content"]


def test_tool_budget_is_enforced():
    many = [reply([call("profile_table", {"env": "dev", "table": "netsuite_transactions"}, i)])
            for i in range(recommend.MAX_TOOL_CALLS + 1)]
    chat = ScriptChat(many + [reply([call(SUBMIT, SUBMISSION, 99)])])
    tb = FakeToolBox()
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend({"tables": TABLES}, tb))
    assert rec.result == SUBMISSION and len(tb.calls) == recommend.MAX_TOOL_CALLS


def test_a_json_answer_in_text_is_accepted_and_prose_is_nudged_then_given_up():
    chat = ScriptChat([reply(content="```json\n" + json.dumps(SUBMISSION) + "\n```")])
    assert asyncio.run(DatabricksRecommender(chat, "m").recommend({}, FakeToolBox())).result == SUBMISSION
    chat = ScriptChat([reply(content="I think amounts look fine.")] * 3)
    rec = asyncio.run(DatabricksRecommender(chat, "m").recommend({}, FakeToolBox()))
    assert rec.result is None and rec.error.startswith(f"no {SUBMIT} call")


def test_the_router_escalates_once_when_the_primary_gives_no_valid_submission():
    routes = {"tasks": {"rule_generation": {"primary": "big", "fallback": "other", "max_tokens": 100}}}
    chat = ScriptChat([RuntimeError("HTTP 503: down"), reply([call(SUBMIT, SUBMISSION, 1)])])
    router = Router(chat, "aidq_dq_recommender", routes=routes, trace_id="t-1")
    rec = asyncio.run(recommend.RoutedRecommender(router).recommend({}, FakeToolBox()))
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
    assert "defect_type" not in p and p["evidence"]["defect_type"] == "negative_amount"
    assert p["evidence"]["prompt_version"] == recommend.PROMPT_VERSION and p["evidence"]["violations"] == 300
    assert wt.findings == [FINDING]
    assert state["exit_code"] == 0
    report = state["report"]
    for part in ("submitted (proposal 41)", "`amount IS NULL OR amount >= 0`", "skipped: an OPEN incident",
                 "## Not recommended", "same-day versions (normal in bronze)", "trace t-1"):
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
    state = run_graph(FakeRecommender(result={"proposals": [], "findings": [], "not_recommended": []}), FakeWriteTools())
    assert state["exit_code"] == 0 and "Nothing to recommend." in state["report"]
