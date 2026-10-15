"""RAG + lineage (plan #70): the graph and its walks, the corpus, and the read-only knowledge tools (fakes only)."""

import pathlib
import sys

import pytest
import yaml

from aidq_knowledge import corpus, graph
from aidq_mcp import config
from aidq_mcp.knowledge_tools import MIN_SCORE, resolve
from aidq_mcp.server import Tools

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "netsuite_ingestion"))
import gold_sql as gs  # noqa: E402

DEFS = [{"source_table": "netsuite_transactions", "business_key": "transaction_internal_id", "watermark_col": "updated_date",
         "load_mode": "Incremental", "has_silver": True},
        {"source_table": "netsuite_transaction_lines", "business_key": "transaction_line_id", "watermark_col": "updated_date",
         "load_mode": "Incremental", "has_silver": True},
        {"source_table": "netsuite_customers", "business_key": "customer_internal_id", "watermark_col": None,
         "load_mode": "FullLoad", "has_silver": False}]
COLS = ([{"source_table": "netsuite_transactions", "column_name": c, "data_type": "TEXT", "is_active": True}
         for c in ("transaction_internal_id", "customer_internal_id", "date", "type", "updated_date")]
        + [{"source_table": "netsuite_transaction_lines", "column_name": c, "data_type": "TEXT", "is_active": True}
           for c in ("transaction_line_id", "transaction_internal_id", "amount", "quantity", "updated_date")]
        + [{"source_table": "netsuite_customers", "column_name": c, "data_type": "TEXT", "is_active": True}
           for c in ("customer_internal_id", "email")])
RULES = [{"rule_id": 4, "source_table": "netsuite_transaction_lines", "rule_name": "Amount Equals Qty x Rate",
          "rule_expr": "amount = quantity * rate", "severity": "SOFT", "is_active": True, "proposal_id": None},
         {"rule_id": 10, "source_table": "netsuite_transactions", "rule_name": "Transaction Type Valid",
          "rule_expr": "type IN ('Cash Sale','Invoice')", "severity": "SOFT", "is_active": True, "proposal_id": 20}]
GOLD = {gs.CUSTOMER_REVENUE: (gs.customer_revenue_sql(*gs.REVENUE_INPUTS), gs.REVENUE_INPUTS)}


@pytest.fixture(scope="module")
def edges():
    return graph.build(DEFS, COLS, RULES, [], [{"incident_id": 6, "source_table": "netsuite_transactions",
                                                  "category": "CREDENTIAL_EXPIRED", "status": "RESOLVED"}], GOLD).rows()[1]


def downstream(edges, start):
    return {x["node"] for x in graph.walk(edges, start)}


def test_a_derived_column_reaches_exactly_its_gold_column_and_rule(edges):
    got = downstream(edges, "source:netsuite_transaction_lines.amount")
    assert {"rule:4", "gold:gold_customer_revenue", "gold:gold_customer_revenue.revenue"} <= got
    assert not any(n.startswith("gold:gold_customer_revenue.") and n != "gold:gold_customer_revenue.revenue" for n in got)


def test_a_filter_or_join_column_reaches_the_gold_table_not_every_gold_column(edges):
    got = downstream(edges, "source:netsuite_transactions.customer_internal_id")
    assert "gold:gold_customer_revenue" in got and "gold:gold_customer_revenue.customer_internal_id" in got
    assert "gold:gold_customer_revenue.revenue" not in got


def test_a_full_load_table_without_silver_has_no_gold_impact(edges):
    assert downstream(edges, "source:netsuite_customers.email") == {"bronze:netsuite_customers.email"}


def test_upstream_of_a_gold_column_includes_its_filters_and_joins(edges):
    up = {x["node"] for x in graph.walk(edges, "gold:gold_customer_revenue.revenue", forward=False)}
    assert {"source:netsuite_transaction_lines.amount", "source:netsuite_transactions.date",
            "source:netsuite_transactions.customer_internal_id"} <= up


def test_every_walk_result_has_a_path(edges):
    for x in graph.walk(edges, "source:netsuite_transactions"):
        assert x["path"].startswith("source:netsuite_transactions") and x["path"].endswith(x["node"])


def test_rules_link_to_their_proposals_and_incidents_to_tables(edges):
    assert {"src": "proposal:20", "dst": "rule:10", "type": "proposed_by"} in edges
    assert {"src": "incident:6", "dst": "source:netsuite_transactions", "type": "caused"} in edges


def test_corpus_has_one_doc_per_object_and_no_identifiers():
    docs = corpus.build(DEFS, COLS, RULES, [{"proposal_id": 20, "proposal_type": "ADD_DQ_RULE", "status": "APPLIED",
                                             "source_table": "netsuite_transactions", "proposed_change": {"rule_expr": "x"},
                                             "rationale": "366 rows, see https://host/x by a@b.com", "review_comment": None,
                                             "proposed_by": "dq_agent@0.1", "created_at": "2026-10-14"}],
                        [], [], {"docs/x.md": "# Title\n\nSome text.\n\n## Part\n\nMore."})
    ids = {d["id"] for d in docs}
    assert {"table:netsuite_transactions", "column:netsuite_customers.email", "rule:10", "proposal:20",
            "doc:docs/x.md#0", "doc:docs/x.md#1"} <= ids
    rule10 = next(d for d in docs if d["id"] == "rule:10")
    assert "proposed by dq_agent v0.1 (proposal 20)" in rule10["text"]          # an agent name, not an e-mail
    assert all("https://" not in d["text"] and "a@b.com" not in d["text"] for d in docs)


def test_chunks_stay_under_the_limit():
    text = "# H\n\n" + "\n\n".join("p" * 500 for _ in range(10))
    assert all(len(b) <= corpus.MAX_CHUNK for _, b in corpus.chunk_markdown("x.md", text))


def test_resolve_accepts_ids_and_bare_names():
    ids = {"source:netsuite_transactions.type", "gold:gold_customer_revenue.revenue", "rule:4"}
    assert resolve("netsuite_transactions.type", ids) == "source:netsuite_transactions.type"
    assert resolve("gold_customer_revenue.revenue", ids) == "gold:gold_customer_revenue.revenue"
    assert resolve("rule:4", ids) == "rule:4"
    with pytest.raises(config.ConfigError, match="unknown object"):
        resolve("nope", ids)


class FakeReads:
    def __init__(self, edges, search_rows=()):
        self.edges, self.search_rows = edges, search_rows

    def sql(self, statement):
        if "knowledge.edges" in statement:
            return self.edges
        if "knowledge.nodes" in statement:
            return [{"id": n} for e in self.edges for n in (e["src"], e["dst"])]
        raise AssertionError(statement)

    def cli(self, *args, body=None):
        assert args[:2] == ("api", "post") and args[2] == "/api/2.0/vector-search/indexes/workspace.knowledge.docs_index/query" and body["num_results"] <= 10
        return {"manifest": {"columns": [{"name": c} for c in ("id", "type", "object", "source", "text", "score")]},
                "result": {"data_array": list(self.search_rows)}}


def test_impact_tool_and_prod_refused(edges):
    t = Tools(FakeReads(edges))
    out = t.get_impact("dev", "netsuite_transaction_lines.amount")
    assert out["status"] == "ok" and out["object"] == "source:netsuite_transaction_lines.amount"
    assert {"rule:4", "gold:gold_customer_revenue.revenue"} <= {n["node"] for n in out["nodes"]["items"]}
    assert t.get_impact("prod", "x")["status"] == "error"
    assert t.get_upstream("dev", "nope")["status"] == "error"


def test_search_keeps_only_relevant_hits_with_citations(edges):
    rows = [["rule:10", "dq_rule", "rule:10", "metadata", "DQ rule 10 ...", MIN_SCORE + 0.2],
            ["doc:README.md#3", "repo_doc", None, "README.md", "unrelated", MIN_SCORE - 0.2]]
    out = Tools(FakeReads(edges, rows)).search_metadata("dev", "why was the type rule added?", 5)
    assert [h["id"] for h in out["hits"]] == ["rule:10"] and out["hits"][0]["source"] == "metadata"
    none = Tools(FakeReads(edges, rows[1:])).search_metadata("dev", "what is the capital of France?")
    assert none["hits"] == [] and "do not answer from memory" in none["answer_rule"]


def test_the_refresh_job_runs_as_the_deployer_daily_before_drift():
    j = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))["targets"]["dev"]["resources"]["jobs"]
    k = j["knowledge_refresh"]
    assert "run_as" not in k and k["schedule"]["quartz_cron_expression"] == "0 15 7 * * ?"
    assert k["tasks"][0]["spark_python_task"]["python_file"] == "src/knowledge_refresh_job.py"
