"""Read-only knowledge tools (docs/phase7_rag_lineage_plan.md step 4; design section 4.4): get_impact, get_upstream,
explain_history, search_metadata. Like the other aidq_mcp tools they only read, refuse env="prod", return no data rows,
and are wrapped by tool_result (status + redaction).

* get_impact / get_upstream: the lineage graph (workspace.knowledge.edges) walked downstream / upstream
  (aidq_knowledge.graph.walk), every result with its path: a deterministic answer, never vector similarity.
* explain_history: proposals, applied changes and incidents for a table, column or rule, in time order (metadata).
* search_metadata: the docs vector index (workspace.knowledge.docs_index); every hit carries its document id and
  source (the citation); no hit above MIN_SCORE -> "no relevant documents" (the caller must not invent an answer).
"""

from __future__ import annotations

from aidq_knowledge.graph import walk

from . import config, logic

INDEX = "workspace.knowledge.docs_index"
MIN_SCORE = 0.55          # relevance floor of a search hit; calibrated by evals/run_rag_eval.py (no-answer questions)
MAX_K = 10
LAYER_PREFIXES = ("source:", "gold:", "bronze:", "silver:")


def resolve(obj: str, node_ids: set[str]) -> str:
    """A node id from what a person types: an id ("source:netsuite_transactions.type", "rule:4", "gold:...") or a
    bare name ("netsuite_transactions.type", "netsuite_transactions", "gold_customer_revenue.revenue")."""
    obj = (obj or "").strip()
    if obj in node_ids:
        return obj
    for prefix in LAYER_PREFIXES:
        if prefix + obj in node_ids:
            return prefix + obj
    raise config.ConfigError(f"unknown object {obj!r}: give a table, table.column, gold table/column or rule:<id>")


class KnowledgeTools:
    """Mixed into aidq_mcp.server.Tools (needs self.reads)."""

    def _graph(self, cfg) -> tuple[set[str], list[dict]]:
        edges = self.reads.sql(f"SELECT src, dst, type FROM {cfg.catalog}.knowledge.edges")
        nodes = {r["id"] for r in self.reads.sql(f"SELECT id FROM {cfg.catalog}.knowledge.nodes")}
        return nodes, edges

    def _walk(self, env: str, obj: str, forward: bool) -> dict:
        cfg = config.env_config(env)
        nodes, edges = self._graph(cfg)
        start = resolve(obj, nodes)
        found = walk(edges, start, forward=forward)
        by_type: dict[str, int] = {}
        for f in found:
            kind = f["node"].split(":", 1)[0]
            by_type[kind] = by_type.get(kind, 0) + 1
        return {"env": env, "object": start, "direction": "downstream" if forward else "upstream", "counts": by_type,
                "nodes": logic.bounded(found, config.HARD_LIMIT),
                "note": "from the lineage graph (metadata + gold SQL parsed with sqlglot); every node with its path"}

    def get_impact(self, env: str = "dev", object: str | None = None) -> dict:  # noqa: A002 -- the tool's argument name
        return self._walk(env, object, forward=True)

    def get_upstream(self, env: str = "dev", object: str | None = None) -> dict:  # noqa: A002
        return self._walk(env, object, forward=False)

    def explain_history(self, env: str = "dev", object: str | None = None) -> dict:  # noqa: A002
        cfg = config.env_config(env)
        obj = (object or "").strip()
        if obj.startswith("rule:"):
            rid = int(obj.split(":", 1)[1])
            rule = self.reads.pg_query(cfg.metadata_endpoint, "SELECT r.rule_id, r.rule_name, r.rule_expr, r.severity, "
                                       "r.is_active, r.proposal_id, d.source_table FROM aidq_metadata.data_quality_rules "
                                       "r JOIN aidq_metadata.source_table_def d USING (table_id) WHERE r.rule_id = %s",
                                       (rid,))
            if not rule:
                raise config.ConfigError(f"rule {rid} not found")
            table, column = rule[0]["source_table"], None
        else:
            name = obj.split(":", 1)[1] if ":" in obj else obj
            table, _, column = name.partition(".")
            column = column or None
            rule = []
        props = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT p.proposal_id, p.proposal_type, p.status, p.proposed_change, p.rationale, p.review_comment, "
            "p.created_at, p.reviewed_at FROM aidq_metadata.config_proposals p LEFT JOIN aidq_metadata.source_table_def d "
            "USING (table_id) WHERE (d.source_table = %s OR p.proposed_change->'table'->>'source_table' = %s) "
            "AND (%s::text IS NULL OR p.proposed_change->>'column_name' = %s OR p.proposed_change->>'rule_expr' LIKE %s) "
            "ORDER BY p.created_at", (table, table, column, column, f"%{column}%" if column else "%"))
        if rule and rule[0].get("proposal_id"):
            props = [p for p in props if p["proposal_id"] == rule[0]["proposal_id"]]
        incidents = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT i.incident_id, i.category, i.status, i.summary, i.detected_at, i.resolved_at FROM "
            "aidq_metadata.incidents i JOIN aidq_metadata.source_table_def d USING (table_id) WHERE d.source_table = %s "
            "ORDER BY i.detected_at", (table,))
        events = ([{"at": logic.truncate(str(p["created_at"]), 32), "kind": "proposal", "id": p["proposal_id"],
                    "what": f"{p['proposal_type']} {p['status']}", "change": p["proposed_change"],
                    "rationale": logic.truncate(p["rationale"]), "review": logic.truncate(p["review_comment"])}
                   for p in props]
                  + [{"at": logic.truncate(str(i["detected_at"]), 32), "kind": "incident", "id": i["incident_id"],
                      "what": f"{i['category']} {i['status']}", "summary": logic.truncate(i["summary"])}
                     for i in incidents])
        events.sort(key=lambda e: e["at"])
        return {"env": env, "object": obj, "rule": rule[0] if rule else None, "events": logic.bounded(events, config.HARD_LIMIT),
                "note": "proposals (with rationale and the reviewer's comment) and incidents, oldest first"}

    def search_metadata(self, env: str = "dev", question: str | None = None, k: int | None = None) -> dict:
        config.env_config(env)
        if not (question or "").strip():
            raise config.ConfigError("search_metadata needs a question")
        k = config.clamp("k", k, 5, MAX_K)
        res = self.reads.cli("vector-search-indexes", "query-index", INDEX,
                             body={"query_text": question, "num_results": k,
                                   "columns": ["id", "type", "object", "source", "text"]})
        cols = [c["name"] for c in (res.get("manifest") or {}).get("columns", [])]
        hits = []
        for row in (res.get("result") or {}).get("data_array") or []:
            h = dict(zip(cols, row))
            score = float(h.get("score") or 0)
            if score >= MIN_SCORE:
                hits.append({"id": h.get("id"), "type": h.get("type"), "object": h.get("object"),
                             "source": h.get("source"), "score": round(score, 3),
                             "excerpt": logic.truncate(h.get("text"), 600)})
        return {"env": env, "question": question, "hits": hits,
                "answer_rule": ("cite the hits' id/source; if there are no hits, say that nothing relevant was found"
                                if hits else "no relevant documents: do not answer from memory"),
                "min_score": MIN_SCORE}
