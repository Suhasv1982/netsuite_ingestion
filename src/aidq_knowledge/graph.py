"""The lineage graph (docs/phase7_rag_lineage_plan.md step 1; design section 4.2). Deterministic, pure builders.

Nodes (id, type, name, attrs): source/bronze/silver table and column, gold table and column, DQ rule, proposal,
incident. Edges (src, dst, type), always upstream -> downstream, so get_impact walks forward and get_upstream backward:

| type          | from -> to                                                   | built from                          |
|---------------|--------------------------------------------------------------|-------------------------------------|
| feeds         | source col -> bronze col -> silver col (tables likewise),    | metadata (bronze = active columns;  |
|               | silver table -> gold table                                   | silver 1:1 for merged tables)       |
| derived_from  | silver col -> gold col                                       | the gold SQL, sqlglot lineage       |
| reads         | silver col -> gold table (every column the query references, | the gold SQL (filters, joins,       |
|               | also in WHERE / JOIN / GROUP BY)                             | GROUP BY count too)                 |
| validated_by  | bronze col -> DQ rule (columns in the rule's expression)     | data_quality_rules (sqlglot)        |
| proposed_by   | proposal -> DQ rule / column it created or changed           | config_proposals (APPLIED)          |
| caused        | incident -> table it is about                                | incidents                           |

Node ids are stable strings: "source:netsuite_transactions.type", "bronze:...", "silver:...", "gold:gold_customer_status",
"gold:gold_customer_status.active_memberships", "rule:10", "proposal:20", "incident:6".
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage

LAYERS = ("source", "bronze", "silver")


@dataclass
class Graph:
    nodes: dict[str, dict] = field(default_factory=dict)
    edges: set[tuple[str, str, str]] = field(default_factory=set)

    def node(self, nid: str, ntype: str, name: str, **attrs) -> str:
        self.nodes.setdefault(nid, {"id": nid, "type": ntype, "name": name, "attrs": {}})["attrs"].update(attrs)
        return nid

    def edge(self, src: str, dst: str, etype: str) -> None:
        self.edges.add((src, dst, etype))

    def rows(self) -> tuple[list[dict], list[dict]]:
        nodes = [{**n, "attrs": json.dumps(n["attrs"], default=str, sort_keys=True)} for n in self.nodes.values()]
        edges = [{"src": s, "dst": d, "type": t} for s, d, t in sorted(self.edges)]
        return sorted(nodes, key=lambda n: n["id"]), edges


def column_refs(expr_sql: str) -> set[str]:
    """Column names referenced by a SQL boolean expression (a DQ rule)."""
    try:
        tree = sqlglot.parse_one(expr_sql, read="databricks")
    except sqlglot.errors.ParseError:
        return set()
    return {c.name for c in tree.find_all(exp.Column)}


def gold_lineage(sql: str, tables: dict[str, list[str]]) -> tuple[dict[str, set[tuple[str, str]]], set[tuple[str, str]]]:
    """(output column -> {(table, column)} it is derived from, {(table, column)} referenced anywhere in the query).
    `tables`: alias-free table name -> its columns (the schema sqlglot resolves against)."""
    schema = {t: {c: "STRING" for c in cols} for t, cols in tables.items()}
    tree = sqlglot.parse_one(sql, read="databricks")
    derived: dict[str, set[tuple[str, str]]] = {}
    for sel in tree.selects:
        out = sel.alias_or_name
        node = lineage(out, sql, schema=schema, dialect="databricks")
        derived[out] = {(n.source.name, n.name.split(".")[-1]) for n in node.walk()
                        if not n.downstream and isinstance(n.source, exp.Table) and n.source.name in tables}
    alias_to_table = {}
    for t in tree.find_all(exp.Table):
        if t.name in tables:
            alias_to_table[t.alias_or_name] = t.name
    reads = set()
    for c in tree.find_all(exp.Column):
        table = alias_to_table.get(c.table) if c.table else None
        if table and c.name in tables[table]:
            reads.add((table, c.name))
        elif not c.table:
            reads |= {(t, c.name) for t, cols in tables.items() if c.name in cols}
    return derived, reads


def build(table_defs: list[dict], columns: list[dict], rules: list[dict], proposals: list[dict], incidents: list[dict],
          gold: dict[str, tuple[str, tuple[str, ...]]]) -> Graph:
    """The whole graph from metadata rows and the gold queries.

    table_defs: source_table, business_key, watermark_col, load_mode, has_silver;  columns: source_table, column_name,
    data_type, is_active;  rules: rule_id, source_table, rule_name, rule_expr, severity, is_active, proposal_id;
    proposals: proposal_id, proposal_type, source_table, status, proposed_change;  incidents: incident_id,
    source_table, category, status;  gold: gold table -> (SQL with the silver table names as given, input tables)."""
    g = Graph()
    active: dict[str, list[str]] = {}
    for c in columns:
        if c.get("is_active", True) is not False:
            active.setdefault(c["source_table"], []).append(c["column_name"])
    defs = {d["source_table"]: d for d in table_defs}
    for t, d in defs.items():
        layers = ["source", "bronze"] + (["silver"] if d.get("has_silver") else [])
        for layer in layers:
            g.node(f"{layer}:{t}", f"{layer}_table", t, load_mode=d.get("load_mode"), business_key=d.get("business_key"),
                   watermark_col=d.get("watermark_col"))
        for a, b in zip(layers, layers[1:]):
            g.edge(f"{a}:{t}", f"{b}:{t}", "feeds")
        for col in active.get(t, []):
            for layer in layers:
                g.node(f"{layer}:{t}.{col}", f"{layer}_column", f"{t}.{col}", table=t)
                g.edge(f"{layer}:{t}", f"{layer}:{t}.{col}", "has_column")
            for a, b in zip(layers, layers[1:]):
                g.edge(f"{a}:{t}.{col}", f"{b}:{t}.{col}", "feeds")
    for gname, (sql, inputs) in gold.items():
        g.node(f"gold:{gname}", "gold_table", gname)
        for t in inputs:
            g.edge(f"silver:{t}", f"gold:{gname}", "feeds")
        derived, reads = gold_lineage(sql, {t: active.get(t, []) for t in inputs})
        for out, srcs in derived.items():
            g.node(f"gold:{gname}.{out}", "gold_column", f"{gname}.{out}", table=gname)
            g.edge(f"gold:{gname}", f"gold:{gname}.{out}", "has_column")
            for t, col in srcs:
                g.edge(f"silver:{t}.{col}", f"gold:{gname}.{out}", "derived_from")
        for t, col in reads:
            g.edge(f"silver:{t}.{col}", f"gold:{gname}", "reads")
    for r in rules:
        rid = g.node(f"rule:{r['rule_id']}", "dq_rule", r["rule_name"], table=r["source_table"], expr=r["rule_expr"],
                     severity=r["severity"], active=r.get("is_active"))
        for col in column_refs(r["rule_expr"]):
            if col in active.get(r["source_table"], []):
                g.edge(f"bronze:{r['source_table']}.{col}", rid, "validated_by")
        if r.get("proposal_id"):
            g.node(f"proposal:{r['proposal_id']}", "proposal", f"proposal {r['proposal_id']}")
            g.edge(f"proposal:{r['proposal_id']}", rid, "proposed_by")
    for p in proposals:
        pid = g.node(f"proposal:{p['proposal_id']}", "proposal", f"proposal {p['proposal_id']}",
                     proposal_type=p["proposal_type"], status=p["status"], table=p.get("source_table"))
        col = (p.get("proposed_change") or {}).get("column_name")
        if p["status"] == "APPLIED" and col and p.get("source_table"):
            g.edge(pid, f"source:{p['source_table']}.{col}", "proposed_by")
    for i in incidents:
        iid = g.node(f"incident:{i['incident_id']}", "incident", f"incident {i['incident_id']}",
                     category=i["category"], status=i["status"])
        if i.get("source_table"):
            g.edge(iid, f"source:{i['source_table']}", "caused")
    return g


FORWARD = ("feeds", "derived_from", "reads", "validated_by")
BACKWARD = FORWARD + ("has_column",)


def walk(edges: list[dict], start: str, forward: bool = True) -> list[dict]:
    """Every node reachable from `start` (downstream if forward, else upstream), with one shortest path each.

    Downstream never expands a table into all its columns (a column that only filters a gold table reaches that
    table, not every one of its columns); a table you start from is expanded into its own columns. Upstream walks
    from a column to its table too, so a gold column's upstream includes the columns its table filters and joins on.
    """
    types = FORWARD if forward else BACKWARD
    adj: dict[str, list[tuple[str, str]]] = {}
    for e in edges:
        if e["type"] not in types:
            continue
        a, b = (e["src"], e["dst"]) if forward else (e["dst"], e["src"])
        adj.setdefault(a, []).append((b, e["type"]))
    if forward:
        adj.setdefault(start, []).extend((e["dst"], "has_column") for e in edges
                                         if e["type"] == "has_column" and e["src"] == start)
    seen, queue, out = {start: [start]}, [start], []
    while queue:
        cur = queue.pop(0)
        for nxt, etype in sorted(adj.get(cur, [])):
            if nxt not in seen:
                seen[nxt] = seen[cur] + [f"-{etype}->", nxt]
                queue.append(nxt)
                out.append({"node": nxt, "via": etype, "path": " ".join(seen[nxt])})
    return out
