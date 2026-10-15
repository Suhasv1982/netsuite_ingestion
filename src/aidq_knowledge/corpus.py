"""The metadata corpus for vector search (docs/phase7_rag_lineage_plan.md step 2; design section 4.3). Pure builders.

One document per table (definition, columns, rules, load mode), column, DQ rule (with the rationale of the proposal
that created it), incident, applied change, and each repo doc (README, design docs, STATUS) chunked by heading.
Rows: id, type, object (the graph node id it describes, when there is one), text, source, updated_at. No environment
identifiers (aidq_mcp.logic.redact: hosts, URLs, emails, tokens) and no data rows: only metadata and prose.
"""

from __future__ import annotations

import json
import re

from aidq_mcp.logic import redact

MAX_CHUNK = 1800


_AGENT = re.compile(r"\b([a-z][a-z_]*)@(\d+(?:\.\d+)*(?:-[\w.]+)?)\b")


def agent_names(text: str) -> str:
    """Agent identities like dq_agent@0.1 read as e-mail addresses to the redaction: write them as 'dq_agent v0.1'."""
    return _AGENT.sub(r"\1 v\2", text)


def _doc(did: str, dtype: str, obj: str | None, text: str, source: str, updated_at=None) -> dict:
    return {"id": did, "type": dtype, "object": obj, "text": redact(agent_names(text.strip())), "source": source,
            "updated_at": None if updated_at is None else str(updated_at)}


def chunk_markdown(path: str, text: str, max_chars: int = MAX_CHUNK) -> list[tuple[str, str]]:
    """(heading, chunk) pairs: split at headings, then at paragraphs when a section is longer than max_chars."""
    sections, heading, buf = [], path, []
    for line in text.splitlines():
        if re.match(r"^#{1,4} ", line):
            if buf:
                sections.append((heading, "\n".join(buf)))
            heading, buf = line.lstrip("# ").strip(), [line]
        else:
            buf.append(line)
    if buf:
        sections.append((heading, "\n".join(buf)))
    out = []
    for h, body in sections:
        body = body.strip()
        if not body:
            continue
        while len(body) > max_chars:
            cut = body.rfind("\n\n", 0, max_chars)
            cut = cut if cut > max_chars // 3 else max_chars
            out.append((h, body[:cut].strip()))
            body = body[cut:].strip()
        if body:
            out.append((h, body))
    return out


def build(table_defs: list[dict], columns: list[dict], rules: list[dict], proposals: list[dict],
          incidents: list[dict], changes: list[dict], repo_docs: dict[str, str]) -> list[dict]:
    """All documents. Inputs as graph.build takes them, plus proposals with rationale / review_comment, incidents
    with summary, changes (config_change_log rows: change_id, table_name, operation, new_row, proposal_id,
    changed_at), and repo_docs (relative path -> markdown)."""
    docs = []
    cols_by_table: dict[str, list[dict]] = {}
    for c in columns:
        cols_by_table.setdefault(c["source_table"], []).append(c)
    rules_by_table: dict[str, list[dict]] = {}
    for r in rules:
        rules_by_table.setdefault(r["source_table"], []).append(r)
    rationale = {p["proposal_id"]: p for p in proposals}
    for d in table_defs:
        t = d["source_table"]
        active = [c for c in cols_by_table.get(t, []) if c.get("is_active", True) is not False]
        trs = rules_by_table.get(t, [])
        col_list = ", ".join("%s (%s)" % (c["column_name"], c.get("data_type")) for c in active)
        rule_list = "; ".join("%s [%s]: %s" % (r["rule_name"], r["severity"], r["rule_expr"]) for r in trs) or "none"
        silver = "has a silver table (merged on the business key)" if d.get("has_silver") else "no silver table"
        docs.append(_doc(f"table:{t}", "table", f"source:{t}",
                         f"Table {t}. Load mode {d.get('load_mode')}; business key {d.get('business_key')}; watermark "
                         f"{d.get('watermark_col') or 'none'}; {silver}. Active columns: {col_list}. DQ rules: {rule_list}.",
                         "metadata"))
        for c in cols_by_table.get(t, []):
            docs.append(_doc(f"column:{t}.{c['column_name']}", "column", f"source:{t}.{c['column_name']}",
                             f"Column {t}.{c['column_name']}, type {c.get('data_type')}, "
                             f"{'active' if c.get('is_active', True) is not False else 'inactive (deactivated)'}."
                             + (" It is the business key." if c["column_name"] == d.get("business_key") else "")
                             + (" It is the watermark column." if c["column_name"] == d.get("watermark_col") else ""),
                             "metadata"))
    for r in rules:
        p = rationale.get(r.get("proposal_id"))
        why = (f" It was proposed by {p.get('proposed_by')} (proposal {p['proposal_id']}): {p.get('rationale')}"
               + (f" Reviewer: {p.get('review_comment')}" if p.get("review_comment") else "")) if p else \
            " It was configured by hand (no proposal)."
        docs.append(_doc(f"rule:{r['rule_id']}", "dq_rule", f"rule:{r['rule_id']}",
                         f"DQ rule {r['rule_id']} '{r['rule_name']}' on {r['source_table']}: {r['rule_expr']}, "
                         f"severity {r['severity']}, {'active' if r.get('is_active') else 'inactive'}.{why}", "metadata"))
    for p in proposals:
        change = p.get("proposed_change") or {}
        docs.append(_doc(f"proposal:{p['proposal_id']}", "proposal", f"proposal:{p['proposal_id']}",
                         f"Proposal {p['proposal_id']} ({p['proposal_type']}) on {p.get('source_table')}, status "
                         f"{p['status']}: {json.dumps(change, default=str)}. Rationale: {p.get('rationale')}"
                         + (f" Review: {p.get('review_comment')}" if p.get("review_comment") else ""),
                         "metadata", p.get("created_at")))
    for i in incidents:
        docs.append(_doc(f"incident:{i['incident_id']}", "incident", f"incident:{i['incident_id']}",
                         f"Incident {i['incident_id']} ({i['category']}, {i['status']}) on "
                         f"{i.get('source_table') or 'the platform'}: {i.get('summary')}"
                         + (f" Suggested fix: {i.get('suggested_fix')}" if i.get("suggested_fix") else ""),
                         "metadata", i.get("detected_at")))
    for ch in changes:
        row = ch.get("new_row") or ch.get("old_row") or {}
        docs.append(_doc(f"change:{ch['change_id']}", "change", None,
                         f"Config change {ch['change_id']}: {ch['operation']} on {ch['table_name']}"
                         + (f" (proposal {ch['proposal_id']})" if ch.get("proposal_id") else "")
                         + ": " + json.dumps({k: row.get(k) for k in ("rule_name", "rule_expr", "severity", "column_name",
                                                                      "is_active", "source_table") if k in row}, default=str),
                         "metadata", ch.get("changed_at")))
    for path, text in sorted(repo_docs.items()):
        for n, (heading, body) in enumerate(chunk_markdown(path, text)):
            docs.append(_doc(f"doc:{path}#{n}", "repo_doc", None, f"{path} - {heading}\n{body}", path))
    return docs
