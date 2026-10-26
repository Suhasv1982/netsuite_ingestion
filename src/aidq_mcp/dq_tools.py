"""Read-only tools for the DQ rule recommender (docs/dq_recommender_design.md v2 section 4; plan #39 step 2).

get_dq_rules, get_open_and_rejected_proposals, profile_table, get_rejects_summary. Like the monitor's tools they
only read (read-only Postgres sessions, fixed SQL templates over allowlisted tables and metadata-listed columns),
never return raw rows (aggregates; for low-cardinality columns, top values; for a not-expressible finding, counts by
snapshot date and up to DETAIL_EXAMPLE_KEYS business-key values), and refuse env="prod".
"""

from __future__ import annotations

import re

from . import config, logic

TOP_VALUES_MAX_DISTINCT = 20
TOP_VALUES_LIMIT = 20         # = TOP_VALUES_MAX_DISTINCT: a low-cardinality column's values are all listed
RARE_SHARE = 0.01             # a value (or NULL) on fewer than 1% of rows of a column that has it on some rows
LATE_LOAD_HOURS = 12          # a new key loaded this long after its snapshot date was first loaded arrived late
EMAIL_PATTERN = "^[^@ ]+@[^@ ]+[.][A-Za-z]+$"   # one @, no spaces, a domain ending in .letters (no escapes in SQL)
METADATA_COLS = ("_row_hash", "_snapshot_date", "_loaded_at")
DETAIL_MAX_DATES = 31         # a finding's counts by snapshot date: at most this many dates (the earliest)
DETAIL_EXAMPLE_KEYS = 5       # and this many example business keys (key values only, never other columns)
DATE_TYPES = ("date", "timestamp")
NUMERIC_TYPES = ("int", "integer", "bigint", "smallint", "numeric", "decimal", "double", "float", "real")


def bq(name: str) -> str:
    """Backtick-quote a Spark identifier (e.g. `date`, a reserved word, is a real column)."""
    return "`" + name.replace("`", "``") + "`"


def sql_lit(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def kind_of(data_type: str) -> str:
    t = (data_type or "").lower()
    if any(t.startswith(d) for d in DATE_TYPES):
        return "date"
    if any(t.startswith(n) for n in NUMERIC_TYPES):
        return "number"
    return "text"


def date_pairs(columns: list[str]) -> list[tuple[str, str]]:
    """(start, end) column pairs by name: x_start_date/x_end_date, start_date/end_date, ..."""
    out = []
    for c in columns:
        if "start" in c:
            end = c.replace("start", "end")
            if end in columns:
                out.append((c, end))
    return out


def profile_sql(relation: str, cols: list[dict], key: str | None) -> str:
    """One aggregate query: row count, per column nulls / distinct / min / max, and the cross-column candidates."""
    names = [c["column_name"] for c in cols]
    parts = ["count(*) AS `rows`"]
    for c in cols:
        n = bq(c["column_name"])
        parts += [f"count_if({n} IS NULL) AS {bq(c['column_name'] + '__nulls')}",
                  f"approx_count_distinct({n}) AS {bq(c['column_name'] + '__distinct')}",
                  f"cast(min({n}) AS string) AS {bq(c['column_name'] + '__min')}",
                  f"cast(max({n}) AS string) AS {bq(c['column_name'] + '__max')}"]
        if kind_of(c.get("data_type")) == "date":
            parts.append(f"count_if({n} > current_date()) AS {bq(c['column_name'] + '__future')}")
        if kind_of(c.get("data_type")) == "number":
            parts.append(f"count_if({n} < 0) AS {bq(c['column_name'] + '__negative')}")
        if kind_of(c.get("data_type")) == "text" and "email" in c["column_name"].lower():
            parts.append(f"count_if({n} IS NOT NULL AND NOT {n} rlike '{EMAIL_PATTERN}') "
                         f"AS {bq(c['column_name'] + '__bad_format')}")
    for s, e in date_pairs(names):
        parts.append(f"count_if({bq(e)} < {bq(s)}) AS {bq('check__' + e + '_before_' + s)}")
    if {"amount", "quantity", "rate"} <= set(names):
        parts.append("count_if(`amount` <> `quantity` * `rate`) AS `check__amount_ne_quantity_x_rate`")
    if key:
        parts.append(f"count_if({bq(key)} IS NULL) AS {bq('check__null_business_key')}")
    return f"SELECT {', '.join(parts)} FROM {relation}"


def duplicates_sql(relation: str, key: str) -> str:
    """Same key, same snapshot day: with different created_date (a duplicate) vs only different content (a same-day
    version, normal since bronze keys rows by content)."""
    return (f"SELECT count_if(created > 1) AS keys_with_conflicting_created_date, "
            f"count_if(created = 1 AND versions > 1) AS keys_with_same_day_versions FROM ("
            f"SELECT {bq(key)}, _snapshot_date, count(DISTINCT created_date) AS created, "
            f"count(DISTINCT _row_hash) AS versions FROM {relation} WHERE {bq(key)} IS NOT NULL "
            f"GROUP BY {bq(key)}, _snapshot_date)")


def late_rows_sql(relation: str, key: str) -> str:
    """New keys (first bronze version) loaded more than LATE_LOAD_HOURS after their snapshot date was first loaded:
    rows that arrived after their day was loaded (bronze's top-up flows). A bronze rebuild reloads every day at
    once, so late rows from before the last rebuild are not visible here."""
    return (f"WITH b AS (SELECT {bq(key)} AS k, _snapshot_date AS d, _loaded_at AS t FROM {relation} "
            f"WHERE {bq(key)} IS NOT NULL), first_load AS (SELECT d, min(t) AS ft FROM b GROUP BY d), "
            f"first_seen AS (SELECT k, min(d) AS fd FROM b GROUP BY k) "
            f"SELECT count(*) AS late_rows, count(DISTINCT b.d) AS late_dates FROM b JOIN first_load f ON b.d = f.d "
            f"JOIN first_seen s ON b.k = s.k WHERE b.d = s.fd AND b.t > f.ft + INTERVAL {LATE_LOAD_HOURS} HOURS")


def reference_sql(relation: str, column: str, target: str, target_key: str) -> str:
    return (f"SELECT count(*) AS n FROM {relation} s WHERE s.{bq(column)} IS NOT NULL AND NOT EXISTS ("
            f"SELECT 1 FROM {target} t WHERE t.{bq(target_key)} = s.{bq(column)})")


# -- finding detail: counts by snapshot date and a few example keys, for the incident's evidence (owner, 2026-10-26)

def _detail_sql(inner: str) -> str:
    """`inner` yields (k, d), one row per affected row; per date: the count and up to DETAIL_EXAMPLE_KEYS keys."""
    return (f"SELECT cast(d AS string) AS d, count(*) AS n, "
            f"slice(array_sort(collect_set(cast(k AS string))), 1, {DETAIL_EXAMPLE_KEYS}) AS examples "
            f"FROM ({inner}) GROUP BY d ORDER BY d")


def duplicates_detail_sql(relation: str, key: str) -> str:
    """The keys counted by duplicates_sql's keys_with_conflicting_created_date, per snapshot date."""
    return _detail_sql(f"SELECT {bq(key)} AS k, _snapshot_date AS d FROM {relation} WHERE {bq(key)} IS NOT NULL "
                       f"GROUP BY {bq(key)}, _snapshot_date HAVING count(DISTINCT created_date) > 1")


def late_rows_detail_sql(relation: str, key: str) -> str:
    """The rows counted by late_rows_sql, per snapshot date."""
    return _detail_sql(
        f"WITH b AS (SELECT {bq(key)} AS k, _snapshot_date AS d, _loaded_at AS t FROM {relation} "
        f"WHERE {bq(key)} IS NOT NULL), first_load AS (SELECT d, min(t) AS ft FROM b GROUP BY d), "
        f"first_seen AS (SELECT k, min(d) AS fd FROM b GROUP BY k) "
        f"SELECT b.k, b.d FROM b JOIN first_load f ON b.d = f.d JOIN first_seen s ON b.k = s.k "
        f"WHERE b.d = s.fd AND b.t > f.ft + INTERVAL {LATE_LOAD_HOURS} HOURS")


def reference_detail_sql(relation: str, row_key: str, column: str, target: str, target_key: str) -> str:
    """The rows counted by reference_sql, per snapshot date; the examples are the referencing rows' own keys."""
    return _detail_sql(f"SELECT s.{bq(row_key)} AS k, s._snapshot_date AS d FROM {relation} s "
                       f"WHERE s.{bq(column)} IS NOT NULL AND NOT EXISTS ("
                       f"SELECT 1 FROM {target} t WHERE t.{bq(target_key)} = s.{bq(column)})")


def shape_detail(rows: list[dict]) -> dict:
    """{counts_by_date: [{date, count}] (earliest DETAIL_MAX_DATES), dates, example_keys (earliest dates first)}."""
    rows = sorted(rows, key=lambda r: str(r["d"]))
    examples: list[str] = []
    for r in rows:
        for k in r.get("examples") or []:
            if len(examples) < DETAIL_EXAMPLE_KEYS and k not in examples:
                examples.append(str(k))
    out = {"counts_by_date": [{"date": str(r["d"]), "count": int(r["n"])} for r in rows[:DETAIL_MAX_DATES]],
           "dates": len(rows), "example_keys": examples}
    if len(rows) > DETAIL_MAX_DATES:
        out["dates_omitted"] = len(rows) - DETAIL_MAX_DATES
    return out


def top_values_sql(relation: str, column: str) -> str:
    return (f"SELECT cast({bq(column)} AS string) AS v, count(*) AS n FROM {relation} GROUP BY 1 "
            f"ORDER BY n DESC LIMIT {TOP_VALUES_LIMIT}")


_SAFE_VALUE = re.compile(r"^[\w .:/+-]{0,40}$")


def shape_profile(agg: dict, cols: list[dict], key: str | None) -> dict:
    rows = int(agg.get("rows") or 0)
    out_cols = []
    for c in cols:
        name = c["column_name"]
        nulls = int(agg.get(f"{name}__nulls") or 0)
        entry = {"column": name, "type": c.get("data_type"), "null_rate": round(nulls / rows, 4) if rows else None,
                 "distinct": int(agg.get(f"{name}__distinct") or 0),
                 "min": logic.truncate(agg.get(f"{name}__min"), 40), "max": logic.truncate(agg.get(f"{name}__max"), 40)}
        for extra in ("future", "negative", "bad_format"):
            if f"{name}__{extra}" in agg:
                entry[f"{extra}_count"] = int(agg[f"{name}__{extra}"] or 0)
        if "bad_format_count" in entry and rows:
            entry["valid_format_rate"] = round(1 - (entry["bad_format_count"] + nulls) / rows, 4) if rows > nulls else None
        out_cols.append(entry)
    checks = {k.removeprefix("check__"): int(v or 0) for k, v in agg.items() if k.startswith("check__")}
    return {"rows": rows, "business_key": key, "columns": out_cols, "candidate_checks": checks}


def rare_values(top: list[dict], rows: int) -> list[dict]:
    """Values of a low-cardinality column that look wrong by shape alone: blank, a case or whitespace variant of a
    more frequent value, or on fewer than RARE_SHARE of the rows. Which values are valid is the model's call."""
    out = []
    norm_first: dict[str, str] = {}
    for t in sorted(top, key=lambda t: -t["count"]):
        v = t["value"]
        if v is None or v == "<masked>":
            continue
        norm = str(v).strip().lower()
        why = ("blank" if not norm else f"variant of {norm_first[norm]!r}" if norm in norm_first
               else f"under {RARE_SHARE:.0%} of rows" if rows and t["count"] / rows < RARE_SHARE else None)
        norm_first.setdefault(norm, v)
        if why:
            out.append({"value": v, "count": t["count"], "why": why})
    return out


def candidates(table: str, profile: dict) -> list[dict]:
    """The checklist the model must answer: every check with violations > 0, as (id, check, count, rate, kind).
    kind row_rule: one row shows it; not_expressible: needs other rows, another table or history."""
    rows = profile.get("rows") or 0
    out = []

    def add(check, count, kind, columns, **extra):
        if count and count > 0:
            out.append({"id": f"{table}:{check}", "check": check, "count": int(count),
                        "rate": round(count / rows, 6) if rows else None, "kind": kind, "columns": columns, **extra})

    key = profile.get("business_key")
    detail = profile.get("finding_detail") or {}

    def with_detail(check):
        return {"detail": detail[check]} if check in detail else {}

    for name, n in (profile.get("candidate_checks") or {}).items():
        if name == "keys_with_conflicting_created_date_same_day":
            add("duplicate_business_key", n, "not_expressible", [key], **with_detail("duplicate_business_key"))
        elif name == "null_business_key":
            add(name, n, "row_rule", [key])
        else:
            add(name, n, "row_rule", [c for c in (name.replace("_before_", " ").replace("_ne_", " ").split())
                                      if c in {x["column"] for x in profile.get("columns", [])}])
    for c in profile.get("columns", []):
        col = c["column"]
        add(f"{col}_future", c.get("future_count"), "row_rule", [col])
        add(f"{col}_negative", c.get("negative_count"), "row_rule", [col])
        add(f"{col}_bad_format", c.get("bad_format_count"), "row_rule", [col], valid_format_rate=c.get("valid_format_rate"))
        rare = c.get("rare_values") or []
        add(f"{col}_rare_values", sum(r["count"] for r in rare), "row_rule", [col], values=rare)
        rate = c.get("null_rate")
        if col != key and rate and rate < RARE_SHARE:
            add(f"{col}_rare_nulls", round(rate * rows), "row_rule", [col])
    for ref, n in (profile.get("references_without_target") or {}).items():
        check = f"orphan:{ref.split(' ')[0]}"
        add(check, n, "not_expressible", [ref.split(" ")[0]], reference=ref, **with_detail(check))
    add("late_rows", (profile.get("late_arrivals") or {}).get("late_rows"), "not_expressible", [key],
        **with_detail("late_rows"))
    return out


def safe_top_values(rows: list[dict]) -> list[dict]:
    """Top values of a low-cardinality column; values that look free-form (long or odd characters) are masked."""
    return [{"value": r["v"] if r["v"] is None or _SAFE_VALUE.match(str(r["v"])) else "<masked>",
             "count": int(r["n"])} for r in rows]


class DqTools:
    """Mixed into aidq_mcp.server.Tools (needs self.reads and self.now)."""

    def _table_meta(self, cfg, table: str) -> tuple[dict, list[dict]]:
        defs = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT table_id, source_table, business_key, watermark_col, load_mode, discard_threshold "
            "FROM aidq_metadata.source_table_def WHERE source_table = %s", (table,))
        if not defs:
            raise config.ConfigError(f"table {table!r} is not in source_table_def")
        cols = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT column_name, data_type, ordinal FROM aidq_metadata.source_columns "
            "WHERE table_id = %s AND coalesce(is_active, true) ORDER BY ordinal NULLS LAST", (defs[0]["table_id"],))
        return defs[0], cols

    def get_dq_rules(self, env: str = "dev", table: str | None = None) -> dict:
        cfg = config.env_config(env)
        tables = list(config.check_table(cfg, table))
        rows = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT d.source_table, r.rule_id, r.rule_name, r.rule_expr, r.severity, r.is_active, r.created_by, "
            "r.proposal_id FROM aidq_metadata.data_quality_rules r JOIN aidq_metadata.source_table_def d "
            "USING (table_id) WHERE d.source_table = ANY(%s) ORDER BY d.source_table, r.rule_id", (tables,))
        return {"env": env, "rules": logic.bounded(rows, config.HARD_LIMIT)}

    def get_open_and_rejected_proposals(self, env: str = "dev", table: str | None = None, days: int | None = None) -> dict:
        cfg = config.env_config(env)
        tables = list(config.check_table(cfg, table))
        days = config.clamp("days", days, 30, 90)
        rows = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT p.proposal_id, d.source_table, p.proposal_type, p.proposed_change, p.status, p.confidence, "
            "p.proposed_by, p.created_at, p.reviewed_at, p.review_comment, p.approved_severity, "
            "p.validation_result->>'summary' AS validation_summary "
            "FROM aidq_metadata.config_proposals p JOIN aidq_metadata.source_table_def d USING (table_id) "
            "WHERE d.source_table = ANY(%s) AND (p.status IN ('PENDING', 'VALIDATED', 'VALIDATION_FAILED') "
            "OR (p.status = 'REJECTED' AND p.reviewed_at >= now() - make_interval(days => %s))) "
            "ORDER BY p.created_at DESC", (tables, days))
        shaped = [{**r, "created_at": str(r["created_at"]), "reviewed_at": r["reviewed_at"] and str(r["reviewed_at"]),
                   "confidence": None if r["confidence"] is None else float(r["confidence"]),
                   "review_comment": logic.truncate(r["review_comment"])} for r in rows]
        return {"env": env, "days": days, "proposals": logic.bounded(shaped, config.HARD_LIMIT),
                "note": "open = PENDING / VALIDATED / VALIDATION_FAILED; REJECTED only within `days`, with the "
                        "reviewer's reason in review_comment"}

    def profile_table(self, env: str = "dev", table: str | None = None) -> dict:
        cfg = config.env_config(env)
        if table is None:
            raise config.ConfigError("profile_table needs a table")
        config.check_table(cfg, table)
        tdef, cols = self._table_meta(cfg, table)
        cols = [c for c in cols if c["column_name"] not in METADATA_COLS]
        key = (tdef.get("business_key") or "").strip() or None
        relation = f"{cfg.catalog}.poc_bronze.{table}"
        [agg] = self.reads.sql(profile_sql(relation, cols, key))
        out = shape_profile(agg, cols, key)
        for c in out["columns"]:
            if 0 < c["distinct"] <= TOP_VALUES_MAX_DISTINCT:
                c["top_values"] = safe_top_values(self.reads.sql(top_values_sql(relation, c["column"])))
                if kind_of(c.get("type")) == "text":
                    c["rare_values"] = rare_values(c["top_values"], out["rows"])
        incremental = (tdef.get("load_mode") or "").lower() == "incremental"
        detail = {}   # finding check -> counts by date + example keys; queried only when the finding exists
        if incremental and key and any(c["column_name"] == "created_date" for c in cols):
            [d] = self.reads.sql(duplicates_sql(relation, key))
            out["candidate_checks"]["keys_with_conflicting_created_date_same_day"] = int(d["keys_with_conflicting_created_date"] or 0)
            out["info_same_day_versions"] = int(d["keys_with_same_day_versions"] or 0)
            if out["candidate_checks"]["keys_with_conflicting_created_date_same_day"]:
                detail["duplicate_business_key"] = shape_detail(self.reads.sql(duplicates_detail_sql(relation, key)))
        refs = {}
        names = [c["column_name"] for c in cols]
        for other, other_key in {**cfg.business_keys, **{t: "customer_internal_id" for t in cfg.full_load_tables}}.items():
            if other != table and other_key in names and other_key != key:
                target = f"{cfg.catalog}.poc_bronze.{other}"
                [r] = self.reads.sql(reference_sql(relation, other_key, target, other_key))
                refs[f"{other_key} -> {other}"] = int(r["n"] or 0)
                if refs[f"{other_key} -> {other}"] and key:
                    detail[f"orphan:{other_key}"] = shape_detail(
                        self.reads.sql(reference_detail_sql(relation, key, other_key, target, other_key)))
        out["references_without_target"] = refs
        if incremental and key:
            [lr] = self.reads.sql(late_rows_sql(relation, key))
            out["late_arrivals"] = {"late_rows": int(lr["late_rows"] or 0), "late_dates": int(lr["late_dates"] or 0),
                                    "note": f"new keys loaded > {LATE_LOAD_HOURS} h after their day was first loaded; "
                                            "not visible from before the last bronze rebuild"}
            if out["late_arrivals"]["late_rows"]:
                detail["late_rows"] = shape_detail(self.reads.sql(late_rows_detail_sql(relation, key)))
        out["finding_detail"] = detail
        out["candidates"] = candidates(table, out)
        out.update({"env": env, "table": table, "load_mode": tdef.get("load_mode"),
                    "watermark_col": tdef.get("watermark_col"),
                    "discard_threshold": None if tdef.get("discard_threshold") is None else float(tdef["discard_threshold"]),
                    "note": "computed on bronze (all rows, including those silver merges away or DQ rejects); "
                            "aggregates only, no raw rows (a finding's detail lists key values only). info_same_day_versions are normal (bronze keys rows by "
                            "content), not duplicates; keys_with_conflicting_created_date_same_day are duplicates. "
                            "Uniqueness, references and late rows need more than one row: they are not row rules. "
                            "`candidates` is the checklist: every check with violations > 0, each to be answered."})
        return out

    def get_rejects_summary(self, env: str = "dev", table: str | None = None, runs: int | None = None) -> dict:
        cfg = config.env_config(env)
        tables = list(config.check_table(cfg, table))
        runs = config.clamp("runs", runs, 7, 30)
        in_list = ", ".join(sql_lit(t) for t in tables)
        current = self.reads.sql(
            f"SELECT source_table, reason, count(*) AS n FROM {cfg.catalog}.poc_reject.rejected_rows "
            f"WHERE source_table IN ({in_list}) GROUP BY 1, 2 ORDER BY 1, 3 DESC")
        audit = self.reads.pg_query(
            cfg.metadata_endpoint,
            "SELECT d.source_table, a.run_id, a.started_at, a.rows_read, a.rows_rejected, a.status "
            "FROM aidq_metadata.run_audit a JOIN aidq_metadata.source_table_def d USING (table_id) "
            "WHERE d.source_table = ANY(%s) AND a.layer = 'dq' ORDER BY a.started_at DESC NULLS LAST LIMIT %s",
            (tables, runs * len(tables)))
        return {"env": env,
                "current_rejects": [{"table": r["source_table"], "reason": r["reason"], "count": int(r["n"])}
                                    for r in current],
                "recent_runs": [{**r, "started_at": str(r["started_at"])} for r in audit],
                "note": "current_rejects: poc_reject.rejected_rows now (recomputed over all bronze each run), per "
                        "failed HARD rule; recent_runs: reject counts per run from run_audit"}
