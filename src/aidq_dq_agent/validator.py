"""Deterministic validator for ADD_DQ_RULE proposals (design v2 section 6; plan #39 step 5).

A rule_expr is inserted into pipeline SQL as-is (dq.py), so parsing is a security boundary. A proposal is
VALIDATED only if every check passes; otherwise VALIDATION_FAILED with the reasons:

1. static: no SQL comments (`--`, `/* */`) and no `;` in the text; parses (sqlglot, Databricks dialect) to
   exactly ONE boolean expression; no subquery, no statement node, only allowlisted functions (deterministic ones,
   plus current_date / current_timestamp, fixed within a run).
2. columns: every column is an active source_column of the proposal's table; reserved names (e.g. `date`) must be
   backtick-quoted.
3. dry run on bronze: counts of rows where the expression is TRUE, FALSE and NULL.
4. NULL check: any row where it is NULL fails the rule (dq.py treats NULL as a reject since #40, but a rule must
   not depend on that; owner decision 2026-10-08).
5. impact: the would-be reject rate (FALSE / rows) is flagged, not failed, when above discard_threshold.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

ALLOWED_FUNCTIONS = {
    # comparison / null handling / logic helpers
    "coalesce", "nvl", "ifnull", "nullif", "isnull", "isnotnull", "if", "greatest", "least",
    # strings
    "lower", "upper", "trim", "ltrim", "rtrim", "length", "char_length", "substring", "substr", "startswith",
    "endswith", "contains", "rlike", "regexp_like", "regexp", "like", "ilike",
    # numbers
    "abs", "round", "floor", "ceil", "ceiling", "sign", "mod",
    # dates (current_date / current_timestamp are fixed within a run: allowed explicitly)
    "current_date", "current_timestamp", "to_date", "to_timestamp", "datediff", "date_add", "date_sub", "year",
    "month", "day", "dayofmonth", "add_months", "months_between",
    # casts
    "cast", "try_cast",
}
# sqlglot node types for functions it models as their own class (name in lower case via exp.Func.sql_name())
STATEMENT_NODES = (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Command,
                   exp.Merge, exp.Union, exp.Subquery)
BOOLEAN_ROOTS = (exp.And, exp.Or, exp.Not, exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Is, exp.In,
                 exp.Between, exp.Like, exp.ILike, exp.RegexpLike, exp.Paren, exp.Boolean, exp.NullSafeEQ)
# logical / comparison operators and casts are expression syntax, not function calls (sqlglot models some as Func)
OPERATOR_NODES = (exp.Connector, exp.Binary, exp.Unary, exp.Predicate, exp.Cast, exp.TryCast)
RESERVED_IDENTIFIERS = {"date", "timestamp", "interval", "order", "group", "select", "from", "where", "table",
                        "user", "case", "when", "end", "limit", "join"}


@dataclass
class StaticResult:
    ok: bool
    problems: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    normalized: str | None = None


def _root(node: exp.Expression) -> exp.Expression:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def static_check(expr: str, active_columns: list[str]) -> StaticResult:
    problems: list[str] = []
    if not isinstance(expr, str) or not expr.strip():
        return StaticResult(False, ["empty expression"])
    if "--" in expr or "/*" in expr or "*/" in expr:
        problems.append("SQL comments are not allowed")
    if ";" in expr:
        problems.append("';' is not allowed")
    if problems:
        return StaticResult(False, problems)
    try:
        parsed = [p for p in sqlglot.parse(expr, read="databricks") if p is not None]
    except sqlglot.errors.ParseError as e:
        return StaticResult(False, [f"does not parse: {str(e).splitlines()[0][:200]}"])
    if len(parsed) != 1:
        return StaticResult(False, [f"must be exactly one expression (found {len(parsed)})"])
    tree = parsed[0]
    if not isinstance(_root(tree), BOOLEAN_ROOTS):
        problems.append(f"must be a boolean expression (found {type(_root(tree)).__name__})")
    for node in tree.walk():
        if isinstance(node, STATEMENT_NODES):
            problems.append(f"{type(node).__name__} is not allowed (no subqueries or statements)")
        elif isinstance(node, exp.Func) and not isinstance(node, OPERATOR_NODES):
            name = (node.sql_name() if not isinstance(node, exp.Anonymous) else node.name).lower()
            if name not in ALLOWED_FUNCTIONS:
                problems.append(f"function {name}() is not allowed")
    cols = []
    active = set(active_columns)
    for col in tree.find_all(exp.Column):
        if col.table:
            problems.append(f"qualified column {col.sql('databricks')} is not allowed")
        name = col.name
        cols.append(name)
        if name not in active:
            problems.append(f"column {name!r} is not an active column of this table")
        ident = col.this
        if name.lower() in RESERVED_IDENTIFIERS and isinstance(ident, exp.Identifier) and not ident.quoted:
            problems.append(f"reserved name {name!r} must be quoted as `{name}`")
    return StaticResult(not problems, sorted(set(problems), key=problems.index), sorted(set(cols)),
                        tree.sql("databricks"))


def dry_run_sql(relation: str, expr: str) -> str:
    """Counts on bronze. Only called after static_check passed (the expression is inserted as-is, like dq.py)."""
    return (f"SELECT count(*) AS `rows`, count_if(({expr}) IS TRUE) AS passed, count_if(({expr}) IS FALSE) AS failed, "
            f"count_if(({expr}) IS NULL) AS nulls FROM {relation}")


def judge(static: StaticResult, counts: dict | None, discard_threshold: float | None, spark_version: str | None) -> dict:
    """The validation_result document and the decision (status)."""
    result: dict = {"checks": {"static": static.ok}, "problems": list(static.problems), "columns": static.columns,
                    "normalized_expr": static.normalized, "spark_version": spark_version, "flags": []}
    if counts is not None:
        rows = int(counts["rows"])
        failed, nulls = int(counts["failed"]), int(counts["nulls"])
        rate = round(failed / rows, 6) if rows else None
        result.update({"rows": rows, "passed": int(counts["passed"]), "failed": failed, "nulls": nulls,
                       "would_reject_rate": rate})
        result["checks"]["null_check"] = nulls == 0
        if nulls:
            result["problems"].append(f"the expression is NULL on {nulls} bronze rows (write it NULL-safe, e.g. "
                                      "`col IS NULL OR ...`)")
        if discard_threshold is not None and rate is not None and rate > discard_threshold:
            result["flags"].append(f"would-reject rate {rate:.4f} exceeds discard_threshold {discard_threshold}")
    ok = static.ok and counts is not None and result["checks"].get("null_check", False)
    result["status"] = "VALIDATED" if ok else "VALIDATION_FAILED"
    result["summary"] = ("VALIDATED" if ok else "FAILED: " + "; ".join(result["problems"][:3])) + (
        f" | rows {result.get('rows')}, fail {result.get('failed')}, null {result.get('nulls')}" if counts else "")
    return result
