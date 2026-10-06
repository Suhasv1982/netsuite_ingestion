"""src/aidq_mcp: pure logic, config checks and the read-only guarantee (no workspace needed)."""

import ast
import datetime as dt
import pathlib
import re

import pytest

from aidq_mcp import config, logic

UTC = dt.timezone.utc
PKG = pathlib.Path(__file__).resolve().parents[1] / "src" / "aidq_mcp"


def t(day, h=5, m=0):
    return dt.datetime(2026, 10, day, h, m, tzinfo=UTC)


# -- bronze vs source ---------------------------------------------------------

SRC = {("netsuite_customers", None): 10, ("netsuite_transactions", "2026-07-11"): 50,
       ("netsuite_transactions", "2026-10-02"): 40}


def test_no_gaps_when_counts_match():
    assert logic.classify_date_gaps(SRC, dict(SRC), {("netsuite_transactions", "2026-07-11"): 5}) == []


def test_gap_equal_to_that_dates_duplicates_is_a_known_defect():
    gaps = logic.classify_date_gaps(SRC, {**SRC, ("netsuite_transactions", "2026-10-02"): 37},
                                    {("netsuite_transactions", "2026-10-02"): 3})
    assert gaps == [{"table": "netsuite_transactions", "date": "2026-10-02", "source": 40, "bronze": 37,
                     "missing": 3, "same_day_extra": 3, "classification": logic.KNOWN_DEFECT}]


def test_duplicates_elsewhere_or_a_different_size_do_not_explain_a_gap():
    short = {**SRC, ("netsuite_transactions", "2026-10-02"): 37}
    [g] = logic.classify_date_gaps(SRC, short, {("netsuite_transactions", "2026-07-11"): 3})
    assert g["classification"] == logic.UNEXPLAINED
    [g] = logic.classify_date_gaps(SRC, short, {("netsuite_transactions", "2026-10-02"): 2})
    assert g["classification"] == logic.UNEXPLAINED


def test_extra_bronze_rows_and_full_load_gaps_are_unexplained():
    gaps = logic.classify_date_gaps(SRC, {**SRC, ("netsuite_customers", None): 9,
                                          ("netsuite_transactions", "2026-10-09"): 1}, {})
    assert [(g["table"], g["date"], g["missing"], g["classification"]) for g in gaps] == [
        ("netsuite_customers", None, 1, logic.UNEXPLAINED),
        ("netsuite_transactions", "2026-10-09", -1, logic.UNEXPLAINED)]


# -- schedules ----------------------------------------------------------------

def test_cron_fire_times_daily():
    assert logic.cron_fire_times("0 30 5 * * ?", t(4, 0), t(6, 0)) == [t(4, 5, 30), t(5, 5, 30)]
    assert logic.cron_fire_times("0 0 5 * * ?", t(4, 5), t(4, 6)) == [t(4, 5)]  # start inclusive
    assert logic.cron_fire_times("0 0 5 * * ?", t(4, 0), t(4, 5)) == []         # end exclusive
    assert logic.cron_fire_times("0 0 6,18 * * ?", t(4, 0), t(5, 0)) == [t(4, 6), t(4, 18)]


@pytest.mark.parametrize("cron", ["0 0 5 ? * MON", "0 0/5 * * * ?", "0 0 5 1 * ?", "0 0 5 * * ? 2027", "0 0 25 * * ?"])
def test_unsupported_crons_raise(cron):
    with pytest.raises(ValueError):
        logic.cron_fire_times(cron, t(4, 0), t(6, 0))


def test_the_10_06_missed_runs_are_detected():
    # generator: 05:00 daily, ran 10-02..10-05, nothing on 10-06; checked at 14:45 on 10-06
    runs = [t(d, 5, 0) + dt.timedelta(seconds=13) for d in (2, 3, 4, 5)]
    assert logic.missed_schedules("0 0 5 * * ?", False, runs, t(2, 0), t(6, 14, 45), first_scheduled_run=runs[0]) == {
        "confirmed": [t(6, 5)], "unconfirmed": []}


def test_misses_before_the_first_scheduled_run_are_unconfirmed():
    # schedule added on 10-01 evening: 09-30 and 10-01 05:00 have no run, but the schedule may not have existed
    runs = [t(d, 5, 0) + dt.timedelta(seconds=13) for d in (2, 3)]
    start = dt.datetime(2026, 9, 30, tzinfo=UTC)
    out = logic.missed_schedules("0 0 5 * * ?", False, runs, start, t(4, 6), first_scheduled_run=runs[0])
    assert out == {"confirmed": [t(4, 5)], "unconfirmed": [dt.datetime(2026, 9, 30, 5, tzinfo=UTC), t(1, 5)]}
    # never ran on schedule: everything is unconfirmed
    assert logic.missed_schedules("0 0 5 * * ?", False, [], t(2, 0), t(3, 6)) == {
        "confirmed": [], "unconfirmed": [t(2, 5), t(3, 5)]}


def test_no_miss_when_paused_or_inside_tolerance_or_not_due_yet():
    first = t(5, 5)
    none = {"confirmed": [], "unconfirmed": []}
    assert logic.missed_schedules("0 0 5 * * ?", True, [], t(2, 0), t(6, 14)) == none
    assert logic.missed_schedules("0 0 5 * * ?", False, [first, t(6, 5, 14)], t(6, 0), t(6, 14), first) == none
    assert logic.missed_schedules("0 0 5 * * ?", False, [first, t(6, 5, 16)], t(6, 0), t(6, 14), first) == {
        "confirmed": [t(6, 5)], "unconfirmed": []}
    assert logic.missed_schedules("0 0 5 * * ?", False, [first], t(6, 0), t(6, 5, 10), first) == none


# -- output shaping -----------------------------------------------------------

def test_redact_masks_identifiers_recursively():
    raw = {"msg": "see https://x.example/run/1 by someone@example.com on dbc-1-2.cloud.databricks.com",
           "items": ["token dapi0123456789abcdef0123", 42, None],
           "host": "ep-abc.database.us-east-2.cloud.databricks.com"}
    assert logic.redact(raw) == {"msg": "see <url> by <email> on <host>", "items": ["token <token>", 42, None],
                                 "host": "<host>"}


def test_actor_role():
    roles = {"owner-user": "owner", "11111111-2222": "ci-dev"}
    assert [logic.actor_role(a, roles) for a in ("owner-user", "11111111-2222", "System-User", None, "x")] == [
        "owner", "ci-dev", "system", "system", "other"]


def test_bounded_and_truncate():
    assert logic.bounded([1, 2, 3], 2) == {"items": [1, 2], "total": 3, "truncated": True}
    assert logic.bounded([1], 2) == {"items": [1], "total": 1, "truncated": False}
    assert logic.truncate("abcdef", 4) == "abc…" and logic.truncate("abc", 4) == "abc" and logic.truncate(None) is None


# -- config -------------------------------------------------------------------

def test_env_config_is_dev_only():
    assert config.env_config("dev").catalog == "workspace"
    with pytest.raises(config.ConfigError, match="prod not enabled"):
        config.env_config("prod")
    with pytest.raises(config.ConfigError):
        config.env_config("qa")


def test_argument_checks():
    cfg = config.env_config("dev")
    assert config.check_table(cfg, None) == cfg.tables and "netsuite_customers" in cfg.tables
    assert config.check_table(cfg, "netsuite_transactions") == ("netsuite_transactions",)
    with pytest.raises(config.ConfigError):
        config.check_table(cfg, "netsuite_transactions; DROP TABLE x")
    assert config.check_alias("job", cfg.jobs, "generator") == {"generator": cfg.jobs["generator"]}
    with pytest.raises(config.ConfigError):
        config.check_alias("job", cfg.jobs, "prod")
    assert config.clamp("days", None, 3, 14) == 3 and config.clamp("days", 14, 3, 14) == 14
    for bad in (0, 15, True, "3"):
        with pytest.raises(config.ConfigError):
            config.clamp("days", bad, 3, 14)


# -- read-only guarantee -------------------------------------------------------

WRITE_SQL = re.compile(r"\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|CREATE|ALTER|DROP|GRANT|REVOKE|TRUNCATE|MERGE\s+INTO|COPY)\b")
WRITE_CLI = {"update-endpoint", "create-endpoint", "run-now", "submit", "start", "stop", "deploy", "destroy",
             "delete", "create", "update", "reset", "repair-run", "cancel-run", "start-update", "put", "patch"}
# "post" is allowed: SQL statements go through POST /api/2.0/sql/statements; WRITE_SQL keeps them read-only.


def _tree(path):
    return ast.parse(path.read_text(encoding="utf-8"))


def _string_constants(tree):
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def _cli_arguments(tree):
    """String arguments of every `<x>.cli(...)` call: the Databricks CLI verbs the package uses."""
    return [a.value for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "cli" for a in n.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]


def _http_methods(tree):
    """The value after every "-X" in a list literal (gh api / curl style method flags)."""
    out = []
    for n in ast.walk(tree):
        if isinstance(n, ast.List):
            vals = [e.value if isinstance(e, ast.Constant) else None for e in n.elts]
            out += [vals[i + 1] for i, v in enumerate(vals[:-1]) if v == "-X"]
    return out


@pytest.mark.parametrize("path", sorted(PKG.glob("*.py")), ids=lambda p: p.name)
def test_package_contains_no_write_statements_or_mutating_cli_verbs(path):
    tree = _tree(path)
    assert not [s for s in _string_constants(tree) if WRITE_SQL.search(s)]
    assert not [a for a in _cli_arguments(tree) if a.lower() in WRITE_CLI]
    assert set(_http_methods(tree)) <= {"GET"}


def test_cli_verb_check_sees_the_calls():
    # guard against the check silently matching nothing
    assert "list-runs" in _cli_arguments(_tree(PKG / "reads.py"))
    assert _http_methods(_tree(PKG / "reads.py")) == ["GET"]
