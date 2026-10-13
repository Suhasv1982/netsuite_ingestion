"""Integration test: a forced primary-model failure escalates to the routed fallback, and llm_calls records it.

Skipped unless NETSUITE_INTEGRATION=1. Uses the real workspace models and the real dev aidq_metadata.llm_calls
(rows are COMMITTED, tagged with a unique trace id `escalation-test-<time>`; the 90-day prune removes them).
The primary for `root_cause` is replaced by a model name that does not exist; the fallback is the one in
src/aidq_llm/routing.yml. The investigation replays the recorded 2026-10-06 tool outputs.

    NETSUITE_INTEGRATION=1 pytest tests/test_routing_escalation_integration.py -s
"""

import asyncio
import copy
import datetime as dt
import json
import os
from pathlib import Path

import pytest

if os.environ.get("NETSUITE_INTEGRATION") != "1":
    pytest.skip("set NETSUITE_INTEGRATION=1 to run the escalation test against the workspace", allow_module_level=True)

import migrate  # noqa: E402

from aidq_agent.dbx import DatabricksChat, RoutedInvestigator  # noqa: E402
from aidq_agent.toolbox import ReplayToolBox  # noqa: E402
from aidq_llm.router import PgCallLogWriter, Router, load_routes  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
PROFILE = os.environ.get("NETSUITE_PROFILE", "DEFAULT")
MISSING_MODEL = "databricks-model-that-does-not-exist"


def test_forced_primary_failure_escalates_and_is_logged():
    routes = copy.deepcopy(load_routes())
    fallback = routes["tasks"]["root_cause"]["fallback"]
    routes["tasks"]["root_cause"]["primary"] = MISSING_MODEL
    trace = f"escalation-test-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}"
    router = Router(DatabricksChat(PROFILE), "aidq_monitor", routes=routes, trace_id=trace)
    cassette = json.loads((ROOT / "evals" / "cassettes" / "2026-10-06_afternoon.json").read_text(encoding="utf-8"))
    signals = ["[dev ci_dev] netsuite_daily_generator: no run started for the scheduled time 2026-10-06T05:00:00+00:00"]

    inv = asyncio.run(RoutedInvestigator(router).investigate("ORCHESTRATION", signals, [], "2026-10-06",
                                                             ReplayToolBox(cassette)))
    assert inv.model == fallback and inv.rca is not None          # the fallback produced the RCA

    lines = router.log.flush(PgCallLogWriter(lambda: migrate.connect("dev", PROFILE)))
    print("\n".join(lines))
    assert lines[0].startswith("LLM calls: ") and "logged to llm_calls" in lines[0]

    c = migrate.connect("dev", PROFILE)
    try:
        rows = c.execute("SELECT model, escalated, escalation_reason, outcome FROM aidq_metadata.llm_calls "
                         "WHERE trace_id = %s ORDER BY call_id", (trace,)).fetchall()
    finally:
        c.rollback()
        c.close()
    print(*rows, sep="\n")
    first, rest = rows[0], rows[1:]
    assert first[0] == MISSING_MODEL and first[1] is False and first[3] == "error"
    assert rest and all(r[0] == fallback and r[1] is True and r[2].startswith("error:") for r in rest)
