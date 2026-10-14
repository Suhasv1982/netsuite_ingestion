"""Integration test: the validator on dev, with the real warehouse dry run on bronze; the proposals live only inside
ONE metadata transaction that is always rolled back. Skipped unless NETSUITE_INTEGRATION=1.

    NETSUITE_INTEGRATION=1 pytest tests/test_dq_validator_integration.py -s
"""

import json
import os

import pytest

if os.environ.get("NETSUITE_INTEGRATION") != "1":
    pytest.skip("set NETSUITE_INTEGRATION=1 to run the validator against dev (rolled back)", allow_module_level=True)

import migrate  # noqa: E402
import validate_proposals as vp  # noqa: E402

from aidq_mcp.reads import Reads  # noqa: E402

PROFILE = os.environ.get("NETSUITE_PROFILE", "DEFAULT")
RULES = {
    "safe": "amount IS NULL OR amount >= 0",                   # 300 negative amounts in bronze: fails some rows
    "null_returning": "amount >= 0 AND line_number < 0 OR NULL",    # NULL on rows: must fail the NULL check
    "unsafe": "amount > 0; DROP TABLE x",
}


def test_validator_on_dev_bronze():
    conn = migrate.connect("dev", PROFILE)
    try:
        ids = {}
        for name, expr in RULES.items():
            ids[name] = conn.execute(
                "INSERT INTO aidq_metadata.config_proposals (proposal_type, table_id, proposed_change, rationale, "
                "proposed_by) SELECT 'ADD_DQ_RULE', table_id, %s::jsonb, 'integration test', 'dq_agent@test' "
                "FROM aidq_metadata.source_table_def WHERE source_table = 'netsuite_transaction_lines' "
                "RETURNING proposal_id", (json.dumps({"rule_name": f"Test {name}", "rule_expr": expr,
                                                      "severity": "SOFT"}),)).fetchone()[0]
        out = {o["proposal_id"]: o for o in vp.validate_all(conn, Reads(PROFILE).sql, "workspace", set_role=False)}
        safe, nul, unsafe = out[ids["safe"]], out[ids["null_returning"]], out[ids["unsafe"]]
        print(safe["summary"], nul["summary"], unsafe["summary"], sep="\n")
        assert safe["status"] == "VALIDATED" and safe["failed"] >= 300 and safe["nulls"] == 0 and safe["spark_version"]
        assert nul["status"] == "VALIDATION_FAILED" and nul["nulls"] > 0
        assert unsafe["status"] == "VALIDATION_FAILED" and "rows" not in unsafe     # never reached the warehouse
        stored = conn.execute("SELECT status FROM aidq_metadata.config_proposals WHERE proposal_id = %s",
                              (ids["safe"],)).fetchone()[0]
        assert stored == "VALIDATED"
    finally:
        conn.rollback()
        conn.close()
