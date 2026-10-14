"""tools/verify_manifest.py: which manifest defects still hold today (fake connection, no database)."""

import json

import manifest_table as mt
import verify_manifest as vm


class Conn:
    """Returns canned rows by the first matching fragment of the SQL text; records the statements."""

    def __init__(self, answers):
        self.answers, self.sql = answers, []

    def execute(self, sql, params=()):
        self.sql.append((sql, params))
        rows = next((v for k, v in self.answers.items() if k in sql), [])
        return type("R", (), {"fetchall": lambda s: rows, "fetchone": lambda s: rows[0] if rows else None})()


def test_only_keys_that_still_carry_the_defect_are_kept():
    entry = {"defect": "customer_null_company_name", "table": "netsuite_customers", "business_keys": [1, 2, 3]}
    keys, method = vm.check_entry(Conn({"company_name IS NULL": [(1,), (3,)]}), entry)
    assert keys == [1, 3] and "company_name IS NULL" in method


def test_orphan_check_qualifies_the_outer_column():
    conn = Conn({"NOT EXISTS": [("L1",)]})
    keys, _ = vm.check_entry(conn, {"defect": "orphan_lines", "table": "netsuite_transaction_lines",
                                    "business_keys": ["L1", "L2"]})
    assert keys == ["L1"] and "x.transaction_internal_id = o.transaction_internal_id" in conn.sql[0][0]


def test_null_keys_are_verified_by_count():
    entry = {"defect": "null_business_key", "table": "netsuite_memberships", "business_keys": [7, 8]}
    assert vm.check_entry(Conn({"IS NULL": [(2,)]}), entry)[0] == [7, 8]
    assert vm.check_entry(Conn({"IS NULL": [(1,)]}), entry)[0] == []


def test_late_rows_must_exist_with_their_dates():
    entry = {"defect": "late_arriving", "table": "netsuite_memberships", "business_keys": [5, 6],
             "rows": [{"key": 5, "created_date": "2026-07-11", "updated_date": "2026-07-12"},
                      {"key": 6, "created_date": "2026-07-11", "updated_date": "2026-07-12"}]}

    class Two(Conn):
        def execute(self, sql, params=()):
            self.sql.append((sql, params))
            hit = [(1,)] if params[0] == 5 else []
            return type("R", (), {"fetchone": lambda s: hit[0] if hit else None})()

    assert vm.check_entry(Two({}), entry)[0] == [5]


def test_loader_refuses_unverified_baseline_and_recomputed(tmp_path, monkeypatch):
    monkeypatch.setattr(mt, "_warehouse_sql", lambda *a: None)
    raw = tmp_path / "m.json"
    raw.write_text(json.dumps({"batch_date": "2026-07-11", "seed": 42, "mode": "init", "defects": []}), encoding="utf-8")
    assert mt.main([str(raw), "--table", "t", "--source", "baseline", "--warehouse-id", "w"]) == 1
    assert mt.main([str(raw), "--table", "t", "--source", "recomputed", "--warehouse-id", "w"]) == 1
