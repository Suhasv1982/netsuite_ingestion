"""tools/migrate.py: file discovery, checksums, transaction-statement detection, plan states, snapshot diff."""

from pathlib import Path

import pytest

from migrate import (
    MIGRATIONS_DIR,
    Migration,
    checksum,
    discover,
    plan,
    snapshot_diff,
    strip_bodies,
    transaction_statements,
)


class TestTransactionStatements:
    @pytest.mark.parametrize(
        "sql, found",
        [
            ("BEGIN;\nSELECT 1;\nCOMMIT;", ["BEGIN", "COMMIT"]),
            ("SELECT 1;BEGIN;COMMIT;", ["BEGIN", "COMMIT"]),
            ("start transaction;\nrollback;", ["START TRANSACTION", "ROLLBACK"]),
            ("BEGIN WORK;\nEND TRANSACTION;", ["BEGIN", "END"]),
            ("SELECT 1;\nCOMMIT", ["COMMIT"]),  # last statement without a semicolon
        ],
    )
    def test_top_level_statements_are_found(self, sql, found):
        assert transaction_statements(sql) == found

    @pytest.mark.parametrize(
        "sql",
        [
            "DO $$ BEGIN IF true THEN PERFORM 1; END IF; END $$;",
            "CREATE FUNCTION f() RETURNS int AS $body$ BEGIN RETURN 1; END; $body$ LANGUAGE plpgsql;",
            "SELECT 'BEGIN;'; SELECT 'it''s COMMIT;';",
            "-- BEGIN;\nSELECT 1; /* COMMIT; */",
            "ALTER TABLE t ADD COLUMN begin_date date;",
        ],
    )
    def test_bodies_strings_and_comments_do_not_count(self, sql):
        assert transaction_statements(sql) == []

    def test_strip_bodies_keeps_the_statements(self):
        assert strip_bodies("SELECT 'x'; DO $$ y $$; -- z\nSELECT 1;") == "SELECT ; DO ; \nSELECT 1;"


class TestChecksum:
    def test_crlf_and_lf_give_the_same_checksum(self):
        assert checksum("a;\r\nb;\r\n") == checksum("a;\nb;\n")

    def test_any_edit_changes_it(self):
        assert checksum("a;\n") != checksum("a; \n")


class TestDiscover:
    def test_repo_migrations_are_valid(self):
        files = discover()
        assert [f.version for f in files] == sorted(f.version for f in files)
        assert {"001", "002", "003"} <= {f.version for f in files}

    def test_rejects_a_file_with_commit(self, tmp_path):
        (tmp_path / "001_x.sql").write_text("CREATE TABLE t (a int);\nCOMMIT;\n", encoding="utf-8")
        with pytest.raises(ValueError, match="COMMIT"):
            discover(tmp_path)

    def test_rejects_bad_names_and_duplicate_versions(self, tmp_path):
        (tmp_path / "1_x.sql").write_text("SELECT 1;", encoding="utf-8")
        with pytest.raises(ValueError, match="NNN_lower_snake"):
            discover(tmp_path)
        (tmp_path / "1_x.sql").unlink()
        (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf-8")
        (tmp_path / "001_b.sql").write_text("SELECT 2;", encoding="utf-8")
        with pytest.raises(ValueError, match="used twice"):
            discover(tmp_path)

    def test_migration_files_are_stored_with_lf(self):
        for f in MIGRATIONS_DIR.glob("*.sql"):
            assert b"\r\n" not in f.read_bytes(), f"{f.name} has CRLF line endings"


def _m(version, text="SELECT 1;"):
    return Migration(version, f"m{version}", Path(f"{version}.sql"), text, checksum(text))


class TestPlan:
    def test_states(self):
        files = [_m("001"), _m("002", "SELECT 2;"), _m("003")]
        applied = {"001": {"name": "m001", "checksum": checksum("SELECT 1;")},
                   "002": {"name": "m002", "checksum": "edited"},
                   "009": {"name": "gone", "checksum": "x"}}
        assert [(m.version, s) for m, s in plan(files, applied)] == [
            ("001", "applied"), ("002", "CHANGED"), ("003", "pending"), ("009", "MISSING")]

    def test_fresh_database_everything_pending(self):
        assert [s for _, s in plan([_m("001"), _m("002")], {})] == ["pending", "pending"]


class TestSnapshotDiff:
    def test_identical_snapshots_have_no_diff(self):
        snap = {"columns": ["t.a integer null=NO default="], "triggers": ["CREATE TRIGGER x"]}
        assert snapshot_diff(snap, {k: list(v) for k, v in snap.items()}) == []

    def test_added_and_removed_lines(self):
        before = {"columns": ["t.a int"], "constraints": ["c1 CHECK (a > 0)"]}
        after = {"columns": ["t.a int", "t.b int"], "constraints": ["c1 CHECK (a > 1)"]}
        assert snapshot_diff(before, after) == [
            "columns: + t.b int", "constraints: - c1 CHECK (a > 0)", "constraints: + c1 CHECK (a > 1)"]
