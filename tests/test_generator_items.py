"""netsuite_items in the generator (onboarding plan #77, step 2): seeding from the lines, in-place updates, new lines
from active items, the four defects, the writer (upsert, backup, init), and the table-missing path."""

import datetime as dt

import pytest

pytest.importorskip("faker")

import netsuite_gen as ng  # noqa: E402
import pg_writer  # noqa: E402
from test_netsuite_gen import BASE, NEXT, SCALE, FakeConn, by_defect, cfg, init  # noqa: E402


def increment(existing, seed=2, batch=NEXT, items_present=True, **rates):
    return ng.generate_increment(ng.Generator(seed), existing, SCALE, batch, cfg(**rates), items_present=items_present)


def seeded(existing):
    """`existing` plus a netsuite_items table seeded the way the first run after the DDL does."""
    return {**existing, ng.T_ITEMS: ng.seed_items([], existing[ng.T_LINES])}


@pytest.fixture()
def existing():
    return init(seed=1).data


# -- seeding ------------------------------------------------------------------

def test_first_run_after_the_ddl_seeds_one_row_per_item_on_the_lines(existing):
    res = increment({**existing, ng.T_ITEMS: []})
    items = res.upserts[ng.T_ITEMS]
    by_id = {r["item_id"]: r for r in items}
    line_items = {(ln["item_id"], ln["item_name"], ln["rate"]) for ln in existing[ng.T_LINES]}
    assert set(by_id) == {i for i, _, _ in line_items}                    # exactly the ids on the lines
    for iid, name, rate in line_items:                                     # name and price match the lines
        assert (by_id[iid]["item_name"], by_id[iid]["base_price"]) == (name, rate)
    for r in items:
        assert (r["item_type"], r["category"]) == ng.ITEM_CATALOG[r["item_id"]]
        assert r["is_inactive"] is False and r["created_date"] == r["updated_date"] == BASE
    assert res.manifest["items"] == {"present": True, "seeded": len(items), "updated": 0}


def test_seeding_is_idempotent_and_the_catalog_covers_the_real_ids(existing):
    assert ng.seed_items(ng.seed_items([], existing[ng.T_LINES]), existing[ng.T_LINES]) == []
    assert set(ng.ITEM_CATALOG) == {101, 102, 103} | set(range(3001, 3021))     # the 23 ids in the source today
    assert all(t in ng.VALID_ITEM_TYPES for t, _ in ng.ITEM_CATALOG.values())


def test_seed_takes_the_first_line_date_and_the_most_frequent_name_and_rate():
    day = lambda d: dt.datetime(2026, 6, d)  # noqa: E731  (lines carry timestamps)
    lines = [{"item_id": 7, "item_name": "A", "rate": 10, "created_date": day(22)},
             {"item_id": 7, "item_name": "A", "rate": 10, "created_date": day(20)},
             {"item_id": 7, "item_name": "B", "rate": 12, "created_date": day(25)}]
    [row] = ng.seed_items([], lines)
    assert (row["item_name"], row["base_price"], row["created_date"]) == ("A", 10, dt.date(2026, 6, 20))
    assert (row["item_type"], row["category"]) == ng.UNKNOWN_ITEM_CLASS


# -- updates and new lines ------------------------------------------------------

def test_updates_are_in_place_with_the_batch_date_and_keep_created_date(existing):
    base = seeded(existing)
    res = increment(base)
    before = {r["item_id"]: r for r in base[ng.T_ITEMS]}
    updates = res.upserts[ng.T_ITEMS]
    assert updates and res.manifest["items"]["seeded"] == 0
    for r in updates:
        old = before[r["item_id"]]
        assert r["created_date"] == old["created_date"] and r["updated_date"] == NEXT
        changed = {c for c in ng.COLUMNS[ng.T_ITEMS] if r[c] != old[c]} - {"updated_date"}
        assert changed in ({"base_price"}, {"is_inactive"}, set())     # price change (may round to the same) or flip
        assert r["base_price"] > 0


def test_new_lines_use_only_active_items_at_their_current_price(existing):
    base = seeded(existing)
    items = base[ng.T_ITEMS]
    items[0]["is_inactive"] = True
    items[1]["base_price"] = 999
    res = increment(base)
    current = {r["item_id"]: r for r in items}
    current.update({r["item_id"]: r for r in res.upserts[ng.T_ITEMS]})
    old_txns = {r["transaction_internal_id"] for r in existing[ng.T_TRANSACTIONS]}
    new_lines = [ln for ln in res.data[ng.T_LINES] if ln["transaction_internal_id"] not in old_txns]
    assert new_lines
    for ln in new_lines:
        item = current[ln["item_id"]]
        assert not item["is_inactive"] and ln["rate"] == ng.item_price(item)


# -- defects --------------------------------------------------------------------

@pytest.mark.parametrize("name,column,bad", [
    ("item_null_category", "category", lambda v: v is None),
    ("item_negative_price", "base_price", lambda v: v < 0),
    ("item_invalid_type", "item_type", lambda v: v in ng.INVALID_ITEM_TYPES),
])
def test_item_defects_land_on_the_written_rows_at_their_rate(existing, name, column, bad):
    res = increment({**existing, ng.T_ITEMS: []}, **{name: 1.0})
    rows = res.upserts[ng.T_ITEMS]
    [entry] = by_defect(res, name, ng.T_ITEMS)
    assert sorted(entry["business_keys"]) == sorted(r["item_id"] for r in rows) and entry["column"] == column
    assert all(bad(r[column]) for r in rows)
    half = increment({**existing, ng.T_ITEMS: []}, **{name: 0.5})
    assert by_defect(half, name)[0]["row_count"] == int(len(rows) * 0.5 + 0.5)


def test_line_inactive_item_references_an_inactive_item_at_a_consistent_amount(existing):
    base = seeded(existing)
    for r in base[ng.T_ITEMS][:3]:
        r["is_inactive"] = True
    res = increment(base, line_inactive_item=1.0)
    [entry] = by_defect(res, "line_inactive_item", ng.T_LINES)
    inactive = {r["item_id"] for r in base[ng.T_ITEMS] if r["is_inactive"]} | \
               {r["item_id"] for r in res.upserts[ng.T_ITEMS] if r["is_inactive"]}
    lines = {ln["transaction_line_id"]: ln for ln in res.data[ng.T_LINES]}
    assert entry["row_count"] > 0
    for key in entry["business_keys"]:
        ln = lines[key]
        assert ln["item_id"] in inactive and ln["amount"] == ln["quantity"] * ln["rate"]


def test_all_item_defects_are_off_by_default_and_in_the_config():
    rates = ng.DefectConfig().rates
    for name in ("item_null_category", "item_negative_price", "item_invalid_type", "line_inactive_item"):
        assert rates[name] == 0.0
    assert all(ng.load_config("increment_daily.yaml").rates[n] == 0.0 for n in ("item_null_category",))


# -- the table does not exist ------------------------------------------------------

def test_without_the_table_nothing_about_items_is_generated_or_drawn(existing):
    without = ng.generate_increment(ng.Generator(2), existing, SCALE, NEXT, cfg(item_null_category=1.0,
                                                                                 line_inactive_item=1.0))
    plain = ng.generate_increment(ng.Generator(2), existing, SCALE, NEXT, cfg())
    assert ng.T_ITEMS not in without.upserts and without.manifest["items"]["present"] is False
    assert not by_defect(without, "item_null_category") and not by_defect(without, "line_inactive_item")
    assert without.data == plain.data           # the item rates draw nothing: same rows as with every rate off


# -- writer ----------------------------------------------------------------------

def test_increment_upserts_items_on_item_id(existing):
    res = increment({**existing, ng.T_ITEMS: []})
    conn = FakeConn()
    pg_writer.load_increment(conn, res)
    [items_sql] = [(sql, rows) for sql, rows in conn.cur.many if '"netsuite_items"' in sql]
    sql, rows = items_sql
    assert 'ON CONFLICT ("item_id") DO UPDATE SET' in sql and '"is_inactive" = EXCLUDED."is_inactive"' in sql
    assert len(rows) == len(res.upserts[ng.T_ITEMS])
    assert conn.cur.many[0][0].startswith('INSERT INTO "netsuite"."netsuite_customers"')   # customers first, as before


def test_backup_and_read_include_items_only_when_the_table_exists(monkeypatch):
    monkeypatch.setattr(pg_writer, "_cli_json", lambda *a, **k: {})
    with_items, without = FakeConn(), FakeConn()
    pg_writer.create_backup(with_items, include_items=True)
    pg_writer.create_backup(without)
    created = lambda c: [s for s in c.cur.executed if s.startswith("CREATE TABLE")]  # noqa: E731
    assert len(created(with_items)) == len(ng.TABLES) + 1 and any("netsuite_items" in s for s in created(with_items))
    assert len(created(without)) == len(ng.TABLES)


def test_init_truncates_and_loads_items_only_when_given():
    conn = FakeConn()
    res = ng.generate_init(ng.Generator(1), SCALE, BASE, cfg(), items_present=True)
    pg_writer.load_init(conn, res.data, items=res.upserts[ng.T_ITEMS])
    assert '"netsuite"."netsuite_items"' in conn.cur.executed[0]
    assert any("netsuite_items" in c.sql for c in conn.cur.copies)
    plain = FakeConn()
    pg_writer.load_init(plain, init().data)
    assert "netsuite_items" not in plain.cur.executed[0]


def test_cli_passes_items_present_from_the_live_columns(existing, tmp_path, monkeypatch):
    seen = {}
    monkeypatch.setattr(pg_writer, "connect", lambda profile, auth="cli": FakeConn())
    monkeypatch.setattr(pg_writer, "read_live_columns",
                        lambda conn: {**{t: ng.COLUMNS[t] for t in ng.TABLES}, ng.T_ITEMS: ng.COLUMNS[ng.T_ITEMS]})
    monkeypatch.setattr(pg_writer, "read_existing", lambda conn, live: {**existing, ng.T_ITEMS: []})
    monkeypatch.setattr(pg_writer, "create_backup",
                        lambda conn, profile, **kw: seen.update(backup=kw) or {"performed": True, "verified": True})
    monkeypatch.setattr(pg_writer, "load_increment", lambda conn, result, **kw: seen.update(items=result.upserts))
    assert ng.main(["--increment", "--seed", "2", "--scale", "0.01", "--manifest", str(tmp_path / "m.json")]) == 0
    assert seen["backup"]["include_items"] is True and seen["items"][ng.T_ITEMS]


def test_verify_manifest_knows_the_new_defects():
    import verify_manifest as vm

    class Conn:
        def __init__(self):
            self.sql = []

        def execute(self, sql, params=()):
            self.sql.append(sql)
            return self

        def fetchall(self):
            return []

    for defect, table in (("item_null_category", ng.T_ITEMS), ("item_negative_price", ng.T_ITEMS),
                          ("item_invalid_type", ng.T_ITEMS), ("line_inactive_item", ng.T_LINES)):
        conn = Conn()
        keys, method = vm.check_entry(conn, {"defect": defect, "table": table, "business_keys": [1]})
        assert keys == [] and method and conn.sql
