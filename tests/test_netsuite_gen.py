"""Unit tests for tools/netsuite_gen.py and tools/pg_writer.py.

No database and no Databricks CLI: the generator is pure, and the writer is
exercised against a recording fake connection. Requires `faker` and `pyyaml`
(the `tools` extra). `tools/` is on pytest's pythonpath (see pyproject.toml).
"""

import datetime as dt
import json
import re

import pytest

pytest.importorskip("faker")

import netsuite_gen as ng  # noqa: E402
import pg_writer  # noqa: E402

SCALE = ng.ScaleConfig().scaled(0.01)  # 20 customers, 25 memberships, 20 certs, 100 txns, 300 lines
BASE = dt.date(2026, 7, 11)
NEXT = dt.date(2026, 8, 1)
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s.]+$")


def cfg(allow_overlap=False, **rates):
    return ng.DefectConfig.from_dict({"allow_overlap": allow_overlap, "rates": rates})


def init(seed=1, **rates):
    return ng.generate_init(ng.Generator(seed), SCALE, BASE, cfg(**rates))


def increment(existing, seed=2, batch=NEXT, apply_drift=False, region_present=False, **rates):
    return ng.generate_increment(
        ng.Generator(seed), existing, SCALE, batch, cfg(**rates),
        apply_drift=apply_drift, region_present=region_present,
    )


def by_defect(result, name, table=None):
    return [d for d in result.manifest["defects"] if d["defect"] == name and (table is None or d["table"] == table)]


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------


class TestConfig:
    def test_every_defect_is_off_by_default(self):
        assert set(ng.DefectConfig().rates) == set(ng.DEFECT_NAMES)
        assert all(rate == 0.0 for rate in ng.DefectConfig().rates.values())

    def test_rejects_unknown_defect(self):
        with pytest.raises(ValueError, match="Unknown defect"):
            ng.DefectConfig.from_dict({"rates": {"made_up": 0.1}})

    def test_rejects_unknown_top_level_key(self):
        with pytest.raises(ValueError, match="Unknown config keys"):
            ng.DefectConfig.from_dict({"rate": {}})

    @pytest.mark.parametrize("bad", [-0.1, 1.5])
    def test_rejects_out_of_range_rate(self, bad):
        with pytest.raises(ValueError, match="between 0 and 1"):
            ng.DefectConfig.from_dict({"rates": {"negative_amount": bad}})

    def test_example_yaml_loads_with_all_defects_off(self):
        from pathlib import Path

        path = Path(__file__).resolve().parent.parent / "tools" / "defects.example.yaml"
        loaded = ng.load_config(str(path))
        assert all(rate == 0.0 for rate in loaded.rates.values())

    def test_digest_changes_with_config(self):
        assert cfg().digest() != cfg(negative_amount=0.1).digest()


# --------------------------------------------------------------------------
# init: determinism and clean data
# --------------------------------------------------------------------------


class TestInitClean:
    def test_same_seed_is_identical(self):
        a, b = init(seed=7), init(seed=7)
        assert a.data == b.data
        assert a.manifest == b.manifest

    def test_different_seed_differs(self):
        assert init(seed=7).data != init(seed=8).data

    def test_row_counts_match_scale(self):
        data = init().data
        assert len(data[ng.T_CUSTOMERS]) == SCALE.customers
        assert len(data[ng.T_MEMBERSHIPS]) == SCALE.memberships
        assert len(data[ng.T_CERTIFICATIONS]) == SCALE.certifications
        assert len(data[ng.T_TRANSACTIONS]) == SCALE.transactions
        assert len(data[ng.T_LINES]) == SCALE.lines

    def test_default_scale_matches_requested_volumes(self):
        s = ng.ScaleConfig()
        assert (s.customers, s.memberships, s.certifications, s.transactions, s.lines) == (2000, 2500, 2000, 10000, 30000)

    def test_columns_match_live_schema(self):
        data = init().data
        for table in ng.TABLES:
            assert set(data[table][0]) == set(ng.COLUMNS[table])

    def test_referential_integrity(self):
        data = init().data
        customers = {r["customer_internal_id"] for r in data[ng.T_CUSTOMERS]}
        txns = {r["transaction_internal_id"] for r in data[ng.T_TRANSACTIONS]}
        for table in (ng.T_MEMBERSHIPS, ng.T_CERTIFICATIONS, ng.T_TRANSACTIONS):
            assert {r["customer_internal_id"] for r in data[table]} <= customers
        assert {r["transaction_internal_id"] for r in data[ng.T_LINES]} <= txns
        assert {r["transaction_internal_id"] for r in data[ng.T_LINES]} == txns  # every txn has a line

    def test_business_keys_unique_and_non_null(self):
        data = init().data
        for table in ng.TABLES:
            keys = [r[ng.BUSINESS_KEY[table]] for r in data[table]]
            assert None not in keys
            assert len(set(keys)) == len(keys)

    def test_line_ids_and_amounts(self):
        for r in init().data[ng.T_LINES]:
            assert r["transaction_line_id"] == f"{r['transaction_internal_id']}_{r['line_number']}"
            assert r["amount"] == r["quantity"] * r["rate"] > 0

    def test_tranid_format(self):
        for r in init().data[ng.T_TRANSACTIONS]:
            assert r["tranid"] == f"TRID-{r['transaction_internal_id']}"

    def test_enums_valid(self):
        data = init().data
        assert {r["membership_status"] for r in data[ng.T_MEMBERSHIPS]} <= set(ng.VALID_MEMBERSHIP_STATUS)
        assert {r["certification_type"] for r in data[ng.T_CERTIFICATIONS]} <= set(ng.VALID_CERT_TYPE)
        assert {r["type"] for r in data[ng.T_TRANSACTIONS]} <= set(ng.VALID_TXN_TYPE)

    def test_dates_are_ordered_and_respect_existing_dq_rule(self):
        data = init().data
        for r in data[ng.T_MEMBERSHIPS]:
            assert r["start_date"] < r["end_date"]
        for r in data[ng.T_CERTIFICATIONS]:
            assert r["certification_start_date"] < r["certification_end_date"]
            assert r["certification_start_date"] < dt.date(2027, 1, 1)

    def test_snapshot_dates(self):
        data = init().data
        for table in ng.TABLES:
            for r in data[table]:
                assert ng._as_date(r["created_date"]) == BASE
                assert ng._as_date(r["updated_date"]) >= ng._as_date(r["created_date"])
        assert isinstance(data[ng.T_LINES][0]["created_date"], dt.datetime)
        assert not isinstance(data[ng.T_MEMBERSHIPS][0]["created_date"], dt.datetime)

    def test_default_config_injects_nothing(self):
        result = init()
        assert result.manifest["defects"] == []
        assert result.manifest["schema_drift"] is None

    def test_email_is_well_formed_by_default(self):
        assert all(EMAIL_RE.match(r["email"]) for r in init().data[ng.T_CUSTOMERS])

    def test_existing_items_are_merged_into_catalog(self):
        gen = ng.Generator(1, extra_items=[(1, "Legacy Item", 10), (3001, "ignored dup id", 1)])
        ids = [i[0] for i in gen.items]
        assert 1 in ids and ids.count(3001) == 1
        assert dict((i[0], i[1]) for i in gen.items)[3001] != "ignored dup id"


# --------------------------------------------------------------------------
# defects
# --------------------------------------------------------------------------


def expected(n, rate):
    return int(n * rate + 0.5)


class TestDefects:
    def test_null_business_key_on_every_eligible_table(self):
        result = init(null_business_key=0.1)
        for table, base in [
            (ng.T_MEMBERSHIPS, SCALE.memberships), (ng.T_CERTIFICATIONS, SCALE.certifications),
            (ng.T_TRANSACTIONS, SCALE.transactions), (ng.T_LINES, SCALE.lines),
        ]:
            key = ng.BUSINESS_KEY[table]
            nulls = [r for r in result.data[table] if r[key] is None]
            (entry,) = by_defect(result, "null_business_key", table)
            assert len(nulls) == entry["row_count"] == expected(base, 0.1)
            assert None not in entry["business_keys"]  # original keys are recorded

    def test_customers_never_get_key_defects(self):
        result = init(null_business_key=1.0, duplicate_business_key=1.0)
        assert by_defect(result, "null_business_key", ng.T_CUSTOMERS) == []
        assert by_defect(result, "duplicate_business_key", ng.T_CUSTOMERS) == []
        keys = [r["customer_internal_id"] for r in result.data[ng.T_CUSTOMERS]]
        assert None not in keys and len(set(keys)) == len(keys)

    def test_duplicate_business_key(self):
        result = init(duplicate_business_key=0.1)
        for table, base in [(ng.T_MEMBERSHIPS, SCALE.memberships), (ng.T_LINES, SCALE.lines)]:
            key = ng.BUSINESS_KEY[table]
            (entry,) = by_defect(result, "duplicate_business_key", table)
            n = expected(base, 0.1)
            assert entry["row_count"] == n
            assert len(result.data[table]) == base + n
            keys = [r[key] for r in result.data[table]]
            assert sorted(k for k in set(keys) if keys.count(k) > 1) == sorted(entry["business_keys"])

    def test_negative_amount(self):
        result = init(negative_amount=0.1)
        (entry,) = by_defect(result, "negative_amount")
        negative = [r["transaction_line_id"] for r in result.data[ng.T_LINES] if r["amount"] < 0]
        assert len(negative) == entry["row_count"] == expected(SCALE.lines, 0.1)
        assert sorted(negative) == sorted(entry["business_keys"])

    def test_amount_mismatch(self):
        result = init(amount_mismatch=0.1)
        (entry,) = by_defect(result, "amount_mismatch")
        bad = [r["transaction_line_id"] for r in result.data[ng.T_LINES] if r["amount"] != r["quantity"] * r["rate"]]
        assert sorted(bad) == sorted(entry["business_keys"])
        assert len(bad) == expected(SCALE.lines, 0.1)

    def test_invalid_enum_values_are_outside_the_valid_sets(self):
        result = init(invalid_enum=0.2)
        valid = {
            "membership_status": set(ng.VALID_MEMBERSHIP_STATUS),
            "certification_type": set(ng.VALID_CERT_TYPE),
            "type": set(ng.VALID_TXN_TYPE),
        }
        for table, column in ng.ENUM_DEFECT_TARGETS:
            (entry,) = by_defect(result, "invalid_enum", table)
            bad = [r for r in result.data[table] if r[column] not in valid[column]]
            assert len(bad) == entry["row_count"] > 0
            assert entry["column"] == column
            assert sum(entry["values"].values()) == entry["row_count"]

    def test_end_before_start(self):
        result = init(end_before_start=0.2)
        bad_m = [r for r in result.data[ng.T_MEMBERSHIPS] if r["end_date"] < r["start_date"]]
        bad_c = [r for r in result.data[ng.T_CERTIFICATIONS] if r["certification_end_date"] < r["certification_start_date"]]
        (em,) = by_defect(result, "end_before_start", ng.T_MEMBERSHIPS)
        (ec,) = by_defect(result, "end_before_start", ng.T_CERTIFICATIONS)
        assert len(bad_m) == em["row_count"] == expected(SCALE.memberships, 0.2)
        assert len(bad_c) == ec["row_count"] == expected(SCALE.certifications, 0.2)

    def test_orphan_lines(self):
        result = init(orphan_lines=0.1)
        txns = {r["transaction_internal_id"] for r in result.data[ng.T_TRANSACTIONS]}
        (entry,) = by_defect(result, "orphan_lines")
        orphans = [r["transaction_line_id"] for r in result.data[ng.T_LINES] if r["transaction_internal_id"] not in txns]
        assert sorted(orphans) == sorted(entry["business_keys"])
        assert len(orphans) == expected(SCALE.lines, 0.1)

    def test_customer_defects_leave_primary_key_alone(self):
        result = init(customer_null_company_name=0.2, customer_malformed_email=0.2, customer_updated_before_created=0.2)
        customers = result.data[ng.T_CUSTOMERS]
        ids = [r["customer_internal_id"] for r in customers]
        assert None not in ids and len(set(ids)) == len(ids)
        (nulls,) = by_defect(result, "customer_null_company_name")
        (mal,) = by_defect(result, "customer_malformed_email")
        (dates,) = by_defect(result, "customer_updated_before_created")
        assert len([r for r in customers if r["company_name"] is None]) == nulls["row_count"] == expected(SCALE.customers, 0.2)
        assert len([r for r in customers if r["email"] is not None and not EMAIL_RE.match(r["email"])]) == mal["row_count"]
        assert len([r for r in customers if r["updated_date"] < r["created_date"]]) == dates["row_count"]
        assert nulls["observable_in_bronze"] is True
        assert dates["observable_in_bronze"] is False

    def test_defect_row_sets_are_disjoint_by_default(self):
        result = init(negative_amount=0.2, amount_mismatch=0.2, orphan_lines=0.2, null_business_key=0.2)
        sets = [set(d["business_keys"]) for d in result.manifest["defects"] if d["table"] == ng.T_LINES]
        assert len(sets) == 4
        for i in range(len(sets)):
            for j in range(i + 1, len(sets)):
                assert not (sets[i] & sets[j])

    def test_overlap_allowed_when_configured(self):
        result = init(allow_overlap=True, negative_amount=1.0, amount_mismatch=1.0)
        assert by_defect(result, "negative_amount")[0]["row_count"] == SCALE.lines

    def test_manifest_counts_match_rows_emitted(self):
        result = init(duplicate_business_key=0.1)
        assert result.manifest["rows_emitted"][ng.T_LINES] == len(result.data[ng.T_LINES])

    def test_defects_are_deterministic(self):
        rates = dict(null_business_key=0.1, invalid_enum=0.1, negative_amount=0.1)
        assert init(seed=3, **rates).manifest == init(seed=3, **rates).manifest

    def test_zero_rate_is_a_no_op(self):
        assert init(negative_amount=0.0).data == init().data


# --------------------------------------------------------------------------
# increment
# --------------------------------------------------------------------------


@pytest.fixture()
def existing():
    return init(seed=1).data


class TestIncrement:
    def test_new_rows_are_stamped_with_the_batch_date_and_every_row_moves_updated_date_forward(self, existing):
        result = increment(existing)
        old_keys = {t: {r[ng.BUSINESS_KEY[t]] for r in existing[t]} for t in ng.INCREMENTAL_TABLES}
        for table in ng.INCREMENTAL_TABLES:
            rows = result.data[table]
            assert rows
            assert all(ng._as_date(r["updated_date"]) >= NEXT for r in rows)
            new_rows = [r for r in rows if r[ng.BUSINESS_KEY[table]] not in old_keys[table]]
            assert new_rows and {ng._as_date(r["created_date"]) for r in new_rows} == {NEXT}

    def test_updated_versions_keep_their_created_date(self, existing):
        result = increment(existing)
        for table in ng.INCREMENTAL_TABLES:
            key = ng.BUSINESS_KEY[table]
            created = {r[key]: r["created_date"] for r in existing[table]}
            versions = [r for r in result.data[table] if r[key] in created]
            assert versions
            assert all(r["created_date"] == created[r[key]] for r in versions)

    def test_watermark_and_next_batch_date_follow_updated_date_not_created_date(self):
        rows = init(seed=1).data
        moved = {t: [dict(r, updated_date=ng._stamp(t, dt.date(2026, 9, 1))) for r in rows[t]] for t in ng.TABLES}
        assert ng.next_batch_date(moved) == dt.date(2026, 9, 1) + dt.timedelta(days=ng.INCREMENT_GAP_DAYS)
        result = ng.generate_increment(ng.Generator(2), moved, SCALE, dt.date(2026, 10, 1), cfg())
        assert result.manifest["increment"]["current_watermarks"][ng.T_MEMBERSHIPS] == "2026-09-01"

    def test_rejects_batch_date_not_after_watermark(self, existing):
        with pytest.raises(ValueError, match="must be later"):
            increment(existing, batch=BASE)

    def test_next_batch_date_is_latest_plus_gap(self, existing):
        assert ng.next_batch_date(existing) == BASE + dt.timedelta(days=ng.INCREMENT_GAP_DAYS) == NEXT

    def test_new_keys_continue_after_existing_ones(self, existing):
        result = increment(existing)
        for table in (ng.T_CUSTOMERS,):
            old_max = max(r[ng.BUSINESS_KEY[table]] for r in existing[table])
            assert min(r[ng.BUSINESS_KEY[table]] for r in result.data[table]) == old_max + 1

    def test_updated_versions_reuse_existing_keys(self, existing):
        result = increment(existing)
        old_keys = {r["membership_internal_id"] for r in existing[ng.T_MEMBERSHIPS]}
        new_rows = result.data[ng.T_MEMBERSHIPS]
        reused = [r for r in new_rows if r["membership_internal_id"] in old_keys]
        assert len(reused) == result.manifest["increment"]["updated_versions"][ng.T_MEMBERSHIPS] > 0
        assert all(ng._as_date(r["updated_date"]) >= NEXT for r in reused)

    def test_customers_upsert_existing_ids_only(self, existing):
        result = increment(existing)
        old_ids = {r["customer_internal_id"] for r in existing[ng.T_CUSTOMERS]}
        assert {r["customer_internal_id"] for r in result.upserts[ng.T_CUSTOMERS]} <= old_ids
        assert not ({r["customer_internal_id"] for r in result.data[ng.T_CUSTOMERS]} & old_ids)
        assert all(r["updated_date"] == NEXT for r in result.upserts[ng.T_CUSTOMERS])

    def test_referential_integrity_of_new_rows(self, existing):
        result = increment(existing)
        customers = {r["customer_internal_id"] for r in existing[ng.T_CUSTOMERS]} | {
            r["customer_internal_id"] for r in result.data[ng.T_CUSTOMERS]
        }
        for table in (ng.T_MEMBERSHIPS, ng.T_CERTIFICATIONS, ng.T_TRANSACTIONS):
            assert {r["customer_internal_id"] for r in result.data[table]} <= customers
        txns = {r["transaction_internal_id"] for r in existing[ng.T_TRANSACTIONS]} | {
            r["transaction_internal_id"] for r in result.data[ng.T_TRANSACTIONS]
        }
        assert {r["transaction_internal_id"] for r in result.data[ng.T_LINES]} <= txns

    def test_updated_lines_keep_amount_consistent(self, existing):
        for r in increment(existing).data[ng.T_LINES]:
            assert r["amount"] == r["quantity"] * r["rate"]

    def test_deterministic_given_same_state(self, existing):
        assert increment(existing, seed=5).manifest == increment(existing, seed=5).manifest

    def test_late_arriving_rows_are_backdated_new_rows(self, existing):
        result = increment(existing, late_arriving=1.0)
        watermark = BASE
        for table in ng.INCREMENTAL_TABLES:
            (entry,) = by_defect(result, "late_arriving", table)
            assert entry["current_watermark"] == str(watermark)
            late_keys = set(entry["business_keys"])
            key = ng.BUSINESS_KEY[table]
            old_keys = {r[key] for r in existing[table]}
            assert late_keys and not (late_keys & old_keys)  # new keys only
            for r in (r for r in result.data[table] if r[key] in late_keys):
                assert ng._as_date(r["created_date"]) < watermark
                assert ng._as_date(r["updated_date"]) < watermark
                assert ng._as_date(r["created_date"]) <= ng._as_date(r["updated_date"])

    def test_late_arriving_never_applies_to_init_or_customers(self):
        assert by_defect(init(late_arriving=1.0), "late_arriving") == []
        assert by_defect(increment(init(seed=1).data, late_arriving=1.0), "late_arriving", ng.T_CUSTOMERS) == []

    def test_late_rows_use_an_existing_older_snapshot_when_there_is_one(self):
        base = ng.generate_init(ng.Generator(1), SCALE, BASE, cfg()).data
        second = ng.generate_increment(ng.Generator(2), base, SCALE, NEXT, cfg()).data
        merged = {t: base[t] + second[t] for t in ng.TABLES}
        watermark = max(ng._as_date(r["updated_date"]) for r in merged[ng.T_MEMBERSHIPS])
        snapshot_dates = {str(ng._as_date(r["updated_date"])) for r in merged[ng.T_MEMBERSHIPS]}
        result = increment(merged, seed=3, batch=dt.date(2026, 9, 30), late_arriving=1.0)
        (entry,) = by_defect(result, "late_arriving", ng.T_MEMBERSHIPS)
        assert entry["current_watermark"] == str(watermark)
        assert {r["created_date"] for r in entry["rows"]} <= snapshot_dates
        assert all(r["updated_date"] < str(watermark) for r in entry["rows"])

    def test_duplicates_in_increment_share_a_snapshot(self, existing):
        result = increment(existing, duplicate_business_key=0.2)
        (entry,) = by_defect(result, "duplicate_business_key", ng.T_TRANSACTIONS)
        rows = [r for r in result.data[ng.T_TRANSACTIONS] if r["transaction_internal_id"] in set(entry["business_keys"])]
        assert len({ng._as_date(r["updated_date"]) for r in rows}) == 1  # the watermark date is shared

    def test_orphan_ids_never_collide_with_real_transactions(self, existing):
        result = increment(existing, orphan_lines=0.2)
        (entry,) = by_defect(result, "orphan_lines")
        all_txns = {r["transaction_internal_id"] for r in existing[ng.T_TRANSACTIONS] + result.data[ng.T_TRANSACTIONS]}
        orphans = [r for r in result.data[ng.T_LINES] if r["transaction_line_id"] in set(entry["business_keys"])]
        assert orphans and all(r["transaction_internal_id"] not in all_txns for r in orphans)


class TestSchemaDrift:
    def test_no_drift_by_default(self, existing):
        result = increment(existing)
        assert result.manifest["schema_drift"] is None
        assert all(ng.DRIFT_COLUMN not in r for r in result.data[ng.T_TRANSACTIONS])

    def test_drift_flag_adds_column_and_records_event(self, existing):
        result = increment(existing, apply_drift=True)
        drift = result.manifest["schema_drift"]
        assert drift["table"] == ng.T_TRANSACTIONS and drift["column"] == "custbody_region"
        assert "ADD COLUMN IF NOT EXISTS" in drift["sql"]
        assert all(r[ng.DRIFT_COLUMN] in ng.REGIONS for r in result.data[ng.T_TRANSACTIONS])
        # only one table drifts
        for table in ng.TABLES:
            if table != ng.T_TRANSACTIONS:
                assert all(ng.DRIFT_COLUMN not in r for r in result.data[table])

    def test_later_increments_keep_populating_the_column_without_a_new_event(self, existing):
        result = increment(existing, region_present=True)
        assert result.manifest["schema_drift"] is None
        assert all(ng.DRIFT_COLUMN in r for r in result.data[ng.T_TRANSACTIONS])

    def test_columns_for_includes_region_only_when_present(self):
        assert ng.DRIFT_COLUMN not in ng.columns_for(ng.T_TRANSACTIONS, False)
        assert ng.columns_for(ng.T_TRANSACTIONS, True)[-1] == ng.DRIFT_COLUMN
        assert ng.columns_for(ng.T_LINES, True) == ng.COLUMNS[ng.T_LINES]


class TestManifest:
    def test_manifest_is_json_serializable_and_complete(self, existing):
        result = increment(existing, negative_amount=0.1, late_arriving=0.5, apply_drift=True)
        text = json.dumps(result.manifest, default=ng._json_default)
        loaded = json.loads(text)
        for field in ("seed", "mode", "batch_date", "config_hash", "config", "rows_emitted", "defects", "schema_drift", "pre_existing"):
            assert field in loaded
        assert loaded["pre_existing"]["customers_dates_not_in_source_columns"]["columns"] == ["created_date", "updated_date"]

    def test_every_defect_entry_has_ground_truth_fields(self, existing):
        result = increment(existing, negative_amount=0.1, null_business_key=0.1, invalid_enum=0.1)
        for d in result.manifest["defects"]:
            assert d["row_count"] == len(d["business_keys"]) > 0
            assert {"defect", "table", "business_keys", "observable_in_bronze"} <= set(d)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


class TestCli:
    def test_init_dry_run_writes_manifest_and_needs_no_database(self, tmp_path, capsys):
        out = tmp_path / "m.json"
        assert ng.main(["--init", "--seed", "1", "--scale", "0.01", "--dry-run", "--manifest", str(out)]) == 0
        manifest = json.loads(out.read_text())
        assert manifest["dry_run"] is True and manifest["mode"] == "init"
        assert manifest["backup"]["performed"] is False
        assert manifest["rows_emitted"][ng.T_CUSTOMERS] == SCALE.customers
        assert "DRY RUN" in capsys.readouterr().out

    def _patch_writer(self, monkeypatch, existing, backup):
        calls = []
        monkeypatch.setattr(pg_writer, "connect", lambda profile, auth="cli": FakeConn())
        monkeypatch.setattr(pg_writer, "read_live_columns", lambda conn: {t: ng.COLUMNS[t] for t in ng.TABLES})
        monkeypatch.setattr(pg_writer, "read_existing", lambda conn, live: existing)
        monkeypatch.setattr(pg_writer, "create_backup", lambda conn, profile, **kw: calls.append("backup") or backup)
        monkeypatch.setattr(pg_writer, "load_increment", lambda *a, **k: calls.append("load"))
        return calls

    def test_increment_backs_up_before_loading(self, existing, tmp_path, monkeypatch):
        calls = self._patch_writer(monkeypatch, existing, {"performed": True, "verified": True})
        out = tmp_path / "m.json"
        assert ng.main(["--increment", "--seed", "2", "--scale", "0.01", "--manifest", str(out)]) == 0
        assert calls == ["backup", "load"]
        assert json.loads(out.read_text())["backup"]["verified"] is True

    def test_increment_refuses_to_load_without_a_verified_backup(self, existing, tmp_path, monkeypatch):
        calls = self._patch_writer(monkeypatch, existing, {"performed": True, "verified": False})
        with pytest.raises(RuntimeError, match="no verified backup"):
            ng.main(["--increment", "--seed", "2", "--scale", "0.01", "--manifest", str(tmp_path / "m.json")])
        assert calls == ["backup"]

    def test_increment_dry_run_takes_no_backup_and_loads_nothing(self, existing, tmp_path, monkeypatch):
        calls = self._patch_writer(monkeypatch, existing, {"performed": True, "verified": True})
        assert ng.main(["--increment", "--seed", "2", "--scale", "0.01", "--dry-run", "--manifest", str(tmp_path / "m.json")]) == 0
        assert calls == []

    def test_schema_drift_flag_requires_increment(self, capsys):
        assert ng.main(["--init", "--seed", "1", "--apply-schema-drift", "--dry-run"]) == 2

    def test_mode_is_required(self):
        with pytest.raises(SystemExit):
            ng.build_parser().parse_args(["--seed", "1"])

    def test_modes_are_mutually_exclusive(self):
        with pytest.raises(SystemExit):
            ng.build_parser().parse_args(["--init", "--increment", "--seed", "1"])


# --------------------------------------------------------------------------
# pg_writer against a recording fake connection
# --------------------------------------------------------------------------


class FakeCopy:
    def __init__(self, cursor, sql):
        self.cursor, self.sql, self.rows = cursor, sql, []

    def __enter__(self):
        if self.cursor.fail_on_copy:
            raise RuntimeError("boom")
        self.cursor.copies.append(self)
        return self

    def __exit__(self, *exc):
        return False

    def write_row(self, row):
        self.rows.append(row)


class FakeCursor:
    def __init__(self, fail_on_copy=False):
        self.executed, self.many, self.copies, self.fail_on_copy = [], [], [], fail_on_copy
        self.params, self.count = [], 5

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append(sql)
        self.params.append(params)

    def fetchone(self):
        return {"n": self.count}

    def executemany(self, sql, rows):
        self.many.append((sql, list(rows)))

    def copy(self, sql):
        return FakeCopy(self, sql)


class FakeConn:
    def __init__(self, fail_on_copy=False):
        self.cur = FakeCursor(fail_on_copy)
        self.commits = self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


class TestBackupGuard:
    @pytest.mark.parametrize("info", [{}, None, {"performed": False}, {"performed": True, "verified": False}])
    def test_refuses_without_a_verified_backup(self, info):
        with pytest.raises(RuntimeError, match="no verified backup"):
            pg_writer.require_backup(info)

    def test_accepts_a_verified_backup(self):
        pg_writer.require_backup({"performed": True, "verified": True})


class TestCreateBackup:
    def test_copies_every_table_verifies_counts_and_uses_bindable_sql(self, monkeypatch):
        def no_branch(*args, **kwargs):
            raise RuntimeError("quota reached")

        monkeypatch.setattr(pg_writer, "_cli_json", no_branch)
        conn = FakeConn()
        info = pg_writer.create_backup(conn)
        sql = conn.cur.executed
        assert info["verified"] is True and info["performed"] is True
        assert info["branch"]["status"] == "failed"  # branch is best effort
        assert info["schema"].startswith("netsuite_backup_")
        assert sum(1 for s in sql if s.startswith("CREATE TABLE")) == len(ng.TABLES)
        assert conn.commits == 1
        # psycopg 3 cannot bind a tuple to `IN %s`; the anomaly queries must use ANY/ALL with a list
        anomaly = [s for s in sql if "membership_status" in s or "certification_type" in s]
        assert anomaly and all(" IN %s" not in s and "<> ALL(%s)" in s for s in anomaly)
        assert all(not isinstance(p, tuple) or all(not isinstance(x, tuple) for x in p) for p in conn.cur.params if p)

    def test_unverified_when_counts_differ(self, monkeypatch):
        monkeypatch.setattr(pg_writer, "_cli_json", lambda *a, **k: {})
        conn = FakeConn()
        counts = iter(range(1, 1000))
        conn.cur.fetchone = lambda: {"n": next(counts)}
        assert pg_writer.create_backup(conn)["verified"] is False


class TestLoadInit:
    def test_truncates_first_then_copies_every_table_in_one_transaction(self):
        conn, data = FakeConn(), init().data
        pg_writer.load_init(conn, data)
        assert conn.cur.executed[0].startswith("TRUNCATE")
        for table in ng.TABLES:
            assert f'"netsuite"."{table}"' in conn.cur.executed[0]
        copied = sum(len(c.rows) for c in conn.cur.copies)
        assert copied == sum(len(v) for v in data.values())
        assert conn.commits == 1 and conn.rollbacks == 0

    def test_copies_in_batches(self, monkeypatch):
        monkeypatch.setattr(pg_writer, "COPY_BATCH", 100)
        conn = FakeConn()
        pg_writer.load_init(conn, init().data)
        lines = [c for c in conn.cur.copies if "netsuite_transaction_lines" in c.sql]
        assert [len(c.rows) for c in lines] == [100, 100, 100]

    def test_identifiers_are_quoted(self):
        conn = FakeConn()
        pg_writer.load_init(conn, init().data)
        txn_copy = next(c for c in conn.cur.copies if "netsuite_transactions" in c.sql)
        assert '"date"' in txn_copy.sql

    def test_rolls_back_on_failure(self):
        conn = FakeConn(fail_on_copy=True)
        with pytest.raises(RuntimeError, match="boom"):
            pg_writer.load_init(conn, init().data)
        assert conn.rollbacks == 1 and conn.commits == 0


class TestLoadIncrement:
    def test_upserts_customers_and_appends_the_rest(self, existing):
        result = increment(existing)
        conn = FakeConn()
        pg_writer.load_increment(conn, result)
        sql, rows = conn.cur.many[0]
        assert "ON CONFLICT" in sql and "DO UPDATE SET" in sql
        assert len(rows) == len(result.upserts[ng.T_CUSTOMERS])
        assert not any("TRUNCATE" in s for s in conn.cur.executed)
        assert not any("ALTER TABLE" in s for s in conn.cur.executed)
        assert conn.commits == 1

    def test_alter_table_only_with_drift_flag_and_before_any_copy(self, existing):
        result = increment(existing, apply_drift=True)
        conn = FakeConn()
        pg_writer.load_increment(conn, result, apply_drift=True)
        assert any("ADD COLUMN IF NOT EXISTS" in s and "custbody_region" in s for s in conn.cur.executed)
        txn_copy = next(c for c in conn.cur.copies if "netsuite_transactions" in c.sql)
        assert "custbody_region" in txn_copy.sql

    def test_region_column_not_copied_when_absent(self, existing):
        conn = FakeConn()
        pg_writer.load_increment(conn, increment(existing))
        txn_copy = next(c for c in conn.cur.copies if "netsuite_transactions" in c.sql)
        assert "custbody_region" not in txn_copy.sql

    def test_rolls_back_on_failure(self, existing):
        conn = FakeConn(fail_on_copy=True)
        with pytest.raises(RuntimeError):
            pg_writer.load_increment(conn, increment(existing))
        assert conn.rollbacks == 1 and conn.commits == 0


class TestItemsFromRows:
    def test_collects_distinct_items(self):
        rows = [
            {"item_id": 1, "item_name": "A", "rate": 10},
            {"item_id": 1, "item_name": "A", "rate": 10},
            {"item_id": 2, "item_name": None, "rate": 5},
        ]
        assert pg_writer.items_from_rows(rows) == [(1, "A", 10)]
