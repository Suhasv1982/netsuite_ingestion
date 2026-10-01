"""Synthetic NetSuite data generator for the netsuite-sample Lakebase project.

Writes to schema `netsuite` (same five tables/columns as the live schema):
netsuite_customers, netsuite_memberships, netsuite_certifications,
netsuite_transactions, netsuite_transaction_lines.

Modes
-----
--init        Back up the current data (Lakebase branch + verified copy in a
              netsuite_backup* schema), then truncate the five tables and load
              a full base snapshot (`created_date` = `updated_date` = --batch-date).
--increment   Append a later snapshot (`updated_date` >= --batch-date): new rows,
              plus new *versions* of existing rows (memberships,
              certifications, transactions, lines) and in-place upserts of
              customers (customers has a primary key and is a FullLoad).

There is no `last_modified` column in the source. The pipeline's watermark is
`updated_date` (source_table_def.watermark_col; bronze loads one snapshot per
distinct date, silver sequences by it). New rows get `created_date` =
`updated_date` = --batch-date; an updated version keeps its `created_date` and
gets a later `updated_date`; late rows are new rows whose `updated_date` is
older than the table's current maximum `updated_date`.

Defects are injected from a YAML config, every rate defaults to 0 (off). The
run writes a JSON manifest listing exactly which rows carry which defect.

The generator itself (this file, minus main()) is pure: it needs no database.
Database access lives in pg_writer.py.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path

TOOL_VERSION = "1.0.0"

SCHEMA = "netsuite"
T_CUSTOMERS = "netsuite_customers"
T_MEMBERSHIPS = "netsuite_memberships"
T_CERTIFICATIONS = "netsuite_certifications"
T_TRANSACTIONS = "netsuite_transactions"
T_LINES = "netsuite_transaction_lines"
TABLES = [T_CUSTOMERS, T_MEMBERSHIPS, T_CERTIFICATIONS, T_TRANSACTIONS, T_LINES]
INCREMENTAL_TABLES = [T_MEMBERSHIPS, T_CERTIFICATIONS, T_TRANSACTIONS, T_LINES]

COLUMNS = {
    T_CUSTOMERS: ["customer_internal_id", "company_name", "email", "created_date", "updated_date"],
    T_MEMBERSHIPS: [
        "membership_internal_id", "customer_internal_id", "membership_type", "start_date",
        "end_date", "membership_status", "created_date", "updated_date",
    ],
    T_CERTIFICATIONS: [
        "certification_internal_id", "customer_internal_id", "certification_type",
        "certification_start_date", "certification_end_date", "created_date", "updated_date",
    ],
    T_TRANSACTIONS: [
        "transaction_internal_id", "tranid", "customer_internal_id", "type", "date",
        "created_date", "updated_date",
    ],
    T_LINES: [
        "transaction_line_id", "transaction_internal_id", "line_number", "item_id", "item_name",
        "quantity", "rate", "amount", "created_date", "updated_date",
    ],
}
BUSINESS_KEY = {
    T_CUSTOMERS: "customer_internal_id",
    T_MEMBERSHIPS: "membership_internal_id",
    T_CERTIFICATIONS: "certification_internal_id",
    T_TRANSACTIONS: "transaction_internal_id",
    T_LINES: "transaction_line_id",
}
# Bronze reads only the columns listed in aidq_metadata.source_columns, which
# omits customers.created_date/updated_date -- pre-existing drift we do NOT fix.
BRONZE_DROPS = {T_CUSTOMERS: {"created_date", "updated_date"}}

ID_START = {T_CUSTOMERS: 1001, T_MEMBERSHIPS: 5000, T_CERTIFICATIONS: 8000, T_TRANSACTIONS: 90001}
BASE_SNAPSHOT_DATE = dt.date(2026, 7, 11)
INCREMENT_GAP_DAYS = 21

VALID_MEMBERSHIP_STATUS = ("Active", "Suspended", "Expired")   # HARD DQ rule
VALID_CERT_TYPE = ("SCP", "CP")                                 # HARD DQ rule
VALID_TXN_TYPE = ("Cash Sale", "Invoice")                       # from current data
MEMBERSHIP_TYPES = ("Professional", "Student", "Other")         # from current data
INVALID_ENUM_VALUES = {
    "membership_status": ["NA", "Pending", "active", "ACTIVE ", "Cancelled", ""],
    "certification_type": ["DB", "XX", "scp", "Unknown", ""],
    "type": ["Credit Memo", "invoice", "Refund", "N/A", ""],
}
ENUM_DEFECT_TARGETS = [
    (T_MEMBERSHIPS, "membership_status"),
    (T_TRANSACTIONS, "type"),
    (T_CERTIFICATIONS, "certification_type"),
]
MALFORMED_EMAILS = ["{u}", "{u}@", "@{d}", "{u}@@{d}", "{u} x@{d}", "{u}@{d}."]
REGIONS = ("NA", "EMEA", "APAC", "LATAM")
DRIFT_TABLE = T_TRANSACTIONS
DRIFT_COLUMN = "custbody_region"

DEFAULT_ITEMS = [
    (3001 + i, name, rate)
    for i, (name, rate) in enumerate(
        [
            ("Annual Membership Fee", 250), ("Certification Exam Fee", 400), ("Study Guide", 60),
            ("Webinar Pass", 45), ("Conference Ticket", 900), ("Training Workshop", 650),
            ("Practice Test Bundle", 120), ("Renewal Fee", 175), ("Late Fee", 25),
            ("Chapter Dues", 80), ("Exam Retake", 300), ("Digital Badge", 15),
            ("Mentorship Program", 500), ("Journal Subscription", 95), ("Career Coaching", 350),
            ("Group Discount Pack", 1200), ("Sponsorship Package", 2500), ("Lab Kit", 210),
            ("Handbook", 40), ("Recertification Fee", 275),
        ]
    )
]

DEFECT_NAMES = [
    "null_business_key",
    "duplicate_business_key",
    "negative_amount",
    "amount_mismatch",
    "invalid_enum",
    "end_before_start",
    "orphan_lines",
    "customer_null_company_name",
    "customer_malformed_email",
    "customer_updated_before_created",
    "late_arriving",
]


# --------------------------------------------------------------------------
# config
# --------------------------------------------------------------------------


@dataclass
class DefectConfig:
    """Rates are fractions (0..1) of the eligible rows emitted in this run."""

    allow_overlap: bool = False
    rates: dict = field(default_factory=lambda: {name: 0.0 for name in DEFECT_NAMES})

    @classmethod
    def from_dict(cls, data: dict | None) -> "DefectConfig":
        data = dict(data or {})
        allow_overlap = bool(data.pop("allow_overlap", False))
        rates_in = data.pop("rates", {}) or {}
        if data:
            raise ValueError(f"Unknown config keys: {sorted(data)}")
        unknown = set(rates_in) - set(DEFECT_NAMES)
        if unknown:
            raise ValueError(f"Unknown defect names: {sorted(unknown)}")
        rates = {name: 0.0 for name in DEFECT_NAMES}
        for name, value in rates_in.items():
            value = float(value)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"Rate for {name} must be between 0 and 1, got {value}")
            rates[name] = value
        return cls(allow_overlap=allow_overlap, rates=rates)

    def to_dict(self) -> dict:
        return {"allow_overlap": self.allow_overlap, "rates": dict(self.rates)}

    def digest(self) -> str:
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True).encode()).hexdigest()[:16]


def resolve_config_path(path: str) -> str:
    """The path as given if it exists, else the same relative path next to this file (a job task's working
    directory is not the repo root, so the daily job passes just `increment_daily.yaml`)."""
    if Path(path).exists() or Path(path).is_absolute():
        return path
    beside = Path(__file__).resolve().parent / path
    return str(beside) if beside.exists() else path


def load_config(path: str | None) -> DefectConfig:
    if not path:
        return DefectConfig()
    path = resolve_config_path(path)
    import yaml  # local import: only needed when a config file is given

    with open(path, encoding="utf-8") as fh:
        return DefectConfig.from_dict(yaml.safe_load(fh) or {})


@dataclass
class ScaleConfig:
    customers: int = 2000
    memberships: int = 2500
    certifications: int = 2000
    transactions: int = 10000
    lines: int = 30000
    increment_new_pct: float = 0.05     # new rows, as a share of the base counts
    increment_update_pct: float = 0.10  # existing keys re-emitted as new versions

    def scaled(self, factor: float) -> "ScaleConfig":
        def s(n: int) -> int:
            return max(1, int(round(n * factor)))

        return ScaleConfig(
            s(self.customers), s(self.memberships), s(self.certifications), s(self.transactions),
            max(s(self.lines), s(self.transactions)), self.increment_new_pct, self.increment_update_pct,
        )


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _as_date(value):
    return value.date() if isinstance(value, dt.datetime) else value


def _stamp(table: str, day: dt.date):
    """created/updated value in the column's native type (lines are timestamps)."""
    return dt.datetime.combine(day, dt.time.min) if table == T_LINES else day


def _add_days(value, days: int):
    return value + dt.timedelta(days=days)


def _latest_by_key(rows: list[dict], key: str) -> dict:
    """Latest version (by created_date, then updated_date) of every non-null key."""
    best: dict = {}
    floor = dt.date.min
    for row in rows:
        k = row.get(key)
        if k is None:
            continue
        version = (_as_date(row.get("created_date")) or floor, _as_date(row.get("updated_date")) or floor)
        current = best.get(k)
        if current is None or version > current[0]:
            best[k] = (version, row)
    return {k: v[1] for k, v in best.items()}


def _max_int(rows: list[dict], key: str, default: int) -> int:
    values = [r[key] for r in rows if isinstance(r.get(key), int)]
    return max(values) if values else default


# --------------------------------------------------------------------------
# base row generation
# --------------------------------------------------------------------------


class Generator:
    def __init__(self, seed: int, extra_items: list[tuple] | None = None):
        from faker import Faker

        self.seed = seed
        self.rng = random.Random(seed)
        self.fake = Faker()
        self.fake.seed_instance(seed)
        items = {i[0]: i for i in DEFAULT_ITEMS}
        for item in extra_items or []:
            items.setdefault(item[0], item)
        self.items = [items[k] for k in sorted(items)]

    # -- rows -------------------------------------------------------------

    def customers(self, n: int, start_id: int, day: dt.date) -> list[dict]:
        rows = []
        for i in range(n):
            rows.append(
                {
                    "customer_internal_id": start_id + i,
                    "company_name": self.fake.company(),
                    "email": self.fake.email(),
                    "created_date": day,
                    "updated_date": day,
                }
            )
        return rows

    def memberships(self, n: int, start_id: int, customer_ids: list[int], day: dt.date) -> list[dict]:
        rows = []
        for i in range(n):
            start = day - dt.timedelta(days=self.rng.randint(0, 3 * 365))
            end = start + dt.timedelta(days=self.rng.randint(180, 730))
            rows.append(
                {
                    "membership_internal_id": start_id + i,
                    "customer_internal_id": self.rng.choice(customer_ids),
                    "membership_type": self.rng.choices(MEMBERSHIP_TYPES, weights=[6, 4, 1])[0],
                    "start_date": start,
                    "end_date": end,
                    "membership_status": self.rng.choices(VALID_MEMBERSHIP_STATUS, weights=[45, 40, 15])[0],
                    "created_date": day,
                    "updated_date": day,
                }
            )
        return rows

    def certifications(self, n: int, start_id: int, customer_ids: list[int], day: dt.date) -> list[dict]:
        cap = min(day, dt.date(2026, 12, 31))  # HARD rule: start date < 2027-01-01
        rows = []
        for i in range(n):
            start = cap - dt.timedelta(days=self.rng.randint(0, 4 * 365))
            end = start + dt.timedelta(days=self.rng.randint(365, 1095))
            rows.append(
                {
                    "certification_internal_id": start_id + i,
                    "customer_internal_id": self.rng.choice(customer_ids),
                    "certification_type": self.rng.choices(VALID_CERT_TYPE, weights=[65, 35])[0],
                    "certification_start_date": start,
                    "certification_end_date": end,
                    "created_date": day,
                    "updated_date": day,
                }
            )
        return rows

    def transactions(self, n: int, start_id: int, customer_ids: list[int], day: dt.date) -> list[dict]:
        rows = []
        for i in range(n):
            tid = start_id + i
            rows.append(
                {
                    "transaction_internal_id": tid,
                    "tranid": f"TRID-{tid}",
                    "customer_internal_id": self.rng.choice(customer_ids),
                    "type": self.rng.choices(VALID_TXN_TYPE, weights=[6, 4])[0],
                    "date": day - dt.timedelta(days=self.rng.randint(0, 60)),
                    "created_date": day,
                    "updated_date": day,
                }
            )
        return rows

    def lines(self, transaction_ids: list[int], total: int, day: dt.date) -> list[dict]:
        """Exactly `total` lines (>= one per transaction), ids `<txn>_<n>`."""
        total = max(total, len(transaction_ids))
        counts = {tid: 1 for tid in transaction_ids}
        for _ in range(total - len(transaction_ids)):
            counts[self.rng.choice(transaction_ids)] += 1
        stamp = _stamp(T_LINES, day)
        rows = []
        for tid in transaction_ids:
            for n in range(1, counts[tid] + 1):
                item_id, item_name, base_rate = self.rng.choice(self.items)
                quantity = self.rng.randint(1, 10)
                rate = base_rate
                rows.append(
                    {
                        "transaction_line_id": f"{tid}_{n}",
                        "transaction_internal_id": tid,
                        "line_number": n,
                        "item_id": item_id,
                        "item_name": item_name,
                        "quantity": quantity,
                        "rate": rate,
                        "amount": quantity * rate,
                        "created_date": stamp,
                        "updated_date": stamp,
                    }
                )
        return rows

    def region(self) -> str:
        return self.rng.choice(REGIONS)


# --------------------------------------------------------------------------
# manifest + defect injection
# --------------------------------------------------------------------------


@dataclass
class Context:
    mode: str                                   # "init" | "increment"
    batch_date: dt.date
    watermarks: dict = field(default_factory=dict)      # table -> current max updated_date
    snapshot_dates: dict = field(default_factory=dict)  # table -> sorted existing created dates
    new_rows: set = field(default_factory=set)          # id() of rows that are brand new


class Manifest:
    def __init__(self):
        self.defects: list[dict] = []

    def record(self, defect: str, table: str, keys: list, **extra) -> None:
        entry = {
            "defect": defect,
            "table": table,
            "row_count": len(keys),
            "business_keys": keys,
            "observable_in_bronze": True,
        }
        entry.update(extra)
        self.defects.append(entry)


class DefectInjector:
    def __init__(self, cfg: DefectConfig, rng: random.Random, ctx: Context, manifest: Manifest):
        self.cfg, self.rng, self.ctx, self.manifest = cfg, rng, ctx, manifest
        self.claimed: set[int] = set()

    def _rate(self, name: str) -> float:
        return self.cfg.rates.get(name, 0.0)

    def _pick(self, rows: list[dict], rate: float, eligible=lambda r: True) -> list[dict]:
        if rate <= 0:
            return []
        candidates = [r for r in rows if eligible(r) and (self.cfg.allow_overlap or id(r) not in self.claimed)]
        n = min(len(candidates), int(len(candidates) * rate + 0.5))
        chosen = self.rng.sample(candidates, n) if n else []
        if not self.cfg.allow_overlap:
            self.claimed.update(id(r) for r in chosen)
        return chosen

    # -- individual defects ----------------------------------------------

    def customer_defects(self, rows: list[dict]) -> None:
        tbl = T_CUSTOMERS
        picked = self._pick(rows, self._rate("customer_null_company_name"))
        for r in picked:
            r["company_name"] = None
        if picked:
            self.manifest.record("customer_null_company_name", tbl, [r["customer_internal_id"] for r in picked], column="company_name")

        picked = self._pick(rows, self._rate("customer_malformed_email"))
        for r in picked:
            user, _, domain = (r["email"] or "user@example.com").partition("@")
            r["email"] = self.rng.choice(MALFORMED_EMAILS).format(u=user, d=domain or "example.com")
        if picked:
            self.manifest.record("customer_malformed_email", tbl, [r["customer_internal_id"] for r in picked], column="email")

        picked = self._pick(rows, self._rate("customer_updated_before_created"))
        for r in picked:
            r["updated_date"] = _add_days(r["created_date"], -self.rng.randint(1, 30))
        if picked:
            self.manifest.record(
                "customer_updated_before_created", tbl, [r["customer_internal_id"] for r in picked],
                column="updated_date", observable_in_bronze=False,
                note="bronze drops customers.created_date/updated_date (not in source_columns)",
            )

    def late_arriving(self, table: str, rows: list[dict]) -> None:
        """New rows backdated to an older snapshot, with updated_date older than
        the table's current watermark, so a high-watermark load would miss them."""
        if self.ctx.mode != "increment":
            return
        watermark = self.ctx.watermarks.get(table)
        if watermark is None:
            return
        older = [d for d in self.ctx.snapshot_dates.get(table, []) if d < watermark]
        picked = self._pick(rows, self._rate("late_arriving"), eligible=lambda r: id(r) in self.ctx.new_rows)
        key = BUSINESS_KEY[table]
        detail = []
        for r in picked:
            created = self.rng.choice(older) if older else watermark - dt.timedelta(days=INCREMENT_GAP_DAYS)
            updated = min(created + dt.timedelta(days=self.rng.randint(0, 2)), watermark - dt.timedelta(days=1))
            r["created_date"], r["updated_date"] = _stamp(table, created), _stamp(table, updated)
            detail.append({"key": r[key], "created_date": str(created), "updated_date": str(updated)})
        if picked:
            self.manifest.record(
                "late_arriving", table, [d["key"] for d in detail],
                current_watermark=str(watermark), rows=detail,
                note="updated_date older than the current watermark (max updated_date); new keys, so only a high-watermark load misses them",
            )

    def invalid_enum(self, table: str, column: str, rows: list[dict]) -> None:
        picked = self._pick(rows, self._rate("invalid_enum"))
        counts: dict = {}
        for r in picked:
            value = self.rng.choice(INVALID_ENUM_VALUES[column])
            r[column] = value
            counts[value] = counts.get(value, 0) + 1
        if picked:
            self.manifest.record(
                "invalid_enum", table, [r[BUSINESS_KEY[table]] for r in picked], column=column, values=counts,
            )

    def end_before_start(self, table: str, start_col: str, end_col: str, rows: list[dict]) -> None:
        picked = self._pick(
            rows, self._rate("end_before_start"),
            lambda r: r[start_col] is not None and r[end_col] is not None and r[start_col] < r[end_col],
        )
        for r in picked:
            r[start_col], r[end_col] = r[end_col], r[start_col]
        if picked:
            self.manifest.record(
                "end_before_start", table, [r[BUSINESS_KEY[table]] for r in picked], column=f"{start_col}/{end_col}",
            )

    def line_defects(self, lines: list[dict], transactions: list[dict]) -> None:
        picked = self._pick(lines, self._rate("negative_amount"), lambda r: r["amount"] > 0)
        for r in picked:
            r["amount"] = -abs(r["amount"])
        if picked:
            self.manifest.record(
                "negative_amount", T_LINES, [r["transaction_line_id"] for r in picked], column="amount",
                also_violates=["amount_ne_quantity_times_rate"],
            )

        picked = self._pick(lines, self._rate("amount_mismatch"), lambda r: r["amount"] == r["quantity"] * r["rate"])
        for r in picked:
            r["amount"] += self.rng.randint(1, 50)
        if picked:
            self.manifest.record(
                "amount_mismatch", T_LINES, [r["transaction_line_id"] for r in picked], column="amount",
            )

        picked = self._pick(lines, self._rate("orphan_lines"))
        base = max(_max_int(transactions, "transaction_internal_id", 0), _max_int(lines, "transaction_internal_id", 0))
        for i, r in enumerate(picked):
            r["transaction_internal_id"] = base + 1_000_000 + i
        if picked:
            self.manifest.record(
                "orphan_lines", T_LINES, [r["transaction_line_id"] for r in picked], column="transaction_internal_id",
            )

    def key_defects(self, table: str, rows: list[dict]) -> None:
        key = BUSINESS_KEY[table]
        picked = self._pick(rows, self._rate("null_business_key"), lambda r: r[key] is not None)
        original = [r[key] for r in picked]
        for r in picked:
            r[key] = None
        if picked:
            self.manifest.record(
                "null_business_key", table, original, column=key,
                note="business_keys lists the original key values that were nulled",
            )

        picked = self._pick(rows, self._rate("duplicate_business_key"), lambda r: r[key] is not None)
        dupes = []
        for r in picked:
            copy_row = copy.deepcopy(r)
            copy_row["created_date"] = _add_days(r["created_date"], -1)  # same updated_date (watermark), so same snapshot
            dupes.append(copy_row)
        rows.extend(dupes)
        if picked:
            self.manifest.record(
                "duplicate_business_key", table, [r[key] for r in picked], column=key,
                note="same key appears twice within the same updated_date snapshot; the appended duplicate has created_date - 1 day",
            )

    # -- driver -----------------------------------------------------------

    def apply(self, data: dict[str, list[dict]]) -> None:
        self.customer_defects(data[T_CUSTOMERS])
        for table in INCREMENTAL_TABLES:
            self.late_arriving(table, data[table])
        for table, column in ENUM_DEFECT_TARGETS:
            self.invalid_enum(table, column, data[table])
        self.end_before_start(T_MEMBERSHIPS, "start_date", "end_date", data[T_MEMBERSHIPS])
        self.end_before_start(T_CERTIFICATIONS, "certification_start_date", "certification_end_date", data[T_CERTIFICATIONS])
        self.line_defects(data[T_LINES], data[T_TRANSACTIONS])
        for table in INCREMENTAL_TABLES:
            self.key_defects(table, data[table])


# --------------------------------------------------------------------------
# init / increment
# --------------------------------------------------------------------------


@dataclass
class GenResult:
    data: dict            # table -> rows to insert
    upserts: dict         # table -> rows to upsert (customers only)
    manifest: dict


def _empty() -> dict:
    return {t: [] for t in TABLES}


def _finish_manifest(mode, seed, batch_date, cfg, scale, data, upserts, injector_manifest, drift, extra) -> dict:
    manifest = {
        "tool_version": TOOL_VERSION,
        "mode": mode,
        "seed": seed,
        "batch_date": str(batch_date),
        "config_hash": cfg.digest(),
        "config": cfg.to_dict(),
        "scale": {k: getattr(scale, k) for k in ("customers", "memberships", "certifications", "transactions", "lines")},
        "rows_emitted": {t: len(data[t]) for t in TABLES},
        "rows_upserted": {t: len(v) for t, v in upserts.items()},
        "defects": injector_manifest.defects,
        "schema_drift": drift,
        "pre_existing": {
            "customers_dates_not_in_source_columns": {
                "columns": ["created_date", "updated_date"],
                "note": "live netsuite_customers has these columns; aidq_metadata.source_columns does not "
                        "list them, so bronze drops them. Left as-is on purpose.",
            }
        },
    }
    manifest.update(extra or {})
    return manifest


def generate_init(gen: Generator, scale: ScaleConfig, batch_date: dt.date, cfg: DefectConfig, populate_region: bool = False) -> GenResult:
    ctx = Context(mode="init", batch_date=batch_date)
    data = _empty()
    data[T_CUSTOMERS] = gen.customers(scale.customers, ID_START[T_CUSTOMERS], batch_date)
    customer_ids = [r["customer_internal_id"] for r in data[T_CUSTOMERS]]
    data[T_MEMBERSHIPS] = gen.memberships(scale.memberships, ID_START[T_MEMBERSHIPS], customer_ids, batch_date)
    data[T_CERTIFICATIONS] = gen.certifications(scale.certifications, ID_START[T_CERTIFICATIONS], customer_ids, batch_date)
    data[T_TRANSACTIONS] = gen.transactions(scale.transactions, ID_START[T_TRANSACTIONS], customer_ids, batch_date)
    txn_ids = [r["transaction_internal_id"] for r in data[T_TRANSACTIONS]]
    data[T_LINES] = gen.lines(txn_ids, scale.lines, batch_date)
    if populate_region:
        for row in data[T_TRANSACTIONS]:
            row[DRIFT_COLUMN] = gen.region()

    manifest = Manifest()
    DefectInjector(cfg, gen.rng, ctx, manifest).apply(data)
    return GenResult(data, {}, _finish_manifest("init", gen.seed, batch_date, cfg, scale, data, {}, manifest, None, {}))


def next_batch_date(existing: dict) -> dt.date:
    dates = [
        _as_date(r["updated_date"])
        for rows in existing.values()
        for r in rows
        if r.get("updated_date") is not None
    ]
    return (max(dates) if dates else BASE_SNAPSHOT_DATE) + dt.timedelta(days=INCREMENT_GAP_DAYS)


def generate_increment(
    gen: Generator,
    existing: dict,
    scale: ScaleConfig,
    batch_date: dt.date,
    cfg: DefectConfig,
    apply_drift: bool = False,
    region_present: bool = False,
    update_spread_days: int = 2,
) -> GenResult:
    """`existing` maps table -> all current rows (every version) as dicts. New versions of existing rows get an
    updated_date of batch_date + 0..update_spread_days (0 for the daily schedule, so no row is dated after its
    batch day and the next day's batch date stays later than the watermark)."""
    rng = gen.rng
    ctx = Context(mode="increment", batch_date=batch_date)
    for table in INCREMENTAL_TABLES:
        dates = sorted({_as_date(r["updated_date"]) for r in existing.get(table, []) if r.get("updated_date") is not None})
        if dates:
            ctx.snapshot_dates[table] = dates
            ctx.watermarks[table] = dates[-1]
    if any(w >= batch_date for w in ctx.watermarks.values()):
        raise ValueError(f"--batch-date {batch_date} must be later than the current watermark {max(ctx.watermarks.values())}")

    populate_region = apply_drift or region_present
    new_n = lambda base: max(1, int(base * scale.increment_new_pct))  # noqa: E731

    data, upserts = _empty(), {T_CUSTOMERS: []}

    # customers: new rows + in-place upserts of existing ones
    cust_existing = existing.get(T_CUSTOMERS, [])
    next_cust = _max_int(cust_existing, "customer_internal_id", ID_START[T_CUSTOMERS] - 1) + 1
    new_customers = gen.customers(new_n(scale.customers), next_cust, batch_date)
    all_customer_ids = sorted({r["customer_internal_id"] for r in cust_existing} | {r["customer_internal_id"] for r in new_customers})
    updated_cust = []
    latest_c = _latest_by_key(cust_existing, "customer_internal_id")
    for key in rng.sample(sorted(latest_c), min(len(latest_c), int(len(latest_c) * scale.increment_update_pct))):
        row = dict(latest_c[key])
        row["company_name"] = gen.fake.company()
        row["email"] = gen.fake.email()
        row["updated_date"] = batch_date
        updated_cust.append(row)
    data[T_CUSTOMERS] = new_customers
    upserts[T_CUSTOMERS] = updated_cust

    # new rows for the incremental tables
    def start(table, rows):
        return _max_int(rows, BUSINESS_KEY[table], ID_START[table] - 1) + 1

    m_ex, c_ex, t_ex = existing.get(T_MEMBERSHIPS, []), existing.get(T_CERTIFICATIONS, []), existing.get(T_TRANSACTIONS, [])
    new_m = gen.memberships(new_n(scale.memberships), start(T_MEMBERSHIPS, m_ex), all_customer_ids, batch_date)
    new_c = gen.certifications(new_n(scale.certifications), start(T_CERTIFICATIONS, c_ex), all_customer_ids, batch_date)
    new_t = gen.transactions(new_n(scale.transactions), start(T_TRANSACTIONS, t_ex), all_customer_ids, batch_date)
    new_l = gen.lines([r["transaction_internal_id"] for r in new_t], new_n(scale.lines), batch_date)

    # new versions of existing rows (same key and created_date, later updated_date)
    def versions(table, mutate):
        latest = _latest_by_key(existing.get(table, []), BUSINESS_KEY[table])
        keys = rng.sample(sorted(latest), min(len(latest), int(len(latest) * scale.increment_update_pct)))
        out = []
        for key in keys:
            row = {c: latest[key].get(c) for c in COLUMNS[table]}
            mutate(row)
            # the watermark is updated_date: an update keeps its created_date and moves updated_date forward
            row["updated_date"] = _stamp(table, _add_days(batch_date, rng.randint(0, update_spread_days)))
            out.append(row)
        return out

    def mut_membership(row):
        row["membership_status"] = rng.choice(VALID_MEMBERSHIP_STATUS)
        row["end_date"] = _add_days(row["end_date"], rng.randint(30, 365)) if row["end_date"] else row["end_date"]

    def mut_cert(row):
        if row["certification_end_date"]:
            row["certification_end_date"] = _add_days(row["certification_end_date"], rng.randint(30, 365))

    def mut_txn(row):
        row["date"] = _add_days(row["date"], rng.randint(1, 5)) if row["date"] else row["date"]

    def mut_line(row):
        row["quantity"] = rng.randint(1, 10)
        row["amount"] = row["quantity"] * row["rate"]

    upd_m, upd_c, upd_t = versions(T_MEMBERSHIPS, mut_membership), versions(T_CERTIFICATIONS, mut_cert), versions(T_TRANSACTIONS, mut_txn)
    upd_l = versions(T_LINES, mut_line)

    data[T_MEMBERSHIPS], data[T_CERTIFICATIONS] = new_m + upd_m, new_c + upd_c
    data[T_TRANSACTIONS], data[T_LINES] = new_t + upd_t, new_l + upd_l

    if populate_region:
        for row in data[T_TRANSACTIONS]:
            row[DRIFT_COLUMN] = gen.region()

    ctx.new_rows = {id(r) for r in new_m + new_c + new_t + new_l}
    manifest = Manifest()
    DefectInjector(cfg, rng, ctx, manifest).apply({**data, T_CUSTOMERS: new_customers + updated_cust})
    # customer defects were applied to the combined list (same dict objects), keep the split
    drift = (
        {"table": DRIFT_TABLE, "column": DRIFT_COLUMN, "type": "text",
         "sql": f"ALTER TABLE {SCHEMA}.{DRIFT_TABLE} ADD COLUMN IF NOT EXISTS {DRIFT_COLUMN} text",
         "note": "bronze should ignore it: the column list comes from aidq_metadata.source_columns"}
        if apply_drift
        else None
    )
    extra = {
        "increment": {
            "current_watermarks": {t: str(w) for t, w in ctx.watermarks.items()},
            "updated_versions": {T_MEMBERSHIPS: len(upd_m), T_CERTIFICATIONS: len(upd_c), T_TRANSACTIONS: len(upd_t), T_LINES: len(upd_l)},
        }
    }
    return GenResult(
        data, upserts,
        _finish_manifest("increment", gen.seed, batch_date, cfg, scale, data, upserts, manifest, drift, extra),
    )


def columns_for(table: str, region_present: bool) -> list[str]:
    cols = list(COLUMNS[table])
    if table == DRIFT_TABLE and region_present:
        cols.append(DRIFT_COLUMN)
    return cols


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _json_default(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    raise TypeError(f"not serializable: {type(value)}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--init", action="store_true", help="back up, truncate and load a base snapshot")
    mode.add_argument("--increment", action="store_true", help="append a later snapshot (new + updated rows)")
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--config", help="YAML defect config (all defects off if omitted)")
    p.add_argument("--manifest", help="manifest output path (default: manifest_<mode>_<seed>.json)")
    p.add_argument("--batch-date", type=dt.date.fromisoformat, help="snapshot date (default: 2026-07-11 for --init, latest updated_date + 21d for --increment)")
    p.add_argument("--scale", type=float, default=1.0, help="multiplier on the default row counts")
    p.add_argument("--apply-schema-drift", action="store_true", help=f"--increment only: add {DRIFT_COLUMN} to {DRIFT_TABLE}")
    p.add_argument("--dry-run", action="store_true", help="generate and write the manifest, change nothing in the database")
    p.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    p.add_argument("--auth", choices=["cli", "sdk"], default="cli",
                   help="cli: Databricks CLI with --profile (local); sdk: databricks-sdk as the job's run-as identity")
    p.add_argument("--backup", choices=["branch-and-schema", "schema"], default="branch-and-schema",
                   help="branch-and-schema: Lakebase branch + verified schema copy; schema: verified copy in a "
                        "netsuite_backup_daily_<stamp> schema only (daily schedule: no branch per day)")
    p.add_argument("--update-spread-days", type=int, default=2,
                   help="--increment: new versions of existing rows are dated batch_date + 0..N days (daily: 0)")
    p.add_argument("--skip-if-not-after-watermark", action="store_true",
                   help="--increment: exit 0 without changes when --batch-date is not later than the source watermark")
    p.add_argument("--backup-keep", type=int,
                   help="with --backup schema: drop daily backup schemas beyond the newest N (others never touched)")
    return p


def current_watermark(existing: dict) -> dt.date | None:
    """Latest updated_date over the incremental tables (what --batch-date must be later than)."""
    dates = [_as_date(r["updated_date"]) for t in INCREMENTAL_TABLES for r in existing.get(t, []) if r.get("updated_date") is not None]
    return max(dates) if dates else None


def pg_writer_daily_prefix() -> str:
    import pg_writer

    return pg_writer.DAILY_BACKUP_PREFIX


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.apply_schema_drift and not args.increment:
        print("--apply-schema-drift is only valid with --increment", file=sys.stderr)
        return 2
    if args.backup_keep is not None and args.backup != "schema":
        print("--backup-keep is only valid with --backup schema", file=sys.stderr)
        return 2
    backup_kwargs = (
        {"with_branch": False, "schema_prefix": pg_writer_daily_prefix()} if args.backup == "schema" else {}
    )
    cfg = load_config(args.config)
    scale = ScaleConfig().scaled(args.scale)
    manifest_path = Path(args.manifest or f"manifest_{'init' if args.init else 'increment'}_{args.seed}.json")

    import pg_writer  # imported late so the generator stays importable without psycopg

    conn = None
    try:
        if args.init:
            extra_items = []
            backup_info = {"performed": False, "reason": "dry-run"}
            if not args.dry_run:
                conn = pg_writer.connect(args.profile, args.auth)
                extra_items = pg_writer.read_items(conn)
                backup_info = pg_writer.create_backup(conn, args.profile, **backup_kwargs)
                pg_writer.require_backup(backup_info)
            gen = Generator(args.seed, extra_items)
            result = generate_init(gen, scale, args.batch_date or BASE_SNAPSHOT_DATE, cfg)
            result.manifest["backup"] = backup_info
            if not args.dry_run:
                pg_writer.load_init(conn, result.data)
        else:
            conn = pg_writer.connect(args.profile, args.auth)  # reads only, unless not --dry-run
            live_cols = pg_writer.read_live_columns(conn)
            region_present = DRIFT_COLUMN in live_cols.get(DRIFT_TABLE, [])
            existing = pg_writer.read_existing(conn, live_cols)
            batch_date = args.batch_date or next_batch_date(existing)
            watermark = current_watermark(existing)
            if args.skip_if_not_after_watermark and watermark is not None and batch_date <= watermark:
                print(f"SKIPPED: --batch-date {batch_date} is not later than the source watermark {watermark}; "
                      "nothing generated, no backup, no write")
                return 0
            gen = Generator(args.seed, pg_writer.items_from_rows(existing.get(T_LINES, [])))
            result = generate_increment(
                gen, existing, scale, batch_date, cfg,
                apply_drift=args.apply_schema_drift, region_present=region_present,
                update_spread_days=args.update_spread_days,
            )
            backup_info = {"performed": False, "reason": "dry-run"}
            if not args.dry_run:
                # fresh backup before every data change, same gate as --init
                backup_info = pg_writer.create_backup(conn, args.profile, **backup_kwargs)
                pg_writer.require_backup(backup_info)
                pg_writer.load_increment(conn, result, apply_drift=args.apply_schema_drift, region_present=region_present)
                if args.backup_keep:
                    backup_info["pruned"] = pg_writer.prune_daily_backups(conn, args.backup_keep)
            result.manifest["backup"] = backup_info
    finally:
        if conn is not None:
            conn.close()

    result.manifest["dry_run"] = args.dry_run
    result.manifest["generated_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    manifest_path.write_text(json.dumps(result.manifest, indent=2, default=_json_default), encoding="utf-8")
    counts = ", ".join(f"{t.replace('netsuite_', '')}={n}" for t, n in result.manifest["rows_emitted"].items())
    print(f"{'DRY RUN: ' if args.dry_run else ''}{result.manifest['mode']} seed={args.seed} rows: {counts}")
    print(f"defects injected: {sum(d['row_count'] for d in result.manifest['defects'])} rows in {len(result.manifest['defects'])} groups")
    print(f"manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    # exit explicitly only on failure: a Databricks Python task reports even SystemExit(0) as a failed run
    rc = main()
    if rc:
        sys.exit(rc)
