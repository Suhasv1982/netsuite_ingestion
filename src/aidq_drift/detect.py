"""Drift detection: deterministic, no model (design 3.1).

For each onboarded table, the source's columns (information_schema.columns of schema `netsuite` on netsuite-sample)
against its active source_columns:

| difference                                   | change kind      | becomes                              |
|----------------------------------------------|------------------|--------------------------------------|
| column in the source, not active in metadata | added            | ADD_COLUMN proposal                  |
| active column in metadata, not in the source | removed          | DEACTIVATE_COLUMN proposal (urgent)  |
| type differs                                 | type_changed     | SCHEMA_TYPE_CHANGE incident          |
| source table not in source_table_def         | unonboarded      | SCHEMA_DRIFT incident (onboarding)   |

The metadata stores Spark-style upper-case type names (DATE, INTEGER, TEXT, TIMESTAMP); a source type without a
mapping is reported, never guessed. Pure functions plus two read helpers.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

SOURCE_SCHEMA = "netsuite"
TYPE_MAP = {
    "date": "DATE", "integer": "INTEGER", "smallint": "INTEGER", "bigint": "BIGINT", "text": "TEXT",
    "character varying": "TEXT", "character": "TEXT", "boolean": "BOOLEAN", "numeric": "DECIMAL",
    "double precision": "DOUBLE", "real": "FLOAT", "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMP",
}
SOURCE_COLUMNS_SQL = ("SELECT table_name, column_name, data_type, ordinal_position FROM information_schema.columns "
                      "WHERE table_schema = %s ORDER BY table_name, ordinal_position")
SOURCE_TABLES_SQL = ("SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND "
                     "table_type = 'BASE TABLE' ORDER BY 1")
METADATA_SQL = ("SELECT d.table_id, d.source_table, d.business_key, d.watermark_col, d.load_mode, c.column_name, "
                "c.data_type, c.ordinal, COALESCE(c.is_active, true) AS is_active FROM aidq_metadata.source_table_def d "
                "LEFT JOIN aidq_metadata.source_columns c USING (table_id) ORDER BY d.source_table, c.ordinal")


def metadata_type(source_type: str | None) -> str | None:
    return TYPE_MAP.get((source_type or "").strip().lower())


def same_type(source_type: str | None, meta_type: str | None) -> bool:
    mapped = metadata_type(source_type)
    return mapped is not None and mapped == (meta_type or "").strip().upper()


@dataclass(frozen=True)
class Change:
    table: str
    kind: str                       # added | removed | type_changed | unonboarded
    column: str | None = None
    source_type: str | None = None
    metadata_type: str | None = None
    proposed_type: str | None = None
    ordinal: int | None = None
    incremental: bool = False
    key_column: bool = False        # the business key or the watermark column (never deactivated)

    @property
    def id(self) -> str:
        return f"{self.table}:{self.kind}" + (f":{self.column}" if self.column else "")

    def as_dict(self) -> dict:
        return {"id": self.id, **asdict(self)}


def diff(source_cols: list[dict], source_tables: list[str], metadata_rows: list[dict]) -> list[Change]:
    """The drift between the source and the metadata (see the module doc). Inputs are the rows of the three queries."""
    tables: dict[str, dict] = {}
    for m in metadata_rows:
        t = tables.setdefault(m["source_table"], {"def": m, "active": {}, "inactive": set(), "max_ordinal": 0})
        if m.get("column_name") is None:
            continue
        if m["is_active"]:
            t["active"][m["column_name"]] = m
        else:
            t["inactive"].add(m["column_name"])
        t["max_ordinal"] = max(t["max_ordinal"], m.get("ordinal") or 0)
    by_table: dict[str, dict[str, dict]] = {}
    for s in source_cols:
        by_table.setdefault(s["table_name"], {})[s["column_name"]] = s
    out: list[Change] = []
    for name, t in sorted(tables.items()):
        d = t["def"]
        incremental = (d.get("load_mode") or "").lower() == "incremental"
        keys = {d.get("business_key"), d.get("watermark_col")} - {None}
        src = by_table.get(name, {})
        next_ordinal = t["max_ordinal"]
        for col, s in src.items():
            if col not in t["active"]:
                next_ordinal += 1
                out.append(Change(name, "added", col, s["data_type"], None, metadata_type(s["data_type"]), next_ordinal,
                                  incremental))
        for col, m in t["active"].items():
            if col not in src:
                out.append(Change(name, "removed", col, None, m.get("data_type"), None, m.get("ordinal"), incremental,
                                  col in keys))
            elif not same_type(src[col]["data_type"], m.get("data_type")):
                out.append(Change(name, "type_changed", col, src[col]["data_type"], m.get("data_type"),
                                  metadata_type(src[col]["data_type"]), m.get("ordinal"), incremental, col in keys))
    for name in sorted(set(source_tables) - set(tables)):
        out.append(Change(name, "unonboarded"))
    return out


def read_drift(pg_query, source_endpoint: str, metadata_endpoint: str) -> list[Change]:
    """`pg_query(endpoint, sql, params)` as Reads / SdkReads provide it (the caller's identity: dq-agent in the job)."""
    return diff(pg_query(source_endpoint, SOURCE_COLUMNS_SQL, (SOURCE_SCHEMA,)),
                [r["table_name"] for r in pg_query(source_endpoint, SOURCE_TABLES_SQL, (SOURCE_SCHEMA,))],
                pg_query(metadata_endpoint, METADATA_SQL))
