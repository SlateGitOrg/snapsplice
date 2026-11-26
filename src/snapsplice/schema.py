"""Table specifications and DDL for the SQLite source and the DuckDB target.

The source is a warehouse-management database. Triggers on every replicated table append to
``_cdc_log``, an offline stand-in for a binlog or WAL. Watermark markers are rows inserted into
``_cdc_watermarks``; a trigger copies them into the same log, so a marker has an LSN that orders
it against every data change.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

Pk = tuple[str, ...]

OP_INSERT = "insert"
OP_UPDATE = "update"
OP_DELETE = "delete"
OP_LOW = "wm_low"
OP_HIGH = "wm_high"
DATA_OPS = (OP_INSERT, OP_UPDATE, OP_DELETE)


@dataclass(frozen=True)
class TableSpec:
    """One replicated table: its columns (all stored as TEXT or INTEGER) and primary key."""

    name: str
    columns: tuple[tuple[str, str], ...]  # (column, SQL type)
    pk: tuple[str, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c for c, _ in self.columns)

    @property
    def pk_indexes(self) -> tuple[int, ...]:
        names = self.column_names
        return tuple(names.index(c) for c in self.pk)

    def pk_of(self, row: tuple) -> Pk:
        return tuple(row[i] for i in self.pk_indexes)

    def row_from_dict(self, data: dict) -> tuple:
        return tuple(data[c] for c in self.column_names)

    def ddl(self) -> str:
        cols = ", ".join(f"{c} {t}" for c, t in self.columns)
        return f"CREATE TABLE {self.name} ({cols}, PRIMARY KEY ({', '.join(self.pk)}))"

    def keyset_where(self) -> str:
        """Row-value predicate for PK-ordered pagination: rows strictly after a key."""
        cols = ", ".join(self.pk)
        marks = ", ".join("?" for _ in self.pk)
        return f"({cols}) > ({marks})"


INVENTORY = TableSpec(
    "inventory_positions",
    (
        ("sku", "TEXT NOT NULL"),
        ("location", "TEXT NOT NULL"),
        ("qty_on_hand", "INTEGER NOT NULL"),
        ("qty_allocated", "INTEGER NOT NULL"),
        ("updated_at", "TEXT NOT NULL"),
    ),
    ("sku", "location"),
)

SHIPMENTS = TableSpec(
    "shipments",
    (
        ("shipment_id", "TEXT NOT NULL"),
        ("order_id", "TEXT NOT NULL"),
        ("status", "TEXT NOT NULL"),
        ("carrier", "TEXT NOT NULL"),
        ("weight_g", "INTEGER NOT NULL"),
        ("updated_at", "TEXT NOT NULL"),
    ),
    ("shipment_id",),
)

TABLES: tuple[TableSpec, ...] = (INVENTORY, SHIPMENTS)
SPECS: dict[str, TableSpec] = {t.name: t for t in TABLES}

STATUS_LIFECYCLE = ("created", "picked", "packed", "shipped", "delivered")


def pk_text(pk: Pk) -> str:
    """Canonical text form of a primary key, as stored in ``_cdc_log.pk_json``."""
    return json.dumps(list(pk), separators=(",", ":"))


def pk_label(pk: Pk) -> str:
    """Human form used on the command line: ``SKU-0042,L3``."""
    return ",".join(pk)


def parse_pk_label(label: str) -> Pk:
    return tuple(label.split(","))


_LOG_DDL = """
CREATE TABLE _cdc_log (
    lsn INTEGER PRIMARY KEY AUTOINCREMENT,
    tbl TEXT NOT NULL,
    op TEXT NOT NULL,
    pk_json TEXT NOT NULL,
    row_json TEXT
);
CREATE TABLE _cdc_watermarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL CHECK (kind IN ('low', 'high')),
    tbl TEXT NOT NULL,
    chunk_id INTEGER NOT NULL
);
CREATE TRIGGER _cdc_watermarks_ai AFTER INSERT ON _cdc_watermarks
BEGIN
    INSERT INTO _cdc_log (tbl, op, pk_json, row_json)
    VALUES (NEW.tbl, 'wm_' || NEW.kind, '[]',
            json_object('wm_id', NEW.id, 'chunk_id', NEW.chunk_id));
END;
CREATE TABLE _synth_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def _json_object(spec: TableSpec, prefix: str) -> str:
    pairs = ", ".join(f"'{c}', {prefix}.{c}" for c in spec.column_names)
    return f"json_object({pairs})"


def _json_pk(spec: TableSpec, prefix: str) -> str:
    return "json_array(" + ", ".join(f"{prefix}.{c}" for c in spec.pk) + ")"


def trigger_ddl(spec: TableSpec) -> str:
    """AFTER INSERT / UPDATE / DELETE triggers feeding ``_cdc_log`` for one table.

    An UPDATE that changes the primary key is logged as a delete of the old key followed by an
    insert of the new one, which is how a key change has to look to a consumer keyed by PK.
    """
    t = spec.name
    same_pk = " AND ".join(f"OLD.{c} IS NEW.{c}" for c in spec.pk)

    def stmt(op: str, prefix: str) -> str:
        return (
            f"INSERT INTO _cdc_log (tbl, op, pk_json, row_json) VALUES "
            f"('{t}', '{op}', {_json_pk(spec, prefix)}, {_json_object(spec, prefix)});"
        )

    return f"""
CREATE TRIGGER {t}_ai AFTER INSERT ON {t} BEGIN {stmt(OP_INSERT, "NEW")} END;
CREATE TRIGGER {t}_au AFTER UPDATE ON {t} WHEN {same_pk}
BEGIN {stmt(OP_UPDATE, "NEW")} END;
CREATE TRIGGER {t}_au_pk AFTER UPDATE ON {t} WHEN NOT ({same_pk})
BEGIN {stmt(OP_DELETE, "OLD")} {stmt(OP_INSERT, "NEW")} END;
CREATE TRIGGER {t}_ad AFTER DELETE ON {t} BEGIN {stmt(OP_DELETE, "OLD")} END;
"""


def source_ddl() -> str:
    parts = [spec.ddl() + ";" for spec in TABLES]
    parts.append(_LOG_DDL)
    parts.extend(trigger_ddl(spec) for spec in TABLES)
    return "\n".join(parts)


TARGET_SYSTEM_DDL = """
CREATE TABLE _key_state (
    tbl TEXT NOT NULL, pk TEXT NOT NULL, lsn BIGINT NOT NULL, deleted BOOLEAN NOT NULL,
    PRIMARY KEY (tbl, pk)
);
CREATE TABLE _snapshot_progress (
    tbl TEXT NOT NULL, chunk_id INTEGER NOT NULL, last_key TEXT, done BOOLEAN NOT NULL
);
CREATE TABLE _reader_offset (lsn BIGINT NOT NULL);
CREATE TABLE _trace (
    seq INTEGER NOT NULL, tbl TEXT NOT NULL, pk TEXT NOT NULL, lsn BIGINT NOT NULL,
    kind TEXT NOT NULL, chunk_id INTEGER, decision TEXT NOT NULL, detail TEXT
);
"""


def target_ddl() -> str:
    return ";\n".join(spec.ddl() for spec in TABLES) + ";\n" + TARGET_SYSTEM_DDL
