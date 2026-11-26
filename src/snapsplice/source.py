"""Access to the SQLite warehouse-management source."""

from __future__ import annotations

import json
import sqlite3

from .events import Event, parse_log_row
from .schema import OP_DELETE, TABLES, Pk, TableSpec, pk_text, source_ddl


def connect(path: str = ":memory:") -> sqlite3.Connection:
    """Open a source connection in autocommit mode so each statement commits with its triggers."""
    conn = sqlite3.connect(path, isolation_level=None)
    return conn


def create_source(conn: sqlite3.Connection) -> None:
    conn.executescript(source_ddl())


def insert_watermark(conn: sqlite3.Connection, kind: str, tbl: str, chunk_id: int) -> int:
    """Insert a watermark marker; its trigger appends the marker to the log. Returns its id."""
    cur = conn.execute(
        "INSERT INTO _cdc_watermarks (kind, tbl, chunk_id) VALUES (?, ?, ?)",
        (kind, tbl, chunk_id),
    )
    assert cur.lastrowid is not None
    return cur.lastrowid


def select_chunk(
    conn: sqlite3.Connection, spec: TableSpec, after: Pk | None, limit: int
) -> list[tuple]:
    """Next ``limit`` rows in primary-key order strictly after ``after``."""
    order = ", ".join(spec.pk)
    cols = ", ".join(spec.column_names)
    if after is None:
        return conn.execute(
            f"SELECT {cols} FROM {spec.name} ORDER BY {order} LIMIT ?", (limit,)
        ).fetchall()
    return conn.execute(
        f"SELECT {cols} FROM {spec.name} WHERE {spec.keyset_where()} ORDER BY {order} LIMIT ?",
        (*after, limit),
    ).fetchall()


def max_lsn(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(lsn), 0) FROM _cdc_log").fetchone()
    return int(row[0])


def read_log(conn: sqlite3.Connection, after_lsn: int, limit: int) -> list[Event]:
    rows = conn.execute(
        "SELECT lsn, tbl, op, pk_json, row_json FROM _cdc_log WHERE lsn > ? ORDER BY lsn LIMIT ?",
        (after_lsn, limit),
    ).fetchall()
    return [parse_log_row(*r) for r in rows]


def table_rows(conn: sqlite3.Connection, spec: TableSpec) -> dict[Pk, tuple]:
    cols = ", ".join(spec.column_names)
    return {spec.pk_of(r): r for r in conn.execute(f"SELECT {cols} FROM {spec.name}")}


def deleted_keys(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    """(table, pk text) of every key that has at least one delete event in the log."""
    rows = conn.execute(
        "SELECT DISTINCT tbl, pk_json FROM _cdc_log WHERE op = ?", (OP_DELETE,)
    ).fetchall()
    return {(t, pk_text(tuple(json.loads(p)))) for t, p in rows}


def set_meta(conn: sqlite3.Connection, key: str, value: object) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _synth_meta (key, value) VALUES (?, ?)", (key, json.dumps(value))
    )


def get_meta(conn: sqlite3.Connection, key: str) -> object | None:
    row = conn.execute("SELECT value FROM _synth_meta WHERE key = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {s.name: conn.execute(f"SELECT COUNT(*) FROM {s.name}").fetchone()[0] for s in TABLES}
