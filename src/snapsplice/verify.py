"""Comparing the target with the source, key by key and by row hash."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass, field

from . import source as src
from .schema import TABLES, Pk, pk_text
from .target import Target

CLASSES = ("stale_value", "resurrected_delete", "missing_row", "extra_row")


def row_hash(row: tuple) -> str:
    return hashlib.sha256(json.dumps(list(row), separators=(",", ":")).encode()).hexdigest()


@dataclass
class TableDiff:
    tbl: str
    source_rows: int
    target_rows: int
    missing: list[Pk] = field(default_factory=list)
    extra: list[Pk] = field(default_factory=list)
    changed: list[Pk] = field(default_factory=list)

    @property
    def divergent(self) -> int:
        return len(self.missing) + len(self.extra) + len(self.changed)


def compare(conn: sqlite3.Connection, target: Target) -> dict[str, TableDiff]:
    out: dict[str, TableDiff] = {}
    for spec in TABLES:
        s_rows = src.table_rows(conn, spec)
        t_rows = target.table_rows(spec)
        diff = TableDiff(spec.name, len(s_rows), len(t_rows))
        diff.missing = sorted(set(s_rows) - set(t_rows))
        diff.extra = sorted(set(t_rows) - set(s_rows))
        diff.changed = sorted(
            pk for pk in set(s_rows) & set(t_rows) if row_hash(s_rows[pk]) != row_hash(t_rows[pk])
        )
        out[spec.name] = diff
    return out


def classify(conn: sqlite3.Connection, diffs: dict[str, TableDiff]) -> Counter[str]:
    """Name every divergent key.

    A changed row is a ``stale_value``. A key only in the source is a ``missing_row``. A key only
    in the target is a ``resurrected_delete`` if the log holds a delete for it, otherwise an
    ``extra_row``.
    """
    deleted = src.deleted_keys(conn)
    counts: Counter[str] = Counter({c: 0 for c in CLASSES})
    for tbl, diff in diffs.items():
        counts["stale_value"] += len(diff.changed)
        counts["missing_row"] += len(diff.missing)
        for pk in diff.extra:
            counts["resurrected_delete" if (tbl, pk_text(pk)) in deleted else "extra_row"] += 1
    return counts


def total_divergent(diffs: dict[str, TableDiff]) -> int:
    return sum(d.divergent for d in diffs.values())
