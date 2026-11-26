"""The DuckDB analytics target: replicated tables plus the bookkeeping tables.

``_key_state`` holds the last applied LSN per key (tombstones included), ``_snapshot_progress``
the last completed chunk per table, ``_reader_offset`` the log position the reader resumes from
and ``_trace`` the decision taken for every event and chunk row when tracing is on.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence

import duckdb

from .events import Decision
from .schema import TABLES, Pk, TableSpec, pk_text, target_ddl

_PARAM_LIMIT = 900


class Target:
    def __init__(self, path: str = ":memory:") -> None:
        self.con = duckdb.connect(path)
        exists = self.con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_key_state'"
        ).fetchone()
        if not exists or exists[0] == 0:
            self.con.execute(target_ddl())

    def close(self) -> None:
        self.con.close()

    # -- transactions -------------------------------------------------------------------------

    def begin(self) -> None:
        self.con.execute("BEGIN")

    def commit(self) -> None:
        self.con.execute("COMMIT")

    def rollback(self) -> None:
        self.con.execute("ROLLBACK")

    # -- replicated data ----------------------------------------------------------------------

    def upsert_rows(self, spec: TableSpec, rows: Sequence[tuple]) -> None:
        """Insert-or-replace by primary key. Delete-then-insert is several times faster in DuckDB
        than ``INSERT OR REPLACE``, whose conflict handling costs a fixed amount per statement."""
        if not rows:
            return
        self.delete_rows(spec, [spec.pk_of(r) for r in rows])
        marks = "(" + ", ".join("?" for _ in spec.columns) + ")"
        self._insert_many(f"INSERT INTO {spec.name} VALUES", marks, rows)

    def delete_rows(self, spec: TableSpec, pks: Sequence[Pk]) -> None:
        if not pks:
            return
        width = len(spec.pk)
        step = max(1, _PARAM_LIMIT // width)
        for i in range(0, len(pks), step):
            part = pks[i : i + step]
            if width == 1:
                cond = f"{spec.pk[0]} IN ({', '.join('?' for _ in part)})"
            else:
                one = "(" + ", ".join("?" for _ in range(width)) + ")"
                cond = f"({', '.join(spec.pk)}) IN ({', '.join(one for _ in part)})"
            self.con.execute(
                f"DELETE FROM {spec.name} WHERE {cond}", [v for pk in part for v in pk]
            )

    def table_rows(self, spec: TableSpec) -> dict[Pk, tuple]:
        cols = ", ".join(spec.column_names)
        return {
            spec.pk_of(r): r for r in self.con.execute(f"SELECT {cols} FROM {spec.name}").fetchall()
        }

    # -- key state ----------------------------------------------------------------------------

    def key_states(self, tbl: str, pk_texts: Sequence[str]) -> dict[str, tuple[int, bool]]:
        out: dict[str, tuple[int, bool]] = {}
        for i in range(0, len(pk_texts), _PARAM_LIMIT):
            part = list(pk_texts[i : i + _PARAM_LIMIT])
            marks = ", ".join("?" for _ in part)
            rows = self.con.execute(
                f"SELECT pk, lsn, deleted FROM _key_state WHERE tbl = ? AND pk IN ({marks})",
                [tbl, *part],
            ).fetchall()
            out.update({pk: (int(lsn), bool(d)) for pk, lsn, d in rows})
        return out

    def write_key_states(self, rows: Sequence[tuple[str, str, int, bool]]) -> None:
        for tbl in {r[0] for r in rows}:
            pks = [r[1] for r in rows if r[0] == tbl]
            for i in range(0, len(pks), _PARAM_LIMIT):
                part = pks[i : i + _PARAM_LIMIT]
                marks = ", ".join("?" for _ in part)
                self.con.execute(
                    f"DELETE FROM _key_state WHERE tbl = ? AND pk IN ({marks})", [tbl, *part]
                )
        self._insert_many("INSERT INTO _key_state VALUES", "(?, ?, ?, ?)", rows)

    def key_state_count(self) -> int:
        return int(self.con.execute("SELECT COUNT(*) FROM _key_state").fetchone()[0])

    # -- snapshot progress and reader offset --------------------------------------------------

    def progress(self) -> dict[str, tuple[int, Pk | None, bool]]:
        """Latest completed chunk per table. The table is append-only; a finished table beats
        any chunk and otherwise the highest chunk id wins, so a late older batch is harmless."""
        rows = self.con.execute(
            "SELECT tbl, chunk_id, last_key, done FROM _snapshot_progress "
            "ORDER BY tbl, done, chunk_id"
        ).fetchall()
        out: dict[str, tuple[int, Pk | None, bool]] = {}
        for t, c, k, d in rows:  # ascending, so the last row per table is the best
            out[t] = (int(c), tuple(json.loads(k)) if k else None, bool(d))
        return out

    def save_progress(self, tbl: str, chunk_id: int, last_key: Pk | None, done: bool) -> None:
        key = pk_text(last_key) if last_key is not None else None
        self.con.execute(
            "INSERT INTO _snapshot_progress VALUES (?, ?, ?, ?)", (tbl, chunk_id, key, done)
        )

    def all_tables_done(self) -> bool:
        progress = self.progress()
        return all(progress.get(t.name, (0, None, False))[2] for t in TABLES)

    def load_offset(self) -> int | None:
        row = self.con.execute("SELECT MAX(lsn) FROM _reader_offset").fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def save_offset(self, lsn: int) -> None:
        self.con.execute("INSERT INTO _reader_offset VALUES (?)", (lsn,))

    # -- trace --------------------------------------------------------------------------------

    def add_trace(self, rows: Sequence[tuple]) -> None:
        self._insert_many("INSERT INTO _trace VALUES", "(?, ?, ?, ?, ?, ?, ?, ?)", rows)

    def has_trace(self) -> bool:
        row = self.con.execute("SELECT COUNT(*) FROM _trace").fetchone()
        return bool(row and row[0])

    def trace_rows(self, tbl: str, pk: Pk) -> list[tuple]:
        return self.con.execute(
            "SELECT seq, lsn, kind, chunk_id, decision, detail FROM _trace "
            "WHERE tbl = ? AND pk = ? ORDER BY lsn, seq",
            (tbl, pk_text(pk)),
        ).fetchall()

    # -- helpers ------------------------------------------------------------------------------

    def _insert_many(self, head: str, marks: str, rows: Iterable[Sequence]) -> None:
        rows = list(rows)
        if not rows:
            return
        step = max(1, _PARAM_LIMIT // marks.count("?"))
        for i in range(0, len(rows), step):
            part = rows[i : i + step]
            sql = f"{head} {', '.join(marks for _ in part)}"
            self.con.execute(sql, [v for row in part for v in row])


class Tracer:
    """Collects decisions in memory and writes them to ``_trace`` in batches."""

    def __init__(self, target: Target, enabled: bool = True) -> None:
        self.target = target
        self.enabled = enabled
        self._rows: list[tuple] = []
        self._seq = 0

    def record(self, decisions: Iterable[Decision]) -> None:
        if not self.enabled:
            return
        for d in decisions:
            self._seq += 1
            self._rows.append(
                (self._seq, d.tbl, pk_text(d.pk), d.lsn, d.kind, d.chunk_id, d.decision, d.detail)
            )

    def flush(self) -> None:
        if self._rows:
            self.target.add_trace(self._rows)
            self._rows = []
