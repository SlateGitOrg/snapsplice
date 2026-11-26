"""One key's timeline: its log events, chunk membership and the decision taken for each."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass

from .schema import SPECS, Pk, pk_text
from .target import Target


@dataclass(frozen=True)
class TraceLine:
    lsn: int
    what: str
    detail: str
    decision: str


def _row_summary(tbl: str, row_json: str | None) -> str:
    if not row_json:
        return ""
    data = json.loads(row_json)
    spec = SPECS[tbl]
    return " ".join(f"{c}={data[c]}" for c in spec.column_names if c not in spec.pk)


def timeline(conn: sqlite3.Connection, target: Target, tbl: str, pk: Pk) -> list[TraceLine]:
    """Merge the key's source log events with the decisions recorded in the target.

    Rows from a chunk are placed at their HIGH watermark's LSN, which is the LSN they carry.
    """
    if tbl not in SPECS:
        raise ValueError(f"unknown table {tbl!r}")
    ordered: list[tuple[int, int, TraceLine]] = []
    decisions = target.trace_rows(tbl, pk)
    by_lsn: dict[int, list[tuple]] = {}
    for row in decisions:
        if row[2] == "event":
            by_lsn.setdefault(row[1], []).append(row)
    log = conn.execute(
        "SELECT lsn, op, row_json FROM _cdc_log WHERE tbl = ? AND pk_json = ? ORDER BY lsn",
        (tbl, pk_text(pk)),
    ).fetchall()
    for lsn, op, row_json in log:
        evs = by_lsn.get(lsn, [])
        verdict = " / ".join(e[4] for e in evs) if evs else "not applied (before backfill start)"
        windows = sorted({e[3] for e in evs if e[3] is not None})
        detail = _row_summary(tbl, row_json)
        if windows:
            detail += f"  (inside window of chunk {windows[0]})"
        ordered.append((lsn, 1, TraceLine(lsn, op, detail.strip(), verdict)))
    for _seq, lsn, kind, chunk_id, decision, detail in decisions:
        if kind == "chunk_row":
            low = re.search(r"low=(\d+)", detail or "")
            if low:
                ordered.append(
                    (
                        int(low.group(1)),
                        0,
                        TraceLine(
                            int(low.group(1)), "wm_low", f"chunk {chunk_id}", "window opened"
                        ),
                    )
                )
            ordered.append(
                (lsn, 2, TraceLine(lsn, "chunk_row", f"chunk {chunk_id}: {detail}", decision))
            )
        elif kind == "snapshot":
            ordered.append(
                (
                    lsn,
                    3,
                    TraceLine(
                        lsn,
                        "snapshot",
                        f"chunk {chunk_id} rows carry lsn {lsn} {detail}".strip(),
                        decision,
                    ),
                )
            )
    ordered.sort(key=lambda t: (t[0], t[1]))
    return [line for _, _, line in ordered]
