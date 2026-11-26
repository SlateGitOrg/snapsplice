"""Event types that flow between the log reader, the snapshotter and the applier."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from .schema import DATA_OPS, OP_HIGH, OP_LOW, SPECS, Pk, pk_text


@dataclass(frozen=True)
class Event:
    """One row of ``_cdc_log``. ``row`` is the full after-image (before-image for deletes)."""

    lsn: int
    tbl: str
    op: str
    pk: Pk
    row: tuple | None = None
    wm_id: int | None = None
    chunk_id: int | None = None

    @property
    def pk_text(self) -> str:
        return pk_text(self.pk)

    @property
    def is_data(self) -> bool:
        return self.op in DATA_OPS

    @property
    def is_marker(self) -> bool:
        return self.op in (OP_LOW, OP_HIGH)


def parse_log_row(lsn: int, tbl: str, op: str, pk_json: str, row_json: str | None) -> Event:
    payload = json.loads(row_json) if row_json else None
    if op in (OP_LOW, OP_HIGH):
        return Event(lsn, tbl, op, (), None, payload["wm_id"], payload["chunk_id"])
    spec = SPECS[tbl]
    return Event(lsn, tbl, op, tuple(json.loads(pk_json)), spec.row_from_dict(payload))


@dataclass(frozen=True)
class Batch:
    """A group of consecutive events of one table, as handed over by the delivery wrapper."""

    read_id: int
    tbl: str
    events: tuple[Event, ...]
    duplicate: bool = False


@dataclass(frozen=True)
class SnapshotBatch:
    """The surviving rows of one chunk, stamped with the HIGH watermark's LSN."""

    tbl: str
    chunk_id: int
    lsn: int
    rows: tuple[tuple, ...]
    last_key: Pk | None
    done: bool
    low_lsn: int = 0
    dropped: int = 0


@dataclass
class Decision:
    """What the applier did with one event or snapshot row."""

    kind: str  # "event" or "snapshot"
    tbl: str
    pk: Pk
    lsn: int
    decision: str
    chunk_id: int | None = None
    detail: str = ""


@dataclass
class RunStats:
    """Counters collected over one scheduled run."""

    ticks: int = 0
    ops: dict[str, int] = field(default_factory=dict)
    events_read: int = 0
    events_applied: int = 0
    events_rejected: int = 0
    chunks_emitted: int = 0
    rows_dropped_in_window: int = 0
    snapshot_rows_applied: int = 0
    snapshot_rows_rejected: int = 0  # all rejections
    snapshot_rows_rejected_tombstone: int = 0  # the subset where the stored state is a delete
    duplicate_batches: int = 0
    delayed_snapshot_batches: int = 0
    crashed_after_chunk: int | None = None
    chunks_redone: int = 0


@dataclass(frozen=True)
class Hint:
    """Where the snapshot currently is, so the workload can aim writes at the chunk in flight.

    ``phase`` is ``before_select`` (LOW inserted, rows not read yet), ``after_select`` (rows
    read, HIGH not yet seen) or ``idle``. ``lo`` is the upper key of the last completed chunk of
    ``table``; ``keys`` are the rows the current chunk read, when it has read them.
    """

    table: str
    phase: str
    lo: Pk | None
    chunk_size: int
    keys: tuple[Pk, ...] | None = None
    done_tables: tuple[str, ...] = ()
