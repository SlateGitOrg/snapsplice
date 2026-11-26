"""The watermark snapshotter: chunked, lock-free, spliced into the log by LOW/HIGH markers.

Per table the snapshotter reads primary-key-ordered chunks of ``chunk_size`` rows after the last
completed chunk's upper key. For each chunk it (1) inserts a LOW watermark, (2) selects the chunk
into memory and (3) inserts a HIGH watermark. The markers travel through the change log, so the
log processor (``observe``) sees them in LSN order with every data change. A key that changes
between LOW and HIGH is removed from the chunk buffer, because the log carries its newer state;
at HIGH the surviving rows are emitted as a ``SnapshotBatch`` stamped with HIGH's LSN.

Only one chunk is in flight at a time. The in-memory state (open chunk, window) is lost on a
crash; a restarted snapshotter rebuilds from the progress persisted in the target and ignores
markers left behind by the dead attempt, because it matches markers by watermark id.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from enum import Enum

from . import source as src
from .events import Decision, Event, Hint, SnapshotBatch
from .schema import OP_HIGH, OP_LOW, SPECS, TABLES, Pk, TableSpec


class Phase(Enum):
    IDLE = "idle"
    LOW = "low"  # LOW inserted, chunk not read yet
    SELECTED = "selected"  # chunk read, HIGH not inserted yet
    HIGH = "high"  # HIGH inserted, waiting for the log processor to reach it
    FINISHED = "finished"


@dataclass
class Chunk:
    tbl: str
    chunk_id: int
    low_wm_id: int
    high_wm_id: int | None = None
    rows: list[tuple] = field(default_factory=list)


@dataclass
class Window:
    """Log-side view of an open chunk: keys that changed since its LOW marker was seen."""

    tbl: str
    chunk_id: int
    low_lsn: int
    touched: dict[Pk, int] = field(default_factory=dict)


@dataclass
class Observation:
    window_chunk: int | None = None
    batch: SnapshotBatch | None = None
    decisions: list[Decision] = field(default_factory=list)


class WatermarkSnapshotter:
    def __init__(
        self,
        conn: sqlite3.Connection,
        chunk_size: int,
        progress: dict[str, tuple[int, Pk | None, bool]] | None = None,
    ) -> None:
        self.conn = conn
        self.chunk_size = chunk_size
        self.phase = Phase.IDLE
        self.chunk: Chunk | None = None
        self.window: Window | None = None
        self.steps_in_chunk = 0
        self._table_idx = 0
        self._chunk_id = 0
        self._last_key: Pk | None = None
        self._enter_table(progress or {})

    # -- table cursor -------------------------------------------------------------------------

    def _enter_table(self, progress: dict[str, tuple[int, Pk | None, bool]]) -> None:
        while self._table_idx < len(TABLES):
            chunk_id, last_key, done = progress.get(TABLES[self._table_idx].name, (0, None, False))
            if not done:
                self._chunk_id, self._last_key = chunk_id, last_key
                return
            self._table_idx += 1
        self.phase = Phase.FINISHED

    @property
    def spec(self) -> TableSpec:
        return TABLES[self._table_idx]

    # -- snapshot side: LOW, SELECT, HIGH -----------------------------------------------------

    def step(self) -> str | None:
        """Advance the chunk state machine by one step; ``None`` if there is nothing to do."""
        if self.phase in (Phase.HIGH, Phase.FINISHED):
            return None
        if self.phase is Phase.IDLE:
            chunk_id = self._chunk_id + 1
            wm = src.insert_watermark(self.conn, "low", self.spec.name, chunk_id)
            self.chunk = Chunk(self.spec.name, chunk_id, wm)
            self.phase = Phase.LOW
            action = "low"
        elif self.phase is Phase.LOW:
            assert self.chunk is not None
            self.chunk.rows = src.select_chunk(
                self.conn, self.spec, self._last_key, self.chunk_size
            )
            self.phase = Phase.SELECTED
            action = "select"
        else:
            assert self.chunk is not None
            self.chunk.high_wm_id = src.insert_watermark(
                self.conn, "high", self.chunk.tbl, self.chunk.chunk_id
            )
            self.phase = Phase.HIGH
            action = "high"
        self.steps_in_chunk += 1
        return action

    # -- log side: windows and emission -------------------------------------------------------

    def observe(self, ev: Event) -> Observation:
        """Process one log event in LSN order; may close a window and emit a chunk."""
        obs = Observation()
        ch = self.chunk
        if ev.op == OP_LOW:
            if ch is not None and self.window is None and ev.wm_id == ch.low_wm_id:
                self.window = Window(ch.tbl, ch.chunk_id, ev.lsn)
            return obs
        if ev.op == OP_HIGH:
            if (
                ch is not None
                and self.window is not None
                and ch.high_wm_id is not None
                and ev.wm_id == ch.high_wm_id
            ):
                obs.batch, obs.decisions = self._close(ev.lsn)
            return obs
        w = self.window
        if w is not None and ev.tbl == w.tbl and ev.lsn > w.low_lsn:
            w.touched.setdefault(ev.pk, ev.lsn)
            obs.window_chunk = w.chunk_id
        return obs

    def _close(self, high_lsn: int) -> tuple[SnapshotBatch, list[Decision]]:
        ch, w = self.chunk, self.window
        assert ch is not None and w is not None
        spec = SPECS[ch.tbl]
        keep: list[tuple] = []
        decisions: list[Decision] = []
        for row in ch.rows:
            pk = spec.pk_of(row)
            conflict = w.touched.get(pk)
            if conflict is None:
                keep.append(row)
                detail = f"low={w.low_lsn} high={high_lsn}"
                decisions.append(
                    Decision("chunk_row", ch.tbl, pk, high_lsn, "emitted", ch.chunk_id, detail)
                )
            else:
                detail = f"low={w.low_lsn} high={high_lsn} conflict_lsn={conflict}"
                decisions.append(
                    Decision(
                        "chunk_row", ch.tbl, pk, high_lsn, "dropped_in_window", ch.chunk_id, detail
                    )
                )
        last_key = spec.pk_of(ch.rows[-1]) if ch.rows else self._last_key
        done = len(ch.rows) < self.chunk_size
        batch = SnapshotBatch(
            ch.tbl,
            ch.chunk_id,
            high_lsn,
            tuple(keep),
            last_key,
            done,
            w.low_lsn,
            len(ch.rows) - len(keep),
        )
        self._last_key, self._chunk_id = last_key, ch.chunk_id
        self.chunk, self.window = None, None
        self.steps_in_chunk = 0
        self.phase = Phase.IDLE
        if done:
            self._table_idx += 1
            self._enter_table({})
        return batch, decisions

    # -- hints for the workload ---------------------------------------------------------------

    def hint(self) -> Hint | None:
        if self.phase is Phase.FINISHED:
            return None
        done = tuple(t.name for t in TABLES[: self._table_idx])
        keys = None
        phase = "idle"
        if self.phase is Phase.LOW:
            phase = "before_select"
        elif self.phase in (Phase.SELECTED, Phase.HIGH) and self.chunk is not None:
            phase = "after_select"
            keys = tuple(self.spec.pk_of(r) for r in self.chunk.rows)
        return Hint(self.spec.name, phase, self._last_key, self.chunk_size, keys, done)
