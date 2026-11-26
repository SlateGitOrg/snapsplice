"""Backfill strategies behind one interface.

``WatermarkStrategy`` is the splice: chunks bracketed by LOW/HIGH markers, rows dropped when the
log shows a change inside the window, every apply guarded by LSN. ``DumpStrategy`` is the
comparison baseline in two flavours that read the table in PK-ordered pages interleaved with
writes, as a long-running dump without a consistent snapshot does:

* ``naive``: tailing starts at the LSN when the dump starts; the dumped rows are applied as plain
  upserts when the dump completes, without LSN guards.
* ``dump-then-tail``: tailing starts at the LSN when the dump ends.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Protocol

from . import source as src
from .applier import apply_guarded, apply_plain, apply_snapshot, upsert_plain
from .events import Batch, Decision, Hint, RunStats, SnapshotBatch
from .schema import TABLES, Pk
from .snapshotter import Phase, WatermarkSnapshotter
from .target import Target, Tracer

STRATEGIES = ("watermark", "naive", "dump-then-tail")


class Strategy(Protocol):
    name: str

    def snapshot_step(self) -> str | None: ...
    def process(self, batch: Batch) -> list[SnapshotBatch]: ...
    def apply_snapshot(self, batch: SnapshotBatch) -> None: ...
    def hint(self) -> Hint | None: ...
    @property
    def finished(self) -> bool: ...


@dataclass(frozen=True)
class CrashPlan:
    """Kill the snapshotter once ``after_chunk`` chunks were emitted, ``phase_steps`` steps into
    the next chunk (0 = between chunks, 1 = after LOW, 2 = after SELECT, 3 = after HIGH), and
    bring it back after ``downtime`` snapshot steps."""

    after_chunk: int
    phase_steps: int
    downtime: int


class WatermarkStrategy:
    name = "watermark"

    def __init__(
        self,
        conn: sqlite3.Connection,
        target: Target,
        chunk_size: int,
        stats: RunStats,
        tracer: Tracer,
        crash: CrashPlan | None = None,
    ) -> None:
        self.conn, self.target, self.chunk_size = conn, target, chunk_size
        self.stats, self.tracer, self.crash = stats, tracer, crash
        self.snap: WatermarkSnapshotter | None = WatermarkSnapshotter(
            conn, chunk_size, target.progress()
        )
        self._down_for = 0
        self._emitted: set[tuple[str, int]] = set()

    # -- snapshot side ------------------------------------------------------------------------

    def snapshot_step(self) -> str | None:
        if self._down_for > 0:
            self._down_for -= 1
            if self._down_for == 0:
                self.snap = WatermarkSnapshotter(self.conn, self.chunk_size, self.target.progress())
            return None
        if self._should_crash():
            self._kill()
            return None
        assert self.snap is not None
        action = self.snap.step()
        if action is not None and self._should_crash():
            self._kill()
        return action

    def _should_crash(self) -> bool:
        plan, snap = self.crash, self.snap
        return (
            plan is not None
            and snap is not None
            and self.stats.crashed_after_chunk is None
            and self.stats.chunks_emitted >= plan.after_chunk
            and snap.steps_in_chunk == plan.phase_steps
        )

    def _kill(self) -> None:
        assert self.crash is not None
        self.stats.crashed_after_chunk = self.stats.chunks_emitted
        self.snap = None
        self._down_for = max(1, self.crash.downtime)

    @property
    def finished(self) -> bool:
        return (
            self._down_for == 0
            and self.snap is not None
            and self.snap.phase is Phase.FINISHED
            and self.target.all_tables_done()
        )

    def hint(self) -> Hint | None:
        return self.snap.hint() if self.snap is not None else None

    # -- log side -----------------------------------------------------------------------------

    def process(self, batch: Batch) -> list[SnapshotBatch]:
        emitted: list[SnapshotBatch] = []
        decisions: list[Decision] = []
        window_of: dict[int, int] = {}
        for ev in batch.events:
            if self.snap is None:
                continue
            obs = self.snap.observe(ev)
            if obs.window_chunk is not None:
                window_of[ev.lsn] = obs.window_chunk
            if obs.batch is not None:
                emitted.append(obs.batch)
                decisions.extend(obs.decisions)
                self.stats.rows_dropped_in_window += obs.batch.dropped
                self.stats.chunks_emitted += 1
                key = (obs.batch.tbl, obs.batch.chunk_id)
                if key in self._emitted:
                    self.stats.chunks_redone += 1
                self._emitted.add(key)
        applied = apply_guarded(self.target, batch.events)
        for d in applied:
            d.chunk_id = window_of.get(d.lsn)
            if d.decision == "applied":
                self.stats.events_applied += 1
            else:
                self.stats.events_rejected += 1
        self.tracer.record(decisions)
        self.tracer.record(applied)
        return emitted

    def apply_snapshot(self, batch: SnapshotBatch) -> None:
        decisions = apply_snapshot(self.target, batch)
        for d in decisions:
            if d.decision == "applied":
                self.stats.snapshot_rows_applied += 1
            else:
                self.stats.snapshot_rows_rejected += 1
                if d.decision == "rejected_tombstone":
                    self.stats.snapshot_rows_rejected_tombstone += 1
        self.tracer.record(decisions)


class DumpStrategy:
    def __init__(
        self,
        conn: sqlite3.Connection,
        target: Target,
        chunk_size: int,
        stats: RunStats,
        tracer: Tracer,
        mode: str,
    ) -> None:
        if mode not in ("naive", "dump-then-tail"):
            raise ValueError(mode)
        self.name = mode
        self.conn, self.target, self.chunk_size = conn, target, chunk_size
        self.stats, self.tracer = stats, tracer
        self._idx = 0
        self._last_key: Pk | None = None
        self._buffer: list[tuple] = []
        self._started = False
        self._tail_from: dict[str, int] = {}
        self._just_read: tuple[Pk, ...] | None = None

    def snapshot_step(self) -> str | None:
        if self.finished:
            return None
        spec = TABLES[self._idx]
        if not self._started:
            self._started = True
            if self.name == "naive":
                self._tail_from[spec.name] = src.max_lsn(self.conn)
        page = src.select_chunk(self.conn, spec, self._last_key, self.chunk_size)
        self._buffer.extend(page)
        self._just_read = tuple(spec.pk_of(r) for r in page)
        if page:
            self._last_key = spec.pk_of(page[-1])
        if len(page) < self.chunk_size:
            upsert_plain(self.target, spec.name, self._buffer)
            if self.name == "dump-then-tail":
                self._tail_from[spec.name] = src.max_lsn(self.conn)
            self._idx += 1
            self._last_key, self._buffer, self._started, self._just_read = None, [], False, None
            self.stats.chunks_emitted += 1
            return "dump_done"
        self.stats.chunks_emitted += 1
        return "read"

    @property
    def finished(self) -> bool:
        return self._idx >= len(TABLES)

    def hint(self) -> Hint | None:
        if self.finished:
            return None
        spec = TABLES[self._idx]
        done = tuple(t.name for t in TABLES[: self._idx])
        phase = "after_select" if self._just_read is not None else "before_select"
        return Hint(spec.name, phase, self._last_key, self.chunk_size, self._just_read, done)

    def process(self, batch: Batch) -> list[SnapshotBatch]:
        start = self._tail_from.get(batch.tbl)
        keep = [e for e in batch.events if start is not None and e.lsn > start]
        kept = {e.lsn for e in keep}
        decisions = [
            Decision(
                "event",
                e.tbl,
                e.pk,
                e.lsn,
                "applied" if e.lsn in kept else "skipped_before_tail",
            )
            for e in batch.events
            if e.is_data
        ]
        self.stats.events_applied += apply_plain(self.target, keep)
        self.tracer.record(decisions)
        return []

    def apply_snapshot(self, batch: SnapshotBatch) -> None:  # pragma: no cover - never emitted
        raise NotImplementedError


def make_strategy(
    name: str,
    conn: sqlite3.Connection,
    target: Target,
    chunk_size: int,
    stats: RunStats,
    tracer: Tracer,
    crash: CrashPlan | None = None,
) -> Strategy:
    if name == "watermark":
        return WatermarkStrategy(conn, target, chunk_size, stats, tracer, crash)
    return DumpStrategy(conn, target, chunk_size, stats, tracer, name)
