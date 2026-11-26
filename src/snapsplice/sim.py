"""The deterministic scheduler.

A ``Simulation`` wires the source, the log pipeline, a strategy and the target, and exposes each
actor as a single step: a writer operation, a snapshot step (LOW / SELECT / HIGH for the
watermark strategy), a log read, a delivery-and-apply, and the late application of delayed
snapshot batches. ``run`` picks the next actor with a seeded RNG until the writer has finished
and everything has drained. Tests drive the same steps by hand to build exact interleavings.

Faults: duplicate redelivery of a batch, delayed application of a chunk's snapshot rows, and a
snapshotter crash after chunk k (watermark strategy only).
"""

from __future__ import annotations

import math
import random
import sqlite3
from dataclasses import dataclass, field

from . import source as src
from .events import RunStats, SnapshotBatch
from .pipeline import Pipeline
from .schema import TABLES
from .strategies import CrashPlan, Strategy, make_strategy
from .synth import SynthConfig
from .target import Target, Tracer
from .workload import Writer

MAX_TICKS = 500_000


@dataclass
class SnapshotLane:
    """Snapshot rows travel separately from the log and can be applied late."""

    held: list[tuple[int, SnapshotBatch]] = field(default_factory=list)

    def submit(self, release_tick: int, batch: SnapshotBatch) -> None:
        self.held.append((release_tick, batch))

    def due(self, tick: int) -> list[SnapshotBatch]:
        ready = [b for t, b in self.held if t <= tick]
        self.held = [(t, b) for t, b in self.held if t > tick]
        return ready

    def __bool__(self) -> bool:
        return bool(self.held)


class Simulation:
    def __init__(
        self,
        conn: sqlite3.Connection,
        target: Target,
        strategy: str,
        cfg: SynthConfig | None = None,
        seed: int = 0,
        chunk_size: int = 25,
        batch_size: int = 48,
        faults: bool = True,
        targeting: bool = True,
        trace: bool = False,
        crash: CrashPlan | None = None,
    ) -> None:
        self.conn, self.target, self.cfg, self.seed = conn, target, cfg, seed
        self.chunk_size = chunk_size
        self.stats = RunStats()
        self.tracer = Tracer(target, trace)
        self.rng = random.Random(f"{seed}:scheduler")
        self.fault_rng = random.Random(f"{seed}:faults")
        self.faults = faults
        self.tick = 0
        dup = cfg.dup_rate if (cfg and faults) else 0.0
        self.delay_rate = cfg.delay_rate if (cfg and faults) else 0.0
        self.pipeline = Pipeline(
            conn, target, random.Random(f"{seed}:delivery"), batch_size=batch_size, dup_rate=dup
        )
        self.writer = Writer(conn, cfg, seed, targeting) if cfg else None
        if crash is None and faults and strategy == "watermark":
            crash = self._crash_plan()
        self.strategy: Strategy = make_strategy(
            strategy, conn, target, chunk_size, self.stats, self.tracer, crash
        )
        self.lane = SnapshotLane()
        self.hold_snapshots = False

    # -- fault plan ---------------------------------------------------------------------------

    def _estimated_chunks(self) -> int:
        counts = src.row_counts(self.conn)
        return sum(math.ceil(counts[t.name] / self.chunk_size) + 1 for t in TABLES)

    def _crash_plan(self) -> CrashPlan:
        total = self._estimated_chunks()
        rng = self.fault_rng
        return CrashPlan(rng.randint(1, max(1, total - 1)), rng.randint(0, 3), rng.randint(2, 10))

    # -- single steps -------------------------------------------------------------------------

    def write_step(self) -> str:
        assert self.writer is not None
        op = self.writer.step(self.strategy.hint())
        self.stats.ops[op] = self.stats.ops.get(op, 0) + 1
        return op

    def snapshot_step(self) -> str | None:
        return self.strategy.snapshot_step()

    def read_step(self) -> int:
        return self.pipeline.read_step()

    def deliver_step(self) -> bool:
        batch = self.pipeline.next_batch()
        if batch is None:
            return False
        if batch.duplicate:
            self.stats.duplicate_batches += 1
        for sb in self.strategy.process(batch):
            delayed = self.fault_rng.random() < self.delay_rate
            if delayed:
                self.stats.delayed_snapshot_batches += 1
            wait = self.fault_rng.randint(3, 25) if delayed else 0
            self.lane.submit(self.tick + wait, sb)
        self.pipeline.ack(batch)
        return True

    def lane_step(self, force: bool = False) -> int:
        if self.hold_snapshots and not force:
            return 0
        ready = self.lane.due(10**12 if force else self.tick)
        for sb in ready:
            self.strategy.apply_snapshot(sb)
        return len(ready)

    # -- draining and scheduling --------------------------------------------------------------

    def pump(self, release_snapshots: bool = True) -> None:
        """Read and apply every pending log event; optionally apply all snapshot batches."""
        while True:
            progressed = self.read_step() > 0
            while self.deliver_step():
                progressed = True
            if not progressed:
                break
        if release_snapshots:
            self.lane_step(force=True)

    @property
    def quiescent(self) -> bool:
        return (
            (self.writer is None or self.writer.finished)
            and self.strategy.finished
            and self.pipeline.idle
            and not self.lane
        )

    def run(self) -> RunStats:
        ops = self.cfg.operations if self.cfg else 0
        snap_weight = 3 * self._estimated_chunks() / (0.7 * ops) if ops else 1.0
        actors = ("write", "snapshot", "read", "deliver")
        weights = (1.0, max(snap_weight, 0.05), 0.5, 0.8)
        while not self.quiescent:
            self.tick += 1
            if self.tick > MAX_TICKS:
                raise RuntimeError("simulation did not quiesce")
            self.lane_step()
            writer_done = self.writer is None or self.writer.finished
            w = (0.0 if writer_done else weights[0], *weights[1:])
            actor = self.rng.choices(actors, w)[0]
            if actor == "write":
                self.write_step()
            elif actor == "snapshot":
                self.snapshot_step()
            elif actor == "read":
                self.read_step()
            else:
                self.deliver_step()
        self.stats.ticks = self.tick
        self.stats.events_read = self.pipeline.events_read
        self.tracer.flush()
        return self.stats
