"""Log reader and the at-least-once delivery wrapper.

``LogReader`` reads ``_cdc_log`` after a position in batches. ``Delivery`` splits each batch by
table and hands the pieces out in a seeded order: different tables interleave differently from
run to run, a table's own batches keep LSN order, and a batch may be delivered twice in a row.
``Pipeline`` ties them to a persisted offset that only advances once every piece of a read has
been applied.
"""

from __future__ import annotations

import random
import sqlite3
from collections import deque
from dataclasses import dataclass, replace

from . import source as src
from .events import Batch, Event
from .target import Target


class LogReader:
    def __init__(self, conn: sqlite3.Connection, position: int, batch_size: int) -> None:
        self.conn = conn
        self.position = position
        self.batch_size = batch_size

    def poll(self) -> list[Event]:
        events = src.read_log(self.conn, self.position, self.batch_size)
        if events:
            self.position = events[-1].lsn
        return events

    @property
    def caught_up(self) -> bool:
        return src.max_lsn(self.conn) <= self.position


class Delivery:
    def __init__(self, rng: random.Random, dup_rate: float) -> None:
        self.rng = rng
        self.dup_rate = dup_rate
        self.queues: dict[str, deque[Batch]] = {}
        self.duplicates_delivered = 0

    def push(self, read_id: int, events: list[Event]) -> int:
        """Queue one batch per table present in ``events``. Returns the number of batches."""
        by_table: dict[str, list[Event]] = {}
        for ev in events:
            by_table.setdefault(ev.tbl, []).append(ev)
        for tbl, evs in by_table.items():
            self.queues.setdefault(tbl, deque()).append(Batch(read_id, tbl, tuple(evs)))
        return len(by_table)

    def pop(self) -> Batch | None:
        ready = sorted(t for t, q in self.queues.items() if q)
        if not ready:
            return None
        queue = self.queues[self.rng.choice(ready)]
        batch = queue.popleft()
        if not batch.duplicate and self.rng.random() < self.dup_rate:
            queue.appendleft(replace(batch, duplicate=True))
        if batch.duplicate:
            self.duplicates_delivered += 1
        return batch

    @property
    def empty(self) -> bool:
        return not any(self.queues.values())


@dataclass
class _Read:
    remaining: int
    max_lsn: int


class Pipeline:
    def __init__(
        self,
        conn: sqlite3.Connection,
        target: Target,
        rng: random.Random,
        batch_size: int = 48,
        dup_rate: float = 0.0,
    ) -> None:
        start = target.load_offset()
        if start is None:
            start = src.max_lsn(conn)
            target.save_offset(start)
        self.target = target
        self.reader = LogReader(conn, start, batch_size)
        self.delivery = Delivery(rng, dup_rate)
        self.committed = start
        self.events_read = 0
        self._read_id = 0
        self._reads: dict[int, _Read] = {}

    def read_step(self) -> int:
        events = self.reader.poll()
        if not events:
            return 0
        self._read_id += 1
        pieces = self.delivery.push(self._read_id, events)
        self._reads[self._read_id] = _Read(pieces, events[-1].lsn)
        self.events_read += len(events)
        return len(events)

    def next_batch(self) -> Batch | None:
        return self.delivery.pop()

    def ack(self, batch: Batch) -> None:
        """Mark a delivered batch applied; advance the persisted offset over finished reads."""
        if batch.duplicate:
            return
        self._reads[batch.read_id].remaining -= 1
        advanced = False
        while self._reads:
            first = min(self._reads)
            if self._reads[first].remaining > 0:
                break
            self.committed = self._reads.pop(first).max_lsn
            advanced = True
        if advanced:
            self.target.save_offset(self.committed)

    @property
    def idle(self) -> bool:
        return self.reader.caught_up and self.delivery.empty
