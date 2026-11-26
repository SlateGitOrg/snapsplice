"""The concurrent writer: a seeded stream of operations against the source.

Operations are 60% updates, 20% inserts, 15% deletes and 5% location moves (a primary-key change
performed as delete + insert in one transaction). When the snapshot position is known, a share of
the operations is aimed at the chunk in flight: updates and deletes of keys the chunk selects
(before the SELECT and just after it), and inserts into the completed range, the chunk's own
range and the range not yet reached.

With ``targeting=False`` the operation stream depends only on the seed, so two runs with
different chunk sizes see exactly the same writes.
"""

from __future__ import annotations

import random
import sqlite3
from bisect import bisect_right, insort
from dataclasses import dataclass, field

from . import source as src
from .events import Hint
from .schema import INVENTORY, SHIPMENTS, STATUS_LIFECYCLE, Pk, TableSpec
from .synth import (
    LOCATIONS,
    SynthConfig,
    inventory_universe,
    new_shipment_row,
    shipment_universe,
    timestamp,
)

_CANCELLABLE = ("created", "picked", "packed")


@dataclass
class Mirror:
    """The writer's own copy of one table: it is the only writer, so it stays exact."""

    rows: dict[Pk, tuple]
    keys: list[Pk] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.keys = sorted(self.rows)

    def add(self, row: tuple, spec: TableSpec) -> None:
        pk = spec.pk_of(row)
        self.rows[pk] = row
        insort(self.keys, pk)

    def remove(self, pk: Pk) -> None:
        del self.rows[pk]
        i = bisect_right(self.keys, pk) - 1
        del self.keys[i]


class Writer:
    def __init__(
        self, conn: sqlite3.Connection, cfg: SynthConfig, seed: int, targeting: bool = True
    ) -> None:
        self.conn, self.cfg, self.targeting = conn, cfg, targeting
        self.rng = random.Random(f"{seed}:writer")
        self.mirrors = {s.name: Mirror(src.table_rows(conn, s)) for s in (INVENTORY, SHIPMENTS)}
        self.universe = {
            INVENTORY.name: sorted(inventory_universe(cfg)),
            SHIPMENTS.name: sorted(shipment_universe(cfg)),
        }
        self.done = 0
        self._tick = 1

    @property
    def finished(self) -> bool:
        return self.done >= self.cfg.operations

    # -- one operation ------------------------------------------------------------------------

    def step(self, hint: Hint | None = None) -> str:
        """Perform one operation and return its kind: update, insert, delete or move."""
        cfg, rng = self.cfg, self.rng
        roll = rng.random()
        if roll < cfg.p_update:
            op = "update"
        elif roll < cfg.p_update + cfg.p_insert:
            op = "insert"
        elif roll < cfg.p_update + cfg.p_insert + cfg.p_delete:
            op = "delete"
        else:
            op = "move"
        spec = INVENTORY if op == "move" or rng.random() < cfg.p_inventory else SHIPMENTS
        aim = self.targeting and hint is not None and rng.random() < cfg.p_target
        self._tick += 1
        mirror = self.mirrors[spec.name]
        if op != "insert" and not mirror.keys:
            op = "insert"
        done = False
        if op == "insert":
            done = self._insert(spec, hint, aim)
        elif op == "delete":
            done = self._delete(spec, hint, aim)
        elif op == "move":
            done = self._move(spec, hint, aim)
        if not done:
            op = "update"
            self._update(spec, hint, aim)
        self.done += 1
        return op

    # -- operations ---------------------------------------------------------------------------

    def _update(self, spec: TableSpec, hint: Hint | None, aim: bool) -> None:
        mirror = self.mirrors[spec.name]
        pk = self._pick_existing(spec, hint, aim, shipments_cancellable=False)
        old = mirror.rows[pk]
        new = self._changed_row(spec, old)
        sets = [c for c in spec.column_names if c not in spec.pk]
        values = [new[spec.column_names.index(c)] for c in sets]
        where = " AND ".join(f"{c} = ?" for c in spec.pk)
        self.conn.execute(
            f"UPDATE {spec.name} SET {', '.join(f'{c} = ?' for c in sets)} WHERE {where}",
            (*values, *pk),
        )
        mirror.rows[pk] = new

    def _changed_row(self, spec: TableSpec, old: tuple) -> tuple:
        rng, stamp = self.rng, timestamp(self._tick)
        if spec is INVENTORY:
            on_hand = max(0, old[2] + rng.randint(-8, 25))
            allocated = min(on_hand, max(0, old[3] + rng.randint(-3, 5)))
            return (old[0], old[1], on_hand, allocated, stamp)
        status = old[2]
        weight = old[4]
        if status != STATUS_LIFECYCLE[-1]:
            status = STATUS_LIFECYCLE[STATUS_LIFECYCLE.index(status) + 1]
        else:
            weight = max(100, weight + rng.randint(-50, 50))
        return (old[0], old[1], status, old[3], weight, stamp)

    def _insert(self, spec: TableSpec, hint: Hint | None, aim: bool) -> bool:
        pk = self._new_key(spec, hint)
        if pk is None:
            return False
        if spec is INVENTORY:
            on_hand = self.rng.randint(0, 500)
            row = (
                pk[0],
                pk[1],
                on_hand,
                self.rng.randint(0, min(on_hand, 40)),
                timestamp(self._tick),
            )
        else:
            row = new_shipment_row(self.rng, pk, "created", self._tick)
        self._insert_row(spec, row)
        return True

    def _insert_row(self, spec: TableSpec, row: tuple) -> None:
        marks = ", ".join("?" for _ in row)
        self.conn.execute(f"INSERT INTO {spec.name} VALUES ({marks})", row)
        self.mirrors[spec.name].add(row, spec)

    def _delete(self, spec: TableSpec, hint: Hint | None, aim: bool) -> bool:
        pk = self._pick_existing(spec, hint, aim, shipments_cancellable=True)
        if spec is SHIPMENTS and self.mirrors[spec.name].rows[pk][2] not in _CANCELLABLE:
            return False  # only a shipment that has not left the building can be cancelled
        self._delete_row(spec, pk)
        return True

    def _delete_row(self, spec: TableSpec, pk: Pk) -> None:
        where = " AND ".join(f"{c} = ?" for c in spec.pk)
        self.conn.execute(f"DELETE FROM {spec.name} WHERE {where}", pk)
        self.mirrors[spec.name].remove(pk)

    def _move(self, spec: TableSpec, hint: Hint | None, aim: bool) -> bool:
        """Location move: the position changes primary key, so it is delete + insert."""
        mirror = self.mirrors[spec.name]
        pk = self._pick_existing(spec, hint, aim, shipments_cancellable=False)
        free = [loc for loc in LOCATIONS if (pk[0], loc) not in mirror.rows]
        if not free:
            return False
        old = mirror.rows[pk]
        new = (pk[0], self.rng.choice(free), old[2], old[3], timestamp(self._tick))
        self.conn.execute("BEGIN")
        self._delete_row(spec, pk)
        self._insert_row(spec, new)
        self.conn.execute("COMMIT")
        return True

    # -- key selection ------------------------------------------------------------------------

    def _chunk_keys(self, spec: TableSpec, hint: Hint | None) -> list[Pk]:
        """Keys of the chunk the snapshot is working on, as far as the hint determines them."""
        if hint is None or hint.table != spec.name:
            return []
        mirror = self.mirrors[spec.name]
        if hint.keys is not None:
            return [k for k in hint.keys if k in mirror.rows]
        if hint.phase == "before_select":
            start = bisect_right(mirror.keys, hint.lo) if hint.lo is not None else 0
            return mirror.keys[start : start + hint.chunk_size]
        return []

    def _pick_existing(
        self, spec: TableSpec, hint: Hint | None, aim: bool, shipments_cancellable: bool
    ) -> Pk:
        mirror = self.mirrors[spec.name]
        pool = self._chunk_keys(spec, hint) if aim else []
        for _ in range(12):
            pk = self.rng.choice(pool) if pool else self.rng.choice(mirror.keys)
            if not (shipments_cancellable and spec is SHIPMENTS):
                return pk
            if mirror.rows[pk][2] in _CANCELLABLE:
                return pk
        return pk

    def _new_key(self, spec: TableSpec, hint: Hint | None) -> Pk | None:
        """A key that does not exist yet, placed in the completed, current or future range."""
        uni = self.universe[spec.name]
        rows = self.mirrors[spec.name].rows
        regions = [(0, len(uni))]
        if self.targeting and hint is not None:
            if hint.table == spec.name:
                lo = bisect_right(uni, hint.lo) if hint.lo is not None else 0
                chunk = self._chunk_keys(spec, hint)
                hi = bisect_right(uni, max(chunk)) if chunk else lo
                regions = [(0, lo), (lo, hi), (hi, len(uni))]
            elif spec.name in hint.done_tables:
                regions = [(0, len(uni))]
        regions = [r for r in regions if r[1] > r[0]]
        a, b = self.rng.choice(regions)
        for _ in range(24):
            pk = uni[self.rng.randrange(a, b)]
            if pk not in rows:
                return pk
        start = self.rng.randrange(len(uni))
        for i in range(len(uni)):
            pk = uni[(start + i) % len(uni)]
            if pk not in rows:
                return pk
        return None
