"""Delivery wrapper, persisted offset and the LSN guard."""

from __future__ import annotations

import random

from helpers import inv
from snapsplice import source as src
from snapsplice.applier import apply_guarded, apply_snapshot
from snapsplice.events import Event, SnapshotBatch
from snapsplice.pipeline import Delivery, Pipeline
from snapsplice.schema import INVENTORY
from snapsplice.target import Target


def make_events(n_per_table: int = 20) -> list[Event]:
    events = []
    for i in range(n_per_table):
        events.append(
            Event(2 * i + 1, "inventory_positions", "update", ("A", str(i)), inv("A", str(i)))
        )
        events.append(Event(2 * i + 2, "shipments", "delete", (f"SH-{i}",), None))
    return events


def test_delivery_keeps_lsn_order_within_each_table_and_interleaves_tables():
    d = Delivery(random.Random(4), dup_rate=0.0)
    events = make_events()
    for i in range(0, len(events), 6):
        d.push(i, events[i : i + 6])
    out = []
    while (b := d.pop()) is not None:
        out.append(b)
    for tbl in ("inventory_positions", "shipments"):
        lsns = [e.lsn for b in out if b.tbl == tbl for e in b.events]
        assert lsns == sorted(lsns) and len(lsns) == 20
    order = [b.tbl for b in out]
    assert order != sorted(order)  # tables are interleaved, not delivered one after the other


def test_delivery_redelivers_a_batch_when_duplication_is_certain():
    d = Delivery(random.Random(1), dup_rate=1.0)
    d.push(1, make_events(2))
    out = []
    while (b := d.pop()) is not None:
        out.append(b)
    assert len(out) == 4  # two table batches, each delivered twice
    assert sum(b.duplicate for b in out) == 2
    assert d.duplicates_delivered == 2


def test_offset_advances_only_over_fully_applied_reads_and_survives_restart():
    conn = src.connect()
    src.create_source(conn)
    target = Target()
    pipe = Pipeline(conn, target, random.Random(0), batch_size=4)
    assert pipe.committed == 0
    for i in range(8):
        conn.execute("INSERT INTO inventory_positions VALUES (?, 'L1', 1, 0, 'x')", (f"S{i}",))
        conn.execute("INSERT INTO shipments VALUES (?, 'o', 'created', 'DHL', 1, 'x')", (f"H{i}",))
    assert pipe.read_step() == 4 and pipe.read_step() == 4
    first = pipe.next_batch()
    assert first is not None
    pipe.ack(first)
    # the first read produced one piece per table; only one is acked, so nothing is committed
    assert target.load_offset() == 0
    while (b := pipe.next_batch()) is not None:
        pipe.ack(b)
    assert pipe.committed == 8
    resumed = Pipeline(conn, target, random.Random(0), batch_size=4)
    assert resumed.reader.position == 8


def test_apply_is_idempotent_and_rejects_stale_events():
    target = Target()
    new = Event(10, "inventory_positions", "update", ("A", "L1"), inv("A", "L1", 50))
    old = Event(7, "inventory_positions", "update", ("A", "L1"), inv("A", "L1", 1))
    first = apply_guarded(target, [new, old])
    assert [d.decision for d in first] == ["applied", "rejected_stale"]
    snapshot = (
        dict(target.table_rows(INVENTORY)),
        target.key_states("inventory_positions", ['["A","L1"]']),
    )
    again = apply_guarded(target, [new, old])
    assert [d.decision for d in again] == ["rejected_stale", "rejected_stale"]
    assert (
        dict(target.table_rows(INVENTORY)),
        target.key_states("inventory_positions", ['["A","L1"]']),
    ) == snapshot
    assert target.table_rows(INVENTORY)[("A", "L1")][2] == 50


def test_tombstone_blocks_a_snapshot_row_with_a_lower_lsn_but_not_a_higher_one():
    target = Target()
    insert = Event(5, "inventory_positions", "insert", ("A", "L1"), inv("A", "L1"))
    delete = Event(9, "inventory_positions", "delete", ("A", "L1"), inv("A", "L1"))
    apply_guarded(target, [insert, delete])
    assert target.table_rows(INVENTORY) == {}
    low = SnapshotBatch("inventory_positions", 1, 8, (inv("A", "L1"),), ("A", "L1"), False)
    assert [d.decision for d in apply_snapshot(target, low)] == ["rejected_tombstone"]
    assert target.table_rows(INVENTORY) == {}
    high = SnapshotBatch("inventory_positions", 2, 12, (inv("A", "L1"),), ("A", "L1"), False)
    assert [d.decision for d in apply_snapshot(target, high)] == ["applied"]
    assert ("A", "L1") in target.table_rows(INVENTORY)


def test_progress_only_moves_forward_when_an_older_batch_lands_late():
    target = Target()
    target.save_progress("shipments", 3, ("SH-00030",), False)
    target.save_progress("shipments", 2, ("SH-00020",), False)  # late, older batch
    assert target.progress()["shipments"] == (3, ("SH-00030",), False)
    target.save_progress("shipments", 4, ("SH-00040",), True)
    target.save_progress("shipments", 3, ("SH-00030",), False)
    assert target.progress()["shipments"] == (4, ("SH-00040",), True)


def test_snapshot_row_behind_a_later_update_is_rejected_as_stale_not_as_tombstone():
    target = Target()
    update = Event(20, "inventory_positions", "update", ("A", "L1"), inv("A", "L1", 9))
    apply_guarded(target, [update])
    batch = SnapshotBatch("inventory_positions", 1, 15, (inv("A", "L1", 1),), ("A", "L1"), False)
    assert [d.decision for d in apply_snapshot(target, batch)] == ["rejected_stale"]
    assert target.table_rows(INVENTORY)[("A", "L1")][2] == 9
