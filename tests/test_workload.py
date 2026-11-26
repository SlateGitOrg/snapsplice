"""The writer and the snapshotter's handling of stale state."""

from __future__ import annotations

import sqlite3

from helpers import SMALL, finish, inv, make_rig, tables_equal
from snapsplice import source as src
from snapsplice.events import Event
from snapsplice.schema import INVENTORY
from snapsplice.snapshotter import Phase, WatermarkSnapshotter
from snapsplice.synth import populate
from snapsplice.workload import Writer


def run_writer(seed: int, targeting: bool) -> tuple[sqlite3.Connection, list[str]]:
    conn = src.connect()
    populate(conn, SMALL, seed)
    w = Writer(conn, SMALL, seed, targeting)
    return conn, [w.step() for _ in range(SMALL.operations)]


def test_untargeted_writer_stream_depends_only_on_the_seed():
    a, ops_a = run_writer(8, targeting=False)
    b, ops_b = run_writer(8, targeting=False)
    assert ops_a == ops_b
    assert src.table_rows(a, INVENTORY) == src.table_rows(b, INVENTORY)


def test_writer_mirror_stays_equal_to_the_source():
    conn = src.connect()
    populate(conn, SMALL, 2)
    w = Writer(conn, SMALL, 2)
    for _ in range(SMALL.operations):
        w.step()
    for name, mirror in w.mirrors.items():
        spec = {"inventory_positions": INVENTORY}.get(name)
        if spec is not None:
            assert mirror.rows == src.table_rows(conn, spec)
        assert mirror.keys == sorted(mirror.rows)


def test_targeted_writes_land_inside_the_chunk_being_snapshotted():
    conn = src.connect()
    populate(conn, SMALL, 6)
    snap = WatermarkSnapshotter(conn, 10)
    snap.step()  # LOW inserted, chunk not read yet
    hint = snap.hint()
    assert hint is not None and hint.phase == "before_select"
    start = src.max_lsn(conn)
    w = Writer(conn, SMALL, 6)
    window = {pk for pk in w._chunk_keys(INVENTORY, hint)}
    assert len(window) == 10
    hits = 0
    for _ in range(40):
        w.step(hint)
    for ev in src.read_log(conn, start, 1000):
        if ev.tbl == "inventory_positions" and ev.op in ("update", "delete"):
            hits += ev.pk in window
    assert hits >= 3  # far more than the ~4% a uniform writer would produce


def test_snapshotter_ignores_markers_left_by_a_dead_attempt():
    conn = src.connect()
    src.create_source(conn)
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0001', 'L1', 1, 0, 'x')")
    dead = WatermarkSnapshotter(conn, 5)
    dead.step()
    dead.step()
    dead.step()  # LOW, SELECT, HIGH inserted; then the process dies
    fresh = WatermarkSnapshotter(conn, 5)
    fresh.step()  # a new attempt of the same chunk
    emitted = [fresh.observe(ev).batch for ev in src.read_log(conn, 0, 100)]
    assert [b for b in emitted if b] == []  # stale LOW and HIGH are not its markers
    assert fresh.phase is Phase.LOW
    fresh.step()
    fresh.step()
    batches = [fresh.observe(ev).batch for ev in src.read_log(conn, 0, 100)]
    batch = next(b for b in batches if b)
    assert [r[0] for r in batch.rows] == ["SKU-0001"]


def test_events_before_the_low_watermark_do_not_drop_chunk_rows():
    conn, target, sim = make_rig([inv("SKU-0001", "L1"), inv("SKU-0002", "L1")])
    conn.execute("UPDATE inventory_positions SET qty_on_hand = 3 WHERE sku = 'SKU-0001'")
    sim.snapshot_step()
    sim.snapshot_step()
    sim.snapshot_step()
    finish(sim)
    assert sim.stats.rows_dropped_in_window == 0
    assert tables_equal(conn, target)


def test_observe_returns_window_membership_only_inside_the_window():
    conn = src.connect()
    src.create_source(conn)
    snap = WatermarkSnapshotter(conn, 5)
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0001', 'L1', 1, 0, 'x')")
    snap.step()
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0002', 'L1', 1, 0, 'x')")
    events: list[Event] = src.read_log(conn, 0, 10)
    inside = [snap.observe(e).window_chunk for e in events]
    assert inside == [None, None, 1]  # insert before LOW, the LOW marker, insert after LOW
