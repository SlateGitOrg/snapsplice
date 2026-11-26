"""Hand-built interleavings: each pins one rule of the splice."""

from __future__ import annotations

import pytest

from helpers import finish, inv, make_rig, ship, tables_equal
from snapsplice.schema import INVENTORY
from snapsplice.strategies import CrashPlan
from snapsplice.trace import timeline

KEY = ("SKU-0003", "L1")
ROWS = [inv(f"SKU-{n:04d}", "L1", on_hand=n * 10) for n in range(1, 6)]


def decisions(conn, target, pk=KEY):
    return [
        (line.what, line.decision) for line in timeline(conn, target, "inventory_positions", pk)
    ]


@pytest.mark.parametrize("when", ["after_low", "after_select"])
def test_update_between_watermarks_drops_chunk_row_and_log_value_lands(when):
    conn, target, sim = make_rig(ROWS)
    assert sim.snapshot_step() == "low"
    if when == "after_low":
        conn.execute("UPDATE inventory_positions SET qty_on_hand = 999 WHERE sku = 'SKU-0003'")
    assert sim.snapshot_step() == "select"
    if when == "after_select":
        conn.execute("UPDATE inventory_positions SET qty_on_hand = 999 WHERE sku = 'SKU-0003'")
    assert sim.snapshot_step() == "high"
    finish(sim)
    assert target.table_rows(INVENTORY)[KEY][2] == 999
    assert tables_equal(conn, target)
    assert ("chunk_row", "dropped_in_window") in decisions(conn, target)
    assert sim.stats.rows_dropped_in_window == 1


def test_update_outside_window_does_not_drop_the_row():
    conn, target, sim = make_rig(ROWS)
    conn.execute("UPDATE inventory_positions SET qty_on_hand = 5 WHERE sku = 'SKU-0003'")
    finish(sim)
    assert ("chunk_row", "emitted") in decisions(conn, target)
    assert sim.stats.rows_dropped_in_window == 0
    assert tables_equal(conn, target)


def test_delete_after_select_is_not_resurrected():
    conn, target, sim = make_rig(ROWS)
    sim.snapshot_step()
    sim.snapshot_step()  # chunk is in memory, including SKU-0003
    conn.execute("DELETE FROM inventory_positions WHERE sku = 'SKU-0003'")
    sim.snapshot_step()
    finish(sim)
    assert KEY not in target.table_rows(INVENTORY)
    assert len(target.table_rows(INVENTORY)) == 4
    assert tables_equal(conn, target)


def test_delete_after_high_with_delayed_snapshot_apply_rejects_the_snapshot_row():
    conn, target, sim = make_rig(ROWS)
    for _ in range(3):
        sim.snapshot_step()
    sim.hold_snapshots = True
    sim.pump(release_snapshots=False)  # HIGH is processed; the chunk waits in the lane
    assert len(sim.lane.held) == 1
    conn.execute("DELETE FROM inventory_positions WHERE sku = 'SKU-0003'")
    sim.pump(release_snapshots=False)  # the tombstone lands first
    batch_lsn = sim.lane.held[0][1].lsn
    state = target.key_states("inventory_positions", ['["SKU-0003","L1"]'])
    tombstone_lsn, deleted = state['["SKU-0003","L1"]']
    assert deleted and tombstone_lsn > batch_lsn
    sim.hold_snapshots = False
    sim.lane_step(force=True)
    assert KEY not in target.table_rows(INVENTORY)
    finish(sim)
    assert ("snapshot", "rejected_tombstone") in decisions(conn, target)
    assert tables_equal(conn, target)


def test_insert_into_completed_and_future_range_appear_exactly_once():
    rows = [inv(f"SKU-{n:04d}", "L1") for n in (10, 20, 30, 40, 50, 60)]
    conn, target, sim = make_rig(rows, chunk_size=2)
    for _ in range(3):
        sim.snapshot_step()
    sim.pump()  # chunk 1 (SKU-0010, SKU-0020) is complete
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0015', 'L2', 7, 0, 'x')")  # behind
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0055', 'L2', 8, 0, 'x')")  # ahead
    finish(sim)
    assert tables_equal(conn, target)
    for sku in ("SKU-0015", "SKU-0055"):
        n = target.con.execute(
            "SELECT COUNT(*) FROM inventory_positions WHERE sku = ?", (sku,)
        ).fetchone()[0]
        assert n == 1
    assert len(target.table_rows(INVENTORY)) == len(rows) + 2


def test_insert_inside_the_open_window_arrives_via_the_log():
    conn, target, sim = make_rig([inv("SKU-0001", "L1")], chunk_size=5)
    sim.snapshot_step()
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0002', 'L1', 1, 0, 'x')")
    sim.snapshot_step()
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0003', 'L1', 1, 0, 'x')")
    sim.snapshot_step()
    finish(sim)
    assert tables_equal(conn, target)


def test_delete_then_reinsert_of_the_same_key_keeps_the_latest_row():
    conn, target, sim = make_rig(ROWS)
    sim.snapshot_step()
    conn.execute("DELETE FROM inventory_positions WHERE sku = 'SKU-0003'")
    sim.snapshot_step()
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0003', 'L1', 77, 0, 'later')")
    sim.snapshot_step()
    finish(sim)
    assert target.table_rows(INVENTORY)[KEY][2] == 77


def test_shipments_table_is_snapshotted_after_inventory():
    conn, target, sim = make_rig(ROWS, [ship(1), ship(2, "shipped"), ship(3, "delivered")])
    finish(sim)
    assert len(target.table_rows(INVENTORY)) == 5
    assert target.con.execute("SELECT COUNT(*) FROM shipments").fetchone()[0] == 3
    assert target.all_tables_done()
    assert tables_equal(conn, target)


def test_crash_redo_path_reapplies_a_held_chunk_after_the_redo_and_ends_equal():
    """Chunk 1 is emitted but its batch is still held in the lane, so no progress was saved.
    The snapshotter is killed, restarts from the older (empty) progress and redoes chunk 1. The
    old batch is applied after the redo batch, with a delete in between."""
    rows = [inv(f"SKU-{n:04d}", "L1", on_hand=n) for n in range(1, 7)]
    conn, target, sim = make_rig(rows, chunk_size=3)
    sim.strategy.crash = CrashPlan(after_chunk=1, phase_steps=0, downtime=1)
    sim.hold_snapshots = True
    for _ in range(3):
        assert sim.snapshot_step() is not None  # LOW, SELECT, HIGH of chunk 1
    sim.pump(release_snapshots=False)
    assert sim.stats.chunks_emitted == 1 and target.progress() == {}
    assert sim.snapshot_step() is None  # killed between chunks
    assert sim.stats.crashed_after_chunk == 1
    assert sim.snapshot_step() is None  # downtime over: restarted from empty progress
    conn.execute("DELETE FROM inventory_positions WHERE sku = 'SKU-0002'")
    conn.execute("UPDATE inventory_positions SET qty_on_hand = 500 WHERE sku = 'SKU-0001'")
    for _ in range(3):
        assert sim.snapshot_step() is not None  # chunk 1 again
    sim.pump(release_snapshots=False)
    assert sim.stats.chunks_redone == 1
    assert len(sim.lane.held) == 2
    sim.lane.held.reverse()  # the redo batch lands first, the old batch after it
    sim.hold_snapshots = False
    sim.lane_step(force=True)
    assert sim.stats.snapshot_rows_rejected_tombstone >= 1  # old row of the deleted key
    assert sim.stats.snapshot_rows_rejected >= 3  # and the old rows the redo already replaced
    finish(sim)
    assert tables_equal(conn, target)
    assert target.table_rows(INVENTORY)[("SKU-0001", "L1")][2] == 500
    assert ("SKU-0002", "L1") not in target.table_rows(INVENTORY)
