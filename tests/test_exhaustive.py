"""Exhaustive check of one chunk: every interleaving of snapshot steps, writes and log pumps.

The snapshot chain is LOW, SELECT, HIGH. The writer chain is three operations. The pump chain is
one drain of the log (read, window processing, apply). Every merge of the three chains is run,
under three delivery conditions: plain, snapshot rows applied only after all the writes
(delayed application), and every batch delivered twice. Each run must end equal to the source.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from helpers import finish, inv, make_rig, tables_equal

ROWS = [inv("SKU-0001", "L1", 10), inv("SKU-0002", "L1", 20), inv("SKU-0003", "L1", 30)]

# Each scenario is a chain of three writer operations; a tuple is one transaction.
SCENARIOS = {
    "same key: update, delete, re-insert": [
        ("UPDATE inventory_positions SET qty_on_hand = 99 WHERE sku = 'SKU-0002'",),
        ("DELETE FROM inventory_positions WHERE sku = 'SKU-0002'",),
        ("INSERT INTO inventory_positions VALUES ('SKU-0002', 'L1', 55, 0, 'later')",),
    ],
    "insert ahead, update first, delete last": [
        ("INSERT INTO inventory_positions VALUES ('SKU-0004', 'L1', 4, 0, 'x')",),
        ("UPDATE inventory_positions SET qty_on_hand = 11 WHERE sku = 'SKU-0001'",),
        ("DELETE FROM inventory_positions WHERE sku = 'SKU-0003'",),
    ],
    "location move, update of the moved key, delete": [
        (
            "DELETE FROM inventory_positions WHERE sku = 'SKU-0002' AND location = 'L1'",
            "INSERT INTO inventory_positions VALUES ('SKU-0002', 'L2', 20, 0, 'moved')",
        ),
        ("UPDATE inventory_positions SET qty_on_hand = 21 WHERE sku = 'SKU-0002'",),
        ("DELETE FROM inventory_positions WHERE sku = 'SKU-0001'",),
    ],
}

CHAINS = {"snap": 3, "write": 3, "pump": 1}


def merges(remaining: dict[str, int]) -> Iterator[tuple[str, ...]]:
    """Every order-preserving merge of the chains, as a sequence of chain names."""
    if not any(remaining.values()):
        yield ()
        return
    for name, left in remaining.items():
        if left:
            rest = {**remaining, name: left - 1}
            for tail in merges(rest):
                yield (name, *tail)


def test_the_enumeration_covers_every_merge():
    assert sum(1 for _ in merges(CHAINS)) == 140  # 7! / (3! * 3! * 1!)


@pytest.mark.parametrize("variant", ["plain", "delayed_snapshot_apply", "duplicate_delivery"])
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_every_interleaving_ends_equal_to_the_source(scenario, variant):
    ops = SCENARIOS[scenario]
    dropped = tombstoned = rejected = 0
    for order in merges(CHAINS):
        conn, target, sim = make_rig(ROWS, chunk_size=5)
        sim.tracer.enabled = False
        if variant == "delayed_snapshot_apply":
            sim.hold_snapshots = True
        if variant == "duplicate_delivery":
            sim.pipeline.delivery.dup_rate = 1.0
        snap = write = 0
        for name in order:
            if name == "snap":
                sim.snapshot_step()
                snap += 1
            elif name == "write":
                conn.execute("BEGIN")
                for sql in ops[write]:
                    conn.execute(sql)
                conn.execute("COMMIT")
                write += 1
            else:
                sim.pump(release_snapshots=variant != "delayed_snapshot_apply")
        finish(sim)
        assert tables_equal(conn, target), f"diverged for interleaving {order}"
        dropped += sim.stats.rows_dropped_in_window
        tombstoned += sim.stats.snapshot_rows_rejected_tombstone
        rejected += sim.stats.snapshot_rows_rejected
    # the enumeration is not vacuous: it reaches the window drop, and with delayed
    # application it reaches the guard: a tombstone rejection when a key is deleted for good,
    # and a stale rejection when the deleted key is re-inserted before the rows land
    assert dropped > 0
    if variant == "delayed_snapshot_apply":
        assert rejected > 0
        if not scenario.startswith("same key"):
            assert tombstoned > 0
