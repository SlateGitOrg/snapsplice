"""Source triggers, the change log and replay."""

from __future__ import annotations

import json

from helpers import SMALL, inv
from snapsplice import source as src
from snapsplice.applier import apply_guarded
from snapsplice.schema import INVENTORY, SHIPMENTS
from snapsplice.synth import TESTS, populate
from snapsplice.target import Target
from snapsplice.workload import Writer


def test_population_has_the_documented_shape():
    conn = src.connect()
    populate(conn, TESTS, seed=3)
    counts = src.row_counts(conn)
    assert counts == {"inventory_positions": 300, "shipments": 200}
    rows = src.table_rows(conn, INVENTORY)
    skus = {pk[0] for pk in rows}
    assert len(skus) == 120
    per_sku = [sum(1 for pk in rows if pk[0] == s) for s in skus]
    assert min(per_sku) >= 1 and max(per_sku) <= 5
    statuses = {r[2] for r in src.table_rows(conn, SHIPMENTS).values()}
    assert statuses <= {"created", "picked", "packed", "shipped", "delivered"}
    assert len(statuses) == 5


def test_population_is_deterministic_per_seed():
    a, b, c = src.connect(), src.connect(), src.connect()
    for conn, seed in ((a, 5), (b, 5), (c, 6)):
        populate(conn, SMALL, seed)
    assert src.table_rows(a, INVENTORY) == src.table_rows(b, INVENTORY)
    assert src.table_rows(a, INVENTORY) != src.table_rows(c, INVENTORY)


def test_triggers_log_every_operation_with_strictly_increasing_lsn():
    conn = src.connect()
    populate(conn, SMALL, seed=1)
    before = src.max_lsn(conn)
    assert before == 50 + 30  # one insert event per populated row
    writer = Writer(conn, SMALL, seed=1, targeting=False)
    ops = [writer.step() for _ in range(SMALL.operations)]
    events = src.read_log(conn, 0, 100_000)
    lsns = [e.lsn for e in events]
    assert lsns == sorted(set(lsns))
    assert all(b > a for a, b in zip(lsns, lsns[1:], strict=False))
    # a move is two events (delete + insert); every other operation is one
    assert len(events) - before == len(ops) + ops.count("move")


def test_replaying_the_whole_log_into_an_empty_target_reproduces_the_source():
    conn = src.connect()
    populate(conn, TESTS, seed=2)
    writer = Writer(conn, TESTS, seed=2, targeting=False)
    while not writer.finished:
        writer.step()
    target = Target()
    pos = 0
    while batch := src.read_log(conn, pos, 64):
        apply_guarded(target, batch)
        pos = batch[-1].lsn
    for spec in (INVENTORY, SHIPMENTS):
        assert target.table_rows(spec) == src.table_rows(conn, spec)


def test_primary_key_change_is_logged_as_delete_plus_insert():
    conn = src.connect()
    src.create_source(conn)
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0001', 'L1', 5, 0, 'x')")
    conn.execute("UPDATE inventory_positions SET location = 'L2' WHERE sku = 'SKU-0001'")
    ops = [(e.op, e.pk) for e in src.read_log(conn, 0, 10)]
    assert ops == [
        ("insert", ("SKU-0001", "L1")),
        ("delete", ("SKU-0001", "L1")),
        ("insert", ("SKU-0001", "L2")),
    ]


def test_watermark_rows_reach_the_log_through_the_trigger():
    conn = src.connect()
    src.create_source(conn)
    low = src.insert_watermark(conn, "low", "shipments", 4)
    high = src.insert_watermark(conn, "high", "shipments", 4)
    events = src.read_log(conn, 0, 10)
    assert [(e.op, e.tbl, e.wm_id, e.chunk_id) for e in events] == [
        ("wm_low", "shipments", low, 4),
        ("wm_high", "shipments", high, 4),
    ]
    assert events[0].lsn < events[1].lsn


def test_delete_event_carries_the_before_image():
    conn = src.connect()
    src.create_source(conn)
    conn.execute("INSERT INTO inventory_positions VALUES ('SKU-0001', 'L1', 5, 2, 'x')")
    conn.execute("DELETE FROM inventory_positions WHERE sku = 'SKU-0001'")
    last = conn.execute("SELECT op, row_json FROM _cdc_log ORDER BY lsn DESC LIMIT 1").fetchone()
    assert last[0] == "delete" and json.loads(last[1])["qty_on_hand"] == 5


def test_chunk_selection_is_keyset_paginated_in_pk_order():
    conn = src.connect()
    src.create_source(conn)
    conn.executemany(
        "INSERT INTO inventory_positions VALUES (?, ?, ?, ?, ?)",
        [
            inv("SKU-0002", "L1"),
            inv("SKU-0001", "L2"),
            inv("SKU-0001", "L1"),
            inv("SKU-0003", "L5"),
        ],
    )
    first = src.select_chunk(conn, INVENTORY, None, 3)
    assert [INVENTORY.pk_of(r) for r in first] == [
        ("SKU-0001", "L1"),
        ("SKU-0001", "L2"),
        ("SKU-0002", "L1"),
    ]
    rest = src.select_chunk(conn, INVENTORY, ("SKU-0002", "L1"), 3)
    assert [INVENTORY.pk_of(r) for r in rest] == [("SKU-0003", "L5")]
