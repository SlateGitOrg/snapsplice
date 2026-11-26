"""Shared builders for the tests."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from snapsplice import source as src
from snapsplice.schema import INVENTORY, SHIPMENTS
from snapsplice.sim import Simulation
from snapsplice.synth import SynthConfig
from snapsplice.target import Target

# A small configuration for tests that run many schedules or many chunk sizes.
SMALL = SynthConfig("small", skus=20, positions=50, shipments=30, operations=120)

STAMP = "2026-03-02T06:00:00"


def inv(sku: str, loc: str, on_hand: int = 10, allocated: int = 1) -> tuple:
    return (sku, loc, on_hand, allocated, STAMP)


def ship(n: int, status: str = "created") -> tuple:
    return (f"SH-{n:05d}", f"ORD-{n:05d}", status, "DHL", 1000 + n, STAMP)


def make_rig(
    inventory: Iterable[tuple] = (),
    shipments: Iterable[tuple] = (),
    strategy: str = "watermark",
    chunk_size: int = 100,
) -> tuple[sqlite3.Connection, Target, Simulation]:
    """A source holding the given rows and a simulation with no random workload or faults."""
    conn = src.connect()
    src.create_source(conn)
    conn.executemany("INSERT INTO inventory_positions VALUES (?, ?, ?, ?, ?)", list(inventory))
    conn.executemany("INSERT INTO shipments VALUES (?, ?, ?, ?, ?, ?)", list(shipments))
    target = Target()
    sim = Simulation(conn, target, strategy, None, 0, chunk_size, faults=False, trace=True)
    return conn, target, sim


def finish(sim: Simulation) -> None:
    """Run the remaining snapshot steps and drain the pipeline."""
    for _ in range(10_000):
        sim.pump()
        if sim.strategy.finished and sim.pipeline.idle and not sim.lane:
            sim.tracer.flush()
            return
        sim.snapshot_step()
    raise AssertionError("snapshot did not finish")


def tables_equal(conn: sqlite3.Connection, target: Target) -> bool:
    return all(
        src.table_rows(conn, spec) == target.table_rows(spec) for spec in (INVENTORY, SHIPMENTS)
    )
