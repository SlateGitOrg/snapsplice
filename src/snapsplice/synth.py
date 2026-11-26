"""Seeded synthetic warehouse data.

Properties of the generated data, which the tests rely on:

* Inventory positions: ``skus`` SKUs, each held in at least one and at most five locations
  (``L1``..``L5``); exactly ``positions`` (sku, location) pairs exist. The pairs that do not
  exist form the pool the workload inserts into.
* Shipments: ``shipments`` rows drawn from an id space twice that size, so later inserts land
  both inside and beyond the range already snapshotted. Status follows the lifecycle
  created -> picked -> packed -> shipped -> delivered; a cancelled shipment is deleted.
* The log starts with the population itself (one insert event per row), so replaying the whole
  log into an empty target reproduces the source.
"""

from __future__ import annotations

import random
import sqlite3
from dataclasses import asdict, dataclass

from .schema import INVENTORY, SHIPMENTS, STATUS_LIFECYCLE, Pk
from .source import create_source, set_meta

LOCATIONS = ("L1", "L2", "L3", "L4", "L5")
CARRIERS = ("DHL", "UPS", "FedEx", "GLS", "DPD")
EPOCH = "2026-03-02T06:00:00"


@dataclass(frozen=True)
class SynthConfig:
    name: str
    skus: int
    positions: int
    shipments: int
    operations: int
    # operation mix, summing to 1.0
    p_update: float = 0.60
    p_insert: float = 0.20
    p_delete: float = 0.15
    p_move: float = 0.05
    # share of operations that hit the inventory table (moves always do)
    p_inventory: float = 0.60
    # share of operations aimed at keys near the chunk being snapshotted
    p_target: float = 0.60
    dup_rate: float = 0.05
    delay_rate: float = 0.10

    @property
    def shipment_universe(self) -> int:
        return self.shipments * 2


TESTS = SynthConfig("tests", skus=120, positions=300, shipments=200, operations=400)
DEMO = SynthConfig("demo", skus=800, positions=3000, shipments=1500, operations=4000)
CONFIGS = {c.name: c for c in (TESTS, DEMO)}


def sku_id(n: int) -> str:
    return f"SKU-{n:04d}"


def shipment_id(n: int) -> str:
    return f"SH-{n:05d}"


def inventory_universe(cfg: SynthConfig) -> list[Pk]:
    return [(sku_id(s), loc) for s in range(1, cfg.skus + 1) for loc in LOCATIONS]


def shipment_universe(cfg: SynthConfig) -> list[Pk]:
    return [(shipment_id(n),) for n in range(1, cfg.shipment_universe + 1)]


def timestamp(tick: int) -> str:
    """Logical clock: one second per tick from a fixed epoch."""
    h, rem = divmod(tick, 3600)
    m, s = divmod(rem, 60)
    day, hour = divmod(6 + h, 24)
    return f"2026-03-{2 + day:02d}T{hour:02d}:{m:02d}:{s:02d}"


def initial_inventory(cfg: SynthConfig, rng: random.Random) -> list[tuple]:
    if not cfg.skus <= cfg.positions <= cfg.skus * len(LOCATIONS):
        raise ValueError("positions must lie between skus and skus * locations")
    chosen: set[Pk] = {(sku_id(s), rng.choice(LOCATIONS)) for s in range(1, cfg.skus + 1)}
    rest = sorted(set(inventory_universe(cfg)) - chosen)
    chosen.update(rng.sample(rest, cfg.positions - len(chosen)))
    rows = []
    for sku, loc in sorted(chosen):
        on_hand = rng.randint(0, 500)
        rows.append((sku, loc, on_hand, rng.randint(0, min(on_hand, 40)), EPOCH))
    return rows


def new_shipment_row(rng: random.Random, pk: Pk, status: str, tick: int) -> tuple:
    return (
        pk[0],
        f"ORD-{rng.randint(1, 99999):05d}",
        status,
        rng.choice(CARRIERS),
        rng.randint(200, 30000),
        timestamp(tick),
    )


def initial_shipments(cfg: SynthConfig, rng: random.Random) -> list[tuple]:
    pks = sorted(rng.sample(shipment_universe(cfg), cfg.shipments))
    weights = (3, 3, 2, 3, 4)  # created, picked, packed, shipped, delivered
    return [new_shipment_row(rng, pk, rng.choices(STATUS_LIFECYCLE, weights)[0], 0) for pk in pks]


def populate(conn: sqlite3.Connection, cfg: SynthConfig, seed: int) -> None:
    """Create the source schema and insert the seeded population (triggers log every row)."""
    create_source(conn)
    rng = random.Random(f"{seed}:population")
    inv = initial_inventory(cfg, rng)
    ship = initial_shipments(cfg, rng)
    conn.execute("BEGIN")
    marks = ", ".join("?" for _ in INVENTORY.columns)
    conn.executemany(f"INSERT INTO {INVENTORY.name} VALUES ({marks})", inv)
    marks = ", ".join("?" for _ in SHIPMENTS.columns)
    conn.executemany(f"INSERT INTO {SHIPMENTS.name} VALUES ({marks})", ship)
    conn.execute("COMMIT")
    set_meta(conn, "config", asdict(cfg))
    set_meta(conn, "seed", seed)


def config_from_meta(meta: object) -> SynthConfig:
    if not isinstance(meta, dict):
        raise ValueError("source database was not created by `snapsplice generate`")
    return SynthConfig(**meta)
