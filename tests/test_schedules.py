"""Seeded model checking: random interleavings of writes, snapshot steps, crashes and faults."""

from __future__ import annotations

import dataclasses

import pytest

from helpers import SMALL
from snapsplice import source as src
from snapsplice.bench import ScheduleResult, bench, run_schedule
from snapsplice.schema import INVENTORY, SHIPMENTS
from snapsplice.sim import Simulation
from snapsplice.strategies import CrashPlan
from snapsplice.synth import TESTS, populate
from snapsplice.target import Target
from snapsplice.verify import CLASSES, classify, compare, total_divergent

SEEDS = 25


@pytest.fixture(scope="module")
def results() -> dict[str, list[ScheduleResult]]:
    out: dict[str, list[ScheduleResult]] = {"watermark": [], "naive": [], "dump-then-tail": []}
    bench(SEEDS, TESTS, on_schedule=lambda r: out[r.strategy].append(r))
    return out


def test_watermark_ends_with_zero_divergent_keys_in_every_schedule(results):
    assert len(results["watermark"]) == SEEDS
    assert [r.divergent for r in results["watermark"]] == [0] * SEEDS


def test_the_schedules_really_exercise_the_hard_cases(results):
    stats = [r.stats for r in results["watermark"]]
    assert sum(s.rows_dropped_in_window for s in stats) > 100
    rejected = sum(s.snapshot_rows_rejected for s in stats)
    tombstoned = sum(s.snapshot_rows_rejected_tombstone for s in stats)
    assert tombstoned > 0  # snapshot rows refused because the key was deleted after HIGH
    assert rejected - tombstoned > 0  # and rows refused because a later update was applied
    assert sum(s.duplicate_batches for s in stats) > 20
    assert sum(s.delayed_snapshot_batches for s in stats) > 5
    assert all(s.crashed_after_chunk is not None for s in stats)
    ops = {k: sum(s.ops.get(k, 0) for s in stats) for k in ("update", "insert", "delete", "move")}
    total = sum(ops.values())
    assert total == SEEDS * TESTS.operations
    assert 0.55 < ops["update"] / total < 0.65
    assert 0.16 < ops["insert"] / total < 0.24
    assert 0.11 < ops["delete"] / total < 0.19
    assert 0.02 < ops["move"] / total < 0.08


@pytest.mark.parametrize("strategy", ["naive", "dump-then-tail"])
def test_naive_strategies_diverge_and_classification_sums_to_the_total(results, strategy):
    runs = results[strategy]
    assert sum(r.divergent > 0 for r in runs) >= 1
    for r in runs:
        assert sum(r.classes[c] for c in CLASSES) == r.divergent
    total = sum(r.divergent for r in runs)
    assert total > 0
    assert sum(sum(r.classes[c] for c in CLASSES) for r in runs) == total


def test_each_baseline_fails_in_its_own_way(results):
    naive = sum(r.classes["stale_value"] for r in results["naive"])
    resurrected = sum(r.classes["resurrected_delete"] for r in results["naive"])
    missing = sum(r.classes["missing_row"] for r in results["dump-then-tail"])
    assert naive > 0 and resurrected > 0
    assert missing > 0
    assert sum(r.classes["missing_row"] for r in results["naive"]) == 0


def test_a_run_is_reproducible_from_its_seed():
    a = run_schedule("naive", 3, SMALL)
    b = run_schedule("naive", 3, SMALL)
    assert (a.divergent, dict(a.classes), a.stats.ops) == (
        b.divergent,
        dict(b.classes),
        b.stats.ops,
    )


def _final_state(chunk_size: int, dup_rate: float = 0.05, faults: bool = True):
    cfg = dataclasses.replace(SMALL, dup_rate=dup_rate)
    conn = src.connect()
    populate(conn, cfg, seed=11)
    target = Target()
    sim = Simulation(conn, target, "watermark", cfg, 11, chunk_size, faults=faults, targeting=False)
    sim.run()
    return conn, target, sim


@pytest.mark.parametrize("dup_rate", [0.0, 1.0])
def test_applying_every_delivered_batch_twice_gives_an_identical_target(dup_rate):
    conn, target, sim = _final_state(7, dup_rate=dup_rate)
    ref_conn, ref_target, _ = _final_state(7, dup_rate=0.0, faults=False)
    assert total_divergent(compare(conn, target)) == 0
    for spec in (INVENTORY, SHIPMENTS):
        assert target.table_rows(spec) == ref_target.table_rows(spec)
    if dup_rate == 1.0:
        assert sim.stats.duplicate_batches >= sim.pipeline.delivery.duplicates_delivered > 0


def test_chunk_sizes_give_identical_final_targets_for_the_same_schedule():
    finals = []
    for size in (1, 7, 50, 10_000):
        conn, target, _ = _final_state(size)
        assert total_divergent(compare(conn, target)) == 0
        finals.append(
            (
                src.table_rows(conn, INVENTORY),
                src.table_rows(conn, SHIPMENTS),
                target.table_rows(INVENTORY),
                target.table_rows(SHIPMENTS),
            )
        )
    assert all(f == finals[0] for f in finals)


@pytest.mark.parametrize("phase", [0, 1, 2, 3])
@pytest.mark.parametrize("seed", [2, 9])
def test_snapshotter_killed_after_chunk_k_restarts_from_persisted_progress(seed, phase):
    k = 3 + seed % 4  # k comes from the seed
    conn = src.connect()
    populate(conn, SMALL, seed)
    target = Target()
    plan = CrashPlan(after_chunk=k, phase_steps=phase, downtime=6)
    sim = Simulation(conn, target, "watermark", SMALL, seed, 8, crash=plan)
    stats = sim.run()
    assert stats.crashed_after_chunk is not None and stats.crashed_after_chunk >= k
    assert total_divergent(compare(conn, target)) == 0
    assert target.all_tables_done()


def test_a_restart_resumes_after_the_last_applied_chunk_instead_of_starting_over():
    conn = src.connect()
    populate(conn, SMALL, 4)
    target = Target()
    plan = CrashPlan(after_chunk=4, phase_steps=0, downtime=3)
    sim = Simulation(conn, target, "watermark", SMALL, 4, 8, faults=True, crash=plan)
    sim.cfg = dataclasses.replace(SMALL, operations=0)
    sim.writer = None
    stats = sim.run()
    # 50 positions at 8 per chunk = 7 chunks, 30 shipments = 4 chunks, plus the two closing ones
    assert stats.chunks_emitted <= 13
    assert total_divergent(compare(conn, target)) == 0


def test_location_moves_keep_per_sku_quantity_totals():
    cfg = dataclasses.replace(
        SMALL, positions=30, p_update=0.0, p_insert=0.0, p_delete=0.0, p_move=1.0
    )
    conn = src.connect()
    populate(conn, cfg, seed=5)
    totals_before = dict(
        conn.execute("SELECT sku, SUM(qty_on_hand) FROM inventory_positions GROUP BY sku")
    )
    target = Target()
    stats = Simulation(conn, target, "watermark", cfg, 5, 9).run()
    assert stats.ops == {"move": cfg.operations}  # no fallback updates: quantity is conserved
    source_totals = dict(
        conn.execute("SELECT sku, SUM(qty_on_hand) FROM inventory_positions GROUP BY sku")
    )
    target_totals = dict(
        target.con.execute(
            "SELECT sku, SUM(qty_on_hand) FROM inventory_positions GROUP BY sku"
        ).fetchall()
    )
    assert source_totals == totals_before == target_totals


def test_verify_reports_exactly_one_missing_one_extra_and_one_changed_key():
    conn, target, _sim = _final_state(10)
    rows = sorted(target.table_rows(INVENTORY))
    target.con.execute("DELETE FROM inventory_positions WHERE sku = ? AND location = ?", rows[0])
    target.con.execute("INSERT INTO inventory_positions VALUES ('SKU-9999', 'L1', 1, 0, 'x')")
    target.con.execute(
        "UPDATE inventory_positions SET qty_on_hand = qty_on_hand + 1 "
        "WHERE sku = ? AND location = ?",
        rows[1],
    )
    diffs = compare(conn, target)
    inventory = diffs["inventory_positions"]
    assert inventory.missing == [rows[0]]
    assert inventory.extra == [("SKU-9999", "L1")]
    assert inventory.changed == [rows[1]]
    assert diffs["shipments"].divergent == 0
    assert total_divergent(diffs) == 3
    assert dict(classify(conn, diffs)) == {
        "stale_value": 1,
        "resurrected_delete": 0,
        "missing_row": 1,
        "extra_row": 1,
    }
