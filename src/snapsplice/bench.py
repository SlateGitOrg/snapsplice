"""Divergence statistics over many seeded schedules."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

from .events import RunStats
from .sim import Simulation
from .source import connect
from .strategies import STRATEGIES
from .synth import SynthConfig, populate
from .target import Target
from .verify import CLASSES, classify, compare, total_divergent


@dataclass
class ScheduleResult:
    strategy: str
    seed: int
    divergent: int
    classes: Counter[str]
    stats: RunStats


@dataclass
class StrategyTotals:
    strategy: str
    schedules: int = 0
    diverged: int = 0
    divergent_keys: int = 0
    classes: Counter[str] = field(default_factory=lambda: Counter({c: 0 for c in CLASSES}))


def run_schedule(
    strategy: str,
    seed: int,
    cfg: SynthConfig,
    chunk_size: int = 25,
    faults: bool = True,
    targeting: bool = True,
) -> ScheduleResult:
    conn = connect()
    populate(conn, cfg, seed)
    target = Target()
    try:
        sim = Simulation(
            conn, target, strategy, cfg, seed, chunk_size, faults=faults, targeting=targeting
        )
        stats = sim.run()
        diffs = compare(conn, target)
        return ScheduleResult(strategy, seed, total_divergent(diffs), classify(conn, diffs), stats)
    finally:
        target.close()
        conn.close()


def bench(
    seeds: int,
    cfg: SynthConfig,
    chunk_size: int = 25,
    strategies: tuple[str, ...] = STRATEGIES,
    on_schedule: Callable[[ScheduleResult], None] | None = None,
) -> list[StrategyTotals]:
    """Run every strategy on seeds 1..``seeds`` and total the divergences.

    What the strategies share for one seed is the synthetic population and the scheduler
    parameters (operation count and mix, chunk size, fault rates). They do not share the exact
    write stream: with write targeting on, the writer aims at the chunk the running strategy is
    snapshotting, so the writes of two strategies differ for the same seed. Each strategy's run
    is still fully reproducible from its seed.
    """
    totals = {s: StrategyTotals(s) for s in strategies}
    for seed in range(1, seeds + 1):
        for name in strategies:
            res = run_schedule(name, seed, cfg, chunk_size)
            t = totals[name]
            t.schedules += 1
            t.diverged += res.divergent > 0
            t.divergent_keys += res.divergent
            t.classes.update(res.classes)
            if on_schedule:
                on_schedule(res)
    return list(totals.values())
