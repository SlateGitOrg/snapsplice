"""Command line: generate, run, verify, bench, trace."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Column, Table

from . import source as src
from .bench import StrategyTotals
from .bench import bench as run_bench
from .schema import SPECS, parse_pk_label
from .sim import Simulation
from .strategies import STRATEGIES
from .synth import CONFIGS, config_from_meta, populate
from .target import Target
from .trace import timeline
from .verify import CLASSES, compare, total_divergent

app = typer.Typer(add_completion=False, no_args_is_help=True, help=__doc__)
console = Console(width=110)


def _check_strategy(name: str) -> str:
    if name not in STRATEGIES:
        raise typer.BadParameter(f"choose one of {', '.join(STRATEGIES)}")
    return name


@app.command()
def generate(
    out: Annotated[Path, typer.Option(help="SQLite file to create.")] = Path("wms.db"),
    config: Annotated[str, typer.Option(help="Size: " + ", ".join(CONFIGS))] = "demo",
    seed: Annotated[int, typer.Option(help="Seed of the synthetic population.")] = 1,
) -> None:
    """Create a seeded warehouse-management source database with its change-log triggers."""
    if config not in CONFIGS:
        raise typer.BadParameter(f"choose one of {', '.join(CONFIGS)}")
    if out.exists():
        raise typer.BadParameter(f"{out} already exists")
    conn = src.connect(str(out))
    populate(conn, CONFIGS[config], seed)
    counts = src.row_counts(conn)
    console.print(f"wrote {out}: {counts}, {src.max_lsn(conn)} log events")


@app.command()
def run(
    source: Annotated[Path, typer.Option(help="Source database from `generate`.")] = Path("wms.db"),
    target: Annotated[Path, typer.Option(help="DuckDB file to create.")] = Path("target.duckdb"),
    strategy: Annotated[str, typer.Option(help="One of: " + ", ".join(STRATEGIES))] = "watermark",
    seed: Annotated[int, typer.Option(help="Seed of the schedule.")] = 1,
    chunk_size: Annotated[int, typer.Option(help="Rows per chunk (or dump page).")] = 50,
    ops: Annotated[
        int, typer.Option(help="Concurrent operations; 0 uses the generated config.")
    ] = 0,
    faults: Annotated[
        bool, typer.Option(help="Duplicate delivery, delayed snapshot rows, a crash.")
    ] = True,
    trace: Annotated[bool, typer.Option(help="Record per-event decisions for `trace`.")] = True,
    overwrite: Annotated[bool, typer.Option(help="Replace an existing target file.")] = False,
) -> None:
    """Backfill the target while a seeded concurrent workload writes to the source."""
    _check_strategy(strategy)
    if target.exists():
        if not overwrite:
            raise typer.BadParameter(f"{target} exists; pass --overwrite")
        target.unlink()
    conn = src.connect(str(source))
    cfg = config_from_meta(src.get_meta(conn, "config"))
    if ops:
        cfg = dataclasses.replace(cfg, operations=ops)
    tgt = Target(str(target))
    sim = Simulation(conn, tgt, strategy, cfg, seed, chunk_size, faults=faults, trace=trace)
    stats = sim.run()
    diffs = compare(conn, tgt)
    table = Table("metric", "value", title=f"run: {strategy}, seed {seed}")
    table.add_row("scheduler ticks", str(stats.ticks))
    table.add_row("writer operations", ", ".join(f"{k} {v}" for k, v in sorted(stats.ops.items())))
    table.add_row("log events read", str(stats.events_read))
    table.add_row(
        "events applied / rejected by LSN guard",
        f"{stats.events_applied} / {stats.events_rejected}",
    )
    table.add_row(
        "chunks emitted / re-emitted after restart",
        f"{stats.chunks_emitted} / {stats.chunks_redone}",
    )
    table.add_row("chunk rows dropped in window", str(stats.rows_dropped_in_window))
    table.add_row(
        "snapshot rows applied / rejected",
        f"{stats.snapshot_rows_applied} / {stats.snapshot_rows_rejected}",
    )
    table.add_row("  of which rejected by a tombstone", str(stats.snapshot_rows_rejected_tombstone))
    table.add_row("duplicate batches delivered", str(stats.duplicate_batches))
    table.add_row("delayed snapshot batches", str(stats.delayed_snapshot_batches))
    table.add_row("snapshotter crashed after chunk", str(stats.crashed_after_chunk))
    table.add_row("divergent keys vs source", str(total_divergent(diffs)))
    console.print(table)
    tgt.close()
    conn.close()


@app.command()
def verify(
    source: Annotated[Path, typer.Option()] = Path("wms.db"),
    target: Annotated[Path, typer.Option()] = Path("target.duckdb"),
) -> None:
    """Compare target to source per table by key and row hash; exit 1 on any divergence."""
    conn = src.connect(str(source))
    tgt = Target(str(target))
    try:
        diffs = compare(conn, tgt)
    finally:
        tgt.close()
        conn.close()
    table = Table("table", "source rows", "target rows", "missing", "extra", "changed")
    for d in diffs.values():
        table.add_row(
            d.tbl,
            str(d.source_rows),
            str(d.target_rows),
            str(len(d.missing)),
            str(len(d.extra)),
            str(len(d.changed)),
        )
    console.print(table)
    for d in diffs.values():
        for label, keys in (("missing", d.missing), ("extra", d.extra), ("changed", d.changed)):
            for pk in keys[:10]:
                console.print(f"  {d.tbl} {label}: {','.join(pk)}")
    if total_divergent(diffs):
        raise typer.Exit(1)
    console.print("target equals source")


def _bench_table(rows: list[StrategyTotals], seeds: int) -> Table:
    table = Table(
        Column("strategy", no_wrap=True),
        "schedules run",
        "schedules diverged",
        "divergent keys",
        "stale value",
        "resurrected delete",
        "missing",
        "extra",
        title=f"divergence over {seeds} seeded schedules",
    )
    for t in rows:
        table.add_row(
            t.strategy,
            str(t.schedules),
            str(t.diverged),
            str(t.divergent_keys),
            *(str(t.classes[c]) for c in CLASSES),
        )
    return table


@app.command()
def bench(
    seeds: Annotated[int, typer.Option(help="Number of seeded schedules per strategy.")] = 200,
    config: Annotated[str, typer.Option(help="Size: " + ", ".join(CONFIGS))] = "tests",
    chunk_size: Annotated[int, typer.Option(help="Rows per chunk (or dump page).")] = 25,
) -> None:
    """Run every strategy on seeds 1..N (same population and scheduler parameters; the write
    stream is aimed at each strategy's own chunk) and classify each divergence."""
    if config not in CONFIGS:
        raise typer.BadParameter(f"choose one of {', '.join(CONFIGS)}")
    rows = run_bench(seeds, CONFIGS[config], chunk_size)
    console.print(_bench_table(rows, seeds))


@app.command()
def trace(
    table: Annotated[str, typer.Option(help="Table name.")],
    key: Annotated[str, typer.Option(help="Primary key, comma separated: SKU-0042,L3")],
    source: Annotated[Path, typer.Option()] = Path("wms.db"),
    target: Annotated[Path, typer.Option()] = Path("target.duckdb"),
) -> None:
    """Print one key's timeline: log events, chunk membership and the decision for each."""
    if table not in SPECS:
        raise typer.BadParameter(f"choose one of {', '.join(SPECS)}")
    conn = src.connect(str(source))
    tgt = Target(str(target))
    try:
        if not tgt.has_trace():
            raise typer.BadParameter(
                "the target holds no trace; it was made with `run --no-trace` or by another tool"
            )
        lines = timeline(conn, tgt, table, parse_pk_label(key))
    finally:
        tgt.close()
        conn.close()
    out = Table("lsn", "record", "detail", "decision", title=f"{table} key {key}")
    for ln in lines:
        out.add_row(str(ln.lsn), ln.what, ln.detail, ln.decision)
    console.print(out)


if __name__ == "__main__":  # pragma: no cover
    app()
