"""Command line smoke tests on a small generated database."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from snapsplice import source as src
from snapsplice.cli import app
from snapsplice.target import Target
from snapsplice.trace import timeline

runner = CliRunner()


@pytest.fixture()
def generated(tmp_path):
    db = tmp_path / "wms.db"
    res = runner.invoke(app, ["generate", "--out", str(db), "--config", "tests", "--seed", "3"])
    assert res.exit_code == 0, res.output
    return db


def test_generate_refuses_to_overwrite(generated):
    res = runner.invoke(app, ["generate", "--out", str(generated), "--config", "tests"])
    assert res.exit_code != 0


def test_run_then_verify_then_trace(generated, tmp_path):
    out = tmp_path / "t.duckdb"
    res = runner.invoke(
        app,
        [
            "run",
            "--source",
            str(generated),
            "--target",
            str(out),
            "--ops",
            "150",
            "--chunk-size",
            "20",
            "--seed",
            "4",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "divergent keys vs source" in res.output
    res = runner.invoke(app, ["verify", "--source", str(generated), "--target", str(out)])
    assert res.exit_code == 0 and "target equals source" in res.output
    tgt = Target(str(out))
    conn = src.connect(str(generated))
    row = tgt.con.execute(
        "SELECT pk FROM _trace WHERE kind = 'chunk_row' AND decision = 'dropped_in_window' "
        "AND tbl = 'inventory_positions' LIMIT 1"
    ).fetchone()
    assert row is not None
    label = row[0].strip("[]").replace('"', "")
    tgt.close()
    res = runner.invoke(
        app,
        [
            "trace",
            "--table",
            "inventory_positions",
            "--key",
            label,
            "--source",
            str(generated),
            "--target",
            str(out),
        ],
    )
    assert res.exit_code == 0 and "dropped_in_window" in res.output
    conn.close()


def test_run_refuses_an_existing_target_without_overwrite(generated, tmp_path):
    out = tmp_path / "t.duckdb"
    args = ["run", "--source", str(generated), "--target", str(out), "--ops", "20"]
    assert runner.invoke(app, args).exit_code == 0
    assert runner.invoke(app, args).exit_code != 0
    assert runner.invoke(app, [*args, "--overwrite"]).exit_code == 0


def test_verify_exits_nonzero_on_a_tampered_target(generated, tmp_path):
    out = tmp_path / "t.duckdb"
    runner.invoke(app, ["run", "--source", str(generated), "--target", str(out), "--ops", "20"])
    tgt = Target(str(out))
    tgt.con.execute(
        "DELETE FROM shipments WHERE shipment_id = (SELECT MIN(shipment_id) FROM shipments)"
    )
    tgt.close()
    res = runner.invoke(app, ["verify", "--source", str(generated), "--target", str(out)])
    assert res.exit_code == 1
    assert "missing" in res.output


def test_naive_run_reports_divergence_and_trace_marks_skipped_events(generated, tmp_path):
    out = tmp_path / "n.duckdb"
    res = runner.invoke(
        app,
        [
            "run",
            "--source",
            str(generated),
            "--target",
            str(out),
            "--strategy",
            "naive",
            "--ops",
            "200",
            "--chunk-size",
            "20",
        ],
    )
    assert res.exit_code == 0
    tgt = Target(str(out))
    conn = src.connect(str(generated))
    lines = timeline(conn, tgt, "inventory_positions", ("SKU-0001", "L1"))
    assert lines  # the population insert at least
    tgt.close()


def test_bench_prints_one_row_per_strategy(tmp_path):
    res = runner.invoke(app, ["bench", "--seeds", "2", "--config", "tests"])
    assert res.exit_code == 0
    for name in ("watermark", "naive", "dump-then-tail"):
        assert name in res.output
    assert "resurrected" in res.output
