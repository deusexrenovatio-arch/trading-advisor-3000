from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from trading_advisor_3000.product_plane.data_plane import run_sample_backfill
from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.spark_jobs import build_sql_plan, run_canonical_bars_spark_job


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests" / "product-plane" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"
WHITELIST = {"BR-6.26", "Si-6.26"}
COMPARE_TABLES = (
    "canonical_bars",
    "canonical_instruments",
    "canonical_contracts",
    "canonical_session_calendar",
    "canonical_roll_map",
)
PROOF_SCRIPT = ROOT / "scripts" / "run_historical_data_spark_proof.py"


def _sorted_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(rows, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))


def _run_spark_proof(tmp_path: Path) -> dict[str, object]:
    output_dir = ROOT / ".tmp" / f"{tmp_path.name}-spark-proof"
    output_json = ROOT / ".tmp" / f"{tmp_path.name}-spark-proof.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(PROOF_SCRIPT),
            "--profile",
            "docker",
            "--source",
            SOURCE_FIXTURE.as_posix(),
            "--output-dir",
            output_dir.as_posix(),
            "--contracts",
            "BR-6.26,Si-6.26",
            "--output-json",
            output_json.as_posix(),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise AssertionError(f"Spark proof command failed: {detail}")
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("Spark proof report must be a JSON object")
    return payload


def test_historical_data_spark_job_executes_and_matches_contract_outputs(tmp_path: Path) -> None:
    spark_report = _run_spark_proof(tmp_path)
    python_report = run_sample_backfill(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path / "python",
        whitelist_contracts=WHITELIST,
    )

    assert spark_report["source_rows"] == 4
    assert spark_report["whitelisted_rows"] == 3
    assert spark_report["canonical_rows"] == 2
    assert spark_report["contract_check_errors"] == []
    assert spark_report["spark_profile"]["master"] == "local[2]"
    assert spark_report["spark_profile"]["delta_writer"] == "spark"
    assert spark_report["proof_profile"] == "docker-linux"

    for table_name in COMPARE_TABLES:
        spark_path = Path(str(spark_report["output_paths"][table_name]))
        python_path = Path(str(python_report["output_paths"][table_name]))
        assert (spark_path / "_delta_log").exists()
        assert _sorted_rows(read_delta_table_rows(spark_path)) == _sorted_rows(read_delta_table_rows(python_path))


def test_historical_data_spark_disprover_fails_when_runtime_bootstrap_is_broken(tmp_path: Path) -> None:
    sql_plan = build_sql_plan()
    assert "ROW_NUMBER() OVER" in sql_plan

    def _broken_factory(app_name: str, master: str) -> object:
        raise RuntimeError(f"broken spark bootstrap for {app_name} ({master})")

    try:
        run_canonical_bars_spark_job(
            source_path=SOURCE_FIXTURE,
            output_dir=tmp_path / "spark-broken",
            whitelist_contracts=WHITELIST,
            spark_session_factory=_broken_factory,
        )
    except RuntimeError as exc:
        assert "broken spark bootstrap" in str(exc)
    else:
        raise AssertionError("expected Spark execution failure while SQL builder remains available")
