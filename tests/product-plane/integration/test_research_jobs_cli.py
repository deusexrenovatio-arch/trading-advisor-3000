from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from trading_advisor_3000.product_plane.data_plane import run_sample_backfill


ROOT = Path(__file__).resolve().parents[3]
RAW_FIXTURE = ROOT / "tests" / "product-plane" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"


def _run_module(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_research_jobs_cli_cover_bootstrap_backtest_and_projection(tmp_path: Path) -> None:
    canonical_dir = tmp_path / "canonical"
    research_dir = tmp_path / "research"
    run_sample_backfill(
        source_path=RAW_FIXTURE,
        output_dir=canonical_dir,
        whitelist_contracts={"BR-6.26", "Si-6.26"},
    )

    bootstrap_report = tmp_path / "bootstrap-report.json"
    bootstrap = _run_module(
        "-m",
        "trading_advisor_3000.product_plane.research.jobs.bootstrap",
        "--canonical-output-dir",
        canonical_dir.as_posix(),
        "--research-output-dir",
        research_dir.as_posix(),
        "--dataset-version",
        "cli-dataset-v1",
        "--timeframes",
        "15m",
        "--report-json",
        bootstrap_report.as_posix(),
    )
    assert bootstrap.returncode == 0, bootstrap.stderr
    bootstrap_payload = _load_json(bootstrap_report)
    assert bootstrap_payload["success"] is True
    assert bootstrap_payload["contract_validation"]["status"] == "passed"
    assert "research_feature_frames" in bootstrap_payload["contract_validation"]["validated_tables"]
    assert Path(str(bootstrap_payload["output_paths"]["research_feature_frames"])).exists()

    backtest_report = tmp_path / "backtest-report.json"
    backtest = _run_module(
        "-m",
        "trading_advisor_3000.product_plane.research.jobs.backtest",
        "--canonical-output-dir",
        canonical_dir.as_posix(),
        "--research-output-dir",
        research_dir.as_posix(),
        "--dataset-version",
        "cli-dataset-v1",
        "--timeframes",
        "15m",
        "--strategy-versions",
        "ma-cross-v1",
        "--combination-count",
        "1",
        "--backtest-timeframe",
        "15m",
        "--report-json",
        backtest_report.as_posix(),
    )
    assert backtest.returncode == 0, backtest.stderr
    backtest_payload = _load_json(backtest_report)
    assert backtest_payload["success"] is True
    assert backtest_payload["contract_validation"]["status"] == "passed"
    assert "research_strategy_rankings" in backtest_payload["contract_validation"]["validated_tables"]
    assert Path(str(backtest_payload["output_paths"]["research_backtest_runs"])).exists()

    projection_report = tmp_path / "projection-report.json"
    projection = _run_module(
        "-m",
        "trading_advisor_3000.product_plane.research.jobs.project_candidates",
        "--canonical-output-dir",
        canonical_dir.as_posix(),
        "--research-output-dir",
        research_dir.as_posix(),
        "--dataset-version",
        "cli-dataset-v1",
        "--timeframes",
        "15m",
        "--strategy-versions",
        "ma-cross-v1",
        "--combination-count",
        "1",
        "--backtest-timeframe",
        "15m",
        "--selection-policy",
        "all_policy_pass",
        "--min-robust-score",
        "0.0",
        "--decision-lag-bars-max",
        "4",
        "--report-json",
        projection_report.as_posix(),
    )
    assert projection.returncode == 0, projection.stderr
    projection_payload = _load_json(projection_report)
    assert projection_payload["success"] is True
    assert projection_payload["contract_validation"]["status"] == "passed"
    assert "research_signal_candidates" in projection_payload["contract_validation"]["validated_tables"]
    assert Path(str(projection_payload["output_paths"]["research_signal_candidates"])).exists()
