from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.product_plane.research.jobs import run_benchmark_job


def test_research_benchmark_job_emits_artifacts_and_threshold_evidence(tmp_path: Path) -> None:
    output_dir = tmp_path / "benchmark"
    report = run_benchmark_job(
        output_dir=output_dir,
        dataset_version="benchmark-test-v1",
        instruments=4,
        bars_per_instrument=72,
        combination_sizes=(10, 50, 100),
        param_batch_size=10,
    )

    assert report["thresholds"]["no_recompute_indicators_features"] is True
    assert report["thresholds"]["param_100_completed"] is True
    assert float(report["thresholds"]["hot_speedup_vs_cold_total"]) > 1.0
    assert float(report["thresholds"]["hot_ratio_vs_cold_total"]) < 1.0
    assert report["cold_backtest"]["cache_hit"] is False
    assert report["hot_backtest"]["cache_hit"] is True
    assert any(int(row["combination_count"]) == 100 for row in report["scalability_runs"])
    assert (output_dir / str(report["artifacts"]["report_json"])).exists()
    assert (output_dir / str(report["artifacts"]["report_md"])).exists()
    assert (output_dir / str(report["artifacts"]["cache_log"])).exists()
