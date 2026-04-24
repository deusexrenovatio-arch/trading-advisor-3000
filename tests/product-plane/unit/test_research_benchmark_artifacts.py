from __future__ import annotations

import json
from pathlib import Path


ARTIFACT_DIR = (
    Path(__file__).resolve().parents[3]
    / "artifacts"
    / "benchmarks"
    / "research-benchmark-2026-04-14"
)


def test_committed_benchmark_artifacts_exist_and_record_threshold_pass() -> None:
    report_json = ARTIFACT_DIR / "research-benchmark-report.json"
    report_md = ARTIFACT_DIR / "research-benchmark-report.md"
    cache_log = ARTIFACT_DIR / "cache-markers.log"

    assert report_json.exists()
    assert report_md.exists()
    assert cache_log.exists()

    payload = json.loads(report_json.read_text(encoding="utf-8-sig"))
    assert payload["dataset"]["scenario"] == "benchmark_small"
    assert payload["thresholds"]["no_recompute_indicators_features"] is True
    assert payload["thresholds"]["hot_path_threshold_pass"] is True
    assert payload["thresholds"]["param_100_completed"] is True
    assert payload["artifacts"]["report_json"] == "research-benchmark-report.json"
    assert payload["artifacts"]["report_md"] == "research-benchmark-report.md"
    assert payload["artifacts"]["cache_log"] == "cache-markers.log"
