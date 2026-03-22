from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BARS_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "research" / "canonical_bars_sample.jsonl"
FRESH_SNAPSHOT_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "quik_live_snapshot_fresh.json"


def _write_bootstrap_report(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "dataset_version": "phase9-moex-futures-pilot-moex-history-20260316T093000Z-sample",
                "output_paths": {
                    "canonical_bars": str(BARS_FIXTURE),
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def test_phase9_strategy_replay_script_emits_backtest_replay_and_live_smoke_evidence(tmp_path: Path) -> None:
    bootstrap_report = tmp_path / "bootstrap.report.json"
    _write_bootstrap_report(bootstrap_report)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase9_strategy_replay.py",
            "--strategy",
            "phase9-moex-breakout-v1",
            "--bootstrap-report",
            str(bootstrap_report),
            "--output-dir",
            str(tmp_path / "strategy-replay"),
            "--snapshot-path",
            str(FRESH_SNAPSHOT_FIXTURE),
            "--as-of-ts",
            "2026-03-20T07:01:00Z",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["strategy_spec"]["historical_source"] == "MOEX"
    assert payload["strategy_spec"]["live_feed"] == "QUIK"
    assert payload["research_summary"]["signal_contracts"] > 0
    assert payload["replay_summary"]["runtime_signal_candidates"] > 0
    assert len(payload["replay_summary"]["runtime_candidate_rows"]) == payload["replay_summary"]["runtime_signal_candidates"]
    assert payload["live_smoke"]["status"] == "ok"
    assert payload["pilot_readiness"]["status"] == "ready_for_shadow_pilot"
    assert "the evidence window is one-sided and still needs multi-session follow-up" in payload["pilot_readiness"]["warnings"]

    for path_text in payload["research_summary"]["output_paths"].values():
        assert Path(path_text).exists()
    for path_text in payload["replay_summary"]["output_paths"].values():
        assert Path(path_text).exists()


def test_phase9_strategy_replay_script_warns_when_live_smoke_is_not_attached(tmp_path: Path) -> None:
    bootstrap_report = tmp_path / "bootstrap.report.json"
    _write_bootstrap_report(bootstrap_report)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase9_strategy_replay.py",
            "--strategy",
            "phase9-moex-breakout-v1",
            "--bootstrap-report",
            str(bootstrap_report),
            "--output-dir",
            str(tmp_path / "strategy-replay"),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["pilot_readiness"]["status"] == "ready_for_shadow_pilot"
    assert "QUIK live-smoke evidence was not attached to this replay run" in payload["pilot_readiness"]["warnings"]
