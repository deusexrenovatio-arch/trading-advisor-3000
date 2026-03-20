from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BACKFILL_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"
FRESH_SNAPSHOT_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "quik_live_snapshot_fresh.json"
STALE_SNAPSHOT_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "quik_live_snapshot_stale.json"


def test_phase9_provider_bootstrap_script_emits_moex_report(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase9_provider_bootstrap.py",
            "--provider",
            "moex-history",
            "--source-path",
            str(BACKFILL_FIXTURE),
            "--output-dir",
            str(tmp_path / "bootstrap"),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["provider"]["external_system"] == "MOEX"
    assert payload["pilot_universe"]["live_provider_id"] == "quik-live"
    assert payload["canonical_rows"] == 2
    assert payload["dataset_version"].startswith("phase9-moex-futures-pilot-moex-history-")
    assert Path(str(payload["output_paths"]["canonical_bars"])).exists()


def test_phase9_live_smoke_script_succeeds_for_fresh_quik_snapshot() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase9_real_data_smoke.py",
            "--provider",
            "quik-live",
            "--snapshot-path",
            str(FRESH_SNAPSHOT_FIXTURE),
            "--as-of-ts",
            "2026-03-20T07:01:00Z",
            "--max-lag-seconds",
            "60",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["provider"]["external_system"] == "QUIK"
    assert payload["missing_contract_ids"] == []


def test_phase9_live_smoke_script_fails_for_stale_or_incomplete_snapshot() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_phase9_real_data_smoke.py",
            "--provider",
            "quik-live",
            "--snapshot-path",
            str(STALE_SNAPSHOT_FIXTURE),
            "--as-of-ts",
            "2026-03-20T07:01:00Z",
            "--max-lag-seconds",
            "60",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "degraded"
    assert payload["missing_contract_ids"] == ["Si-6.26"]
    assert payload["stale_contract_ids"] == ["BR-6.26"]
