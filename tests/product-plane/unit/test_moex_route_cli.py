from __future__ import annotations

from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_ROOT = REPO_ROOT / "scripts"


def _help_text(script_name: str) -> str:
    result = subprocess.run(
        [sys.executable, str(SCRIPTS_ROOT / script_name), "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    return result.stdout


def test_raw_ingest_cli_help_is_operator_clear() -> None:
    help_text = _help_text("run_moex_raw_ingest.py").lower()
    assert "raw-ingest tool" in help_text
    assert "bootstrap" in help_text


def test_canonical_refresh_cli_help_is_operator_clear() -> None:
    help_text = _help_text("run_moex_canonical_refresh.py").lower()
    assert "canonical refresh tool" in help_text
    assert "spark" in help_text


def test_dagster_route_cli_help_requires_staging_binding_and_hides_integration_downgrade_flag() -> None:
    help_text = _help_text("prove_moex_dagster_route.py")
    assert "--staging-binding-report-path" in help_text
    assert "--allow-integration-proof" not in help_text
