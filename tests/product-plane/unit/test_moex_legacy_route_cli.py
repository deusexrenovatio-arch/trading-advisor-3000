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


def test_phase01_cli_help_marks_route_as_legacy_migration_artifact() -> None:
    help_text = _help_text("run_moex_phase01_foundation.py").lower()
    assert "legacy migration artifact" in help_text
    assert "raw-ingest workflow" in help_text
    assert "sanctioned manual" not in help_text


def test_phase02_cli_help_marks_route_as_legacy_migration_aid() -> None:
    help_text = _help_text("run_moex_phase02_canonical.py").lower()
    assert "canonicalization contour" in help_text
    assert "rebuild/repair aid" in help_text
    assert "sanctioned manual" not in help_text


def test_phase03_cli_help_requires_staging_binding_and_hides_integration_downgrade_flag() -> None:
    help_text = _help_text("run_moex_phase03_dagster_cutover.py")
    assert "--staging-binding-report-path" in help_text
    assert "--allow-integration-proof" not in help_text
