from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SCRIPT = ROOT / "scripts" / "run_ta3000_product_staging_bootstrap.cmd"
COMPOSE_BIND = (
    ROOT
    / "deployment"
    / "docker"
    / "dagster-staging"
    / "docker-compose.dagster-product-main-bind.yml"
)
RUNBOOK = ROOT / "docs" / "runbooks" / "app" / "ta3000-production-nightly.md"
DEV_WORKFLOW = ROOT / "docs" / "DEV_WORKFLOW.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_product_staging_bootstrap_owns_nightly_runtime() -> None:
    text = _read(BOOTSTRAP_SCRIPT)

    assert "dagster-product-staging" in text
    assert "docker-compose.dagster-staging.yml" in text
    assert "docker-compose.dagster-product-main-bind.yml" in text
    assert "TA3000_PRODUCT_MAIN_WORKTREE" in text
    assert "TA3000_MOEX_PRODUCT_STAGING_ROOT_HOST" in text
    assert "ta3000-dagster-webserver" in text
    assert "ta3000-dagster-daemon" in text
    assert "moex_baseline_daily_update_schedule" in text
    assert "moex_baseline_update_job" in text
    assert "scripts\\run_moex_baseline_update.py" not in text
    assert "scripts/run_moex_baseline_update.py" not in text


def test_product_staging_compose_defaults_to_production_checkout() -> None:
    text = _read(COMPOSE_BIND)

    assert "TA3000_PRODUCT_MAIN_WORKTREE:-D:/TA3000-production" in text
    assert "D:/CodexHome/worktrees/moex-product-main" not in text
    assert "TA3000_MOEX_PRODUCT_STAGING_ROOT_HOST" in text
    assert "/ta3000-data/moex-historical" in text


def test_production_nightly_runbook_forbids_host_python_baseline_update() -> None:
    runbook = _read(RUNBOOK)
    workflow = _read(DEV_WORKFLOW)

    for text in (runbook, workflow):
        assert "Dagster daemon" in text
        assert "run_ta3000_product_staging_bootstrap.cmd" in text
        assert "host Python" in text

    for link in (
        "../deployment/runtime-instances/moex-runtime-instances.v1.yaml",
        "../deployment/docker/dagster-staging/docker-compose.dagster-staging.yml",
        "../deployment/docker/dagster-staging/docker-compose.dagster-product-main-bind.yml",
        "../scripts/run_ta3000_product_staging_bootstrap.cmd",
        "docs/runbooks/app/ta3000-production-nightly.md",
        "../tests/process/test_ta3000_product_staging_nightly_contract.py",
    ):
        assert link in workflow

    assert "python scripts/run_moex_baseline_update.py" not in runbook
    assert "python scripts/run_moex_baseline_update.py" not in workflow


def test_product_staging_bootstrap_cmd_parses_when_cmd_is_available() -> None:
    cmd = shutil.which("cmd")
    if cmd is None:
        pytest.skip("cmd.exe is not available in this environment")

    result = subprocess.run(
        [cmd, "/d", "/c", "echo", "syntax-smoke"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert BOOTSTRAP_SCRIPT.exists()
