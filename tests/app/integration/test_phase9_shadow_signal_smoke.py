from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest


ROOT = Path(__file__).resolve().parents[3]


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _ensure_docker_runtime() -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker is not available")
    info = subprocess.run(
        ["docker", "info"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if info.returncode != 0:
        pytest.skip("docker daemon is not available")


def _wait_for_postgres(dsn: str) -> None:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - optional dependency path
        pytest.skip(f"psycopg is not available: {exc}")

    deadline = time.time() + 60.0
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, connect_timeout=1):
                return
        except Exception as exc:  # pragma: no cover - timing-sensitive bootstrap path
            last_error = exc
            time.sleep(1.0)
    raise RuntimeError(f"postgres did not become ready in time: {last_error}")


@contextmanager
def _postgres_container() -> Iterator[str]:
    _ensure_docker_runtime()
    host_port = _find_free_port()
    container_name = f"ta3000-pg-wsc-{uuid.uuid4().hex[:8]}"
    run = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            container_name,
            "-e",
            "POSTGRES_DB=ta3000",
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_PASSWORD=postgres",
            "-p",
            f"127.0.0.1:{host_port}:5432",
            "postgres:16-alpine",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if run.returncode != 0:
        pytest.skip(f"unable to start postgres container: {run.stderr.strip()}")

    dsn = f"postgresql://postgres:postgres@127.0.0.1:{host_port}/ta3000"
    try:
        _wait_for_postgres(dsn)
        yield dsn
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )


def test_phase9_shadow_signal_smoke_script_produces_restart_safe_evidence_bundle(tmp_path: Path) -> None:
    with _postgres_container() as dsn:
        env = {
            **dict(os.environ),
            "TA3000_APP_DSN": dsn,
            "TA3000_TELEGRAM_BOT_TOKEN": "telegram-bot-token-001",
            "TA3000_TELEGRAM_SHADOW_CHANNEL": "@ta3000_shadow",
            "TA3000_TELEGRAM_ADVISORY_CHANNEL": "@ta3000_advisory",
            "TA3000_PROMETHEUS_BASE_URL": "http://127.0.0.1:9090",
            "TA3000_LOKI_BASE_URL": "http://127.0.0.1:3100",
            "TA3000_GRAFANA_DASHBOARD_URL": "http://127.0.0.1:3000/d/phase9",
        }
        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_phase9_shadow_signal_smoke.py",
                "--output-dir",
                str(tmp_path / "phase9-shadow-smoke"),
            ],
            cwd=ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        payload = json.loads(result.stdout)
        assert payload["preflight"]["status"] == "ok"
        assert payload["initial_batch"]["replay_report"]["published"] == 4
        assert payload["restart_probe"]["published_delta"] == 0
        assert payload["edit_batch"]["replay_report"]["edited"] == 2
        assert payload["publication_audit"]["status"] == "ok"
        assert payload["publication_audit"]["publication_type_counts"] == {
            "cancel": 2,
            "close": 2,
            "create": 4,
            "edit": 2,
        }
        assert payload["publication_audit"]["active_signal_total"] == 0
        assert payload["publication_audit"]["lifecycle_total"] == 10

        for path_text in payload["output_paths"].values():
            assert Path(path_text).exists()

        metrics_text = Path(str(payload["output_paths"]["observability_prometheus_metrics"])).read_text(
            encoding="utf-8"
        )
        assert 'ta3000_phase9_telegram_publications_total{publication_type="create"} 4' in metrics_text
        assert "ta3000_phase9_restart_published_delta 0" in metrics_text
