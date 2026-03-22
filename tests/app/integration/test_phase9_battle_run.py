from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator

import pytest


ROOT = Path(__file__).resolve().parents[3]
BARS_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "research" / "canonical_bars_sample.jsonl"
FRESH_SNAPSHOT_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "quik_live_snapshot_fresh.json"


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
    container_name = f"ta3000-pg-wse-{uuid.uuid4().hex[:8]}"
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


@contextmanager
def _sidecar_gateway_server(*, route: str = "stocksharp->quik->finam") -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def _json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._json(200, {"status": "ok", "route": route, "service": "mock-sidecar-gateway"})
                return
            if self.path == "/ready":
                self._json(200, {"ready": True, "reason": "ok", "route": route})
                return
            if self.path == "/metrics":
                body = b"gateway_metric 1\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self._json(404, {"error_code": "not_found", "message": "unknown path"})

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


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


def test_phase9_battle_run_script_integrates_ws_a_to_ws_d_surfaces(tmp_path: Path) -> None:
    with _postgres_container() as dsn, _sidecar_gateway_server() as base_url:
        bootstrap_report = tmp_path / "bootstrap.report.json"
        _write_bootstrap_report(bootstrap_report)
        env = {
            **dict(os.environ),
            "PYTHONPATH": str(ROOT / "src"),
            "TA3000_ENABLE_LIVE_EXECUTION": "1",
            "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
            "TA3000_ENABLE_QUIK_CONNECTOR": "1",
            "TA3000_ENABLE_FINAM_TRANSPORT": "1",
            "TA3000_SIDECAR_TRANSPORT": "http",
            "TA3000_SIDECAR_BASE_URL": base_url,
            "TA3000_STOCKSHARP_API_KEY": "stocksharp-secret-001",
            "TA3000_FINAM_API_TOKEN": "finam-secret-001",
            "TA3000_APP_DSN": dsn,
            "TA3000_TELEGRAM_BOT_TOKEN": "telegram-bot-token-001",
            "TA3000_TELEGRAM_SHADOW_CHANNEL": "@ta3000_shadow",
            "TA3000_TELEGRAM_ADVISORY_CHANNEL": "@ta3000_advisory",
        }
        result = subprocess.run(
            [
                sys.executable,
                "scripts/run_phase9_battle_run.py",
                "--output-dir",
                str(tmp_path / "phase9-battle-run"),
                "--bootstrap-report",
                str(bootstrap_report),
                "--snapshot-path",
                str(FRESH_SNAPSHOT_FIXTURE),
                "--as-of-ts",
                "2026-03-20T07:01:00Z",
                "--phase8-proving-mode",
                "skip",
                "--mode",
                "advisory",
                "--sidecar-base-url",
                base_url,
            ],
            cwd=ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stdout + "\n" + result.stderr
        payload = json.loads(result.stdout)
        assert payload["phase9a_status"] == "ready_for_review"
        assert payload["strategy_replay"]["live_smoke"]["status"] == "ok"
        assert payload["runtime_smoke"]["publication_posture"] == "advisory"
        assert payload["runtime_smoke"]["publisher_channel"] == "@ta3000_advisory"
        assert payload["runtime_smoke"]["publication_audit"]["lifecycle_total"] == 10
        assert payload["sidecar_preflight"]["status"] == "ok"
        assert any("Phase 8 proving is not attached" in item for item in payload["warnings"])

        for path_text in payload["output_paths"].values():
            assert Path(path_text).exists()

        evidence_text = Path(str(payload["output_paths"]["evidence_markdown"])).read_text(encoding="utf-8")
        assert "phase9a integration status: `ready_for_review`" in evidence_text
        assert "publisher channel: `@ta3000_advisory`" in evidence_text
