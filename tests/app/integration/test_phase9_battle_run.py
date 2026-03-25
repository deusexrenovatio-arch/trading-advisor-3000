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
RAW_BACKFILL_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "raw_backfill_strategy_ready.jsonl"
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


@contextmanager
def _telegram_api_server() -> Iterator[tuple[str, list[dict[str, object]]]]:
    request_log: list[dict[str, object]] = []
    next_message_id = 1000

    class Handler(BaseHTTPRequestHandler):
        def _json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            nonlocal next_message_id
            content_length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(content_length).decode("utf-8")
            payload = json.loads(raw) if raw else {}
            request_log.append(
                {
                    "path": self.path,
                    "payload": payload,
                }
            )
            if self.path.endswith("/sendMessage"):
                next_message_id += 1
                self._json(
                    200,
                    {
                        "ok": True,
                        "result": {
                            "message_id": next_message_id,
                            "chat": {"id": payload.get("chat_id")},
                            "text": payload.get("text", ""),
                        },
                    },
                )
                return
            if self.path.endswith("/editMessageText"):
                self._json(
                    200,
                    {
                        "ok": True,
                        "result": {
                            "message_id": payload.get("message_id"),
                            "chat": {"id": payload.get("chat_id")},
                            "text": payload.get("text", ""),
                        },
                    },
                )
                return
            self._json(404, {"ok": False, "description": "unknown telegram method"})

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield base_url, request_log
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_phase9_battle_run_script_integrates_ws_a_to_ws_d_surfaces(tmp_path: Path) -> None:
    with _postgres_container() as dsn, _sidecar_gateway_server() as base_url, _telegram_api_server() as (
        telegram_base_url,
        telegram_requests,
    ):
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
            "TA3000_TELEGRAM_TRANSPORT": "bot-api",
            "TA3000_TELEGRAM_API_BASE_URL": telegram_base_url,
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
                "--source-path",
                str(RAW_BACKFILL_FIXTURE),
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
        assert payload["bootstrap"]["source_path"].endswith("raw_backfill_strategy_ready.jsonl")
        assert payload["bootstrap"]["materialization_mode"] == "manifest_only_jsonl_samples"
        assert payload["strategy_replay"]["live_smoke"]["status"] == "ok"
        assert payload["strategy_replay"]["pilot_readiness"]["status"] == "ready_for_shadow_pilot"
        assert payload["signal_continuity"]["status"] == "matched"
        assert payload["runtime_smoke"]["publication_posture"] == "advisory"
        assert payload["runtime_smoke"]["publisher_channel"] == "@ta3000_advisory"
        assert payload["runtime_smoke"]["publication_transport"] == "bot-api"
        assert payload["runtime_smoke"]["signal_source"] == "strategy_replay"
        assert payload["runtime_smoke"]["publication_audit"]["lifecycle_total"] == 10
        assert payload["sidecar_preflight"]["status"] == "ok"
        assert any("Phase 8 proving is not attached" in item for item in payload["warnings"])
        assert (
            payload["strategy_replay"]["replay_summary"]["runtime_signal_ids"]
            == payload["runtime_smoke"]["source_signal_ids"]
        )

        for path_text in payload["output_paths"].values():
            assert Path(path_text).exists()

        evidence_text = Path(str(payload["output_paths"]["evidence_markdown"])).read_text(encoding="utf-8")
        assert "phase9a integration status: `ready_for_review`" in evidence_text
        assert "publisher channel: `@ta3000_advisory`" in evidence_text
        assert "signal continuity: `matched`" in evidence_text

        send_calls = [item for item in telegram_requests if str(item["path"]).endswith("/sendMessage")]
        edit_calls = [item for item in telegram_requests if str(item["path"]).endswith("/editMessageText")]
        assert len(send_calls) == 4
        assert len(edit_calls) == 6
        assert {str(item["payload"]["chat_id"]) for item in send_calls} == {"@ta3000_advisory"}
        assert {str(item["payload"]["chat_id"]) for item in edit_calls} == {"@ta3000_advisory"}
