from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from urllib import error as urllib_error
from urllib import request as urllib_request

import pytest


ROOT = Path(__file__).resolve().parents[3]
SIDECAR_SCRIPT_DIR = ROOT / "deployment" / "stocksharp-sidecar" / "scripts"
BUILD_SCRIPT = SIDECAR_SCRIPT_DIR / "build.ps1"
PUBLISH_SCRIPT = SIDECAR_SCRIPT_DIR / "publish.ps1"
PUBLISH_DIR = ROOT / "artifacts" / "tests" / "staging-rollout-sidecar"
SIDECAR_BINARY = PUBLISH_DIR / "TradingAdvisor3000.StockSharpSidecar.dll"
CONNECTOR_STUB_SCRIPT = ROOT / "deployment" / "docker" / "staging-gateway" / "gateway" / "sidecar_gateway_stub.py"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _ensure_sidecar_binary() -> Path:
    build = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(BUILD_SCRIPT),
            "-Configuration",
            "Release",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert build.returncode == 0, build.stdout + "\n" + build.stderr

    publish = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(PUBLISH_SCRIPT),
            "-Configuration",
            "Release",
            "-OutputDir",
            str(PUBLISH_DIR),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert publish.returncode == 0, publish.stdout + "\n" + publish.stderr
    assert SIDECAR_BINARY.exists(), f"compiled sidecar binary missing: {SIDECAR_BINARY}"
    return SIDECAR_BINARY


def _wait_for_health(base_url: str, *, timeout_seconds: float = 20.0) -> None:
    deadline = time.time() + timeout_seconds
    url = f"{base_url.rstrip('/')}/health"
    last_error = "sidecar did not respond"
    while time.time() < deadline:
        try:
            with urllib_request.urlopen(url, timeout=1.0) as response:
                body = response.read().decode("utf-8")
            payload = json.loads(body) if body.strip() else {}
            if isinstance(payload, dict) and str(payload.get("status", "")).strip() == "ok":
                return
            last_error = "health payload is not ok object"
        except (urllib_error.URLError, urllib_error.HTTPError, TimeoutError, socket.timeout, json.JSONDecodeError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.25)
    raise TimeoutError(f"timeout waiting for compiled sidecar health: {last_error}")


@contextmanager
def _running_connector_stub() -> Iterator[str]:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = dict(os.environ)
    env.update(
        {
            "TA3000_GATEWAY_HOST": "127.0.0.1",
            "TA3000_GATEWAY_PORT": str(port),
            "TA3000_GATEWAY_ROUTE": "stocksharp->quik->finam",
            "TA3000_CONNECTOR_MODE": "staging-real",
            "TA3000_CONNECTOR_BACKEND": "stocksharp-quik-finam",
            "TA3000_CONNECTOR_BINDING_SOURCE": "integration-test-stub",
            "TA3000_CONNECTOR_SESSION_ID": "integration-session-001",
        }
    )
    process = subprocess.Popen(
        [sys.executable, str(CONNECTOR_STUB_SCRIPT)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    logs_tail = ""
    try:
        _wait_for_health(base_url)
        yield base_url
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process.stdout is not None:
            logs_tail = process.stdout.read().strip()
        if logs_tail:
            tail_lines = logs_tail.splitlines()[-20:]
            print("\n".join(tail_lines))


@contextmanager
def _running_compiled_sidecar(sidecar_binary: Path, *, connector_base_url: str) -> Iterator[str]:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = dict(os.environ)
    env.update(
        {
            "ASPNETCORE_URLS": base_url,
            "TA3000_GATEWAY_ROUTE": "stocksharp->quik->finam",
            "TA3000_CONNECTOR_MODE": "staging-real",
            "TA3000_CONNECTOR_BACKEND": "stocksharp-quik-finam",
            "TA3000_BROKER_CONNECTOR_BASE_URL": connector_base_url,
            "TA3000_BROKER_CONNECTOR_API_PREFIX": "v1",
            "TA3000_BROKER_CONNECTOR_AUTH_TOKEN": "integration-test-token",
        }
    )
    process = subprocess.Popen(
        ["dotnet", str(sidecar_binary)],
        cwd=sidecar_binary.parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    logs_tail = ""
    try:
        _wait_for_health(base_url)
        yield base_url
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if process.stdout is not None:
            logs_tail = process.stdout.read().strip()
        if logs_tail:
            tail_lines = logs_tail.splitlines()[-20:]
            print("\n".join(tail_lines))


def _script_env() -> dict[str, str]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    src_path = str(ROOT / "src")
    env["PYTHONPATH"] = src_path if not existing_pythonpath else f"{src_path}{os.pathsep}{existing_pythonpath}"
    env.update(
        {
            "TA3000_ENABLE_LIVE_EXECUTION": "1",
            "TA3000_ENABLE_STOCKSHARP_BRIDGE": "1",
            "TA3000_ENABLE_QUIK_CONNECTOR": "1",
            "TA3000_ENABLE_FINAM_TRANSPORT": "1",
            "TA3000_ENFORCE_LIVE_SECRETS": "1",
            "TA3000_STOCKSHARP_API_KEY": "4f7a2c29c1534b1f97194f1f0df4dcb0",
            "TA3000_FINAM_API_TOKEN": "9c955b27a0a547129f80f88e9a96bcb3",
            "TA3000_GATEWAY_ROUTE": "stocksharp->quik->finam",
            "TA3000_CONNECTOR_MODE": "staging-real",
            "TA3000_CONNECTOR_BACKEND": "stocksharp-quik-finam",
            "TA3000_CONNECTOR_READY": "1",
        }
    )
    return env


@pytest.fixture(scope="module")
def sidecar_binary() -> Path:
    return _ensure_sidecar_binary()


def test_staging_rollout_script_passes_all_stages_with_compiled_sidecar(sidecar_binary: Path) -> None:
    with _running_connector_stub() as connector_base_url:
        with _running_compiled_sidecar(sidecar_binary, connector_base_url=connector_base_url) as base_url:
            result = subprocess.run(
                [
                    sys.executable,
                    "scripts/run_staging_real_execution_rollout.py",
                    "--base-url",
                    base_url,
                    "--stage",
                    "all",
                    "--batch-size",
                    "2",
                    "--format",
                    "json",
                ],
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
                env=_script_env(),
            )
            assert result.returncode == 0, result.stdout + "\n" + result.stderr
            payload = json.loads(result.stdout)
            assert payload["status"] == "ok"
            statuses = {item["stage"]: item["status"] for item in payload["stages"]}
            assert statuses == {"connectivity": "ok", "canary": "ok", "batch": "ok"}
            connectivity = next(item for item in payload["stages"] if item["stage"] == "connectivity")
            details = connectivity["details"]
            assert details["connector_mode"] == "staging-real"
            assert details["connector_backend"] == "stocksharp-quik-finam"
            assert details["connector_ready"] is True
            assert details["connector_session_id"] == "integration-session-001"
            assert details["connector_binding_source"] == "integration-test-stub"
            assert details["connector_last_heartbeat"]
            assert details["connector_errors"] == []


def test_staging_rollout_script_fails_closed_with_unavailable_connector() -> None:
    unreachable_base_url = f"http://127.0.0.1:{_free_port()}"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_staging_real_execution_rollout.py",
            "--base-url",
            unreachable_base_url,
            "--stage",
            "all",
            "--batch-size",
            "2",
            "--kill-switch-on-failure",
            "--format",
            "json",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        env=_script_env(),
    )
    assert result.returncode == 1, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "failed"
    assert payload["stages"]
    first_stage = payload["stages"][0]
    assert first_stage["stage"] == "connectivity"
    assert first_stage["status"] == "failed"
    degraded = first_stage["details"]["degraded_transports"]
    assert isinstance(degraded, list) and degraded
    connector_errors = first_stage["details"]["connector_errors"]
    assert isinstance(connector_errors, list) and connector_errors
