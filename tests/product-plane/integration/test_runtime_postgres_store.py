from __future__ import annotations

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

from trading_advisor_3000.product_plane.contracts import (
    DecisionCandidate,
    IndicatorContextRef,
    Mode,
    Timeframe,
    TradeSide,
)
from trading_advisor_3000.product_plane.interfaces.api import RuntimeAPI
from trading_advisor_3000.product_plane.runtime import build_runtime_stack_from_env
from trading_advisor_3000.product_plane.runtime.config import StrategyVersion


ROOT = Path(__file__).resolve().parents[3]


def _candidate(*, ts_decision: str = "2026-03-18T10:16:00Z") -> DecisionCandidate:
    return DecisionCandidate(
        signal_id="SIG-20260318-0001",
        contract_id="BR-6.26",
        timeframe=Timeframe.M15,
        strategy_version_id="ma-cross-v1",
        mode=Mode.SHADOW,
        side=TradeSide.LONG,
        entry_ref=82.45,
        stop_ref=81.70,
        target_ref=83.95,
        confidence=0.77,
        ts_decision=ts_decision,
        indicator_context=IndicatorContextRef(
            dataset_version="bars-whitelist-v1",
            snapshot_id="FS-20260318-0001",
        ),
    )


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
    container_name = f"ta3000-pg-{uuid.uuid4().hex[:8]}"
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
        migrate = subprocess.run(
            [sys.executable, "scripts/apply_app_migrations.py", "--dsn", dsn],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if migrate.returncode != 0:
            raise RuntimeError(migrate.stdout + migrate.stderr)
        yield dsn
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )


def _build_api(*, dsn: str) -> RuntimeAPI:
    bootstrap = build_runtime_stack_from_env(
        {
            "TA3000_RUNTIME_PROFILE": "staging",
            "TA3000_SIGNAL_STORE_BACKEND": "postgres",
            "TA3000_APP_DSN": dsn,
            "TA3000_TELEGRAM_CHANNEL": "@ta3000_signals",
        }
    )
    assert bootstrap.config.durable_runtime_required is True
    stack = bootstrap.runtime_stack
    stack.strategy_registry.register(
        StrategyVersion(
            strategy_version_id="ma-cross-v1",
            status="active",
            allowed_contracts=frozenset({"BR-6.26"}),
            allowed_timeframes=frozenset({Timeframe.M15}),
            allowed_modes=frozenset({Mode.SHADOW}),
            activated_from="2026-03-18T09:00:00Z",
        )
    )
    return RuntimeAPI(runtime_stack=stack)


def test_runtime_postgres_store_postgres_signal_store_survives_restart_and_reuses_publication_state() -> None:
    with _postgres_container() as dsn:
        api_first = _build_api(dsn=dsn)
        first = api_first.replay_candidates([_candidate()])

        assert first["replay_report"]["accepted"] == 1
        assert first["replay_report"]["published"] == 1
        assert len(first["active_signals"]) == 1
        assert len(first["publications"]) == 1

        api_second = _build_api(dsn=dsn)
        second = api_second.replay_candidates([_candidate()])

        assert second["replay_report"]["accepted"] == 1
        assert second["replay_report"]["published"] == 0
        assert len(second["active_signals"]) == 1
        assert len(second["publications"]) == 1
        assert second["publications"][0]["status"] == "published"

        closed = api_second.close_signal(
            signal_id="SIG-20260318-0001",
            closed_at="2026-03-18T10:30:00Z",
            reason_code="manual_close",
        )
        assert closed["active_signals"] == []
        assert closed["publications"][0]["status"] == "closed"

        api_third = _build_api(dsn=dsn)
        third_events = {row["event_type"] for row in api_third.list_signal_events()}
        third_publications = api_third._runtime_stack.signal_store.list_publication_events()  # noqa: SLF001

        assert {"signal_opened", "signal_activated", "signal_closed"} <= third_events
        assert [item.status.value for item in third_publications] == ["published", "closed"]

