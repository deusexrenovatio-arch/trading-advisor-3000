from __future__ import annotations

import importlib.util
import urllib.error
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "launch_moex_baseline_staging_run.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("launch_moex_baseline_staging_run", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load script module: {SCRIPT_PATH.as_posix()}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_launch_staging_graphql_url_rejects_non_http_scheme() -> None:
    module = _load_script_module()

    with pytest.raises(ValueError, match="http/https"):
        module._post_graphql(
            url="file:///tmp/dagster.sock",
            query="query { __typename }",
            variables={},
            timeout_sec=1,
        )


def test_launch_staging_graphql_network_failure_returns_structured_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()

    def _raise_url_error(*args, **kwargs):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(module.urllib.request, "urlopen", _raise_url_error)

    payload = module._post_graphql(
        url="http://127.0.0.1:3000/graphql",
        query="query { __typename }",
        variables={},
        timeout_sec=1,
    )

    assert "connection refused" in payload["errors"][0]["message"]


def test_launch_staging_graphql_registry_url_tracks_configured_host_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()
    monkeypatch.setenv("TA3000_DAGSTER_PORT", "4317")

    url = module._resolve_graphql_url(
        "http://127.0.0.1:3000/graphql",
        dagster={"graphql_port_env": "TA3000_DAGSTER_PORT"},
        override=False,
    )

    assert url == "http://127.0.0.1:4317/graphql"
