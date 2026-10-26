from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import urllib.error
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

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


def test_launch_staging_timeout_must_be_positive() -> None:
    module = _load_script_module()

    assert module._positive_int("1") == 1
    with pytest.raises(argparse.ArgumentTypeError, match="positive"):
        module._positive_int("0")


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


def test_launch_staging_graphql_non_object_success_is_structured_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_script_module()

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self) -> bytes:
            return b'["not", "an", "object"]'

    monkeypatch.setattr(module.urllib.request, "urlopen", lambda *args, **kwargs: _Response())

    payload = module._post_graphql(
        url="http://127.0.0.1:3000/graphql",
        query="query { __typename }",
        variables={},
        timeout_sec=1,
    )

    assert "JSON object" in payload["errors"][0]["message"]


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


@pytest.fixture
def guarded_launch(monkeypatch):
    module = _load_script_module()
    monkeypatch.delenv("TA3000_MOEX_RUNTIME_INSTANCES_REGISTRY", raising=False)
    registry = module.load_moex_runtime_instances_registry(repo_root=REPO_ROOT)
    instance = registry.instance("moex_branch_staging")
    fixture = REPO_ROOT / "tests/product-plane/fixtures/contracts/staging_acceptance.v1.json"
    plan = json.loads(fixture.read_text(encoding="utf-8"))
    plan.update(stage="branch", attempt_kind="small", require_scheduled=False)
    plan["root_job"] = instance.dagster["job"]
    plan["jobs"] = {plan["root_job"]: {"upstream_job": None, "max_runs": 1}}
    plan["data_root"] = instance.paths["data_root"]
    config = {
        key: f"{plan['data_root']}/{key}"
        for key in (
            "raw_table_path",
            "canonical_bars_path",
            "canonical_provenance_path",
            "canonical_session_intervals_path",
            "canonical_session_calendar_path",
            "canonical_roll_map_path",
            "raw_economics_root",
            "canonical_economics_root",
            "evidence_root",
        )
    }
    config.update(run_id="native-captured", ingest_till_utc="2030-01-02T00:00:00Z", timeframes="1d")
    plan["producer_parameters"] = {
        "run_moex_baseline_update": {
            "evidence_dir" if key == "evidence_root" else key: value
            for key, value in config.items()
            if key not in {"run_id", "ingest_till_utc"}
        }
    }
    plan["producer_parameters"]["run_moex_baseline_update"]["timeframes"] = ["1d"]
    plan["source"] = {
        "native_schedule": {
            "run_config": {
                "ops": {"moex_baseline_update": {"config": config}},
                "execution": {"config": {"multiprocess": {"max_concurrent": 1}}},
            }
        }
    }
    with TemporaryDirectory(prefix="ta3000-protected-plan-") as protected:
        path = Path(protected) / "protected-plan.json"
        if path.resolve().is_relative_to(REPO_ROOT.resolve()):
            raise RuntimeError("protected fixture requires OS temporary storage outside checkout")
        path.write_text(json.dumps(plan), encoding="utf-8")
        monkeypatch.setenv("TA3000_STAGING_PLAN", str(path))
        monkeypatch.delenv("TA3000_STAGING_PORT", raising=False)
        monkeypatch.delenv("TA3000_DAGSTER_PORT", raising=False)
        yield module, registry, instance, plan, path


def test_guarded_launch_requires_plan_and_exact_registered_endpoint(guarded_launch, monkeypatch):
    module, registry, instance, plan, _ = guarded_launch
    assert (
        module._guarded_launch_plan(registry, instance, graphql_url=instance.dagster["graphql_url"])
        == plan
    )
    for endpoint in ("http://127.0.0.1:3000/graphql", "http://localhost:3000/graphql"):
        with pytest.raises(ValueError, match="endpoint override"):
            module._guarded_launch_plan(registry, instance, graphql_url=endpoint)
    monkeypatch.delenv("TA3000_STAGING_PLAN")
    with pytest.raises(ValueError, match="protected plan"):
        module._guarded_launch_plan(registry, instance, graphql_url=instance.dagster["graphql_url"])


def test_guarded_launch_rejects_registry_or_legacy_bypass_and_root_drift(guarded_launch):
    module, registry, instance, plan, path = guarded_launch
    with pytest.raises(ValueError, match="repository-owned"):
        module._guarded_launch_plan(
            registry, instance, graphql_url=instance.dagster["graphql_url"], bypass_flags=True
        )
    with pytest.raises(ValueError, match="repository-owned"):
        module._guarded_launch_plan(
            replace(registry, source_path=path),
            instance,
            graphql_url=instance.dagster["graphql_url"],
        )
    plan["data_root"] = "/another-root"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="stage/root/job"):
        module._guarded_launch_plan(registry, instance, graphql_url=instance.dagster["graphql_url"])


def test_guarded_staging_cannot_share_production_endpoint(guarded_launch):
    module, registry, instance, _, _ = guarded_launch
    instance = replace(
        instance, dagster={**instance.dagster, "graphql_url": "http://localhost:3000/graphql"}
    )
    with pytest.raises(ValueError, match="production endpoint"):
        module._guarded_launch_plan(registry, instance, graphql_url=instance.dagster["graphql_url"])


def test_guarded_acceptance_manual_route_is_refused_before_network(guarded_launch, monkeypatch):
    module, registry, _, plan, path = guarded_launch
    instance = registry.instance("moex_acceptance_staging")
    plan.update(stage="acceptance", attempt_kind="acceptance", require_scheduled=True)
    path.write_text(json.dumps(plan))
    monkeypatch.setattr(module, "_post_graphql", lambda **kwargs: pytest.fail("must not launch"))
    monkeypatch.setattr(sys, "argv", ["launch", "--instance-id", instance.instance_id])
    with pytest.raises(ValueError, match="actual schedule"):
        module.main()


def test_guarded_branch_launch_sends_server_verifiable_plan_identity(guarded_launch, monkeypatch):
    module, _, instance, plan, path = guarded_launch
    captured = {}
    original = deepcopy(plan)
    saved = path.read_bytes()

    def post(**kwargs):
        captured.update(kwargs)
        return {"data": {"launchRun": {"__typename": "LaunchRunSuccess"}}}

    monkeypatch.setattr(module, "_post_graphql", post)
    monkeypatch.setattr(
        module,
        "build_moex_baseline_run_config_for_instance",
        lambda *args, **kwargs: pytest.fail("Windows host must not reconstruct container paths"),
    )
    monkeypatch.setattr(
        sys, "argv", ["launch", "--instance-id", instance.instance_id, "--run-id", "unit"]
    )
    module.main()
    from trading_advisor_3000.product_plane.common.staging_contracts import digest

    tags = {
        item["key"]: item["value"]
        for item in captured["variables"]["executionParams"]["executionMetadata"]["tags"]
    }
    assert tags["ta3000/staging_plan_sha256"] == digest(plan)
    assert tags["ta3000/candidate_head_sha"] == plan["candidate"]["head_sha"]
    assert captured["url"] == instance.dagster["graphql_url"]
    expected = deepcopy(plan["source"]["native_schedule"]["run_config"])
    expected["ops"]["moex_baseline_update"]["config"]["run_id"] = "unit"
    assert captured["variables"]["executionParams"]["runConfigData"] == expected
    assert plan == original and path.read_bytes() == saved


@pytest.mark.parametrize(
    "port,override,expected,error",
    [
        ("", "", "http://127.0.0.1:3011/graphql", None),
        ("4311", "", "http://127.0.0.1:4311/graphql", None),
        ("4311", "http://127.0.0.1:4312/graphql", None, "endpoint override"),
        ("3000", "", None, "production endpoint"),
    ],
)
def test_guarded_branch_port_binding_before_network(
    guarded_launch, monkeypatch, port, override, expected, error
):
    module, _, instance, _, _ = guarded_launch
    if port:
        monkeypatch.setenv("TA3000_STAGING_PORT", port)
    captured = []

    def post(**kwargs):
        assert error is None, "invalid endpoint must be refused before HTTP"
        captured.append(kwargs["url"])
        return {"data": {"launchRun": {"__typename": "LaunchRunSuccess"}}}

    monkeypatch.setattr(module, "_post_graphql", post)
    argv = ["launch", "--instance-id", instance.instance_id]
    if override:
        argv.extend(["--graphql-url", override])
    monkeypatch.setattr(sys, "argv", argv)
    if error:
        with pytest.raises(ValueError, match=error):
            module.main()
        assert not captured
    else:
        module.main()
        assert captured == [expected]


def test_guarded_branch_cutoff_override_is_rejected_before_network(guarded_launch, monkeypatch):
    module, _, instance, _, _ = guarded_launch
    monkeypatch.setattr(module, "_post_graphql", lambda **kwargs: pytest.fail("must not launch"))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "launch",
            "--instance-id",
            instance.instance_id,
            "--ingest-till-utc",
            "2030-01-01T12:00:00Z",
        ],
    )
    with pytest.raises(ValueError, match="cutoff override"):
        module.main()


@pytest.mark.parametrize(
    "mutation", ["missing", "wrong-op", "windows-path", "escaped-path", "parameters"]
)
def test_guarded_branch_native_config_drift_is_rejected_before_network(
    guarded_launch, monkeypatch, mutation
):
    module, _, instance, plan, path = guarded_launch
    config = plan["source"]["native_schedule"]["run_config"]
    if mutation == "missing":
        plan["source"]["native_schedule"].pop("run_config")
    elif mutation == "wrong-op":
        config["ops"]["other"] = config["ops"].pop("moex_baseline_update")
    elif mutation == "parameters":
        plan["producer_parameters"]["run_moex_baseline_update"]["timeframes"] = ["1m"]
    else:
        config["ops"]["moex_baseline_update"]["config"]["raw_table_path"] = (
            "D:/ta3000-data/raw.delta"
            if mutation == "windows-path"
            else f"{plan['data_root']}/../other/raw.delta"
        )
    path.write_text(json.dumps(plan))
    monkeypatch.setattr(module, "_post_graphql", lambda **kwargs: pytest.fail("must not launch"))
    monkeypatch.setattr(sys, "argv", ["launch", "--instance-id", instance.instance_id])
    with pytest.raises(ValueError, match="native|POSIX"):
        module.main()


def test_legacy_launch_still_uses_registry_builder(guarded_launch, monkeypatch):
    module, registry, _, _, _ = guarded_launch
    instance = registry.default_product_runtime()
    calls = []
    sentinel = {"ops": {"legacy": {"config": {"host_path": "D:/legacy"}}}}

    def builder(selected, **kwargs):
        calls.append((selected, kwargs))
        return sentinel

    def post(**kwargs):
        assert kwargs["variables"]["executionParams"]["runConfigData"] == sentinel
        return {"data": {"launchRun": {"__typename": "LaunchRunSuccess"}}}

    monkeypatch.setattr(module, "build_moex_baseline_run_config_for_instance", builder)
    monkeypatch.setattr(module, "_post_graphql", post)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "launch",
            "--instance-id",
            instance.instance_id,
            "--allow-product-runtime-write",
            "--run-id",
            "legacy",
            "--ingest-till-utc",
            "2030-01-02T00:00:00Z",
        ],
    )
    module.main()
    assert calls == [(instance, {"run_id": "legacy", "ingest_till_utc": "2030-01-02T00:00:00Z"})]
