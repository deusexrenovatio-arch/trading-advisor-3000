"""Native tiny-Delta protocol fixtures; no production, deployment or market acceptance."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pyarrow as pa
import pytest
from deltalake import write_deltalake

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "src")]
import validate_staging_promotion as promotion  # noqa: E402
from test_staging_acceptance_gate import NOW  # noqa: E402
from test_staging_acceptance_gate import receipt_repository as receipt_repository  # noqa: E402
from validate_staging_promotion import validate_production_state, validate_promotion  # noqa: E402

from trading_advisor_3000.product_plane.common.data_snapshots import (  # noqa: E402
    capture_table,
    create_snapshot,
    current_snapshot,
    publish_snapshot,
    save_snapshot,
)
from trading_advisor_3000.product_plane.common.staging_contracts import sign_receipt  # noqa: E402
from trading_advisor_3000.product_plane.research.admission import _parent, _seal  # noqa: E402
from trading_advisor_3000.product_plane.research.compute_contract import (  # noqa: E402
    semantic_compute_contract,
)
from trading_advisor_3000.product_plane.research.data_snapshots import (  # noqa: E402
    snapshot_registry,
)
from trading_advisor_3000.product_plane.research.hash_compatibility import (  # noqa: E402
    REQUIRED_QUALITY,
)


@pytest.fixture(scope="module")
def compute_contract():
    return semantic_compute_contract()


@pytest.fixture
def production(tmp_path_factory, compute_contract):
    workspace = tmp_path_factory.getbasetemp() / uuid4().hex[:8]
    root = workspace / "production"
    canonical = root / "canonical/moex/baseline-4y-current"
    output = root / "research/gold/current"
    for path in (canonical / "bars.delta", output / "indicators.delta"):
        write_deltalake(path, pa.table({"value": [1.0]}))
    data = create_snapshot(
        tables={"canonical_bars": capture_table(canonical / "bars.delta")},
        required_tables=["canonical_bars"],
        dataset_version="data",
        run_id="data",
        code_version="moex-baseline-update",
        quality_checks={"canonical": "PASS"},
        parameters={"downstream_scope": {"full_refresh": False}},
    )
    data_path = save_snapshot(canonical / ".data_snapshots", data)
    publish_snapshot(canonical / ".data_snapshots", data_path, expected_current_id=None)
    tables = {**data["tables"], "research_indicators": capture_table(output / "indicators.delta")}
    params = {
        "series_mode": "continuous_front",
        "semantic_compute_contract": compute_contract,
        "continuous_front_policy": {"roll": "causal"},
    }
    registry = snapshot_registry(output, "research", "pit_active_front")

    def research(parameters=params, parent=data, inputs=tables):
        return create_snapshot(
            tables=inputs,
            required_tables=list(inputs),
            dataset_version="research",
            run_id="research",
            code_version="fixture",
            parameters=parameters,
            quality_checks={name: "PASS" for name in REQUIRED_QUALITY},
            parents=[parent["snapshot_id"]],
        )

    source = research()
    source_path = save_snapshot(registry, source)
    publish_snapshot(registry, source_path, expected_current_id=None)
    accepted = research()
    accepted_path = save_snapshot(workspace / "accepted-output", accepted)
    return root, registry, source, source_path, accepted, accepted_path, research


def test_current_exact_checkpoint_and_retained_pins_pass(production):
    root, _, source, source_path, _, output_path, _ = production
    report = validate_production_state(source_path, output_path, root)
    assert report["research"]["snapshot_id"] == source["snapshot_id"]
    assert report["schema_rollback_sha256"]


def test_candidate_output_changed_policy_or_schema_blocks(production):
    root, _, _, source_path, accepted, output_path, _ = production
    changed = copy.deepcopy(accepted["parameters"])
    changed["continuous_front_policy"] = {"roll": "different"}
    invalid = production[-1](changed)
    path = save_snapshot(output_path.parents[1], invalid)
    with pytest.raises(ValueError, match="policy"):
        validate_production_state(source_path, path, root)
    write_deltalake(root / "replacement.delta", pa.table({"changed": [1.0]}))
    tables = {
        **accepted["tables"],
        "research_indicators": capture_table(root / "replacement.delta"),
    }
    invalid = create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="research",
        run_id="changed",
        code_version="fixture",
        parents=accepted["parents"],
        quality_checks=accepted["quality_checks"],
        parameters=accepted["parameters"],
    )
    path = save_snapshot(output_path.parents[1], invalid)
    with pytest.raises(ValueError, match="schema/layout"):
        validate_production_state(source_path, path, root)


def test_unknown_intermediate_transition_and_data_handoff_block(production):
    root, registry, source, source_path, accepted, output_path, research = production
    next_research = research()
    next_path = save_snapshot(registry, next_research)
    publish_snapshot(registry, next_path, expected_current_id=source["snapshot_id"])
    with pytest.raises(ValueError, match="required|Unknown or rebuild"):
        validate_production_state(source_path, output_path, root)
    publish_snapshot(registry, source_path, expected_current_id=next_research["snapshot_id"])
    data_registry = root / "canonical/moex/baseline-4y-current/.data_snapshots"
    data = current_snapshot(data_registry)
    changed = create_snapshot(
        tables=data["tables"],
        required_tables=data["required_tables"],
        dataset_version="data",
        run_id="new-data",
        code_version="moex-baseline-update",
        quality_checks={"canonical": "PASS"},
        parameters=data["parameters"],
    )
    publish_snapshot(
        data_registry,
        save_snapshot(data_registry, changed),
        expected_current_id=data["snapshot_id"],
    )
    with pytest.raises(ValueError, match="Data parent"):
        validate_production_state(source_path, output_path, root)


def advance(production, *, mutate=lambda admission: None, policy=None, parameter_changes=None):
    root, registry, _, _, _, _, research = production
    data_registry = root / "canonical/moex/baseline-4y-current/.data_snapshots"
    old_data, old_research = current_snapshot(data_registry), current_snapshot(registry)
    for name in (
        "canonical/moex/baseline-4y-current/bars.delta",
        "research/gold/current/indicators.delta",
    ):
        write_deltalake(root / name, pa.table({"value": [2.0]}), mode="append")
    data = create_snapshot(
        tables={
            "canonical_bars": capture_table(root / "canonical/moex/baseline-4y-current/bars.delta")
        },
        required_tables=old_data["required_tables"],
        dataset_version="data",
        run_id="data-next",
        code_version="moex-baseline-update",
        quality_checks=old_data["quality_checks"],
        parents=[old_data["snapshot_id"]],
        parameters=old_data["parameters"],
    )
    save_snapshot(data_registry, data)
    publish_snapshot(
        data_registry,
        data_registry / "manifests" / f"{data['snapshot_id']}.json",
        expected_current_id=old_data["snapshot_id"],
    )
    admission = {
        "schema_version": 1,
        "operation": "daily",
        "outcome": "SCOPED",
        "research_parent": _parent(registry, old_research),
        "data_parent": _parent(data_registry, data),
        "inputs": copy.deepcopy(data["tables"]),
        "domain": {
            "materialized_output_dir": str(root / "research/gold/current"),
            "dataset_version": "research",
        },
        "original_windows": [],
        "effective_windows": [
            {
                "instrument_id": "fixture",
                "timeframe": "1h",
                "start_ts": "2026-10-24T00:00:00Z",
                "end_ts": "2026-10-25T00:00:00Z",
            }
        ],
        "reason": "fixture",
        "hash_compatibility": {},
    }
    mutate(admission)
    parameters = {
        **old_research["parameters"],
        "refresh_admission": _seal(admission),
        "operation": "daily",
        "refresh_scope_version": 1,
        "force_full_refresh": False,
    }
    if policy:
        parameters["continuous_front_policy"] = policy
    parameters.update(parameter_changes or {})
    tables = {
        **data["tables"],
        "research_indicators": capture_table(root / "research/gold/current/indicators.delta"),
    }
    result = research(parameters, data, tables)
    path = save_snapshot(registry, result)
    publish_snapshot(registry, path, expected_current_id=old_research["snapshot_id"])
    return result


def test_two_retained_native_transitions_pass_and_report_all_nodes(production):
    first, current = advance(production), advance(production)
    root, _, source, source_path, _, output_path, _ = production
    report = validate_production_state(source_path, output_path, root)
    assert [item["snapshot_id"] for item in report["lineage"]["research"]] == [
        current["snapshot_id"],
        first["snapshot_id"],
        source["snapshot_id"],
    ]
    assert len(report["lineage"]["data"]) == 3


@pytest.mark.parametrize(
    "mutation",
    [
        lambda admission: admission["research_parent"].update(manifest_sha256="f" * 64),
        lambda admission: admission["research_parent"].update(
            manifest_path="/unknown/manifest.json"
        ),
        lambda admission: admission.update(operation="rebuild"),
        lambda admission: admission.update(effective_windows=[]),
        lambda admission: admission["domain"].update(dataset_version="unknown"),
        lambda admission: admission["inputs"]["canonical_bars"].update(delta_version=999),
    ],
)
def test_unknown_or_tampered_retained_transition_blocks(production, mutation):
    advance(production, mutate=mutation)
    root, _, _, source_path, _, output_path, _ = production
    with pytest.raises(ValueError, match="path/hash|rebuild|input pins"):
        validate_production_state(source_path, output_path, root)


def test_policy_changed_then_reverted_in_intermediate_node_still_blocks(production):
    advance(production, policy={"roll": "different"})
    advance(production, policy={"roll": "causal"})
    root, _, _, source_path, _, output_path, _ = production
    with pytest.raises(ValueError, match="policy"):
        validate_production_state(source_path, output_path, root)


def test_lineage_has_exact_128_node_limit(production):
    previous = advance(production)
    root, registry, _, source_path, _, output_path, research = production
    data = current_snapshot(root / "canonical/moex/baseline-4y-current/.data_snapshots")
    for index in range(127):
        if index == 126:
            report = validate_production_state(source_path, output_path, root)
            assert len(report["lineage"]["research"]) == 128
        parameters = copy.deepcopy(previous["parameters"])
        admission = parameters["refresh_admission"]
        admission["research_parent"] = _parent(registry, previous)
        parameters["refresh_admission"] = _seal(
            {key: value for key, value in admission.items() if key != "admission_id"}
        )
        following = research(parameters, data, previous["tables"])
        path = save_snapshot(registry, following)
        publish_snapshot(registry, path, expected_current_id=previous["snapshot_id"])
        previous = following
    with pytest.raises(ValueError, match="exceeds 128"):
        validate_production_state(source_path, output_path, root)


def test_daily_scope_version_requires_integer(production):
    advance(production, parameter_changes={"refresh_scope_version": True})
    root, _, _, source_path, _, output_path, _ = production
    with pytest.raises(ValueError, match="Unknown or rebuild"):
        validate_production_state(source_path, output_path, root)


def test_full_promotion_binds_main_image_and_actual_rollback_code(receipt_repository, production):
    repo, _, head, private, payload = receipt_repository
    root, _, source, source_path, output, output_path, _ = production
    payload["source"] = {key: source[key] for key in ("snapshot_id", "manifest_sha256")}
    payload["outputs"].update({key: output[key] for key in ("snapshot_id", "manifest_sha256")})
    payload["outputs"]["data_snapshot_id"] = output["parents"][0]
    payload["consumer"]["snapshot_id"] = output["snapshot_id"]
    envelope = sign_receipt(payload, private, "test-issuer")

    def check(image):
        return validate_promotion(
            repo, head, envelope, image, source_path, output_path, root, repo, head, now=NOW
        )

    assert check(payload["candidate"]["image_digest"])["status"] == "PROMOTION_ADMITTED"
    with pytest.raises(ValueError, match="artifact differs"):
        check("sha256:" + "f" * 64)
    (repo / "src/app.py").write_text("UNREVIEWED = True\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Deployed code differs"):
        check(payload["candidate"]["image_digest"])


@pytest.fixture
def host_bridge(receipt_repository, production, monkeypatch):
    import run_staging_control as control

    repo, _, head, private, payload = receipt_repository
    root, registry, source, source_path, output, output_path, _ = production
    payload["source"] = {key: source[key] for key in ("snapshot_id", "manifest_sha256")}
    payload["outputs"].update({key: output[key] for key in ("snapshot_id", "manifest_sha256")})
    payload["outputs"]["data_snapshot_id"] = output["parents"][0]
    payload["consumer"]["snapshot_id"] = output["snapshot_id"]
    receipt = repo.parent / "receipt.json"
    receipt.write_text(json.dumps(sign_receipt(payload, private, "test-issuer")), encoding="utf-8")
    state = validate_production_state(source_path, output_path, root)
    state["registry"] = str(promotion.DATA_ROOT / registry.relative_to(root).as_posix())
    actual = {
        "Name": "/ta3000-dagster-webserver",
        "Id": "actual-production",
        "Image": "sha256:old",
        "State": {"Running": True},
        "Config": {
            "Labels": {
                "com.docker.compose.project": "dagster-product-staging",
                "com.docker.compose.service": "dagster-webserver",
            }
        },
        "Mounts": [
            {"Type": "bind", "Destination": str(promotion.DATA_ROOT), "Source": str(root)},
            {"Type": "bind", "Destination": "/workspace", "Source": str(repo)},
        ],
        "NetworkSettings": {"Ports": {"3000/tcp": [{"HostPort": "3000"}]}},
    }
    monkeypatch.setattr(control, "inspect_container", lambda name: copy.deepcopy(actual))
    monkeypatch.setattr(control, "graphql_runs", lambda url: [])
    commands = []
    monkeypatch.setattr(
        control, "checked", lambda *cmd, **kw: commands.append((cmd, kw)) or json.dumps(state)
    )
    args = SimpleNamespace(
        repo=repo,
        main_ref=head,
        receipt=receipt,
        source_manifest=source_path,
        output_manifest=output_path,
        image_digest=payload["candidate"]["image_digest"],
        production_code_ref=head,
    )
    return args, state, actual, commands, control


def test_host_native_bridge_uses_one_real_inspect_bound_readonly_command(host_bridge):
    args, _, _, commands, _ = host_bridge
    report = promotion.validate_host(args, now=NOW)
    assert report["status"] == "PROMOTION_ADMITTED"
    assert report["production_container_id"] == "actual-production"
    assert report["rollback_image_digest"] == "sha256:old"
    assert len(commands) == 1
    command, options = commands[0]
    assert command[:3] == ("docker", "run", "--rm") and options["timeout"] == 180
    assert "--read-only" in command and command[command.index("--network") + 1] == "none"
    assert command[command.index("--mode") + 1] == "native"
    assert args.image_digest in command and "--memory" in command and "--pids-limit" in command
    bootstrap = command[command.index("-c") + 1]
    assert "-I" in command and "-B" in command
    assert "'/staging-verifier/scripts','/staging-verifier/src'" in bootstrap
    assert command[command.index("--workdir") + 1] == "/"
    mounts = [command[index + 1] for index, value in enumerate(command) if value == "--mount"]
    assert len(mounts) == 4 and all(value.endswith(",readonly") for value in mounts)
    assert all("docker.sock" not in value and "dagster_home" not in value for value in mounts)


@pytest.mark.parametrize("mutation", ["container", "active", "pointer"])
def test_host_native_bridge_rejects_changed_actual_binding_or_publication(
    host_bridge, monkeypatch, mutation
):
    args, state, actual, _, control = host_bridge
    if mutation == "pointer":
        state["data"]["manifest_sha256"] = "f" * 64
    elif mutation == "active":
        calls = iter([[], [{"runId": "started-during-inspection"}]])
        monkeypatch.setattr(control, "graphql_runs", lambda url: next(calls))
    else:
        changed = {**actual, "Image": "sha256:changed"}
        calls = iter([actual, changed])
        monkeypatch.setattr(control, "inspect_container", lambda name: next(calls))
    with pytest.raises(ValueError, match="changed|active"):
        promotion.validate_host(args, now=NOW)


@pytest.mark.parametrize("nested", ["/workspace/src", str(promotion.DATA_ROOT) + "/research"])
def test_host_native_bridge_rejects_nested_production_mounts(host_bridge, nested):
    args, _, actual, commands, _ = host_bridge
    actual["Mounts"].append({"Type": "bind", "Destination": nested, "Source": "/unknown"})
    with pytest.raises(ValueError, match="Nested"):
        promotion.validate_host(args, now=NOW)
    assert not commands


def test_native_cli_performs_no_git_or_host_inspection(monkeypatch, capsys):
    paths = []
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "promotion",
            "--mode",
            "native",
            "--source-manifest",
            "/s.json",
            "--output-manifest",
            "/o.json",
        ],
    )
    monkeypatch.setattr(promotion, "readonly_mount", paths.append)
    monkeypatch.setattr(
        promotion, "validate_production_state", lambda *args: {"native": "metadata-only"}
    )
    monkeypatch.setattr(
        promotion, "resolve", lambda *args: pytest.fail("Linux must not read Windows Git metadata")
    )
    assert promotion.main() == 0
    assert json.loads(capsys.readouterr().out) == {"native": "metadata-only"}
    assert len(paths) == 4
