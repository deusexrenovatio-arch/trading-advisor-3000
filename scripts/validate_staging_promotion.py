"""Read-only schema-preserving promotion check; protected host owns production mount identity.

Run native reads in Linux with the actual production root mounted read-only at its
usual logical path. No migration, rebuild, data copy or deployment is performed.
Changed schemas/layouts/checkpoint contracts require a separately proven transition.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path, PurePosixPath

from build_staging_candidate import (
    config_fingerprint,
    executable_fingerprint,
    execution_manifest,
    git,
    resolve,
    verify_deployed_files,
)
from validate_staging_acceptance import KEYRING, ROOT, load_envelope, validate_acceptance

sys.path.insert(0, str(ROOT / "src"))


def validate_production_state(source_path: Path, output_path: Path, root: Path) -> dict:
    from trading_advisor_3000.product_plane.common.data_snapshots import (
        assert_tables_unchanged,
        current_snapshot,
        load_snapshot,
        open_pinned_table,
    )
    from trading_advisor_3000.product_plane.common.staging_contracts import digest
    from trading_advisor_3000.product_plane.contracts.schema_validation import (
        load_schema,
        validate_schema,
    )
    from trading_advisor_3000.product_plane.research import admission as admission_owner
    from trading_advisor_3000.product_plane.research.compute_contract import compare_contracts
    from trading_advisor_3000.product_plane.research.data_snapshots import (
        _parameters,
        snapshot_registry,
    )
    from trading_advisor_3000.product_plane.research.hash_compatibility import (
        REQUIRED_QUALITY,
        SCOPE_PARAMETER_KEYS,
    )

    root = root.resolve()
    if (root / ".ta3000-staging.json").exists():
        raise ValueError("A staging root cannot stand in for production")
    source, output = load_snapshot(source_path), load_snapshot(output_path)
    if len(output.get("parents", [])) != 1:
        raise ValueError("Accepted output requires exactly one Data-parent checkpoint")
    data_registry = root / "canonical/moex/baseline-4y-current/.data_snapshots"
    contour = (
        "pit_active_front"
        if source["parameters"].get("series_mode") == "continuous_front"
        else "native_tradable"
    )
    registry = snapshot_registry(root / "research/gold/current", source["dataset_version"], contour)
    if any(not path.resolve().is_relative_to(root) for path in (registry, data_registry)):
        raise ValueError("Production snapshot registry escapes the actual production root")
    admission_schema = load_schema(
        Path(admission_owner.__file__).parents[1]
        / "contracts/schemas/research_refresh_admission.v1.json"
    )
    data, research = current_snapshot(data_registry), current_snapshot(registry)
    parents = source.get("parents")
    if (
        data is None
        or research is None
        or not isinstance(parents, list)
        or len(parents) != 1
        or not re.fullmatch(r"[0-9a-f]{32}", str(parents[0]))
    ):
        raise ValueError("Current Data/Research or retained accepted Data parent is missing")
    old_data = load_snapshot(data_registry / "manifests" / f"{source['parents'][0]}.json")

    def retained(registry, identifier, expected=None):
        if not re.fullmatch(r"[0-9a-f]{32}", str(identifier)):
            raise ValueError("Invalid retained checkpoint ID")
        path = registry / "manifests" / f"{identifier}.json"
        if not path.resolve().is_relative_to(registry.resolve()):
            raise ValueError("Retained manifest escapes its production registry")
        manifest = load_snapshot(path)
        if manifest["snapshot_id"] != identifier or (
            expected
            and (
                Path(expected["registry_root"]).resolve() != registry.resolve()
                or Path(expected["manifest_path"]).resolve() != path.resolve()
                or any(expected[k] != manifest[k] for k in ("snapshot_id", "manifest_sha256"))
            )
        ):
            raise ValueError("Retained checkpoint path/hash/identity mismatch")
        return manifest

    def lineage(current, endpoint, registry, research_chain=False):
        chain, seen = [], set()
        for _ in range(128):
            if current["snapshot_id"] in seen:
                raise ValueError("Production checkpoint lineage contains a cycle")
            chain.append(current)
            seen.add(current["snapshot_id"])
            if current["snapshot_id"] == endpoint["snapshot_id"]:
                if current["manifest_sha256"] != endpoint["manifest_sha256"]:
                    raise ValueError("Retained source checkpoint hash differs")
                return chain
            if research_chain:
                admission = current["parameters"].get("refresh_admission", {})
                validate_schema(admission_schema, admission)
                if (
                    admission.get("operation") != "daily"
                    or admission.get("outcome") != "SCOPED"
                    or admission.get("admission_id")
                    != admission_owner._digest(
                        {k: v for k, v in admission.items() if k != "admission_id"}
                    )
                    or not admission["effective_windows"]
                    or current["parameters"].get("operation") != "daily"
                    or type(current["parameters"].get("refresh_scope_version")) is not int
                    or current["parameters"].get("refresh_scope_version") != 1
                    or current["parameters"].get("force_full_refresh", False)
                    or admission["domain"].get("dataset_version") != current["dataset_version"]
                    or Path(admission["domain"]["materialized_output_dir"]).resolve()
                    != (root / "research/gold/current").resolve()
                ):
                    raise ValueError("Unknown or rebuild Research checkpoint transition")
                from trading_advisor_3000.product_plane.research.refresh_scope import _stamp

                if any(
                    _stamp(w["start_ts"]) > _stamp(w["end_ts"])
                    for w in admission["effective_windows"]
                ):
                    raise ValueError("Research checkpoint transition window is invalid")
                parent = admission.get("research_parent")
                if not isinstance(parent, dict):
                    raise ValueError("Intervening Research parent checkpoint is missing")
                current = retained(registry, parent["snapshot_id"], parent)
            else:
                if (
                    current["code_version"] != "moex-baseline-update"
                    or current["parameters"].get("downstream_scope", {}).get("full_refresh")
                    is not False
                ):
                    raise ValueError("Unknown/reset/recovery Data checkpoint transition")
                parent = current.get("parents", [])
                if len(parent) != 1:
                    raise ValueError("Intervening Data parent checkpoint is missing")
                current = retained(registry, parent[0])
        raise ValueError("Production checkpoint lineage exceeds 128 retained nodes")

    data_chain = lineage(data, old_data, data_registry)
    research_chain = lineage(research, source, registry, True)
    data_by_id = {item["snapshot_id"]: item for item in data_chain}
    data_positions = {item["snapshot_id"]: index for index, item in enumerate(data_chain)}
    research_positions = []
    for child in research_chain:
        child_parents = child.get("parents", [])
        if len(child_parents) != 1 or child_parents[0] not in data_by_id:
            raise ValueError("Research Data parent is outside the retained Data lineage")
        parent = data_by_id[child_parents[0]]
        research_positions.append(data_positions[parent["snapshot_id"]])
        if child["parents"] != [parent["snapshot_id"]] or any(
            child["tables"].get(name) != ref for name, ref in parent["tables"].items()
        ):
            raise ValueError("Research does not bind the exact Data parent and complete input pins")
        if child["snapshot_id"] != source["snapshot_id"]:
            declared = child["parameters"]["refresh_admission"].get("data_parent")
            if not isinstance(declared, dict):
                raise ValueError("Research transition Data parent checkpoint is missing")
            retained(data_registry, parent["snapshot_id"], declared)
            if any(
                child["tables"].get(name) != ref
                for name, ref in child["parameters"]["refresh_admission"]["inputs"].items()
            ):
                raise ValueError("Research transition input pins differ from its checkpoint")
    if research_positions != sorted(research_positions):
        raise ValueError("Research checkpoints reverse the retained Data lineage")
    if research["parents"] != [data["snapshot_id"]]:
        raise ValueError("Current Research does not bind the current Data parent")
    references = {}
    for manifest in (*data_chain, *research_chain, output):
        if (
            not manifest.get("quality_checks")
            or any(
                status not in {"PASS", "PASS-NOOP"}
                for status in manifest["quality_checks"].values()
            )
            or not manifest.get("physical_schemas")
        ):
            raise ValueError("Unknown physical schema or non-PASS accepted checkpoint")
        for reference in manifest["tables"].values():
            if not Path(reference["table_path"]).resolve().is_relative_to(root):
                raise ValueError("Snapshot table escapes the actual production root")
            if manifest is not output:
                references[(reference["table_path"], reference["delta_version"])] = reference
    for reference in references.values():
        open_pinned_table(reference)
    transitions = [
        (before, current, True)
        for chain in (data_chain, research_chain)
        for current, before in zip(chain, chain[1:], strict=False)
    ] + [(output, research, False)]
    for before, current, versions in transitions:
        if (
            before["dataset_version"] != current["dataset_version"]
            or set(before["required_tables"]) != set(current["required_tables"])
            or set(before["tables"]) != set(current["tables"])
            or before["physical_schemas"] != current["physical_schemas"]
        ):
            raise ValueError("Changed table/schema/layout needs a proven transition")
        for name, reference in before["tables"].items():
            actual = current["tables"][name]
            if any(
                reference.get(key) != actual.get(key)
                for key in (
                    "table_path",
                    "table_id",
                    "schema_hash",
                    "layout_version",
                    "partition_columns",
                )
            ) or (versions and actual["delta_version"] < reference["delta_version"]):
                raise ValueError("Changed table identity/layout or regressed checkpoint")
    for before, current in zip(data_chain, data_chain[1:], strict=False):
        volatile = {
            "runtime_provenance",
            "changed_windows",
            "downstream_scope",
            "economics_stage_inputs",
        }
        if {k: v for k, v in before["parameters"].items() if k not in volatile} != {
            k: v for k, v in current["parameters"].items() if k not in volatile
        }:
            raise ValueError("Changed persisted Data policy/configuration")
    for before in (*research_chain, output):

        def projected(manifest):
            return {
                k: v
                for k, v in _parameters(manifest["parameters"]).items()
                if k not in SCOPE_PARAMETER_KEYS
            }

        if (
            not REQUIRED_QUALITY.issubset(before["quality_checks"])
            or not REQUIRED_QUALITY.issubset(research["quality_checks"])
            or projected(before) != projected(research)
            or not compare_contracts(
                before["parameters"].get("semantic_compute_contract"),
                research["parameters"].get("semantic_compute_contract"),
            )
        ):
            raise ValueError("Unknown or changed persisted policy/compute/checkpoint contract")
    assert_tables_unchanged(research["tables"])
    if any(
        current_snapshot(path)["manifest_sha256"] != manifest["manifest_sha256"]
        for path, manifest in ((data_registry, data), (registry, research))
    ):
        raise ValueError("Production publication changed during promotion inspection")
    return {
        "source": {key: source[key] for key in ("snapshot_id", "manifest_sha256")},
        "accepted_output": {key: output[key] for key in ("snapshot_id", "manifest_sha256")},
        "accepted_output_data_snapshot_id": output["parents"][0],
        "data": {key: data[key] for key in ("snapshot_id", "manifest_sha256")},
        "research": {key: research[key] for key in ("snapshot_id", "manifest_sha256")},
        "pins_sha256": digest(research["tables"]),
        "registry": str(registry),
        "lineage": {
            label: [
                {key: item[key] for key in ("snapshot_id", "manifest_sha256")} for item in chain
            ]
            for label, chain in (("data", data_chain), ("research", research_chain))
        },
        "schema_rollback_sha256": digest(research["physical_schemas"]),
    }


def validate_artifact(
    repo, main_ref, envelope, image_digest, production_code_root, production_code_ref, *, now=None
):
    from trading_advisor_3000.product_plane.common.staging_route_proof import (
        verify_acceptance_route,
    )

    main = resolve(repo, main_ref)
    keys = json.loads(git(repo, "show", f"{main}:{KEYRING}"))["keys"]
    accepted = verify_acceptance_route(envelope, keys, now=now)
    candidate = accepted["candidate"]
    validate_acceptance(
        repo,
        base_ref=candidate["base_sha"],
        head_ref=candidate["head_sha"],
        envelope=envelope,
        now=now,
    )
    git(repo, "merge-base", "--is-ancestor", candidate["head_sha"], main)
    if (
        git(repo, "rev-parse", f"{main}^{{tree}}") != candidate["tree_sha"]
        or executable_fingerprint(repo, main) != candidate["executable_sha256"]
        or config_fingerprint(repo, main) != candidate["config_sha256"]
        or image_digest != candidate["image_digest"]
    ):
        raise ValueError("Actual main/artifact differs from the accepted candidate")
    rollback = resolve(repo, production_code_ref)
    code = verify_deployed_files(production_code_root, execution_manifest(repo, rollback))
    return accepted, {"sha": rollback, "executable_sha256": code}


def promotion_report(accepted, rollback, main, image_digest, state):
    if state["accepted_output_data_snapshot_id"] != accepted["outputs"]["data_snapshot_id"]:
        raise ValueError("Accepted output Data-parent checkpoint differs from signed proof")
    if any(
        state[field][key] != accepted[signed][key]
        for field, signed in (("source", "source"), ("accepted_output", "outputs"))
        for key in ("snapshot_id", "manifest_sha256")
    ):
        raise ValueError("Compatibility source is not the signed accepted source")
    return {
        "status": "PROMOTION_ADMITTED",
        "main_sha": main,
        "image_digest": image_digest,
        "compatibility": "unchanged-native-schema-layout-and-compute-contract",
        "production_state": state,
        "rollback_code": rollback,
        "scope": "read-only preflight; host production binding and deployment are separate",
    }


def validate_promotion(
    repo,
    main_ref,
    envelope,
    image_digest,
    source_path,
    output_path,
    production_root,
    production_code_root,
    production_code_ref,
    *,
    now=None,
):
    accepted, rollback = validate_artifact(
        repo, main_ref, envelope, image_digest, production_code_root, production_code_ref, now=now
    )
    return promotion_report(
        accepted,
        rollback,
        resolve(repo, main_ref),
        image_digest,
        validate_production_state(source_path, output_path, production_root),
    )


DATA_ROOT = PurePosixPath("/ta3000-data/moex-historical")
PRODUCTION_PROJECT = "dagster-product-staging"


def production_binding(control):
    actual = control.inspect_container("ta3000-dagster-webserver")
    labels = actual["Config"].get("Labels", {})
    if (
        actual.get("Name") != "/ta3000-dagster-webserver"
        or not actual["State"]["Running"]
        or labels.get("com.docker.compose.project") != PRODUCTION_PROJECT
        or labels.get("com.docker.compose.service") != "dagster-webserver"
    ):
        raise ValueError("Actual production container identity is not the canonical runtime")
    mounts = {}
    for destination in (str(DATA_ROOT), "/workspace"):
        if any(
            PurePosixPath(item["Destination"]) != PurePosixPath(destination)
            and PurePosixPath(item["Destination"]).is_relative_to(PurePosixPath(destination))
            for item in actual["Mounts"]
        ):
            raise ValueError("Nested production code/data mounts require a proven topology")
        selected = [m for m in actual["Mounts"] if m["Destination"] == destination]
        if len(selected) != 1 or selected[0]["Type"] != "bind":
            raise ValueError("Actual production code/data bind mount is missing or ambiguous")
        mounts[destination] = control.host_path(selected[0]["Source"])
    url = (
        "http://127.0.0.1:"
        + actual["NetworkSettings"]["Ports"]["3000/tcp"][0]["HostPort"]
        + "/graphql"
    )
    if control.graphql_runs(url):
        raise ValueError("Production has active native runs; promotion inspection is blocked")
    return actual, mounts


def validate_host(args, *, now=None):
    """Bind host Git and Docker truth; Linux performs native metadata reads only."""
    import run_staging_control as control

    actual, mounts = production_binding(control)
    envelope = load_envelope(args.receipt)
    accepted, rollback = validate_artifact(
        args.repo,
        args.main_ref,
        envelope,
        args.image_digest,
        mounts["/workspace"],
        args.production_code_ref,
        now=now,
    )
    sources = [(args.source_manifest, "source"), (args.output_manifest, "outputs")]
    for path, field in sources:
        document = control._read_manifest(path)
        if any(document[key] != accepted[field][key] for key in ("snapshot_id", "manifest_sha256")):
            raise ValueError("Host source/output transport differs from signed acceptance")
    command = [
        "docker",
        "run",
        "--rm",
        "--pull",
        "never",
        "--read-only",
        "--network",
        "none",
        "--workdir",
        "/",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--memory",
        "2g",
        "--cpus",
        "1",
        "--pids-limit",
        "128",
        "--tmpfs",
        "/tmp:size=268435456,mode=1777",
        "--entrypoint",
        "python",
    ]
    for key, value in {
        "NUMBA_CACHE_DIR": "/tmp/numba",
        "MPLCONFIGDIR": "/tmp/mpl",
    }.items():
        command += ["--env", f"{key}={value}"]
    bindings = {
        str(DATA_ROOT): mounts[str(DATA_ROOT)],
        "/staging-verifier": ROOT,
        "/promotion-inputs/source.json": args.source_manifest.resolve(),
        "/promotion-inputs/output.json": args.output_manifest.resolve(),
    }
    for destination, source in bindings.items():
        command += ["--mount", f"type=bind,source={source},target={destination},readonly"]
    command += [
        args.image_digest,
        "-I",
        "-B",
        "-c",
        "import runpy,signal,sys;"
        "sys.path[:0]=['/staging-verifier/scripts','/staging-verifier/src'];"
        "signal.alarm(170);sys.argv=sys.argv[1:];"
        "runpy.run_path(sys.argv[0],run_name='__main__')",
        "/staging-verifier/scripts/validate_staging_promotion.py",
        "--mode",
        "native",
        "--source-manifest",
        "/promotion-inputs/source.json",
        "--output-manifest",
        "/promotion-inputs/output.json",
    ]
    state = json.loads(control.checked(*command, timeout=180))
    fresh, fresh_mounts = production_binding(control)
    if fresh_mounts != mounts or any(fresh[k] != actual[k] for k in ("Id", "Image")):
        raise ValueError("Actual production code/image/mount binding changed during inspection")
    verify_deployed_files(mounts["/workspace"], execution_manifest(args.repo, rollback["sha"]))
    logical_registry = PurePosixPath(state["registry"])
    if ".." in logical_registry.parts or not logical_registry.is_relative_to(DATA_ROOT):
        raise ValueError("Native Research registry escapes the actual production root")
    for relative, field in (
        (PurePosixPath("canonical/moex/baseline-4y-current/.data_snapshots"), "data"),
        (logical_registry.relative_to(DATA_ROOT), "research"),
    ):
        registry = mounts[str(DATA_ROOT)].joinpath(*relative.parts)
        pointer = json.loads((registry / "published_current.json").read_text("utf-8"))
        if not re.fullmatch(r"[0-9a-f]{32}", str(pointer.get("snapshot_id"))):
            raise ValueError("Production pointer has an invalid checkpoint ID")
        current = control._read_manifest(registry / "manifests" / f"{pointer['snapshot_id']}.json")
        if any(
            pointer[k] != state[field][k] or current[k] != pointer[k]
            for k in ("snapshot_id", "manifest_sha256")
        ):
            raise ValueError("Production publication changed after native inspection")
    report = promotion_report(
        accepted, rollback, resolve(args.repo, args.main_ref), args.image_digest, state
    )
    report.update(
        production_container_id=actual["Id"],
        rollback_image_digest=actual["Image"],
        scope="actual host-bound read-only preflight; no deployment",
    )
    return report


def readonly_mount(path: Path) -> None:
    if not Path("/proc/self/mountinfo").is_file():
        raise ValueError("CLI requires a protected Linux read-only validation container")
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        if fields[4] == str(path) and "ro" in fields[5].split(","):
            return
    raise ValueError(f"Actual kernel mount is not read-only: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("host", "native"), default="host")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--main-ref", default="origin/main")
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--image-digest")
    parser.add_argument("--production-code-ref")
    args = parser.parse_args()
    try:
        if args.mode == "native":
            for path in (Path(str(DATA_ROOT)), ROOT, args.source_manifest, args.output_manifest):
                readonly_mount(path)
            result = validate_production_state(
                args.source_manifest, args.output_manifest, Path(str(DATA_ROOT))
            )
        else:
            if not all((args.receipt, args.image_digest, args.production_code_ref)):
                raise ValueError("Host mode requires receipt/image/retained production code ref")
            main = resolve(args.repo, args.main_ref)
            if resolve(ROOT, "HEAD") != main or git(
                ROOT, "status", "--porcelain", "--untracked-files=no"
            ):
                raise ValueError("Promotion verifier must be the clean protected main revision")
            verify_deployed_files(ROOT, execution_manifest(args.repo, main))
            result = validate_host(args)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (ValueError, KeyError, TypeError, IndexError, OSError, RuntimeError) as exc:
        print(f"staging promotion: BLOCKED ({exc})")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
