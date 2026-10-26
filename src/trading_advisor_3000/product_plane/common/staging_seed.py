"""Initial independent native Delta copy of an accepted Data/Research snapshot pair."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

from .data_snapshots import (
    _file_path,
    atomic_json,
    current_snapshot,
    load_snapshot,
    open_pinned_table,
)
from .staging_contracts import digest as _digest
from .staging_contracts import load_plan


def _under(path: Path, root: Path) -> Path:
    try:
        return path.resolve().relative_to(root.resolve())
    except ValueError:
        raise ValueError(f"seed path escapes its root: {path}") from None


def _registered_support(root: Path, prepared: dict) -> dict:
    from deltalake import DeltaTable

    from ..data_plane.moex.staging_input_facts import registered_history_refresh_kwargs

    path = root / prepared["path"]
    _under(path, root)
    current = DeltaTable(str(path))
    if (
        current.version() != prepared["delta_version"]
        or current.metadata().id != prepared["table_id"]
    ):
        raise ValueError("seed prepared pin must equal current source identity/version")
    return registered_history_refresh_kwargs(root)["economics_parameter_evidence"]


def _frozen_accepted(target: Path, research: dict, data: dict) -> bool:
    """A protected prior native readback can freeze an accepted checkpoint."""
    marker = target / ".ta3000-staging.json"
    if not marker.is_file():
        return False
    binding = json.loads(marker.read_text("utf-8"))
    control = Path(binding["plan_path"]).resolve()
    if control.is_relative_to(target):
        raise ValueError("Frozen seed control must be outside writable output")
    task = load_plan(control)
    if binding.get("plan_digest") != _digest(task) or Path(task["data_root"]).resolve() != target:
        raise ValueError("Frozen seed protected plan binding changed")
    source = task.get("source", {})
    anchor = source.get("accepted_observation")
    if not anchor:
        return False
    path = Path(anchor["evidence_path"])
    _under(path, control.parent)
    if path.resolve().is_relative_to(target) or path.is_symlink():
        raise ValueError("Frozen seed evidence must be outside writable data")
    raw = path.read_bytes()
    if len(raw) > 64 * 1024 or hashlib.sha256(raw).hexdigest() != anchor["evidence_sha256"]:
        raise ValueError("Frozen seed native readback digest changed")
    proof = json.loads(raw)
    observed = datetime.fromisoformat(proof["observed_at_utc"].replace("Z", "+00:00"))
    if (
        proof["observed_at_utc"] != anchor["observed_at_utc"]
        or observed.tzinfo is None
        or observed > datetime.now(UTC)
    ):
        raise ValueError("Frozen seed native observation time is invalid")
    item = proof["readback"]["item"]
    if item["type"] != "CommandExecution" or item["status"] != "completed" or item["exit_code"]:
        raise ValueError("Frozen seed requires a successful original native readback")
    payload = json.loads(item["aggregated_output"])
    matches = [
        r
        for r in payload["research"]
        if (
            r["snapshot_id"] == research["snapshot_id"]
            and r["manifest_sha256"] == research["manifest_sha256"]
            and r["data_parent"] == data["snapshot_id"]
        )
    ]
    if len(matches) != 1 or any(
        source.get(k) != research[k] for k in ("snapshot_id", "manifest_sha256")
    ):
        raise ValueError("Frozen seed does not match the observed accepted Research/Data pair")
    return True


def plan_seed(
    *,
    source: Path,
    target: Path,
    logical_root: Path,
    data: Path,
    research: Path,
    max_files: int = 100_000,
    max_manifests: int = 128,
) -> dict:
    """Read metadata only; include accepted ancestry and every pinned active file."""
    source, target, logical_root = source.resolve(), target.resolve(), logical_root.resolve()
    if source == target or source.is_relative_to(target) or target.is_relative_to(source):
        raise ValueError("seed source and target must be disjoint")
    roots = {data.parent.resolve(), research.parent.resolve()}
    heads = []
    observed_data = None
    for index, path in enumerate((data, research)):
        _under(path, source)
        manifest = load_snapshot(path)
        accepted = current_snapshot(path.parent.parent)
        if accepted is None or (
            index == 1
            and accepted["manifest_sha256"] != manifest["manifest_sha256"]
            and not _frozen_accepted(target, manifest, heads[0])
        ):
            raise ValueError("seed requires current accepted Research and its exact Data parent")
        if index == 0:
            observed_data = accepted["snapshot_id"]
        heads.append(manifest)
    if heads[1]["parents"] != [heads[0]["snapshot_id"]]:
        raise ValueError("Research seed does not depend on the selected accepted Data snapshot")
    manifests, pending, files, references, metadata = {}, [data, research], {}, [], {}
    versions: dict[Path, int] = {}

    def include(path: Path) -> None:
        relative = _under(path, source)
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"seed requires an independent regular file: {path}")
        files[relative.as_posix()] = path.stat().st_size
        if len(files) > max_files:
            raise ValueError("seed file bound exceeded")

    support_sha256 = {}
    support_prepared = {}
    prepared = heads[0]["tables"].get("prepared_raw_moex_contract_securities")
    if prepared:
        support_prepared = {
            "path": _under(Path(prepared["table_path"]), logical_root).as_posix(),
            **{key: prepared[key] for key in ("delta_version", "table_id")},
        }
        evidence = _registered_support(source, support_prepared)
        for key in ("regimes_path", "dated_official_tc_path"):
            path = Path(evidence[key])
            include(path)
            with path.open("rb") as stream:
                support_sha256[_under(path, source).as_posix()] = hashlib.file_digest(
                    stream, "sha256"
                ).hexdigest()

    while pending:
        path = pending.pop()
        manifest = load_snapshot(path)
        identifier = manifest["snapshot_id"]
        if identifier in manifests:
            if manifests[identifier] != manifest["manifest_sha256"]:
                raise ValueError("seed ancestry has conflicting snapshot identities")
            continue
        if len(manifests) >= max_manifests:
            raise ValueError("seed manifest bound exceeded")
        checks = manifest["quality_checks"]
        if not checks or any(value not in {"PASS", "PASS-NOOP"} for value in checks.values()):
            raise ValueError("seed ancestry contains unaccepted quality checks")
        manifests[identifier] = manifest["manifest_sha256"]
        include(path)
        for parent in manifest["parents"]:
            matches = [
                root / f"{parent}.json" for root in roots if (root / f"{parent}.json").is_file()
            ]
            if len(matches) != 1:
                raise ValueError(f"seed ancestry missing or ambiguous: {parent}")
            pending.append(matches[0])
        for reference in manifest["tables"].values():
            relative = _under(Path(reference["table_path"]), logical_root)
            table_path = source / relative
            physical = {**reference, "table_path": str(table_path)}
            table = open_pinned_table(physical)
            references.append({**reference, "table_path": str(target / relative)})
            versions[table_path] = max(versions.get(table_path, 0), reference["delta_version"])
            for uri in table.file_uris():
                file = _file_path(uri)
                _under(file, table_path)
                include(file)
    for table_path, maximum in versions.items():
        log = table_path / "_delta_log"
        for path in log.iterdir():
            match = re.match(r"^(\d{20})(?:\.|$)", path.name)
            if match and int(match[1]) <= maximum and path.is_file():
                include(path)
            elif path.name == "_last_checkpoint":
                checkpoint = json.loads(path.read_text(encoding="utf-8"))
                if checkpoint["version"] <= maximum:
                    metadata[_under(path, source).as_posix()] = checkpoint
        # Native checkpoint sidecars are immutable and required by v2 checkpoints.
        for path in (log / "_sidecars").glob("*"):
            include(path)
    for path, manifest in zip((data, research), heads, strict=True):
        metadata[_under(path.parent.parent / "published_current.json", source).as_posix()] = {
            "schema_version": "data_snapshot_publication.v1",
            "snapshot_id": manifest["snapshot_id"],
            "manifest_sha256": manifest["manifest_sha256"],
        }
    return {
        "schema_version": "staging_seed.v1",
        "source": str(source),
        "target": str(target),
        "logical_root": str(logical_root),
        "manifests": manifests,
        "heads": [item["snapshot_id"] for item in heads],
        "source_current_data_snapshot_id": observed_data,
        "references": references,
        "files": files,
        "metadata": metadata,
        **(
            {"support_sha256": support_sha256, "support_prepared_pin": support_prepared}
            if support_sha256
            else {}
        ),
        "total_bytes": sum(files.values())
        + sum(len(json.dumps(v).encode()) for v in metadata.values()),
    }


def apply_seed(plan: dict, *, max_bytes: int) -> dict:
    """Never overwrite: partial failure retains its marker and requires explicit recovery."""
    source, target = Path(plan["source"]), Path(plan["target"])
    if max_bytes <= 0 or plan["total_bytes"] > max_bytes:
        raise ValueError("seed byte bound exceeded")
    marker = target / ".ta3000-staging.json"
    binding = json.loads(marker.read_text(encoding="utf-8"))
    if binding.get("schema_version") != "staging_root.v1":
        raise ValueError("seed requires a protected staging root marker")
    control = Path(binding["plan_path"])
    _under(target, Path(plan["logical_root"]))
    if target.resolve() != Path(plan["logical_root"]).resolve():
        raise ValueError("seed target must preserve the logical container root")
    if control.resolve().is_relative_to(target.resolve()):
        raise ValueError("seed control must live outside the writable data root")
    task = load_plan(control)
    if binding["plan_digest"] != _digest(task) or Path(task["data_root"]).resolve() != target:
        raise ValueError("seed protected root binding changed")
    if any(path.name != marker.name for path in target.iterdir()):
        raise ValueError("seed target must be empty; restore is a separate operation")
    if plan.get("support_sha256"):
        _registered_support(source, plan["support_prepared_pin"])
    atomic_json(target / ".seed-in-progress.json", {"plan_sha256": _digest(plan)}, immutable=True)
    for relative, size in plan["files"].items():
        src, dst = source / relative, target / relative
        _under(src, source)
        _under(dst, target)
        if not src.is_file() or src.is_symlink() or src.stat().st_size != size:
            raise ValueError("seed source file changed after planning")
        dst.parent.mkdir(parents=True, exist_ok=True)
        with src.open("rb") as reader, dst.open("xb") as writer:
            shutil.copyfileobj(reader, writer)
    for relative, payload in plan["metadata"].items():
        path = target / relative
        _under(path, target)
        atomic_json(path, payload, immutable=True)
    for reference in plan["references"]:
        open_pinned_table(reference)
    for identifier, digest in plan["manifests"].items():
        paths = [target / path for path in plan["files"] if path.endswith(f"/{identifier}.json")]
        if len(paths) != 1 or load_snapshot(paths[0])["manifest_sha256"] != digest:
            raise ValueError("seed manifest changed after planning")
    for relative, digest in plan.get("support_sha256", {}).items():
        for root in (source, target):
            path = root / relative
            _under(path, root)
            if not path.is_file() or path.is_symlink():
                raise ValueError("seed support file changed after planning")
            with path.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                    raise ValueError("seed support file changed after planning")
    if plan.get("support_sha256"):
        _registered_support(target, plan["support_prepared_pin"])
    receipt = {
        "schema_version": "staging_seed_receipt.v1",
        "status": "PASS",
        "plan_sha256": _digest(plan),
        "manifests": plan["manifests"],
        "heads": plan["heads"],
        "source_current_data_snapshot_id": plan["source_current_data_snapshot_id"],
        "file_count": len(plan["files"]),
        "total_bytes": plan["total_bytes"],
    }
    atomic_json(target / ".staging-seed.json", receipt, immutable=True)
    (target / ".seed-in-progress.json").unlink()
    return receipt
