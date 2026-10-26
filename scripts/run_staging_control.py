"""Protected host supervisor: inspect isolated mounts before permitting candidate computation."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import TextIO
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from build_staging_candidate import verify_deployed_files  # noqa: E402

from trading_advisor_3000.product_plane.common.staging_contracts import (  # noqa: E402
    digest,
    execution_window_end,
    load_plan,
    sign_receipt,
)

ACTIVE = {"QUEUED", "STARTING", "STARTED", "CANCELING"}
FAILED = {"FAILURE", "CANCELED"}
PREFIX = "TA3000_STAGING_"
ENV_KEYS = {
    PREFIX + key
    for key in (
        "IMAGE",
        "DATA_ROOT",
        "CONTROL_ROOT",
        "QUOTA_ROOT",
        "CANDIDATE_WORKTREE",
        "VERIFIER_WORKTREE",
        "HOST_STATE",
        "TASK_SLUG",
        "PORT",
        "MEMORY_LIMIT",
        "CPUS",
        "SPARK_MEMORY",
        "MAX_GROWTH_BYTES",
        "MIN_FREE_BYTES",
        "SCRATCH_BYTES",
        "DATASET_VERSION",
    )
} | {"TA3000_ACCEPTANCE_MEMORY_LIMIT"}


def run(*command, env=None, timeout=30):
    return subprocess.run(
        command, capture_output=True, text=True, check=False, timeout=timeout, env=env
    )


def checked(*command, **kwargs):
    result = run(*command, **kwargs)
    if result.returncode:
        raise ValueError(f"Control command failed: {command[0]} {command[1]}")
    return result.stdout


def host_path(value):
    if os.name == "nt":
        value = re.sub(
            r"^/(?:run/desktop/mnt/host|host_mnt|mnt)/([a-z])/(.*)",
            lambda m: f"{m[1]}:/{m[2]}",
            str(value),
        )
    return Path(value).resolve()


def overlaps(a, b):
    return a == b or a.is_relative_to(b) or b.is_relative_to(a)


def data_root_file_bytes(root, *, max_entries=100000, max_seconds=10):
    """Bound the isolated root's payload inventory; never read file contents or follow links."""
    pending, total, entries = [Path(root)], 0, 1
    deadline = time.monotonic() + max_seconds
    while pending:
        if time.monotonic() >= deadline:
            raise ValueError("Staging data inventory time cap exceeded")
        path = pending.pop()
        metadata = path.stat(follow_symlinks=False)
        if stat.S_ISLNK(metadata.st_mode) or getattr(metadata, "st_file_attributes", 0) & 0x400:
            raise ValueError("Staging data inventory refuses symlinks/reparse points")
        if stat.S_ISREG(metadata.st_mode):
            total += metadata.st_size
        elif stat.S_ISDIR(metadata.st_mode):
            with os.scandir(path) as children:
                for child in children:
                    entries += 1
                    if entries > max_entries or time.monotonic() >= deadline:
                        raise ValueError("Staging data inventory count/time cap exceeded")
                    pending.append(Path(child.path))
        else:
            raise ValueError("Staging data inventory refuses non-regular files")
    if time.monotonic() >= deadline:
        raise ValueError("Staging data inventory time cap exceeded")
    return total


def read_environment(path):
    values = {}
    for line in path.read_text("utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if not separator or key not in ENV_KEYS or key in values or "$" in value:
            raise ValueError("Control env file requires unique literal staging variables")
        values[key] = value
    return values


def graphql_read(url, query, variables=None):
    if re.search(r"\bmutation\b", query, re.IGNORECASE):
        raise ValueError("Runtime metadata helper accepts read-only queries")
    request = urllib.request.Request(
        url,
        data=json.dumps({"query": query, "variables": variables or {}}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.load(response)
    if payload.get("errors") or not isinstance(payload.get("data"), dict):
        raise ValueError("Runtime readback could not be established")
    return payload["data"]


def graphql_runs(url, *, plan_digest=None, tags=None, job_name=None):
    selectors = dict(tags or {})
    if plan_digest:
        selectors["ta3000/staging_plan_sha256"] = plan_digest
    filters = (
        {"tags": [{"key": key, "value": value} for key, value in selectors.items()]}
        if selectors
        else {}
    )
    if job_name:
        filters["pipelineName"] = job_name
    if not filters:
        filters["statuses"] = sorted(ACTIVE)
    result = graphql_read(
        url,
        """query Runs($filter:RunsFilter!){
      runsOrError(filter:$filter,limit:100){__typename ... on Runs{
        results{runId status pipelineName startTime endTime tags{key value}}
      }}}""",
        {"filter": filters},
    )["runsOrError"]
    if result.get("__typename") != "Runs":
        raise ValueError("Runtime activity could not be established")
    return result["results"]


def run_tags(run):
    return {item["key"]: item["value"] for item in run.get("tags", [])}


def graphql_schedule(prepared, url):
    """Read native schedule state; visible Run.tags intentionally omit hidden Dagster tags."""
    result = graphql_read(
        url,
        """{repositoriesOrError {__typename ... on RepositoryConnection {
      nodes {name location{name} schedules{name cronSchedule executionTimezone
      scheduleState{id status ticks(limit:100){id status timestamp runIds}}}}}}}""",
    )["repositoriesOrError"]
    matches = [
        (repo, schedule)
        for repo in result.get("nodes", [])
        for schedule in repo.get("schedules", [])
        if schedule["name"] == "moex_baseline_daily_update_schedule"
    ]
    if result.get("__typename") != "RepositoryConnection" or len(matches) != 1:
        raise ValueError("Native schedule repository is missing or ambiguous")
    repo, schedule = matches[0]
    selector = (repo["name"], repo["location"]["name"], schedule["name"])
    expected = (
        "__repository__",
        "trading_advisor_3000.dagster_defs:product_plane_definitions",
        "moex_baseline_daily_update_schedule",
    )
    selectors = prepared.setdefault("native_schedule_selectors", {})
    if selector != expected or selectors.setdefault(url, selector) != selector:
        raise ValueError("Native schedule repository identity changed")
    declared, state = prepared["plan"]["source"]["native_schedule"], schedule["scheduleState"]
    if (
        schedule["cronSchedule"] != declared["cron"]
        or schedule["executionTimezone"] != declared["timezone"]
        or state["status"] != "RUNNING"
    ):
        raise ValueError("Actual native schedule cron/timezone/state differs from protected route")
    seen = set()
    for tick in state["ticks"]:
        timestamp = float(tick["timestamp"])
        if (
            timestamp <= 0
            or tick["id"] != state["id"].split("::")[0] + ":" + str(timestamp)
            or tick["id"] in seen
            or not isinstance(tick["runIds"], list)
            or any(not isinstance(value, str) or not value for value in tick["runIds"])
            or len(set(tick["runIds"])) != len(tick["runIds"])
        ):
            raise ValueError("Native schedule tick identity or run linkage is invalid")
        seen.add(tick["id"])
    return state


def _root_tick(state, run_id):
    matches = [
        tick for tick in state["ticks"] if tick["status"] == "SUCCESS" and run_id in tick["runIds"]
    ]
    return matches[0] if len(matches) == 1 else None


def begin_native_observation(prepared, url):
    roots = graphql_runs(url, job_name=prepared["plan"]["root_job"])
    prepared["native_observation_boundary"] = {
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "root_run_ids": {item["runId"] for item in roots if item["status"] in {"SUCCESS", *FAILED}},
        "tick_ids": {
            tick["id"]
            for tick in graphql_schedule(prepared, url)["ticks"]
            if tick["status"] in {"SUCCESS", "FAILURE", "SKIPPED"}
        }
        if prepared["plan"]["stage"] == "acceptance"
        else set(),
    }


def check_native_failures(prepared, url):
    boundary = prepared["native_observation_boundary"]
    for item in graphql_runs(url, job_name=prepared["plan"]["root_job"]):
        if item["runId"] not in boundary["root_run_ids"] and item["status"] in FAILED:
            raise ValueError("New native root failed before/after admission: " + item["runId"])
    if prepared["plan"]["stage"] == "acceptance":
        for tick in graphql_schedule(prepared, url)["ticks"]:
            if tick["id"] not in boundary["tick_ids"] and tick["status"] == "FAILURE":
                raise ValueError("New native schedule tick failed before admission: " + tick["id"])


def _production_identity(container):
    homes = [
        item
        for item in container.get("Mounts", [])
        if item["Destination"] == "/opt/dagster/dagster_home"
    ]
    if len(homes) != 1 or homes[0].get("Type") != "volume" or not homes[0].get("Name"):
        return None
    return {
        "id": container["Id"],
        "image": container["Image"],
        "home": homes[0],
        "labels": container["Config"].get("Labels") or {},
        "mounts_sha256": digest(sorted(container["Mounts"], key=lambda item: item["Destination"])),
    }


def production_barrier(prepared, root, now):
    """Resource admission uses an operator window and live idle state, never market proof."""
    prepared["permit_window_end"] = None
    if graphql_runs(prepared["production_url"]):
        return False
    if prepared["plan"]["stage"] == "acceptance":
        if (
            root is None
            or _root_tick(graphql_schedule(prepared, prepared["staging_url"]), root["runId"])
            is None
        ):
            return False
    end = execution_window_end(prepared["plan"], now)
    if end is None:
        return False
    expected = prepared.get("production_runtime_identity")
    if expected is not None:
        current = inspect_container("ta3000-dagster-webserver")
        if not current["State"]["Running"] or _production_identity(current) != expected:
            raise ValueError("Production runtime identity changed during resource admission")
    prepared["permit_window_end"] = end
    return True


def inspect_container(identifier):
    return json.loads(checked("docker", "inspect", identifier))[0]


def owned(project):
    identifiers = checked(
        "docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"
    ).split()
    return [inspect_container(value) for value in identifiers]


def stop_owned(project, containers):
    for previous in containers:
        current = inspect_container(previous["Id"])
        labels = current["Config"].get("Labels") or {}
        service = labels.get("com.docker.compose.service")
        collector = (
            service == "collector" and labels.get("ta3000.staging.owner") == "protected-observer"
        )
        if labels.get("com.docker.compose.project") != project or (
            service not in {"webserver", "daemon"} and not collector
        ):
            raise ValueError("Refusing to stop a foreign container")
        checked("docker", "stop", "--time", "10", current["Id"], timeout=20)


def branch_control_target(prepared, web):
    if prepared["plan"]["stage"] != "branch" or not prepared["project"].startswith(
        "ta3000-branch-"
    ):
        raise ValueError("Schedule control is limited to the own branch staging project")
    current = inspect_container(web["Id"])
    labels = current["Config"].get("Labels") or {}
    if (
        labels.get("com.docker.compose.project") != prepared["project"]
        or labels.get("com.docker.compose.service") != "webserver"
    ):
        raise ValueError("Schedule control requires the verified own branch container")
    verify_runtime(current, prepared)
    bindings = current["NetworkSettings"]["Ports"]["3000/tcp"]
    if len(bindings) != 1 or bindings[0]["HostIp"] != "127.0.0.1":
        raise ValueError("Branch schedule control requires one fixed localhost binding")
    target = f"http://127.0.0.1:{int(bindings[0]['HostPort'])}/graphql"
    if target == prepared["production_url"]:
        raise ValueError("Branch control cannot address the actual production endpoint")
    return target


@dataclass
class HostLease:
    path: Path
    stream: TextIO
    nonce: str


def _host_control_paths(prepared):
    if not prepared.get("host_identity_checked"):
        raise ValueError("Host identity/mount isolation must be checked before control writes")
    paths = prepared["paths"]
    for key in ("HOST_STATE", "CONTROL_ROOT"):
        if any(
            overlaps(paths[key].resolve(), mount.resolve())
            for mount in prepared["production_mounts"]
        ):
            raise ValueError("Host control path overlaps production")
    if overlaps(paths["HOST_STATE"], paths["CONTROL_ROOT"]):
        raise ValueError("Host lease and protected control roots must be independent")
    return paths["HOST_STATE"] / "heavy-compute.lock", paths[
        "CONTROL_ROOT"
    ] / "execution-permit.json"


def _control_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + uuid4().hex + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def acquire_heavy(prepared):
    path, _ = _host_control_paths(prepared)
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("x", encoding="utf-8")
    nonce = uuid4().hex
    json.dump(
        {
            "schema_version": "staging_host_lock.v1",
            "project": prepared["project"],
            "plan_sha256": digest(prepared["plan"]),
            "lease_nonce": nonce,
            "pid": os.getpid(),
        },
        stream,
    )
    stream.flush()
    os.fsync(stream.fileno())
    return HostLease(path, stream, nonce)


def _owned_lease(prepared, lease):
    path, target = _host_control_paths(prepared)
    if lease is None or lease.stream.closed or lease.path != path:
        raise ValueError("Permit requires a live owned host lease")
    live, held = path.stat(), os.fstat(lease.stream.fileno())
    document = json.loads(path.read_text("utf-8"))
    if (live.st_ino, live.st_dev) != (held.st_ino, held.st_dev) or document != {
        "schema_version": "staging_host_lock.v1",
        "project": prepared["project"],
        "plan_sha256": digest(prepared["plan"]),
        "lease_nonce": lease.nonce,
        "pid": os.getpid(),
    }:
        raise ValueError("Owned host lease identity changed")
    return target


def renew_permit(prepared, lease, now=None):
    target = _owned_lease(prepared, lease)
    now = now or datetime.now(UTC)
    deadline = min(now + timedelta(seconds=30), prepared["deadline"])
    if prepared.get("permit_window_end") is not None:
        deadline = min(deadline, prepared["permit_window_end"])
    if deadline <= now:
        raise ValueError("Owned host lease cannot extend the protected deadline")
    _control_json(
        target,
        {
            "schema_version": "staging_execution_permit.v1",
            "plan_sha256": digest(prepared["plan"]),
            "lease_nonce": lease.nonce,
            "issued_at_utc": now.isoformat(),
            "deadline_utc": deadline.isoformat(),
        },
    )


def renew_candidate_permit(prepared, lease, root):
    plan = prepared["plan"]
    if (
        plan["stage"] == "acceptance"
        and plan.get("mode", "candidate") == "candidate"
        and root is not None
        and root["status"] == "QUEUED"
    ):
        checked_roots = prepared.setdefault("fresh_main_roots", set())
        if root["runId"] not in checked_roots:
            remote = checked(
                "git",
                "-C",
                str(prepared["paths"]["CANDIDATE_WORKTREE"]),
                "ls-remote",
                "origin",
                "refs/heads/main",
                timeout=30,
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "Never"},
            ).split()
            if (
                len(remote) != 2
                or remote[1] != "refs/heads/main"
                or remote[0] != plan["candidate"]["base_sha"]
            ):
                raise ValueError(
                    "Remote main is stale or unknown; recompose candidate before compute"
                )
            checked_roots.add(root["runId"])
    renew_permit(prepared, lease)


def revoke_permit(prepared, lease):
    target = _owned_lease(prepared, lease)
    if target.exists():
        document = json.loads(target.read_text("utf-8"))
        if document.get("lease_nonce") != lease.nonce or document.get("plan_sha256") != digest(
            prepared["plan"]
        ):
            raise ValueError("Refusing to remove another host's permit")
        target.unlink()


def release_heavy(prepared, lease):
    try:
        revoke_permit(prepared, lease)
        _owned_lease(prepared, lease)
    finally:
        lease.stream.close()
    lease.path.unlink()


def _read_manifest(path):
    value = json.loads(path.read_text("utf-8"))
    expected = hashlib.sha256(
        json.dumps(
            {k: v for k, v in value.items() if k != "manifest_sha256"},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    if value.get("manifest_sha256") != expected:
        raise ValueError("Snapshot manifest hash changed")
    return value


def _source_pair(paths, research):
    parents = research.get("parents", [])
    if len(parents) != 1 or not re.fullmatch(r"[0-9a-f]{32}", parents[0]):
        raise ValueError("Research requires its exact accepted Data parent")
    data = _read_manifest(
        paths["DATA_ROOT"]
        / "canonical/moex/baseline-4y-current/.data_snapshots/manifests"
        / (parents[0] + ".json")
    )
    if data["snapshot_id"] != parents[0]:
        raise ValueError("Source Data manifest does not match the exact Research parent")
    pair = {}
    for name, value in (("research", research), ("data", data)):
        if not value.get("quality_checks") or any(
            v not in {"PASS", "PASS-NOOP"} for v in value["quality_checks"].values()
        ):
            raise ValueError("Source snapshot QC is not accepted")
        pair[name] = {k: value[k] for k in ("snapshot_id", "manifest_sha256")}
        pair[name]["table_pins"] = value["tables"]
    return pair


def _write_reference_cache(path, value):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    value = {key: item for key, item in value.items() if key != "attestation"}
    private = Path(os.environ["TA3000_STAGING_OPERATOR_PRIVATE_KEY"]).read_bytes()
    signed = Ed25519PrivateKey.from_private_bytes(private).sign(digest(value).encode())
    _control_json(
        path,
        {
            **value,
            "attestation": {
                "key_id": os.environ["TA3000_STAGING_OPERATOR_KEY_ID"],
                "signature": base64.b64encode(signed).decode(),
            },
        },
    )


def _read_reference_cache(path):
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    value = json.loads(path.read_text("utf-8"))
    signed = value.pop("attestation", {})
    keys = json.loads((ROOT / "configs/staging-acceptance-keys.v1.json").read_text("utf-8"))["keys"]
    try:
        Ed25519PublicKey.from_public_bytes(
            base64.b64decode(keys[signed["key_id"]], validate=True)
        ).verify(base64.b64decode(signed["signature"], validate=True), digest(value).encode())
    except (InvalidSignature, KeyError, ValueError) as exc:
        raise ValueError("Reference cache lacks a valid protected host attestation") from exc
    return value


def _accepted_source(plan, paths, research):
    from trading_advisor_3000.product_plane.common.staging_route_proof import (
        verify_acceptance_route,
    )

    cache_path = paths["HOST_STATE"] / "ta3000-acceptance-reference-cycles.json"
    cache = _read_reference_cache(cache_path) if cache_path.is_file() else {}
    reference = plan.get("mode", "candidate") == "reference"
    envelope = plan["reference"]["receipt"] if reference else cache.get("accepted_receipt")
    if not envelope:
        raise ValueError("Source is neither the initial seed nor accepted main/reference output")
    keys = json.loads((ROOT / "configs/staging-acceptance-keys.v1.json").read_text("utf-8"))["keys"]
    accepted = verify_acceptance_route(
        envelope,
        keys,
        now=datetime.fromisoformat(envelope["payload"]["issued_at_utc"].replace("Z", "+00:00")),
    )
    same = cache.get("accepted_receipt") == envelope
    main = cache.get("accepted_main_sha") if same else plan["reference"]["main_sha"]
    target = plan["reference"]["main_sha"] if reference else plan["candidate"]["base_sha"]
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", str(main)):
        raise ValueError("Source lacks its actual accepted main identity")
    checked(
        "git", "-C", str(paths["CANDIDATE_WORKTREE"]), "merge-base", "--is-ancestor", main, target
    )
    if (
        checked(
            "git", "-C", str(paths["CANDIDATE_WORKTREE"]), "rev-parse", main + "^{tree}"
        ).strip()
        != accepted["candidate"]["tree_sha"]
    ):
        raise ValueError("Accepted source main tree differs from its signed candidate")
    pair = _source_pair(paths, research)
    cycles = cache.get("completed", {}) if same else {}
    if cycles:
        latest = max(cycles.values(), key=lambda cycle: cycle["schedule"]["timestamp"])
        if latest.get("status") not in {"PASS", "PASS-NOOP"} or any(
            latest.get("outputs", {}).get(name, {}).get(key) != value
            for name, proof in pair.items()
            for key, value in proof.items()
        ):
            raise ValueError("Source differs from the latest successful protected reference output")
    else:
        output = accepted["outputs"]
        if (
            any(research[k] != output[k] for k in ("snapshot_id", "manifest_sha256"))
            or pair["data"]["snapshot_id"] != output["data_snapshot_id"]
            or digest(research["tables"]) != output.get("pins_sha256")
        ):
            raise ValueError("Source differs from the signed accepted candidate output")
        if same and cache.get("accepted_outputs") != pair:
            raise ValueError("Accepted source Data/Research pins changed")
    return pair


def _verifier_preflight(plan, *, historical_observation=False):
    from build_staging_candidate import relevant

    old = plan.get("verifier_revision", "")
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", old):
        raise ValueError("A pinned verifier revision is required")
    revision = checked("git", "-C", str(ROOT), "rev-parse", "HEAD").strip()
    if historical_observation:
        checked("git", "-C", str(ROOT), "merge-base", "--is-ancestor", old, revision)
        changed = checked(
            "git", "-C", str(ROOT), "diff", "--no-renames", "--name-only", "-z", old, revision, "--"
        )
        if any(relevant(name) for name in changed.split("\0") if name):
            raise ValueError("Historical verifier executable/configuration closure changed")
    elif revision != old:
        raise ValueError("Actual protected verifier differs from its pinned revision")
    changed = checked(
        "git", "-C", str(ROOT), "diff", "--no-renames", "--name-only", "-z", "HEAD", "--"
    )
    untracked = checked("git", "-C", str(ROOT), "ls-files", "--others", "--exclude-standard", "-z")
    if any(relevant(name) for name in (changed + "\0" + untracked).split("\0") if name):
        raise ValueError("Protected verifier has unpinned executable/configuration files")
    return revision


def source_preflight(plan, paths, values, *, historical_observation=False):
    """Validate host-readable transport before any native graph can be admitted."""
    if plan.get("required_scenarios"):
        raise ValueError("Unsupported scenario-specific proof; no compute admission")
    consumer = plan["consumer"]
    for key in (
        "dataset_version",
        "indicator_set_version",
        "derived_indicator_set_version",
        "contour_id",
        "timeframe",
    ):
        if not isinstance(consumer.get(key), str) or not consumer[key].strip():
            raise ValueError(f"Consumer requires a nonempty {key}")
    for key in ("instrument_ids", "price_columns", "indicator_columns", "derived_columns"):
        items = consumer.get(key)
        if (
            not isinstance(items, list)
            or not items
            or any(not isinstance(item, str) or not item.strip() for item in items)
            or len(items) != len(set(items))
        ):
            raise ValueError(f"Consumer requires explicit populated {key}")
    if consumer["timeframe"] not in plan["scope"]["timeframes"] or not set(
        consumer["instrument_ids"]
    ).issubset(plan["scope"]["instrument_ids"]):
        raise ValueError("Consumer slice lies outside the protected source scope")
    if values.get(PREFIX + "DATASET_VERSION") != consumer["dataset_version"]:
        raise ValueError("Runtime dataset differs from the protected consumer")
    source = plan.get("source", {})
    if source.get("fixture"):
        from trading_advisor_3000.product_plane.common.staging_test_fixture import (
            validate_branch_fixture,
        )

        if historical_observation:
            raise ValueError("Synthetic fixture proof cannot reattest candidate acceptance")
        fixture = validate_branch_fixture(plan, fixture_root=paths["CONTROL_ROOT"] / "fixture")
        if (
            consumer["dataset_version"] != fixture["dataset_version"]
            or set(consumer["instrument_ids"]) != set(fixture["instrument_ids"])
            or set(plan["scope"]["instrument_ids"]) != set(fixture["instrument_ids"])
            or set(plan["scope"]["timeframes"]) != set(fixture["timeframes"])
        ):
            raise ValueError("Consumer/scope must cover the declared miniature fixture domain")
        if any(path.name != ".ta3000-staging.json" for path in paths["DATA_ROOT"].iterdir()):
            raise ValueError(
                "Fixture run requires fresh isolated outputs; retained attempts stay spent"
            )
        _verifier_preflight(plan)
        return fixture
    if not re.fullmatch(r"[0-9a-f]{32}", source.get("snapshot_id", "")) or not re.fullmatch(
        r"[0-9a-f]{64}", source.get("manifest_sha256", "")
    ):
        raise ValueError("Source requires the exact accepted snapshot identity/hash")
    logical = PurePosixPath(source.get("research_manifest_path", ""))
    if ".." in logical.parts or not logical.is_relative_to(PurePosixPath(plan["data_root"])):
        raise ValueError("Source manifest must lie within the staged logical data root")
    host = paths["DATA_ROOT"].joinpath(*logical.relative_to(plan["data_root"]).parts).resolve()
    if not host.is_relative_to(paths["DATA_ROOT"]):
        raise ValueError("Source manifest escapes the staged host root")
    document = _read_manifest(host)
    if any(source.get(key) != document.get(key) for key in ("snapshot_id", "manifest_sha256")):
        raise ValueError("Staged source manifest differs from its protected identity")
    if document["dataset_version"] != consumer["dataset_version"]:
        raise ValueError("Seed dataset differs from the protected consumer/runtime")
    _verifier_preflight(plan, historical_observation=historical_observation)
    seed = json.loads((paths["DATA_ROOT"] / ".staging-seed.json").read_text("utf-8"))
    if seed.get("status") != "PASS" or (
        plan.get("seed_receipt_sha256") and digest(seed) != plan["seed_receipt_sha256"]
    ):
        raise ValueError("Staging seed receipt is unavailable or changed")
    initial = seed.get("manifests", {}).get(source["snapshot_id"]) == source["manifest_sha256"]
    if not historical_observation and (plan.get("mode") == "reference" or not initial):
        _accepted_source(plan, paths, document)
    return document


def signer_preflight(args, control):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    key = Ed25519PrivateKey.from_private_bytes(args.private_key.read_bytes())
    public = key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    keys = json.loads((control / "operator-keyring.json").read_text("utf-8"))["keys"]
    if base64.b64decode(keys.get(args.key_id, ""), validate=True) != public:
        raise ValueError("Private signing key does not match the protected public key ID")
    admitted = json.loads((ROOT / "configs/staging-acceptance-keys.v1.json").read_text("utf-8"))[
        "keys"
    ]
    if base64.b64decode(admitted.get(args.key_id, ""), validate=True) != public:
        raise ValueError("Signing key is not admitted by the protected verifier keyring")


def prepare(args, *, historical_observation=False):
    plan = load_plan(args.plan)
    if plan["stage"] != args.stage:
        raise ValueError("Requested stage differs from the protected plan")
    schedule = plan.get("source", {}).get("native_schedule")
    recipe = schedule.get("run_config") if isinstance(schedule, dict) else None
    if not historical_observation and (
        not isinstance(recipe, dict) or not isinstance(recipe.get("ops"), dict) or not recipe["ops"]
    ):
        raise ValueError("Both stages require the captured native run config before admission")
    if historical_observation and (
        args.stage != "acceptance" or plan.get("mode", "candidate") != "candidate"
    ):
        raise ValueError("Read-only reattestation requires an original candidate acceptance plan")
    if not historical_observation and not any(
        plan.get(key) for key in ("execution_window", "daily_execution_window_utc")
    ):
        raise ValueError("Compute requires a protected absolute or daily execution window")
    values = read_environment(args.env_file)
    if plan.get("required_scenarios"):
        raise ValueError("Unsupported scenario-specific proof; no compute admission")
    paths = {
        key: host_path(values[PREFIX + key])
        for key in (
            "DATA_ROOT",
            "CONTROL_ROOT",
            "QUOTA_ROOT",
            "CANDIDATE_WORKTREE",
            "VERIFIER_WORKTREE",
            "HOST_STATE",
        )
    }
    if paths["VERIFIER_WORKTREE"] != ROOT:
        raise ValueError("Supervisor must run from the protected verifier worktree")
    protected = paths["CONTROL_ROOT"]
    if (
        args.plan.resolve() != protected / "plan.json"
        or args.execution_manifest.resolve() != protected / "execution-manifest.json"
    ):
        raise ValueError("Plan and execution manifest must be the mounted protected files")
    for key in ("DATA_ROOT", "CONTROL_ROOT", "QUOTA_ROOT", "HOST_STATE"):
        for other in paths:
            if key != other and overlaps(paths[key], paths[other]):
                raise ValueError("Host staging roots must be independent of data and code")
    keys = [args.private_key]
    operator_key = os.environ.get("TA3000_STAGING_OPERATOR_PRIVATE_KEY")
    if operator_key:
        keys.append(Path(operator_key))
    if any(key and key.resolve().is_relative_to(path) for key in keys for path in paths.values()):
        raise ValueError("Private signing key must remain outside every container mount")
    reference = plan.get("mode", "candidate") == "reference"
    if reference and not getattr(args, "prepare_only", False):
        if not args.authorization_id or not args.reason:
            raise ValueError("Reference activation requires explicit authorization ID and reason")
        key = Path(os.environ.get("TA3000_STAGING_OPERATOR_PRIVATE_KEY", ""))
        if not key.is_file() or len(key.read_bytes()) != 32:
            raise ValueError("Reference activation requires its host-only operator key")
        signer_preflight(
            argparse.Namespace(
                private_key=key, key_id=os.environ.get("TA3000_STAGING_OPERATOR_KEY_ID", "")
            ),
            protected,
        )
    if not getattr(args, "prepare_only", False) and args.stage == "acceptance" and not reference:
        if not args.private_key or len(args.private_key.read_bytes()) != 32:
            raise ValueError("Acceptance requires a valid host-only Ed25519 signing key")
        if not args.key_id or not args.output or args.output.exists():
            raise ValueError("Acceptance requires a key ID and a new receipt output path")
        if not args.output.parent.is_dir() or any(
            args.output.resolve().is_relative_to(paths[key]) for key in ("DATA_ROOT", "QUOTA_ROOT")
        ):
            raise ValueError("Receipt output requires an existing host-only directory")
        signer_preflight(args, protected)
    for name in ("MAX_GROWTH_BYTES", "MIN_FREE_BYTES", "SCRATCH_BYTES"):
        if int(values[PREFIX + name]) <= 0:
            raise ValueError("Positive growth/free-space/scratch caps are required")
    marker = json.loads((protected / "root-marker.json").read_text("utf-8"))
    if marker != {
        "schema_version": "staging_root.v1",
        "plan_path": "/staging-control/plan.json",
        "plan_digest": digest(plan),
    }:
        raise ValueError("Protected staging root marker differs from the plan")
    manifest = json.loads(args.execution_manifest.read_text("utf-8"))
    if (
        verify_deployed_files(paths["CANDIDATE_WORKTREE"], manifest)
        != plan["candidate"]["executable_sha256"]
    ):
        raise ValueError("Frozen candidate files differ from the execution manifest")
    source_document = source_preflight(
        plan, paths, values, historical_observation=historical_observation
    )
    if plan.get("source", {}).get("fixture") and getattr(args, "output", None):
        if (
            args.output.exists()
            or not args.output.parent.is_dir()
            or any(
                args.output.resolve().is_relative_to(paths[key])
                for key in ("DATA_ROOT", "CONTROL_ROOT", "QUOTA_ROOT")
            )
        ):
            raise ValueError("Branch report requires a new host-only output path")
    production = inspect_container("ta3000-dagster-webserver")
    for mount in production["Mounts"]:
        if (
            mount["Type"] == "bind"
            and getattr(args, "output", None)
            and overlaps(args.output.resolve(), host_path(mount["Source"]))
        ):
            raise ValueError("Receipt output overlaps a production mount")
        if mount["Type"] == "bind" and any(
            overlaps(path, host_path(mount["Source"]))
            for path in (
                paths["DATA_ROOT"],
                paths["CONTROL_ROOT"],
                paths["QUOTA_ROOT"],
                paths["HOST_STATE"],
            )
        ):
            raise ValueError("Staging host roots overlap an actual production mount")
    port = production["NetworkSettings"]["Ports"]["3000/tcp"][0]["HostPort"]
    production_url = f"http://127.0.0.1:{int(port)}/graphql"
    production_busy = bool(graphql_runs(production_url))
    slug = values.get(PREFIX + "TASK_SLUG", "")
    if args.stage == "branch" and not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,40}", slug):
        raise ValueError("A safe branch task slug is required")
    project = "ta3000-acceptance" if args.stage == "acceptance" else "ta3000-branch-" + slug
    compose = (
        ROOT / "deployment/docker/dagster-staging" / f"docker-compose.dagster-{args.stage}.yml"
    )
    command = [
        "docker",
        "compose",
        "--env-file",
        str(args.env_file),
        "-p",
        project,
        "-f",
        str(compose),
    ]
    environment = {**os.environ, **values}
    for key in ("COMPOSE_PROFILES", "COMPOSE_FILE", "COMPOSE_PROJECT_NAME"):
        environment.pop(key, None)
    config = json.loads(checked(*command, "config", "--format", "json", env=environment))
    validate_config(config, paths, plan)
    hardware = json.loads(checked("docker", "info", "--format", "{{json .}}"))
    validate_resources(config, hardware, plan["stage"])
    free = shutil.disk_usage(paths["DATA_ROOT"]).free
    if free < int(values[PREFIX + "MIN_FREE_BYTES"]) + int(values[PREFIX + "MAX_GROWTH_BYTES"]):
        raise ValueError("Insufficient host disk reserve for the declared growth cap")
    deadline = datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00"))
    if historical_observation:
        deadline = datetime.now(UTC) + timedelta(seconds=180)
    elif datetime.now(UTC) >= deadline:
        raise ValueError("Protected acceptance deadline already expired")
    return dict(
        plan=plan,
        paths=paths,
        project=project,
        command=command,
        env=environment,
        config=config,
        values=values,
        deadline=deadline,
        data_bytes=data_root_file_bytes(paths["DATA_ROOT"]),
        production_url=production_url,
        production_mounts=[
            host_path(mount["Source"]) for mount in production["Mounts"] if mount["Type"] == "bind"
        ],
        host_identity_checked=True,
        production_busy=production_busy,
        production_container_id=production.get("Id"),
        production_runtime_identity=_production_identity(production),
        reference=reference,
        source_document=source_document,
        historical_observation=historical_observation,
        observer_verifier_revision=(
            checked("git", "-C", str(ROOT), "rev-parse", "HEAD").strip()
            if historical_observation
            else plan.get("verifier_revision")
        ),
    )


def prepare_reattest(args):
    """Inspect retained evidence without granting a new compute lease or editing its plan."""
    prepared = prepare(args, historical_observation=True)
    original = prepared["paths"]["CONTROL_ROOT"] / "original-receipt.json"
    if not original.is_file() or original.resolve() != original:
        raise ValueError("Reattestation requires the canonical protected original receipt")
    return prepared


def _spark_memory_bytes(value):
    match = re.fullmatch(r"([1-9][0-9]*)([gmk])", str(value).strip().lower())
    if not match:
        raise ValueError("Effective Spark driver memory must be positive g/m/k bytes")
    return int(match[1]) * 1024 ** {"g": 3, "m": 2, "k": 1}[match[2]]


def validate_resources(config, hardware, stage):
    """Conservative admission floors; reserves do not prove measured process RSS."""
    gib = 1024**3
    services = config["services"]
    for service in services.values():
        if (
            int(service["mem_limit"]) > hardware["MemTotal"]
            or float(service["cpus"]) > hardware["NCPU"]
        ):
            raise ValueError("Staging caps exceed Docker hardware resources")
    if (
        sum(int(service["mem_limit"]) for service in services.values()) + 2 * gib
        > hardware["MemTotal"]
    ):
        raise ValueError("Staging total memory caps plus host reserve exceed Docker hardware")
    if stage not in {"branch", "acceptance"}:
        raise ValueError("Unknown staging resource owner")
    heavy = "webserver" if stage == "branch" else "daemon"
    for name, service in services.items():
        memory = int(service["mem_limit"])
        if int(service.get("memswap_limit", 0)) != memory:
            raise ValueError("Staging memory and swap caps must match; extra swap is forbidden")
        environment = service.get("environment", {})
        if environment.get("DAGSTER_MULTIPROCESS_EXECUTOR_MAX_CONCURRENT") != "1":
            raise ValueError("Staging requires one native Dagster step at a time")
        heap = _spark_memory_bytes(environment.get("TA3000_SPARK_DRIVER_MEMORY"))
        scratch = service.get("tmpfs", [])
        match = (
            re.fullmatch(r"/tmp:size=([1-9][0-9]*),mode=1777,exec,nosuid,nodev", scratch[0])
            if isinstance(scratch, list) and len(scratch) == 1
            else None
        )
        if not match:
            raise ValueError("Scratch must be one byte-bounded /tmp tmpfs")
        scratch_bytes = int(match[1])
        required = scratch_bytes + (heap + 2 * gib if name == heavy else gib)
        if required > memory:
            raise ValueError(f"Staging {name} memory policy reserve exceeds its container cap")


def validate_config(config, paths, plan):
    import yaml

    profile = (
        "dagster-branch-instance.yaml"
        if plan["stage"] == "branch"
        else "dagster-staging-instance.yaml"
    )
    instance = yaml.safe_load(
        (ROOT / "deployment/docker/dagster-staging" / profile).read_text("utf-8")
    )
    coordinator = (
        "StagingDirectRunCoordinator" if plan["stage"] == "branch" else "StagingRunCoordinator"
    )
    for owner, implementation in (
        ("run_launcher", "StagingRunLauncher"),
        ("run_coordinator", coordinator),
    ):
        if (
            instance.get(owner, {}).get("module")
            != "trading_advisor_3000.product_plane.common.staging_execution"
            or instance[owner].get("class") != implementation
        ):
            raise ValueError("Staging must use the protected native launcher and coordinator")
    services = {"webserver"} if plan["stage"] == "branch" else {"webserver", "daemon"}
    if set(config["services"]) != services:
        raise ValueError("Only the two known staging services are permitted")
    allowed = {
        "/workspace": paths["CANDIDATE_WORKTREE"],
        "/staging-verifier": paths["VERIFIER_WORKTREE"],
        "/staging-control": paths["CONTROL_ROOT"],
        "/staging-quota": paths["QUOTA_ROOT"],
        "/staging-host-state": paths["HOST_STATE"],
        plan["data_root"]: paths["DATA_ROOT"],
        plan["data_root"] + "/.ta3000-staging.json": paths["CONTROL_ROOT"] / "root-marker.json",
        "/opt/dagster/dagster_home/dagster.yaml": ROOT
        / "deployment/docker/dagster-staging"
        / profile,
    }
    for relative in (
        "common/staging_execution.py",
        "common/staging_contracts.py",
        "common/staging_source_scope.py",
        "common/staging_test_fixture.py",
        "common/staging_route_proof.py",
        "contracts/schemas/staging_acceptance.v1.json",
    ):
        suffix = "src/trading_advisor_3000/product_plane/" + relative
        allowed["/workspace/" + suffix] = paths["VERIFIER_WORKTREE"] / suffix
    for service in config["services"].values():
        if (
            service.get("privileged")
            or service.get("devices")
            or service.get("network_mode") == "host"
        ):
            raise ValueError("Privileged staging configuration is forbidden")
        if service["image"] != plan["candidate"]["image_digest"]:
            raise ValueError("Compose image must be the pinned actual image ID")
        if (
            not service.get("mem_limit")
            or float(service.get("cpus", 0)) <= 0
            or not service.get("tmpfs")
        ):
            raise ValueError("Staging requires CPU, memory, and bounded scratch")
        scratch = service["tmpfs"]
        if (
            not isinstance(scratch, list)
            or len(scratch) != 1
            or not re.fullmatch(r"/tmp:size=[1-9][0-9]*,mode=1777,exec,nosuid,nodev", scratch[0])
        ):
            raise ValueError("Scratch must be one byte-bounded /tmp tmpfs")
        seen = set()
        for mount in service["volumes"]:
            target = mount["target"]
            if mount["type"] == "volume" and target == "/opt/dagster/dagster_home":
                continue
            if target not in allowed or host_path(mount["source"]) != allowed[target]:
                raise ValueError("Compose contains an unapproved host mount")
            if target != plan["data_root"] and not mount.get("read_only"):
                raise ValueError("Protected code/control mounts must be read-only")
            seen.add(target)
        if seen != set(allowed):
            raise ValueError("Compose is missing protected isolation mounts")


def verify_runtime(container, prepared, *, allow_stopped=False):
    plan = prepared["plan"]
    labels = container["Config"].get("Labels") or {}
    service = labels.get("com.docker.compose.service")
    if labels.get("com.docker.compose.project") != prepared["project"] or service not in {
        "webserver",
        "daemon",
    }:
        raise ValueError("Unexpected staging container identity")
    if container["Image"] != plan["candidate"]["image_digest"]:
        raise ValueError("Actual container image differs from the plan")
    if not allow_stopped and not container["State"]["Running"]:
        raise ValueError("Candidate container is no longer running")
    expected = prepared["config"]["services"][service]
    mounts = {item["Destination"]: item for item in container["Mounts"]}
    home = mounts.get("/opt/dagster/dagster_home", {})
    if (
        home.get("Type") != "volume"
        or home.get("Name") != prepared["config"]["volumes"]["staging-home"]["name"]
    ):
        raise ValueError("Actual Dagster HOME is not the project's independent volume")
    expected_binds = {item["target"] for item in expected["volumes"] if item["type"] == "bind"}
    if {
        item["Destination"] for item in container["Mounts"] if item["Type"] == "bind"
    } != expected_binds:
        raise ValueError("Actual container has unexpected bind mounts")
    for item in expected["volumes"]:
        if item["type"] != "bind":
            continue
        actual = mounts.get(item["target"], {})
        if host_path(actual.get("Source", ".")) != host_path(item["source"]) or bool(
            actual.get("RW")
        ) == bool(item.get("read_only")):
            raise ValueError("Actual container mounts differ from approved isolation")
    host = container["HostConfig"]
    scratch = expected["tmpfs"][0].split(":", 1)[1]
    if (
        host.get("Privileged")
        or host.get("Memory") != int(expected["mem_limit"])
        or host.get("MemorySwap") != int(expected["memswap_limit"])
        or host.get("NanoCpus") != int(float(expected["cpus"]) * 1_000_000_000)
        or set(host.get("Tmpfs", {}).get("/tmp", "").split(",")) != set(scratch.split(","))
    ):
        raise ValueError("Actual container resource/isolation settings are missing")


def observe_protected(prepared, web, *, timeout, mode="observe", root_run_id=None):
    """The writer container never executes its own acceptance verifier."""
    if prepared.get("historical_observation") and mode != "reattest":
        raise ValueError("Historical observation permits read-only reattestation only")
    timeout = min(timeout, 180)
    plan, paths, project = prepared["plan"], prepared["paths"], prepared["project"]
    name = project + "-collector-" + uuid4().hex[:12]
    home = next(
        item["Name"] for item in web["Mounts"] if item["Destination"] == "/opt/dagster/dagster_home"
    )
    memory = min(int(prepared["config"]["services"]["webserver"]["mem_limit"]), 2 * 1024**3)
    scratch = min(int(prepared["values"][PREFIX + "SCRATCH_BYTES"]), 256 * 1024**2)
    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        name,
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--memory",
        str(memory),
        "--cpus",
        "1",
        "--tmpfs",
        f"/tmp:size={scratch},mode=1777",
        "--label",
        f"com.docker.compose.project={project}",
        "--label",
        "com.docker.compose.service=collector",
        "--label",
        "ta3000.staging.owner=protected-observer",
        "--env",
        "DAGSTER_HOME=/opt/dagster/dagster_home",
        "--env",
        "TA3000_STAGING_PLAN=/staging-control/plan.json",
        "--env",
        "NUMBA_CACHE_DIR=/tmp/numba",
        "--env",
        "PYTHONDONTWRITEBYTECODE=1",
    ]
    bindings = {
        "/workspace": paths["CANDIDATE_WORKTREE"],
        "/staging-verifier": paths["VERIFIER_WORKTREE"],
        "/staging-control": paths["CONTROL_ROOT"],
        "/staging-quota": paths["QUOTA_ROOT"],
        plan["data_root"]: paths["DATA_ROOT"],
        plan["data_root"] + "/.ta3000-staging.json": paths["CONTROL_ROOT"] / "root-marker.json",
        "/opt/dagster/dagster_home/dagster.yaml": ROOT
        / "deployment/docker/dagster-staging"
        / (
            "dagster-branch-instance.yaml"
            if plan.get("stage") == "branch"
            else "dagster-staging-instance.yaml"
        ),
    }
    for destination, source in bindings.items():
        command += ["--mount", f"type=bind,source={source},target={destination},readonly"]
    command += [
        "--mount",
        f"type=volume,source={home},target=/opt/dagster/dagster_home,readonly",
        plan["candidate"]["image_digest"],
        "python",
        "-I",
        "/staging-verifier/scripts/collect_staging_acceptance.py",
        mode,
        "--plan",
        "/staging-control/plan.json",
    ]
    if root_run_id is not None:
        command += ["--root-run-id", root_run_id]
    if mode == "reattest":
        command += ["--original-receipt", "/staging-control/original-receipt.json"]
    try:
        return run(*command, timeout=timeout)
    finally:
        leftover = run("docker", "inspect", name)
        if leftover.returncode == 0:
            stop_owned(project, json.loads(leftover.stdout))


def runtime_services(prepared, *, include_stopped=False):
    containers = [
        item for item in owned(prepared["project"]) if include_stopped or item["State"]["Running"]
    ]
    for container in containers:
        if include_stopped:
            verify_runtime(container, prepared, allow_stopped=True)
        else:
            verify_runtime(container, prepared)
    web = [
        item
        for item in containers
        if item["Config"]["Labels"]["com.docker.compose.service"] == "webserver"
    ]
    if len(web) != 1:
        raise ValueError("Expected one verified staging webserver")
    return web[0], containers


def runtime_url(prepared, web):
    if prepared["plan"]["stage"] == "branch":
        return branch_control_target(prepared, web)
    port = web["NetworkSettings"]["Ports"]["3000/tcp"][0]["HostPort"]
    url = f"http://127.0.0.1:{int(port)}/graphql"
    if url == prepared["production_url"]:
        raise ValueError("Staging endpoint cannot alias production")
    return url


def daemon_state(prepared, enabled):
    if prepared["plan"]["stage"] == "branch":
        if enabled:
            raise ValueError("Direct branch staging never starts a schedule daemon")
        return
    if enabled:
        checked(
            *prepared["command"],
            "up",
            "-d",
            "--no-deps",
            "--no-build",
            "--pull",
            "never",
            "daemon",
            env=prepared["env"],
            timeout=60,
        )
    else:
        stop_owned(
            prepared["project"],
            [
                item
                for item in owned(prepared["project"])
                if item["State"]["Running"]
                and item["Config"]["Labels"].get("com.docker.compose.service") == "daemon"
            ],
        )


def watch_limits(prepared):
    if datetime.now(UTC) >= prepared["deadline"]:
        raise ValueError("Staging deadline/runtime cap exceeded")
    free = shutil.disk_usage(prepared["paths"]["DATA_ROOT"]).free
    if free < int(prepared["values"][PREFIX + "MIN_FREE_BYTES"]):
        raise ValueError("Host staging disk reserve cap exceeded")
    if data_root_file_bytes(prepared["paths"]["DATA_ROOT"]) - prepared["data_bytes"] > int(
        prepared["values"][PREFIX + "MAX_GROWTH_BYTES"]
    ):
        raise ValueError("Own staging data file growth cap exceeded")


def observed_result(prepared, web, mode, root_run_id=None):
    timeout = max(1, min(180, (prepared["deadline"] - datetime.now(UTC)).total_seconds()))
    result = observe_protected(prepared, web, mode=mode, root_run_id=root_run_id, timeout=timeout)
    if result.returncode:
        raise ValueError("Protected read-only preflight/route observer rejected candidate")
    return json.loads(result.stdout)


def launch_branch_fixture(prepared, web):
    """Single manual native submission; a transport failure is terminal, never retried."""
    from launch_moex_baseline_staging_run import _launch_mutation, _post_graphql

    plan = prepared["plan"]
    if (
        plan["stage"] != "branch"
        or plan["root_job"] != "research_data_prep_job"
        or not plan.get("source", {}).get("fixture")
        or plan["require_scheduled"]
    ):
        raise ValueError("Automatic fixture launch requires the manual Research branch target")
    native = plan["source"]["native_schedule"]
    run_id = str(native.get("manual_run_id") or "")
    if not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", run_id):
        raise ValueError("Protected Research recipe requires its fixed manual run ID")
    result = _post_graphql(
        url=runtime_url(prepared, web),
        query=_launch_mutation(),
        timeout_sec=30,
        variables={
            "executionParams": {
                "selector": {
                    "repositoryLocationName": (
                        "trading_advisor_3000.dagster_defs:product_plane_definitions"
                    ),
                    "repositoryName": "__repository__",
                    "jobName": plan["root_job"],
                },
                "runConfigData": native["run_config"],
                "executionMetadata": {
                    "runId": run_id,
                    "tags": [
                        {"key": "ta3000/staging_plan_sha256", "value": digest(plan)},
                        {
                            "key": "ta3000/candidate_head_sha",
                            "value": plan["candidate"]["head_sha"],
                        },
                    ],
                },
            }
        },
    )
    launched = result.get("data", {}).get("launchRun", {})
    if (
        result.get("errors")
        or launched.get("__typename") != "LaunchRunSuccess"
        or launched.get("run", {}).get("runId") != run_id
    ):
        raise ValueError("Native fixture launch failed; no automatic retry")


def active_bindings(runs):
    return sorted((item["runId"], item["status"], item["pipelineName"]) for item in runs)


def supervise(args, prepared):
    if prepared.get("historical_observation"):
        raise ValueError("Read-only reattestation cannot activate a compute supervisor")
    project, plan = prepared["project"], prepared["plan"]
    reference = prepared.get("reference", False)
    state = prepared["paths"]["HOST_STATE"]
    state.mkdir(parents=True, exist_ok=True)
    session = state / (project + "-supervisor.lock")
    session_stream = session.open("x", encoding="utf-8")
    lease, previous_phase, started, success, began = None, None, False, False, None
    completed_path = state / (project + "-reference-cycles.json")
    completed = {}
    authorization = None
    branch_submitted = False
    try:
        session_stream.write(json.dumps({"pid": os.getpid(), "plan_sha256": digest(plan)}))
        session_stream.flush()
        existing = owned(project)
        live = [item for item in existing if item["State"]["Running"]]
        if live:
            previous_web = next(
                (
                    item
                    for item in live
                    if item["Config"]["Labels"].get("com.docker.compose.service") == "webserver"
                ),
                None,
            )
            if previous_web is None or previous_web["Id"] == prepared.get(
                "production_container_id"
            ):
                raise ValueError("Existing staging identity is not safe for an owned transition")
            port = previous_web["NetworkSettings"]["Ports"]["3000/tcp"][0]["HostPort"]
            previous_url = f"http://127.0.0.1:{int(port)}/graphql"
            if previous_url == prepared["production_url"] or graphql_runs(previous_url):
                raise ValueError("Existing staging graph requires completion/metadata recovery")
            stop_owned(project, live)
        started = True
        checked(
            *prepared["command"],
            "up",
            "-d",
            "--no-build",
            "--pull",
            "never",
            "webserver",
            env=prepared["env"],
            timeout=60,
        )
        web, _ = runtime_services(prepared)
        boot = time.monotonic()
        while web["State"].get("Health", {}).get("Status") != "healthy":
            watch_limits(prepared)
            if time.monotonic() - boot > 120:
                raise ValueError("Staging webserver failed bounded startup")
            time.sleep(2)
            web = inspect_container(web["Id"])
            verify_runtime(web, prepared)
        watch_limits(prepared)
        url = runtime_url(prepared, web)
        prepared["staging_url"] = url
        if any(
            run_tags(item).get("ta3000/staging_plan_sha256") != digest(plan)
            for item in graphql_runs(url)
        ):
            raise ValueError(
                "Stale queued/running staging graph requires explicit metadata recovery"
            )
        if observed_result(prepared, web, "preflight").get("status") != "PREFLIGHT_PASS":
            raise ValueError("Native source/consumer preflight did not pass")
        begin_native_observation(prepared, url)
        if reference:
            from trading_advisor_3000.product_plane.common.staging_execution import (
                _key,
                authorize_reference_mode,
            )

            authorize_reference_mode(
                args.plan,
                authorization_id=args.authorization_id,
                reason=args.reason,
                repository_root=prepared["paths"]["CANDIDATE_WORKTREE"],
                runtime_image_digest=web["Image"],
                quota_home=prepared["paths"]["QUOTA_ROOT"],
            )
            authorization = _key(
                "staging-reference-auth-v1", plan["task_id"], args.authorization_id
            )
            cache_header = {
                "accepted_main_sha": plan["reference"]["main_sha"],
                "accepted_receipt": plan["reference"]["receipt"],
                "accepted_outputs": _source_pair(prepared["paths"], prepared["source_document"]),
            }
            if completed_path.exists():
                checkpoint = _read_reference_cache(completed_path)
                if (
                    checkpoint.get("accepted_main_sha") == cache_header["accepted_main_sha"]
                    and checkpoint.get("accepted_receipt") == cache_header["accepted_receipt"]
                ):
                    completed = checkpoint["completed"]
                    cache_header["accepted_outputs"] = checkpoint["accepted_outputs"]
            _write_reference_cache(
                completed_path,
                {
                    **cache_header,
                    "plan_sha256": digest(plan),
                    "authorization": authorization,
                    "completed": completed,
                },
            )
        if plan["stage"] == "acceptance":
            daemon_state(prepared, True)
        while True:
            watch_limits(prepared)
            web, containers = runtime_services(prepared)
            url = runtime_url(prepared, web)
            check_native_failures(prepared, url)
            runs = graphql_runs(url, plan_digest=digest(plan))
            if reference:
                runs = [
                    item
                    for item in runs
                    if run_tags(item).get("ta3000/reference_authorization") == authorization
                ]
            roots = [
                item
                for item in runs
                if item["pipelineName"] == plan["root_job"] and item["runId"] not in completed
            ]
            if reference and len(roots) > 1:
                raise ValueError("Reference backlog requires explicit metadata recovery")
            root = roots[-1] if roots else None
            if root is not None:
                group = [
                    item
                    for item in runs
                    if item["runId"] == root["runId"]
                    or run_tags(item).get("ta3000/staging_root_run_id") == root["runId"]
                ]
            else:
                group = []
            actual = [
                item for item in group if item["status"] in {"STARTING", "STARTED", "CANCELING"}
            ]
            if actual and began is None:
                began = time.monotonic()
            if began is not None and time.monotonic() - began > plan["max_runtime_seconds"]:
                raise ValueError("Staging actual graph runtime cap exceeded")
            if any(item["status"] in FAILED for item in group):
                raise ValueError("Candidate/reference graph failed or was canceled")
            barrier = production_barrier(prepared, root, datetime.now(UTC))
            if not barrier and actual:
                raise ValueError(
                    "Unexpected production activity/window closure during candidate compute"
                )
            may_start = barrier and (root is not None or plan["stage"] == "branch")
            if may_start:
                if lease is None:
                    try:
                        lease = acquire_heavy(prepared)
                    except FileExistsError:
                        pass
                if lease is not None:
                    renew_candidate_permit(prepared, lease, root)
                    if (
                        plan["stage"] == "branch"
                        and plan.get("source", {}).get("fixture")
                        and root is None
                        and not branch_submitted
                    ):
                        branch_submitted = True
                        launch_branch_fixture(prepared, web)
            elif lease is not None and not actual:
                release_heavy(prepared, lease)
                lease = None
            phase = (
                "RUNNING"
                if actual
                else "WAITING_FOR_PRODUCTION"
                if not barrier
                else "QUEUED"
                if root
                else "WAITING_FOR_TRIGGER"
            )
            if phase != previous_phase:
                print(json.dumps({"status": phase, "project": project}), flush=True)
                previous_phase = phase
            complete = root is not None and {
                item["pipelineName"] for item in group if item["status"] == "SUCCESS"
            } >= set(plan["jobs"])
            inspect_reference = (
                reference and root is not None and root["status"] == "SUCCESS" and not actual
            )
            if complete or inspect_reference:
                if lease is not None:
                    revoke_permit(prepared, lease)
                daemon_state(prepared, False)
                before = graphql_runs(url, plan_digest=digest(plan))
                if any(item["status"] in {"STARTING", "STARTED", "CANCELING"} for item in before):
                    raise ValueError("Producer activity remained before protected observation")
                if reference:
                    observed = observed_result(prepared, web, "reference-cycle", root["runId"])
                    outcome = observed.get("status")
                    if outcome == "PENDING":
                        daemon_state(prepared, True)
                        time.sleep(10)
                        continue
                    if outcome not in {"PASS", "PASS-NOOP"}:
                        raise ValueError(
                            "Reference graph was blocked or incomplete: "
                            + str(observed.get("reason"))
                        )
                    completed[root["runId"]] = observed
                    _write_reference_cache(
                        completed_path,
                        {
                            **cache_header,
                            "plan_sha256": digest(plan),
                            "authorization": authorization,
                            "completed": completed,
                        },
                    )
                    began = None
                    if lease is not None:
                        release_heavy(prepared, lease)
                        lease = None
                    print(
                        json.dumps(
                            {
                                "status": outcome,
                                "root_run_id": root["runId"],
                                "reference_only": True,
                            }
                        ),
                        flush=True,
                    )
                    daemon_state(prepared, True)
                    continue
                if plan["stage"] == "branch" and plan.get("source", {}).get("fixture"):
                    payload = observed_result(prepared, web, "branch")
                    if payload.get("status") != "PASS" or payload.get("stage") != "branch":
                        raise ValueError("Short Research graph/consumer proof failed")
                    if args.output is not None:
                        with args.output.open("x", encoding="utf-8") as output:
                            json.dump(payload, output, sort_keys=True)
                    print(json.dumps(payload, sort_keys=True), flush=True)
                if plan["stage"] == "acceptance":
                    payload = observed_result(prepared, web, "observe")
                    after = graphql_runs(url, plan_digest=digest(plan))
                    if active_bindings(before) != active_bindings(after) or graphql_runs(
                        prepared["production_url"]
                    ):
                        raise ValueError("Live graph/production state changed before signing")
                    for item in containers:
                        if item["Config"]["Labels"]["com.docker.compose.service"] != "daemon":
                            verify_runtime(inspect_container(item["Id"]), prepared)
                    quota = json.loads(
                        (prepared["paths"]["QUOTA_ROOT"] / "storage.json").read_text("utf-8")
                    )
                    if quota.get("backend") != "postgres":
                        raise ValueError(
                            "Actual quota backend is not the protected PostgreSQL authority"
                        )
                    payload["runtime"]["isolation"] = {
                        "status": "PASS",
                        "project": project,
                        "mounts_sha256": digest(web["Mounts"]),
                        "home_volume": next(
                            item["Name"]
                            for item in web["Mounts"]
                            if item["Destination"] == "/opt/dagster/dagster_home"
                        ),
                        "quota_backend": quota["backend"],
                        "observer": "separate-readonly-container",
                    }
                    signer_preflight(args, prepared["paths"]["CONTROL_ROOT"])
                    envelope = sign_receipt(payload, args.private_key.read_bytes(), args.key_id)
                    with args.output.open("x", encoding="utf-8") as output:
                        output.write(json.dumps(envelope, indent=2))
                success = True
                print(json.dumps({"status": "PASS", "project": project}), flush=True)
                return
            time.sleep(10)
    except BaseException as exc:
        if reference and authorization is not None:
            from trading_advisor_3000.product_plane.common.staging_execution import (
                halt_reference_graph,
            )

            halt_reference_graph(
                args.plan, reason=str(exc), quota_home=prepared["paths"]["QUOTA_ROOT"]
            )
        raise
    finally:
        try:
            if lease is not None:
                release_heavy(prepared, lease)
            cleanup = owned(project) if started else []
            if success and plan["stage"] == "acceptance":
                cleanup = [
                    item
                    for item in cleanup
                    if item["Config"]["Labels"].get("com.docker.compose.service") == "daemon"
                ]
            stop_owned(project, cleanup)
        finally:
            session_stream.close()
            session.unlink()


def candidate_action(args):
    """Choose once; untrusted or inconclusive evidence never grants computation."""
    import reattest_staging_acceptance as reuse

    if args.stage != "acceptance":
        raise ValueError("Candidate dispatch requires acceptance; branch perf is optional")
    base = reuse.resolve(args.repo, args.base_ref)
    if reuse.resolve(ROOT, "HEAD") != base or reuse.git(
        ROOT, "status", "--porcelain", "--untracked-files=no"
    ):
        raise ValueError("Candidate dispatch requires clean protected current main")
    proposed = json.loads(args.candidate.read_text("utf-8"))
    candidate = reuse.checked_candidate(args.repo, proposed.get("candidate", proposed), base)
    plan = load_plan(args.plan)
    if plan.get("mode", "candidate") != "candidate" or plan["stage"] != "acceptance":
        raise ValueError("Candidate dispatch cannot activate reference or branch mode")
    if args.original_receipt is not None:
        original = json.loads(args.original_receipt.read_text("utf-8"))
        keys = json.loads(reuse.git(args.repo, "show", f"{base}:{reuse.KEYRING}"))["keys"]
        native = reuse.verify_acceptance_route(
            original,
            keys,
            now=datetime.fromisoformat(original["payload"]["issued_at_utc"].replace("Z", "+00:00")),
        )
        if "compute_origin" in native:
            raise ValueError("Candidate dispatch requires the original native receipt")
        if all(
            candidate[key] == native["candidate"][key]
            for key in ("executable_sha256", "config_sha256", "image_digest")
        ):
            if native["plan_sha256"] != digest(plan):
                raise ValueError("Unchanged artifact requires its original plan for reuse")
            return "REUSE"
    if plan["candidate"] != candidate:
        raise ValueError("A fresh acceptance plan must bind the new candidate")
    if plan["limits"]["acceptance"] != 1 or plan["limits"]["rebuild"] != 0:
        raise ValueError("Candidate dispatch permits one acceptance and zero rebuilds")
    return "RUN"


def dispatch_candidate(args):
    """One route, no retries or compute fallback after an observer failure."""
    import reattest_staging_acceptance as reuse

    action = candidate_action(args)
    if args.prepare_only:
        if action == "REUSE":
            prepare_reattest(args)
        else:
            prepare(args)
        print(
            json.dumps(
                {
                    "status": "PREPARED",
                    "action": action,
                    "compute_attempts": 0,
                    "native_consumer": "CONSUMER_NOT_CHECKED",
                }
            )
        )
    elif action == "REUSE":
        envelope = reuse.reattest(args)
        print(
            json.dumps(
                {"status": "REATTESTED", "compute_attempts": 0, "receipt_sha256": digest(envelope)}
            )
        )
    else:
        prepared = prepare(args)
        supervise(args, prepared)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("branch", "acceptance"), default="acceptance")
    parser.add_argument(
        "--candidate", type=Path, help="Select reuse or one acceptance automatically"
    )
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base-ref", default="origin/main")
    parser.add_argument("--original-receipt", type=Path)
    for field in ("env-file", "plan", "execution-manifest"):
        parser.add_argument("--" + field, type=Path, required=True)
    parser.add_argument("--private-key", type=Path)
    parser.add_argument("--key-id")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--authorization-id")
    parser.add_argument("--reason")
    args = parser.parse_args()
    try:
        if args.candidate is not None:
            dispatch_candidate(args)
            return 0
        if args.original_receipt is not None:
            raise ValueError("Original receipt requires automatic candidate dispatch")
        prepared = prepare(args)
        if args.prepare_only:
            print(
                json.dumps(
                    {
                        "status": "PREPARED",
                        "project": prepared["project"],
                        "plan_sha256": digest(prepared["plan"]),
                        "signer": "SIGNER_NOT_CHECKED",
                        "native_consumer": "CONSUMER_NOT_CHECKED",
                    }
                )
            )
        else:
            if (
                args.stage == "acceptance"
                and not prepared.get("reference")
                and not all((args.private_key, args.key_id, args.output))
            ):
                raise ValueError("Acceptance signing requires host-only key, key ID and output")
            supervise(args, prepared)
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({"status": "BLOCKED", "reason": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
