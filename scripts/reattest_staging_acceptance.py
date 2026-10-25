"""Reattest unchanged computation using the protected live read-only observer."""

from __future__ import annotations

import argparse
import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from build_staging_candidate import config_fingerprint, executable_fingerprint, git, resolve
from validate_staging_acceptance import KEYRING, ROOT

from trading_advisor_3000.product_plane.common.staging_contracts import (
    digest,
    load_plan,
    sign_receipt,
    validate_document,
)
from trading_advisor_3000.product_plane.common.staging_route_proof import (
    check_route,
    verify_acceptance_route,
)


def checked_candidate(repo: Path, candidate: dict, base_ref: str) -> dict:
    base, head = resolve(repo, base_ref), resolve(repo, candidate["head_sha"])
    expected = {
        "base_sha": base,
        "head_sha": head,
        "tree_sha": git(repo, "rev-parse", f"{head}^{{tree}}"),
        "executable_sha256": executable_fingerprint(repo, head),
        "config_sha256": config_fingerprint(repo, head),
    }
    if any(candidate.get(key) != value for key, value in expected.items()):
        raise ValueError("New candidate does not match the actual current Git binding")
    git(repo, "merge-base", "--is-ancestor", base, head)
    for branch in candidate.get("branch_heads", {}).values():
        git(repo, "merge-base", "--is-ancestor", resolve(repo, branch), head)
    return copy.deepcopy(candidate)


def reattested_payload(
    original: dict,
    keys: dict,
    candidate: dict,
    observation: dict,
    verifier: str,
    started: datetime,
    issued: datetime,
) -> dict:
    native = verify_acceptance_route(
        original,
        keys,
        now=datetime.fromisoformat(original["payload"]["issued_at_utc"].replace("Z", "+00:00")),
    )
    if "compute_origin" in native:
        raise ValueError("Use the original native receipt, not nested reattestations")
    if any(
        candidate[key] != native["candidate"][key]
        for key in ("executable_sha256", "config_sha256", "image_digest")
    ):
        raise ValueError(
            "Changed executable/configuration/image requires fresh bounded computation"
        )
    if (
        observation.get("status") != "APPLICABLE"
        or observation.get("compute_attempts") != 0
        or observation.get("original_receipt_sha256") != digest(original)
        or observation.get("source") != native["source"]
        or observation.get("outputs") != native["outputs"]
        or observation.get("metadata_observation", {}).get("observation_complete") is not True
    ):
        raise ValueError("Protected live applicability observation is missing or inconsistent")
    read = datetime.fromisoformat(observation["consumer"]["read_at_utc"].replace("Z", "+00:00"))
    if read.tzinfo is None or not started <= read <= issued:
        raise ValueError("Fresh independent consumer read is outside this observation")
    value = copy.deepcopy(native)
    value.update(
        candidate=candidate,
        issued_at_utc=issued.isoformat(),
        expires_at_utc=(issued + timedelta(hours=24)).isoformat(),
        verifier_revision=verifier,
        consumer=observation["consumer"],
        compute_origin={"receipt_sha256": digest(original), "signed_receipt": original},
    )
    value["runtime"]["reattestation_observation"] = observation["metadata_observation"]
    check_route(value, compute_candidate=native["candidate"])
    return validate_document("receipt", value)


def reattest(args) -> dict:
    import run_staging_control as control

    base = resolve(args.repo, args.base_ref)
    if resolve(ROOT, "HEAD") != base or git(ROOT, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Reattestation must execute from the clean protected current main")
    keys = json.loads(git(args.repo, "show", f"{base}:{KEYRING}"))["keys"]
    proposed = json.loads(args.candidate.read_text("utf-8"))
    if not isinstance(proposed, dict):
        raise ValueError("Candidate must be a Git-bound object")
    candidate = checked_candidate(args.repo, proposed.get("candidate", proposed), args.base_ref)
    original = json.loads(args.original_receipt.read_text("utf-8"))
    prepared = control.prepare_reattest(args)
    if (
        args.original_receipt.resolve()
        != prepared["paths"]["CONTROL_ROOT"] / "original-receipt.json"
    ):
        raise ValueError("Original receipt must be the fixed protected read-only control file")
    if prepared["plan"].get("mode", "candidate") != "candidate":
        raise ValueError("Reference operation cannot issue candidate reattestation")
    # Reject a changed closure before starting even a bounded read-only observer.
    native = verify_acceptance_route(
        original,
        keys,
        now=datetime.fromisoformat(original["payload"]["issued_at_utc"].replace("Z", "+00:00")),
    )
    if any(
        candidate[key] != native["candidate"][key]
        for key in ("executable_sha256", "config_sha256", "image_digest")
    ):
        raise ValueError("Changed candidate requires a new authorized compute attempt")
    web, _ = control.runtime_services(prepared, include_stopped=True)
    started = datetime.now(UTC)
    result = control.observe_protected(prepared, web, timeout=180, mode="reattest")
    if result.returncode:
        raise ValueError("Protected live read-only reattestation observer rejected applicability")
    observation = json.loads(result.stdout)
    control.verify_runtime(control.inspect_container(web["Id"]), prepared, allow_stopped=True)
    if digest(load_plan(args.plan)) != native["plan_sha256"]:
        raise ValueError("Original protected computation plan changed")
    payload = reattested_payload(
        original, keys, candidate, observation, base, started, datetime.now(UTC)
    )
    envelope = sign_receipt(payload, args.private_key.read_bytes(), args.key_id)
    verify_acceptance_route(envelope, keys)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(envelope, stream, sort_keys=True)
        stream.write("\n")
    return envelope


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base-ref", default="origin/main")
    for field in (
        "candidate",
        "original-receipt",
        "env-file",
        "plan",
        "execution-manifest",
        "private-key",
        "output",
    ):
        parser.add_argument("--" + field, type=Path, required=True)
    parser.add_argument("--key-id", required=True)
    args = parser.parse_args()
    args.stage, args.prepare_only, args.authorization_id, args.reason = (
        "acceptance",
        False,
        None,
        None,
    )
    try:
        value = reattest(args)
        print(
            json.dumps(
                {
                    "status": "REATTESTED",
                    "head_sha": value["payload"]["candidate"]["head_sha"],
                    "compute_attempts": 0,
                    "receipt_sha256": digest(value),
                }
            )
        )
        return 0
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print(f"staging reattestation: BLOCKED ({exc})")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
