"""Fail-closed pre-main admission using a receipt from a protected external collector."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from build_staging_candidate import (
    config_fingerprint,
    executable_fingerprint,
    git,
    relevant,
    resolve,
)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from trading_advisor_3000.product_plane.common import staging_route_proof  # noqa: E402

KEYRING = "configs/staging-acceptance-keys.v1.json"
JOBS, STAGES = staging_route_proof.JOBS, staging_route_proof.STAGES
check_route = staging_route_proof.check_route
FENCE = re.compile(r"^```staging-acceptance[ \t]*\r?\n(.*?)^```[ \t]*$", re.M | re.S)


def requires_acceptance(paths: list[str]) -> bool:
    for path in paths:
        if path.startswith(("src/", "configs/", "deployment/")) and relevant(path):
            return True
        if path.startswith("scripts/") and Path(path).name.startswith(
            (
                "run_moex",
                "run_research",
                "run_historical",
                "run_ta3000",
                "run_staging",
                "launch_moex",
                "staging_",
                "seed_staging",
                "prepare_staging",
                "initialize_staging",
                "authorize_staging",
                "extend_staging",
                "reattest_staging",
                "merge_staging",
                "collect_staging",
                "build_staging",
                "validate_staging",
                "proof_runtime_contract",
            )
        ):
            return True
        if relevant(path) and not path.startswith(("scripts/", "docs/")):
            return True
    return False


def load_envelope(receipt: Path | None, *, event_path: Path | None = None) -> dict:
    if receipt is not None:
        text = receipt.read_text(encoding="utf-8")
    else:
        if event_path is None:
            raise ValueError("Missing signed staging acceptance receipt")
        event = json.loads(event_path.read_text(encoding="utf-8"))
        body = str(event.get("pull_request", {}).get("body") or "")
        blocks = FENCE.findall(body)
        if len(blocks) != 1:
            raise ValueError("PR requires exactly one signed staging-acceptance block")
        text = blocks[0]
    if len(text.encode("utf-8")) > 262144:
        raise ValueError("Staging receipt exceeds 256 KiB")
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("Staging receipt must be an object")
    return value


def validate_acceptance(
    root: Path, *, base_ref: str, head_ref: str, envelope: dict, now: datetime | None = None
) -> dict:
    from trading_advisor_3000.product_plane.common.staging_route_proof import (
        verify_acceptance_route,
    )

    base, head = resolve(root, base_ref), resolve(root, head_ref)
    # Key bytes come from the protected Git base, never from candidate files or PR URLs.
    try:
        keyring = json.loads(git(root, "show", f"{base}:{KEYRING}"))
        keys = keyring["keys"]
        if not isinstance(keys, dict) or not keys:
            raise ValueError("Empty protected keyring")
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError("Protected verifier/key bootstrap is pending") from exc
    payload = verify_acceptance_route(envelope, keys, now=now or datetime.now(timezone.utc))
    expected = {
        "base_sha": base,
        "head_sha": head,
        "tree_sha": git(root, "rev-parse", f"{head}^{{tree}}"),
        "executable_sha256": executable_fingerprint(root, head),
        "config_sha256": config_fingerprint(root, head),
    }
    if any(payload["candidate"].get(key) != value for key, value in expected.items()):
        raise ValueError("Stale main/candidate/tree/executable/configuration receipt")
    git(root, "merge-base", "--is-ancestor", base, head)
    verifier = resolve(root, payload["verifier_revision"])
    git(root, "merge-base", "--is-ancestor", verifier, base)
    if git(
        root,
        "diff",
        "--name-only",
        f"{verifier}..{base}",
        "--",
        "scripts/validate_staging_acceptance.py",
        "scripts/build_staging_candidate.py",
        "src/trading_advisor_3000/product_plane/common/staging_contracts.py",
        "src/trading_advisor_3000/product_plane/common/staging_route_proof.py",
        "src/trading_advisor_3000/product_plane/contracts/schemas/staging_acceptance.v1.json",
        KEYRING,
    ):
        raise ValueError("Verifier or trust policy changed; fresh attestation required")
    for branch_head in payload["candidate"].get("branch_heads", {}).values():
        git(root, "merge-base", "--is-ancestor", resolve(root, branch_head), head)
    return payload


def check_staging_acceptance(
    changed_files: list[str],
    *,
    base_ref: str | None = None,
    head_ref: str | None = None,
    receipt: str | None = None,
    repo_root: Path | None = None,
    attestation_output: Path | None = None,
) -> int:
    root = repo_root or Path.cwd()
    try:
        if base_ref and head_ref:
            # Independent collection catches moves/deletions even when caller scope is narrow.
            changed_files = git(
                root,
                "diff",
                "--name-only",
                "--no-renames",
                f"{resolve(root, base_ref)}..{resolve(root, head_ref)}",
            ).splitlines()
        if not requires_acceptance(changed_files):
            if attestation_output is not None:
                write_merge_attestation(root, base_ref, head_ref, None, attestation_output)
            print("staging acceptance: NOT_REQUIRED (no executable product route change)")
            return 0
        if not base_ref or not head_ref:
            raise ValueError("Product admission requires immutable base-ref and head-ref")
        event = os.environ.get("GITHUB_EVENT_PATH")
        envelope = load_envelope(
            Path(receipt) if receipt else None, event_path=Path(event) if event else None
        )
        payload = validate_acceptance(
            root,
            base_ref=base_ref,
            head_ref=head_ref,
            envelope=envelope,
        )
        if attestation_output is not None:
            write_merge_attestation(root, base_ref, head_ref, envelope, attestation_output)
        print(f"staging acceptance: PASS (candidate={payload['candidate']['head_sha']})")
        return 0
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(f"staging acceptance: BLOCKED ({exc})")
        return 2


def write_merge_attestation(root, base_ref, head_ref, envelope, output: Path) -> None:
    """The protected workflow publishes this witness only after every check succeeds."""
    from trading_advisor_3000.product_plane.common.staging_contracts import digest

    if not base_ref or not head_ref:
        raise ValueError("Merge attestation requires immutable base and head")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    event = json.loads(Path(event_path).read_text(encoding="utf-8")) if event_path else {}
    pr = event.get("pull_request", {})
    value = {
        "schema_version": "staging_merge_admission.v1",
        "repository": event.get("repository", {}).get("full_name"),
        "pull_number": pr.get("number"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "base_sha": resolve(root, base_ref),
        "head_sha": resolve(root, head_ref),
        "tree_sha": git(root, "rev-parse", f"{resolve(root, head_ref)}^{{tree}}"),
        "verifier_revision": resolve(ROOT, "HEAD"),
        "receipt_sha256": digest(envelope) if envelope is not None else None,
        "pr_body_sha256": hashlib.sha256(str(pr.get("body") or "").encode()).hexdigest(),
        "kind": "real-route" if envelope is not None else "no-product-route-change",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--head-ref", required=True)
    parser.add_argument("--receipt")
    parser.add_argument("--trusted-root", type=Path, default=ROOT)
    parser.add_argument("--attestation-output", type=Path)
    args = parser.parse_args()
    try:
        trusted = args.trusted_root.resolve()
        if trusted != ROOT.resolve() or resolve(trusted, "HEAD") != resolve(
            args.repo, args.base_ref
        ):
            raise ValueError("Verifier must execute from its protected base checkout")
        if git(trusted, "status", "--porcelain", "--untracked-files=no"):
            raise ValueError("Protected verifier checkout has changed")
    except (ValueError, OSError) as exc:
        print(f"staging acceptance: BLOCKED ({exc})")
        return 2
    return check_staging_acceptance(
        [],
        base_ref=args.base_ref,
        head_ref=args.head_ref,
        receipt=args.receipt,
        repo_root=args.repo,
        attestation_output=args.attestation_output,
    )


if __name__ == "__main__":
    raise SystemExit(main())
