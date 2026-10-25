"""Canonical host merge admission; other GitHub merge routes need separate source protection."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import zipfile
from pathlib import Path

import yaml
from build_staging_candidate import git, resolve
from validate_staging_acceptance import FENCE, ROOT, requires_acceptance, validate_acceptance

WORKFLOW = ".github/workflows/staging-admission.yml"
CONTEXT = "staging-admission"
REQUIRED_CI_CONTEXT = "pr-lane"
ARTIFACT = "staging-admission-context"


class GithubAPI:
    def request(self, path: str, *, method="GET", body=None, binary=False):
        command = ["gh", "api", "--method", method, path]
        if body is not None:
            command += ["--input", "-"]
        result = subprocess.run(
            command,
            input=json.dumps(body).encode() if body is not None else None,
            capture_output=True,
            timeout=60,
            check=False,
        )
        if result.returncode:
            raise ValueError(f"GitHub {method} request failed: {path}")
        return result.stdout if binary else json.loads(result.stdout)


def artifact_context(archive: bytes) -> dict:
    if len(archive) > 262144:
        raise ValueError("Admission artifact exceeds the bounded size")
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        entries = zipped.infolist()
        if len(entries) != 1 or entries[0].filename != "staging-admission.json":
            raise ValueError("Unexpected admission artifact contents")
        if entries[0].file_size > 262144:
            raise ValueError("Expanded admission artifact exceeds the bounded size")
        return json.loads(zipped.read(entries[0]))


def strict_pr_lane(api, prefix: str) -> bool:
    """Read actual rules; the protected workflow is verified independently of status names."""
    try:
        rules = api.request(f"{prefix}/rules/branches/main")
    except ValueError:
        rules = []
    if any(
        rule.get("type") == "required_status_checks"
        and rule.get("parameters", {}).get("strict_required_status_checks_policy") is True
        and any(
            check.get("context") == REQUIRED_CI_CONTEXT
            for check in rule["parameters"].get("required_status_checks", [])
        )
        for rule in rules
    ):
        return True
    try:
        classic = api.request(f"{prefix}/branches/main/protection/required_status_checks")
    except ValueError:
        return False
    contexts = set(classic.get("contexts", []))
    contexts.update(check.get("context") for check in classic.get("checks", []))
    return classic.get("strict") is True and REQUIRED_CI_CONTEXT in contexts


def inspect_admission(
    api, root: Path, repository: str, number: int, run_id: int, *, now=None
) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("Repository must be owner/name")
    prefix = f"repos/{repository}"
    pr = api.request(f"{prefix}/pulls/{number}")
    if pr["state"] != "open" or pr.get("draft") or pr["base"]["ref"] != "main":
        raise ValueError("Only an open ready integration PR into main can be admitted")
    base, head = pr["base"]["sha"], pr["head"]["sha"]
    workflow = api.request(f"{prefix}/actions/workflows/staging-admission.yml")
    run = api.request(f"{prefix}/actions/runs/{run_id}")
    if (
        workflow.get("path") != WORKFLOW
        or run.get("workflow_id") != workflow.get("id")
        or run.get("path", "").split("@", 1)[0] != WORKFLOW
        or run.get("event") != "pull_request_target"
        or run.get("head_sha") != base
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
    ):
        raise ValueError("Run is not the successful exact protected workflow at current main")
    declared = yaml.safe_load(git(root, "show", f"{base}:{WORKFLOW}"))
    definition = declared["jobs"][CONTEXT]
    if "if" in definition:
        raise ValueError("Protected admission job must not be conditionally skipped")
    expected_steps = [step["name"] for step in definition["steps"]]
    jobs = api.request(f"{prefix}/actions/runs/{run_id}/jobs?filter=latest&per_page=100")
    selected = [job for job in jobs["jobs"] if job.get("name") == CONTEXT]
    if jobs.get("total_count", 0) > 100 or len(selected) != 1:
        raise ValueError("Missing or ambiguous protected admission job")
    job = selected[0]
    if job.get("conclusion") != "success" or any(
        len(
            [
                step
                for step in job.get("steps", [])
                if step.get("name") == name and step.get("conclusion") == "success"
            ]
        )
        != 1
        for name in expected_steps
    ):
        raise ValueError("A required protected admission step did not actually succeed")
    artifacts = api.request(f"{prefix}/actions/runs/{run_id}/artifacts?per_page=100")
    selected = [item for item in artifacts["artifacts"] if item.get("name") == ARTIFACT]
    if artifacts.get("total_count", 0) > 100 or len(selected) != 1:
        raise ValueError("Missing or ambiguous protected admission artifact")
    item = selected[0]
    if item.get("expired") or item.get("size_in_bytes", 262145) > 262144:
        raise ValueError("Admission artifact is expired or too large")
    witness = artifact_context(
        api.request(f"{prefix}/actions/artifacts/{item['id']}/zip", binary=True)
    )
    body = str(pr.get("body") or "")
    expected = {
        "schema_version": "staging_merge_admission.v1",
        "repository": repository,
        "pull_number": number,
        "run_id": str(run_id),
        "base_sha": base,
        "head_sha": head,
        "tree_sha": git(root, "rev-parse", f"{resolve(root, head)}^{{tree}}"),
        "verifier_revision": base,
        "pr_body_sha256": hashlib.sha256(body.encode()).hexdigest(),
    }
    if any(witness.get(key) != value for key, value in expected.items()):
        raise ValueError(
            "Protected admission artifact does not bind this current PR/base/head/body"
        )
    paths = git(root, "diff", "--name-only", "--no-renames", f"{base}..{head}").splitlines()
    if requires_acceptance(paths):
        from trading_advisor_3000.product_plane.common.staging_contracts import digest

        blocks = FENCE.findall(body)
        if len(blocks) != 1:
            raise ValueError("Current PR requires one signed acceptance receipt")
        envelope = json.loads(blocks[0])
        if witness.get("kind") != "real-route" or witness.get("receipt_sha256") != digest(envelope):
            raise ValueError("Protected admission attests a different receipt")
        validate_acceptance(root, base_ref=base, head_ref=head, envelope=envelope, now=now)
    elif witness.get("kind") != "no-product-route-change" or witness.get("receipt_sha256"):
        raise ValueError("Unexpected admission kind for unchanged executable route")
    if not strict_pr_lane(api, prefix):
        raise ValueError("Strict server-side pr-lane protection is not active")
    return {
        "status": "ADMITTED",
        "repository": repository,
        "pull_number": number,
        "run_id": run_id,
        "base_sha": base,
        "head_sha": head,
        "tree_sha": expected["tree_sha"],
        "pr_body_sha256": expected["pr_body_sha256"],
        "scope": "canonical-controller-only",
    }


def merge_candidate(
    api, root: Path, repository: str, number: int, run_id: int, *, merge=False, now=None
):
    admission = inspect_admission(api, root, repository, number, run_id, now=now)
    if not merge:
        return admission
    prefix = f"repos/{repository}"
    fresh = api.request(f"{prefix}/pulls/{number}")
    if (
        fresh["base"]["sha"] != admission["base_sha"]
        or fresh["head"]["sha"] != admission["head_sha"]
        or hashlib.sha256(str(fresh.get("body") or "").encode()).hexdigest()
        != admission["pr_body_sha256"]
    ):
        raise ValueError("PR/main changed before merge; new admission required")
    result = api.request(
        f"{prefix}/pulls/{number}/merge",
        method="PUT",
        body={"sha": admission["head_sha"], "merge_method": "merge"},
    )
    if result.get("merged") is not True:
        raise ValueError("GitHub refused the SHA-bound protected merge")
    committed = api.request(f"{prefix}/git/commits/{result['sha']}")
    if committed["tree"]["sha"] != admission["tree_sha"]:
        raise RuntimeError(f"MERGED_WITH_UNEXPECTED_TREE: inspect {result['sha']}")
    return {**admission, "status": "MERGED", "merge_sha": result["sha"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pull", type=int, required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument(
        "--merge", action="store_true", help="Explicitly execute the admitted merge"
    )
    args = parser.parse_args()
    try:
        api = GithubAPI()
        current = api.request(f"repos/{args.repository}/pulls/{args.pull}")
        if resolve(ROOT, "HEAD") != current["base"]["sha"] or git(
            ROOT, "status", "--porcelain", "--untracked-files=no"
        ):
            raise ValueError("Controller must execute from a clean protected main checkout")
        result = merge_candidate(
            api, args.checkout, args.repository, args.pull, args.run_id, merge=args.merge
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    except (
        ValueError,
        KeyError,
        TypeError,
        OSError,
        RuntimeError,
        subprocess.SubprocessError,
        zipfile.BadZipFile,
    ) as exc:
        print(f"staging merge: BLOCKED ({exc})")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
