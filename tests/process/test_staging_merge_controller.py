"""Unit API fixtures: these tests do not establish GitHub deployment or native-route acceptance."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "src")]
from build_staging_candidate import git  # noqa: E402
from merge_staging_candidate import ARTIFACT, CONTEXT, WORKFLOW, merge_candidate  # noqa: E402
from test_staging_acceptance_gate import NOW  # noqa: E402
from test_staging_acceptance_gate import receipt_repository as receipt_repository  # noqa: E402

from trading_advisor_3000.product_plane.common.staging_contracts import (  # noqa: E402
    digest,
    sign_receipt,
)


class FakeAPI:
    def __init__(self, responses):
        self.responses, self.calls, self.fresh_pr = responses, [], None

    def request(self, path, *, method="GET", body=None, binary=False):
        self.calls.append((path, method, body))
        if (
            path.endswith("/pulls/7")
            and self.fresh_pr
            and len([call for call in self.calls if call[0] == path]) > 1
        ):
            return copy.deepcopy(self.fresh_pr)
        return copy.deepcopy(self.responses[path])


@pytest.fixture
def admitted_api(receipt_repository):
    root, base, head, private, payload = receipt_repository
    envelope = sign_receipt(payload, private, "test-issuer")
    body = "```staging-acceptance\n" + json.dumps(envelope) + "\n```"
    pr = {
        "number": 7,
        "state": "open",
        "draft": False,
        "body": body,
        "base": {"ref": "main", "sha": base},
        "head": {"sha": head},
    }
    tree = payload["candidate"]["tree_sha"]
    witness = {
        "schema_version": "staging_merge_admission.v1",
        "repository": "example/repo",
        "pull_number": 7,
        "run_id": "123",
        "base_sha": base,
        "head_sha": head,
        "tree_sha": tree,
        "verifier_revision": base,
        "receipt_sha256": digest(envelope),
        "pr_body_sha256": hashlib.sha256(body.encode()).hexdigest(),
        "kind": "real-route",
    }
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("staging-admission.json", json.dumps(witness))
    steps = yaml.safe_load(git(root, "show", f"{base}:{WORKFLOW}"))["jobs"][CONTEXT]["steps"]
    prefix = "repos/example/repo"
    return root, FakeAPI(
        {
            f"{prefix}/pulls/7": pr,
            f"{prefix}/actions/workflows/staging-admission.yml": {"id": 42, "path": WORKFLOW},
            f"{prefix}/actions/runs/123": {
                "workflow_id": 42,
                "path": WORKFLOW,
                "event": "pull_request_target",
                "head_sha": base,
                "status": "completed",
                "conclusion": "success",
            },
            f"{prefix}/actions/runs/123/jobs?filter=latest&per_page=100": {
                "total_count": 1,
                "jobs": [
                    {
                        "name": CONTEXT,
                        "conclusion": "success",
                        "steps": [
                            {"name": step["name"], "conclusion": "success"} for step in steps
                        ],
                    }
                ],
            },
            f"{prefix}/actions/runs/123/artifacts?per_page=100": {
                "total_count": 1,
                "artifacts": [
                    {
                        "name": ARTIFACT,
                        "id": 9,
                        "expired": False,
                        "size_in_bytes": len(archive.getvalue()),
                    }
                ],
            },
            f"{prefix}/actions/artifacts/9/zip": archive.getvalue(),
            f"{prefix}/rules/branches/main": [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": True,
                        "required_status_checks": [{"context": "pr-lane"}],
                    },
                }
            ],
            f"{prefix}/branches/main/protection/required_status_checks": {
                "strict": False,
                "contexts": [],
            },
            f"{prefix}/pulls/7/merge": {"merged": True, "sha": head},
            f"{prefix}/git/commits/{head}": {"tree": {"sha": tree}},
        }
    )


def test_controller_defaults_to_read_only_and_merge_is_sha_bound(admitted_api):
    root, api = admitted_api
    assert merge_candidate(api, root, "example/repo", 7, 123, now=NOW)["status"] == "ADMITTED"
    assert all(method == "GET" for _, method, _ in api.calls)
    api.calls.clear()
    assert (
        merge_candidate(api, root, "example/repo", 7, 123, now=NOW, merge=True)["status"]
        == "MERGED"
    )
    writes = [call for call in api.calls if call[1] != "GET"]
    assert (
        len(writes) == 1
        and writes[0][2]["sha"] == api.responses["repos/example/repo/pulls/7"]["head"]["sha"]
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("workflow_id", 99),
        ("path", ".github/workflows/fake.yml"),
        ("event", "pull_request"),
        ("head_sha", "f" * 40),
    ],
)
def test_same_named_status_from_wrong_workflow_cannot_admit(admitted_api, field, value):
    root, api = admitted_api
    api.responses["repos/example/repo/actions/runs/123"][field] = value
    with pytest.raises(ValueError, match="exact protected workflow"):
        merge_candidate(api, root, "example/repo", 7, 123, now=NOW, merge=True)
    assert all(method == "GET" for _, method, _ in api.calls)


def test_skipped_step_stale_body_and_inactive_server_protection_refuse(admitted_api):
    root, api = admitted_api
    jobs = api.responses["repos/example/repo/actions/runs/123/jobs?filter=latest&per_page=100"]
    jobs["jobs"][0]["steps"][0]["conclusion"] = "skipped"
    with pytest.raises(ValueError, match="actually succeed"):
        merge_candidate(api, root, "example/repo", 7, 123, now=NOW)
    jobs["jobs"][0]["steps"][0]["conclusion"] = "success"
    pr = api.responses["repos/example/repo/pulls/7"]
    old = pr["body"]
    pr["body"] += "\nChanged after admission"
    with pytest.raises(ValueError, match="current PR/base/head/body"):
        merge_candidate(api, root, "example/repo", 7, 123, now=NOW)
    pr["body"] = old
    api.responses["repos/example/repo/rules/branches/main"] = []
    with pytest.raises(ValueError, match="server-side"):
        merge_candidate(api, root, "example/repo", 7, 123, now=NOW, merge=True)


@pytest.mark.parametrize("field", ["base", "head", "body"])
def test_change_between_admission_and_merge_refuses_write(admitted_api, field):
    root, api = admitted_api
    api.fresh_pr = copy.deepcopy(api.responses["repos/example/repo/pulls/7"])
    if field == "body":
        api.fresh_pr["body"] += "\nChanged immediately before merge"
    else:
        api.fresh_pr[field]["sha"] = "f" * 40
    with pytest.raises(ValueError, match="changed before merge"):
        merge_candidate(api, root, "example/repo", 7, 123, now=NOW, merge=True)
    assert all(method == "GET" for _, method, _ in api.calls)


def test_actual_classic_protection_fallback_requires_strict_pr_lane(admitted_api):
    root, api = admitted_api
    api.responses["repos/example/repo/rules/branches/main"] = []
    classic = api.responses["repos/example/repo/branches/main/protection/required_status_checks"]
    classic.update(strict=True, checks=[{"context": "pr-lane"}])
    assert merge_candidate(api, root, "example/repo", 7, 123, now=NOW)["status"] == "ADMITTED"
    classic["strict"] = False
    with pytest.raises(ValueError, match="server-side"):
        merge_candidate(api, root, "example/repo", 7, 123, now=NOW)
