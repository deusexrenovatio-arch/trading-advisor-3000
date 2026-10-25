from __future__ import annotations

import base64
import copy
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "src")]
from build_staging_candidate import config_fingerprint, executable_fingerprint, git  # noqa: E402
from validate_staging_acceptance import (  # noqa: E402
    JOBS,
    STAGES,
    check_staging_acceptance,
    load_envelope,
    requires_acceptance,
    validate_acceptance,
)

from trading_advisor_3000.product_plane.common.staging_contracts import (  # noqa: E402
    digest,
    sign_receipt,
)
from trading_advisor_3000.product_plane.common.staging_route_proof import (  # noqa: E402
    verify_acceptance_route,
)

NOW = datetime(2026, 10, 25, tzinfo=UTC)


@pytest.fixture
def receipt_repository(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q")
    git(root, "config", "user.name", "Gate test")
    git(root, "config", "user.email", "gate-test@localhost")
    (root / "configs").mkdir()
    (root / "src").mkdir()
    (root / ".github/workflows").mkdir(parents=True)
    (root / ".github/workflows/staging-admission.yml").write_text(
        (ROOT / ".github/workflows/staging-admission.yml").read_text(), encoding="utf-8"
    )
    private = Ed25519PrivateKey.generate()
    public = base64.b64encode(private.public_key().public_bytes_raw()).decode()
    (root / "configs/staging-acceptance-keys.v1.json").write_text(
        json.dumps({"keys": {"test-issuer": public}}), encoding="utf-8"
    )
    (root / "src/app.py").write_text("VALUE = 1\n", encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-qm", "protected base")
    base = git(root, "rev-parse", "HEAD")
    (root / "src/app.py").write_text("VALUE = 2\n", encoding="utf-8")
    git(root, "commit", "-qam", "candidate")
    head = git(root, "rev-parse", "HEAD")
    candidate = {
        "base_sha": base,
        "head_sha": head,
        "tree_sha": git(root, "rev-parse", "HEAD^{tree}"),
        "executable_sha256": executable_fingerprint(root, head),
        "config_sha256": config_fingerprint(root, head),
        "image_digest": "sha256:" + "1" * 64,
    }
    runs = {
        job: {
            "run_id": f"run-{index}",
            "status": "SUCCESS",
            "candidate_head_sha": head,
            **({"upstream_run_id": f"run-{index - 1}"} if index else {}),
        }
        for index, job in enumerate(JOBS)
    }
    payload = {
        "schema_version": "staging_receipt.v1",
        "task_id": "gate-test",
        "stage": "acceptance",
        "candidate": candidate,
        "plan_sha256": "a" * 64,
        "verifier_revision": base,
        "issued_at_utc": NOW.isoformat(),
        "expires_at_utc": (NOW + timedelta(hours=1)).isoformat(),
        "source": {"snapshot_id": "before", "manifest_sha256": "b" * 64},
        "outputs": {
            "snapshot_id": "after",
            "manifest_sha256": "c" * 64,
            "data_snapshot_id": "data-after",
        },
        "runs": runs,
        "schedule": {
            "name": "moex_baseline_daily_update_schedule",
            "status": "SUCCESS",
            "tick_id": "tick-1",
            "run_id": "run-0",
        },
        "changed_windows": 1,
        "qc": {"status": "PASS"},
        "consumer": {
            "status": "PASS",
            "rows": 1,
            "snapshot_id": "after",
            "cache_hit": False,
            "entrypoint": "research.io.load_backtest_frames",
            "process_id": "fresh-1",
            "digest": "d" * 64,
            "read_at_utc": NOW.isoformat(),
        },
        "coverage": {
            "status": "PASS",
            "required_stages": list(STAGES),
            "executed_stages": list(STAGES),
            "required_scenarios": [],
            "executed_scenarios": [],
        },
        "runtime": {
            "code_sha256": candidate["executable_sha256"],
            "config_sha256": candidate["config_sha256"],
            "image_digest": candidate["image_digest"],
            "budget": {
                "status": "PASS",
                "plan_sha256": "a" * 64,
                "root_reservation_id": "e" * 32,
                "native_claims": {"producer": 1},
            },
        },
    }
    return root, base, head, private.private_bytes_raw(), payload


def validate(case, payload=None, envelope=None, now=NOW):
    root, base, head, private, original = case
    return validate_acceptance(
        root,
        base_ref=base,
        head_ref=head,
        now=now,
        envelope=envelope or sign_receipt(payload or original, private, "test-issuer"),
    )


def test_accepts_bound_signed_route_and_ignores_candidate_key_file(receipt_repository):
    root, _, _, _, payload = receipt_repository
    (root / "configs/staging-acceptance-keys.v1.json").write_text('{"keys": {}}', encoding="utf-8")
    assert validate(receipt_repository)["candidate"] == payload["candidate"]


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("schedule", "tick_id", ""),
        ("qc", "status", "FAIL"),
        ("consumer", "snapshot_id", "wrong"),
        ("consumer", "cache_hit", True),
        ("consumer", "entrypoint", "raw_table_scan"),
        ("consumer", "rows", 0),
        ("coverage", "executed_stages", []),
        ("runtime", "code_sha256", "f" * 64),
        ("runtime", "mode", "reference"),
        ("coverage", "required_scenarios", ["rollover"]),
        ("candidate", "base_sha", "f" * 40),
    ],
)
def test_even_signed_incomplete_route_is_rejected(receipt_repository, section, key, value):
    payload = copy.deepcopy(receipt_repository[-1])
    payload[section][key] = value
    with pytest.raises(ValueError):
        validate(receipt_repository, payload)


def test_forgery_expiry_lineage_and_missing_evidence_block(receipt_repository):
    _, _, _, private, payload = receipt_repository
    envelope = sign_receipt(payload, private, "test-issuer")
    envelope["payload"]["changed_windows"] = 2
    with pytest.raises(ValueError, match="signature"):
        validate(receipt_repository, envelope=envelope)
    with pytest.raises(ValueError, match="expired"):
        validate(receipt_repository, now=NOW + timedelta(days=1))
    bad = copy.deepcopy(payload)
    bad["runs"][JOBS[1]]["upstream_run_id"] = "unrelated"
    with pytest.raises(ValueError, match="lineage"):
        validate(receipt_repository, bad)
    assert check_staging_acceptance(["src/app.py"]) == 2
    assert check_staging_acceptance(["docs/guide.md"]) == 0


def test_missing_budget_and_duplicate_actual_run_are_rejected(receipt_repository):
    payload = copy.deepcopy(receipt_repository[-1])
    payload["runtime"].pop("budget")
    with pytest.raises(ValueError, match="budget"):
        validate(receipt_repository, payload)
    payload = copy.deepcopy(receipt_repository[-1])
    payload["runs"][JOBS[2]]["run_id"] = payload["runs"][JOBS[1]]["run_id"]
    with pytest.raises(ValueError, match="distinct"):
        validate(receipt_repository, payload)


def test_shared_product_verifier_rejects_branch_or_toy_success(receipt_repository):
    _, _, _, private, original = receipt_repository
    key = Ed25519PrivateKey.from_private_bytes(private).public_key().public_bytes_raw()
    keys = {"test-issuer": base64.b64encode(key).decode()}
    assert verify_acceptance_route(sign_receipt(original, private, "test-issuer"), keys, now=NOW)
    for stage in ("branch", "acceptance"):
        payload = copy.deepcopy(original)
        payload["stage"] = stage
        payload["runs"] = {"toy_job": {"status": "SUCCESS", "run_id": "toy"}}
        with pytest.raises(ValueError):
            verify_acceptance_route(sign_receipt(payload, private, "test-issuer"), keys, now=NOW)


def test_docs_reattest_keeps_immutable_compute_origin_and_rejects_changed_code(receipt_repository):
    root, base, _, private, original = receipt_repository
    signed = sign_receipt(original, private, "test-issuer")
    (root / "README.md").write_text("Docs change after native proof\n", encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-qm", "documentation only")
    new_head = git(root, "rev-parse", "HEAD")
    payload = copy.deepcopy(original)
    payload["candidate"].update(head_sha=new_head, tree_sha=git(root, "rev-parse", "HEAD^{tree}"))
    payload["compute_origin"] = {"receipt_sha256": digest(signed), "signed_receipt": signed}
    later = NOW + timedelta(days=1)
    payload["issued_at_utc"] = later.isoformat()
    payload["expires_at_utc"] = (later + timedelta(hours=1)).isoformat()
    payload["consumer"].update(process_id="fresh-2", read_at_utc=later.isoformat())
    value = validate_acceptance(
        root,
        base_ref=base,
        head_ref=new_head,
        now=later,
        envelope=sign_receipt(payload, private, "test-issuer"),
    )
    assert value["runs"][JOBS[0]]["candidate_head_sha"] == original["candidate"]["head_sha"]
    (root / "src/app.py").write_text("VALUE = 99\n", encoding="utf-8")
    git(root, "commit", "-qam", "changed executable")
    changed = git(root, "rev-parse", "HEAD")
    payload["candidate"].update(
        head_sha=changed,
        tree_sha=git(root, "rev-parse", "HEAD^{tree}"),
        executable_sha256=executable_fingerprint(root, changed),
    )
    payload["runtime"]["code_sha256"] = payload["candidate"]["executable_sha256"]
    with pytest.raises(ValueError, match="not reusable"):
        validate_acceptance(
            root,
            base_ref=base,
            head_ref=changed,
            now=later,
            envelope=sign_receipt(payload, private, "test-issuer"),
        )


def test_receipt_body_requires_exactly_one_block(tmp_path):
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"pull_request": {"body": "```staging-acceptance\n{}\n```"}}))
    assert load_envelope(None, event_path=event) == {}
    event.write_text(json.dumps({"pull_request": {"body": "No evidence"}}))
    with pytest.raises(ValueError, match="exactly one"):
        load_envelope(None, event_path=event)


def test_deployment_dependencies_and_launchers_cannot_take_docs_shortcut():
    for path in (
        "deployment/docker/app.yml",
        "configs/route.yaml",
        "uv.lock",
        "pyproject.toml",
        "scripts/run_moex_baseline_update.py",
        "scripts/staging_admission.py",
        "scripts/run_staging_control.py",
        "scripts/seed_staging_snapshot.py",
        "scripts/prepare_staging_plan.py",
        "scripts/initialize_staging_schedule.py",
        "scripts/authorize_staging_budget.py",
        "scripts/extend_staging_budget.py",
        "scripts/reattest_staging_acceptance.py",
        "scripts/merge_staging_candidate.py",
    ):
        assert requires_acceptance([path])
    assert not requires_acceptance(["docs/runbooks/app/guide.md"])
    assert not requires_acceptance(["scripts/build_governance_dashboard.py"])
    assert not requires_acceptance(["scripts/validate_docs_links.py"])


def test_protected_admission_never_skips_and_preserves_existing_ci():
    workflow = yaml.safe_load((ROOT / ".github/workflows/staging-admission.yml").read_text())
    assert "pull_request_target" in workflow.get("on", workflow.get(True))
    lane = workflow["jobs"]["staging-admission"]
    assert "if" not in lane
    assert all(permission == "read" for permission in workflow["permissions"].values())
    text = json.dumps(lane)
    assert "required admission cannot be skipped" in text
    assert "protected verifier bootstrap is pending" in text
    assert "python trusted/scripts/validate_staging_acceptance.py" in text
    assert "pip install -e" not in text and '"persist-credentials": false' in text
    existing = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    assert "pull_request" in existing.get("on", existing.get(True))
    assert "pr-lane" in existing["jobs"]
