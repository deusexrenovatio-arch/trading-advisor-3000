"""Signature/Git protocol fixtures, not evidence of an actual protected runtime observation."""

from __future__ import annotations

import base64
import copy
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "src")]
from build_staging_candidate import git  # noqa: E402
from reattest_staging_acceptance import checked_candidate, reattested_payload  # noqa: E402
from test_staging_acceptance_gate import NOW  # noqa: E402
from test_staging_acceptance_gate import receipt_repository as receipt_repository  # noqa: E402
from validate_staging_acceptance import validate_acceptance  # noqa: E402

from trading_advisor_3000.product_plane.common.staging_contracts import (  # noqa: E402
    digest,
    sign_receipt,
)
from trading_advisor_3000.product_plane.common.staging_route_proof import (  # noqa: E402
    verify_acceptance_route,
)


@pytest.fixture
def reattestation(receipt_repository):
    root, base, _, private, native = receipt_repository
    original = sign_receipt(native, private, "test-issuer")
    key = Ed25519PrivateKey.from_private_bytes(private).public_key().public_bytes_raw()
    keys = {"test-issuer": base64.b64encode(key).decode()}
    (root / "README.md").write_text("Equivalent candidate after integration\n", encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-qm", "documentation-only rebase")
    candidate = {
        **native["candidate"],
        "head_sha": git(root, "rev-parse", "HEAD"),
        "tree_sha": git(root, "rev-parse", "HEAD^{tree}"),
    }
    checked_candidate(root, candidate, base)
    started, issued = NOW + timedelta(days=1), NOW + timedelta(days=1, seconds=2)
    observation = {
        "status": "APPLICABLE",
        "compute_attempts": 0,
        "original_receipt_sha256": digest(original),
        "source": native["source"],
        "outputs": native["outputs"],
        "consumer": {
            **native["consumer"],
            "read_at_utc": (started + timedelta(seconds=1)).isoformat(),
        },
        "metadata_observation": {"observation_complete": True},
    }
    return root, base, private, original, keys, candidate, observation, started, issued


def payload(case):
    _, base, _, original, keys, candidate, observation, started, issued = case
    return reattested_payload(original, keys, candidate, observation, base, started, issued)


def test_reattest_keeps_every_computation_identity_and_original_signature(reattestation):
    case = reattestation
    value = payload(case)
    native = case[3]["payload"]
    for field in (
        "task_id",
        "stage",
        "plan_sha256",
        "source",
        "outputs",
        "runs",
        "schedule",
        "changed_windows",
        "qc",
        "coverage",
    ):
        assert value[field] == native[field]
    assert value["runtime"]["budget"] == native["runtime"]["budget"]
    assert value["compute_origin"]["signed_receipt"] == case[3]
    fresh = sign_receipt(value, case[2], "test-issuer")
    assert verify_acceptance_route(fresh, case[4], now=case[-1])["candidate"] == case[5]


@pytest.mark.parametrize(
    "field,value",
    [
        ("executable_sha256", "f" * 64),
        ("config_sha256", "f" * 64),
        ("image_digest", "sha256:" + "f" * 64),
    ],
)
def test_changed_closure_cannot_reuse_proof(reattestation, field, value):
    case = list(copy.deepcopy(reattestation))
    case[5][field] = value
    with pytest.raises(ValueError, match="Changed executable"):
        payload(case)


def test_changed_git_unobserved_data_or_stale_reader_refuse(reattestation):
    case = reattestation
    root, base = case[:2]
    (root / "src/app.py").write_text("VALUE = 99\n", encoding="utf-8")
    git(root, "commit", "-qam", "executable mutation")
    candidate = {
        **case[5],
        "head_sha": git(root, "rev-parse", "HEAD"),
        "tree_sha": git(root, "rev-parse", "HEAD^{tree}"),
    }
    with pytest.raises(ValueError, match="actual current Git"):
        checked_candidate(root, candidate, base)
    for mutation in ("compute_attempts", "outputs", "reader"):
        changed = list(copy.deepcopy(case))
        if mutation == "reader":
            changed[6]["consumer"]["read_at_utc"] = NOW.isoformat()
        else:
            changed[6][mutation] = 1 if mutation == "compute_attempts" else {}
        with pytest.raises(ValueError):
            payload(changed)


def test_bootstrap_origin_verifier_precedes_identical_tree_main_merge(receipt_repository):
    root, old_base, raw_head, private, native = receipt_repository
    native["verifier_revision"] = raw_head
    original = sign_receipt(native, private, "test-issuer")
    with pytest.raises(ValueError, match="merge-base"):
        validate_acceptance(root, base_ref=old_base, head_ref=raw_head, envelope=original, now=NOW)
    tree = native["candidate"]["tree_sha"]
    main = git(root, "commit-tree", tree, "-p", old_base, "-p", raw_head, data="bootstrap merge\n")
    next_head = git(root, "commit-tree", tree, "-p", main, data="equivalent candidate\n")
    candidate = {**native["candidate"], "base_sha": main, "head_sha": next_head}
    checked_candidate(root, candidate, main)
    key = Ed25519PrivateKey.from_private_bytes(private).public_key().public_bytes_raw()
    keys = {"test-issuer": base64.b64encode(key).decode()}
    started, issued = NOW + timedelta(days=1), NOW + timedelta(days=1, seconds=2)
    observation = {
        "status": "APPLICABLE",
        "compute_attempts": 0,
        "original_receipt_sha256": digest(original),
        "source": native["source"],
        "outputs": native["outputs"],
        "metadata_observation": {"observation_complete": True},
        "consumer": {**native["consumer"], "read_at_utc": started.isoformat()},
    }
    value = reattested_payload(original, keys, candidate, observation, main, started, issued)
    assert value["verifier_revision"] == main
    assert value["compute_origin"]["signed_receipt"] == original
    assert value["runs"] == native["runs"]
    assert value["runtime"]["budget"] == native["runtime"]["budget"]
    admitted = validate_acceptance(
        root,
        base_ref=main,
        head_ref=next_head,
        now=issued,
        envelope=sign_receipt(value, private, "test-issuer"),
    )
    assert admitted["candidate"]["base_sha"] == main
