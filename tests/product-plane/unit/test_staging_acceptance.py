from __future__ import annotations

import pytest

from trading_advisor_3000.product_plane.common.staging_acceptance import (
    JOBS,
    STAGES,
    validate_native_result,
)


def proof():
    parent = None
    runs = {}
    for index, job in enumerate(JOBS):
        run = f"native-{index}"
        runs[job] = dict(run_id=run, status="SUCCESS", upstream_run_id=parent)
        parent = run
    return dict(
        version=1,
        stage="acceptance",
        status="PASS",
        base_sha="a" * 40,
        head_sha="b" * 40,
        tree_sha="c" * 40,
        ci_run_id="42",
        image_digest="sha256:" + "d" * 64,
        input_pins={"data": "native-data-before"},
        runs=runs,
        executed_stages=list(STAGES),
        changed_windows=1,
        qc={"native_qc": "PASS"},
        snapshot_id="published-research",
        consumer=dict(
            entrypoint="research.io.load_backtest_frames",
            rows=5,
            snapshot_id="published-research",
            cache_hit=False,
        ),
    )


def test_native_owner_emits_native_envelope_only_after_validation():
    value = proof()
    assert validate_native_result(value)["evidence_level"] == "native"
    assert "evidence_level" not in value


@pytest.mark.parametrize(
    "key,value",
    [
        ("stage", "branch"),
        ("status", "FAILED"),
        ("image_digest", "unpinned"),
        ("input_pins", {}),
        ("executed_stages", []),
        ("changed_windows", 0),
        ("qc", {}),
        ("snapshot_id", None),
    ],
)
def test_incomplete_product_proof_cannot_emit_native_level(key, value):
    result = proof()
    result[key] = value
    with pytest.raises(ValueError):
        validate_native_result(result)
    assert "evidence_level" not in result


@pytest.mark.parametrize(
    "mutation",
    ["failed", "lineage", "duplicate", "consumer-cache", "consumer-empty", "consumer-other"],
)
def test_route_and_fresh_consumer_are_required(mutation):
    result = proof()
    if mutation == "failed":
        result["runs"][JOBS[1]]["status"] = "FAILURE"
    elif mutation == "lineage":
        result["runs"][JOBS[1]]["upstream_run_id"] = "foreign"
    elif mutation == "duplicate":
        result["runs"][JOBS[2]]["run_id"] = result["runs"][JOBS[1]]["run_id"]
    elif mutation == "consumer-cache":
        result["consumer"]["cache_hit"] = True
    elif mutation == "consumer-empty":
        result["consumer"]["rows"] = 0
    else:
        result["consumer"]["snapshot_id"] = "foreign"
    with pytest.raises(ValueError):
        validate_native_result(result)
