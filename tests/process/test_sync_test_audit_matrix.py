from __future__ import annotations

import csv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from sync_test_audit_matrix import (  # noqa: E402
    AUDIT_UPDATE_COLUMNS,
    classify_work_package,
    detect_file_signals,
    merge_rows,
    validate_rows,
)


def _run(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "sync_test_audit_matrix.py"), *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_update(path: Path, row: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_UPDATE_COLUMNS)
        writer.writeheader()
        writer.writerow(row)


def test_classify_work_package_uses_stable_domain_blocks() -> None:
    cases = {
        "tests/architecture/test_context_coverage.py::test_context": "G1",
        "tests/process/test_process_reports.py::test_report": "G2",
        "tests/product-plane/contracts/test_release_blocking_contracts.py::test_contract": "C",
        "tests/product-plane/contracts/test_continuous_front_contracts.py::test_contract": "C",
        "tests/product-plane/unit/test_historical_data_quality.py::test_quality": "D",
        "tests/product-plane/unit/test_product_plane_namespace_bridge.py::test_export": "D",
        "tests/product-plane/unit/test_continuous_front.py::test_roll": "CF",
        "tests/product-plane/unit/test_moex_raw_ingest_errors.py::test_error": "M1",
        "tests/product-plane/unit/test_moex_canonical_route.py::test_route": "M2",
        "tests/product-plane/unit/test_research_indicator_layer.py::test_indicator": "R1",
        "tests/product-plane/unit/test_research_backtest_layer.py::test_backtest": "R2",
        "tests/product-plane/unit/test_research_registry_store.py::test_registry": "R2",
        "tests/product-plane/unit/test_strategy_evaluation_profile.py::test_profile": "R2",
        "tests/product-plane/unit/test_runtime_components.py::test_runtime": "X",
    }

    assert {nodeid: classify_work_package(nodeid) for nodeid in cases} == cases


def test_merge_rows_preserves_manual_audit_fields_and_adds_new_tests() -> None:
    existing = [
        {
            "nodeid": "tests/process/test_process_reports.py::test_report",
            "work_package": "G2",
            "test_kind": "process",
            "audit_status": "reviewed",
            "decision": "good",
            "problem_codes": "none",
            "skip_policy": "none",
            "static_signals": "",
            "behavior_contract": "Report exposes the governed status totals.",
            "evidence": "pytest tests/process/test_process_reports.py -q",
            "reviewed_by": "agent-g2",
        }
    ]
    nodeids = [
        existing[0]["nodeid"],
        "tests/product-plane/unit/test_runtime_components.py::test_runtime",
    ]

    rows = merge_rows(nodeids, existing, static_signals_by_nodeid={})

    assert [row["nodeid"] for row in rows] == sorted(nodeids)
    assert rows[0]["audit_status"] == "reviewed"
    assert rows[0]["behavior_contract"] == "Report exposes the governed status totals."
    assert rows[1]["audit_status"] == "pending"
    assert rows[1]["decision"] == ""


def test_validate_rows_rejects_collection_drift_and_incomplete_reviews() -> None:
    rows = merge_rows(
        ["tests/process/test_process_reports.py::test_report"],
        [],
        static_signals_by_nodeid={
            "tests/process/test_process_reports.py::test_report": ("skip-site",)
        },
    )
    rows[0].update(
        audit_status="reviewed",
        decision="good",
        problem_codes="none",
        reviewed_by="agent-g2",
    )

    errors = validate_rows(
        rows,
        [
            "tests/process/test_process_reports.py::test_report",
            "tests/process/test_process_reports.py::test_new_report",
        ],
    )

    assert any("missing nodeids" in error for error in errors)
    assert any("behavior_contract" in error for error in errors)
    assert any("evidence" in error for error in errors)
    assert any("skip_policy" in error for error in errors)


def test_require_complete_rejects_unverified_remediation() -> None:
    nodeid = "tests/product-plane/unit/test_runtime_components.py::test_runtime"
    rows = merge_rows([nodeid], [], static_signals_by_nodeid={})
    rows[0].update(
        audit_status="reviewed",
        decision="rewrite",
        problem_codes="implementation-coupled",
        behavior_contract="Runtime enforces cooldown through its public command seam.",
        evidence="focused review recorded; rewrite still pending",
        reviewed_by="agent-x",
    )

    errors = validate_rows(rows, [nodeid], require_complete=True)

    assert any("remediation is not verified" in error for error in errors)

    rows[0]["audit_status"] = "verified"
    assert validate_rows(rows, [nodeid], require_complete=True) == []


def test_detect_file_signals_marks_skip_and_source_structure_checks(tmp_path: Path) -> None:
    test_file = tmp_path / "test_example.py"
    test_file.write_text(
        """import inspect
import pytest

def test_behavior():
    source = inspect.getsource(object)
    if not source:
        pytest.skip("runtime unavailable")
""",
        encoding="utf-8",
    )

    signals = detect_file_signals(test_file)

    assert signals["test_behavior"] == ("skip-site", "source-structure")


def test_cli_write_preserves_reviews_and_check_detects_drift(tmp_path: Path) -> None:
    collection = tmp_path / "nodeids.txt"
    matrix = tmp_path / "test-audit-matrix.csv"
    summary = tmp_path / "test-audit-summary.md"
    first = "tests/process/test_process_reports.py::test_report"
    second = "tests/product-plane/unit/test_runtime_components.py::test_runtime"
    third = "tests/product-plane/unit/test_moex_raw_ingest_errors.py::test_error"
    collection.write_text(f"{first}\n{second}\n", encoding="utf-8")

    write = _run(
        "--write",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        cwd=tmp_path,
    )
    assert write.returncode == 0, write.stdout + "\n" + write.stderr
    assert len(_read_rows(matrix)) == 2
    assert "generated-by: scripts/sync_test_audit_matrix.py" in summary.read_text(encoding="utf-8")

    rows = _read_rows(matrix)
    rows[0].update(
        audit_status="reviewed",
        decision="good",
        problem_codes="none",
        behavior_contract="Report exposes governed status totals.",
        evidence="pytest focused proof",
        reviewed_by="agent-g2",
    )
    _write_rows(matrix, rows)
    collection.write_text(f"{first}\n{second}\n{third}\n", encoding="utf-8")

    rewrite = _run(
        "--write",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        cwd=tmp_path,
    )
    assert rewrite.returncode == 0, rewrite.stdout + "\n" + rewrite.stderr
    assert _read_rows(matrix)[0]["behavior_contract"] == "Report exposes governed status totals."

    check = _run(
        "--check",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        cwd=tmp_path,
    )
    assert check.returncode == 0, check.stdout + "\n" + check.stderr

    collection.write_text(f"{first}\n{second}\n", encoding="utf-8")
    drift = _run(
        "--check",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        cwd=tmp_path,
    )
    assert drift.returncode != 0
    assert "stale nodeids" in (drift.stdout + drift.stderr)


def test_package_update_fragment_overlays_matrix_and_rejects_wrong_owner(tmp_path: Path) -> None:
    nodeid = "tests/process/test_process_reports.py::test_report"
    collection = tmp_path / "nodeids.txt"
    matrix = tmp_path / "test-audit-matrix.csv"
    summary = tmp_path / "test-audit-summary.md"
    updates = tmp_path / "updates"
    collection.write_text(f"{nodeid}\n", encoding="utf-8")

    update = {
        "nodeid": nodeid,
        "collection_state": "active",
        "audit_status": "reviewed",
        "decision": "good",
        "problem_codes": "none",
        "skip_policy": "none",
        "behavior_contract": "Report exposes governed status totals.",
        "evidence": "pytest focused proof",
        "reviewed_by": "agent-g2",
    }
    _write_update(updates / "G2.csv", update)

    write = _run(
        "--write",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        "--updates-dir",
        str(updates),
        cwd=tmp_path,
    )

    assert write.returncode == 0, write.stdout + "\n" + write.stderr
    assert _read_rows(matrix)[0]["behavior_contract"] == update["behavior_contract"]

    bad_updates = tmp_path / "bad-updates"
    _write_update(bad_updates / "X.csv", update)
    rejected = _run(
        "--write",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        "--updates-dir",
        str(bad_updates),
        cwd=tmp_path,
    )

    assert rejected.returncode != 0
    assert "belongs to G2, not X" in (rejected.stdout + rejected.stderr)


def test_invalid_update_does_not_mutate_generated_state(tmp_path: Path) -> None:
    nodeid = "tests/process/test_process_reports.py::test_report"
    collection = tmp_path / "nodeids.txt"
    matrix = tmp_path / "test-audit-matrix.csv"
    summary = tmp_path / "test-audit-summary.md"
    updates = tmp_path / "updates"
    collection.write_text(f"{nodeid}\n", encoding="utf-8")

    initial = _run(
        "--write",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        cwd=tmp_path,
    )
    assert initial.returncode == 0, initial.stdout + "\n" + initial.stderr
    matrix_before = matrix.read_bytes()
    summary_before = summary.read_bytes()

    _write_update(
        updates / "G2.csv",
        {
            "nodeid": nodeid,
            "collection_state": "active",
            "audit_status": "reviewed",
            "decision": "good",
            "problem_codes": "none",
            "skip_policy": "none",
            "behavior_contract": "",
            "evidence": "",
            "reviewed_by": "agent-g2",
        },
    )
    rejected = _run(
        "--write",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        "--updates-dir",
        str(updates),
        cwd=tmp_path,
    )

    assert rejected.returncode != 0
    assert "requires behavior_contract" in (rejected.stdout + rejected.stderr)
    assert matrix.read_bytes() == matrix_before
    assert summary.read_bytes() == summary_before


def test_update_fragment_requires_deterministic_nodeid_order(tmp_path: Path) -> None:
    first = "tests/process/test_agent_process_telemetry.py::test_metrics"
    second = "tests/process/test_process_reports.py::test_report"
    collection = tmp_path / "nodeids.txt"
    matrix = tmp_path / "test-audit-matrix.csv"
    summary = tmp_path / "test-audit-summary.md"
    update_path = tmp_path / "updates" / "G2.csv"
    collection.write_text(f"{first}\n{second}\n", encoding="utf-8")
    update_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for nodeid in (second, first):
        rows.append(
            {
                "nodeid": nodeid,
                "collection_state": "active",
                "audit_status": "reviewed",
                "decision": "good",
                "problem_codes": "none",
                "skip_policy": "none",
                "behavior_contract": "Observable process report behavior.",
                "evidence": "pytest focused proof",
                "reviewed_by": "agent-g2",
            }
        )
    with update_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=AUDIT_UPDATE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    rejected = _run(
        "--write",
        "--collection-file",
        str(collection),
        "--matrix",
        str(matrix),
        "--summary",
        str(summary),
        "--updates-dir",
        str(update_path.parent),
        cwd=tmp_path,
    )

    assert rejected.returncode != 0
    assert "nodeids must be sorted" in (rejected.stdout + rejected.stderr)
