from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from report_product_plane_module_imports import (  # noqa: E402
    MARKET_DATA_FOUNDATION,
    PUBLIC_API,
    RESEARCH_DATA_FACTORY,
    REVIEW_REQUIRED,
    RUNTIME_PLANE,
    STRATEGY_FACTORY,
    collect_import_records,
)


def _write_product_file(repo_root: Path, relative_path: str, text: str) -> None:
    path = repo_root / "src" / "trading_advisor_3000" / "product_plane" / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_inventory_classifies_runtime_to_strategy_import_as_review_required(
    tmp_path: Path,
) -> None:
    _write_product_file(
        tmp_path,
        "runtime/decision/engine.py",
        "from trading_advisor_3000.product_plane.research.ids import candidate_id\n",
    )

    records, parse_errors = collect_import_records(tmp_path)

    assert not parse_errors
    assert len(records) == 1
    record = records[0]
    assert record.origin_module == RUNTIME_PLANE
    assert record.target_module == STRATEGY_FACTORY
    assert record.classification == REVIEW_REQUIRED


def test_inventory_classifies_research_data_to_public_delta_helper_as_public_api(
    tmp_path: Path,
) -> None:
    _write_product_file(
        tmp_path,
        "research/indicators/store.py",
        "from trading_advisor_3000.product_plane.data_plane.delta_runtime import read_delta\n",
    )

    records, parse_errors = collect_import_records(tmp_path)

    assert not parse_errors
    assert len(records) == 1
    record = records[0]
    assert record.origin_module == RESEARCH_DATA_FACTORY
    assert record.target_module == MARKET_DATA_FOUNDATION
    assert record.classification == PUBLIC_API


def test_product_plane_module_import_inventory_cli_json_runs() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/report_product_plane_module_imports.py",
            "--format",
            "json",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + "\n" + result.stderr
    payload = json.loads(result.stdout)
    assert payload["report"] == "product_plane_module_import_inventory"
    assert payload["report_only"] is True
    assert payload["summary"]["status"] == "report_only"
    assert isinstance(payload["records"], list)


def test_runtime_does_not_import_research_candidate_id_helpers() -> None:
    records, parse_errors = collect_import_records(ROOT)

    assert not parse_errors
    offenders = [
        record
        for record in records
        if record.origin_module == RUNTIME_PLANE
        and record.imported_module == "trading_advisor_3000.product_plane.research.ids"
    ]
    assert not offenders


def test_runtime_plane_does_not_import_market_research_or_strategy_planes() -> None:
    records, parse_errors = collect_import_records(ROOT)

    assert not parse_errors
    forbidden_targets = {
        MARKET_DATA_FOUNDATION,
        RESEARCH_DATA_FACTORY,
        STRATEGY_FACTORY,
    }
    offenders = [
        record
        for record in records
        if record.origin_module == RUNTIME_PLANE and record.target_module in forbidden_targets
    ]
    assert not offenders


def test_runtime_and_execution_planes_do_not_depend_on_each_other_internally() -> None:
    records, parse_errors = collect_import_records(ROOT)

    assert not parse_errors
    forbidden_edges = {
        (RUNTIME_PLANE, "Execution Plane"),
        ("Execution Plane", RUNTIME_PLANE),
    }
    offenders = [
        record
        for record in records
        if (record.origin_module, record.target_module) in forbidden_edges
    ]
    assert not offenders


def test_product_plane_import_inventory_has_no_review_required_edges() -> None:
    records, parse_errors = collect_import_records(ROOT)

    assert not parse_errors
    offenders = [record for record in records if record.classification == REVIEW_REQUIRED]
    assert not offenders
