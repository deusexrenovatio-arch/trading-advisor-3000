from __future__ import annotations

import json
from pathlib import Path

from dagster import Definitions

from trading_advisor_3000.app.data_plane import run_sample_backfill
from trading_advisor_3000.app.data_plane.delta_runtime import read_delta_table_rows
from trading_advisor_3000.dagster_defs import materialize_phase2a_assets
from trading_advisor_3000.dagster_defs import phase2a_assets as phase2a_assets_module


ROOT = Path(__file__).resolve().parents[3]
SOURCE_FIXTURE = ROOT / "tests" / "app" / "fixtures" / "data_plane" / "raw_backfill_sample.jsonl"
WHITELIST = {"BR-6.26", "Si-6.26"}
COMPARE_TABLES = (
    "canonical_bars",
    "canonical_instruments",
    "canonical_contracts",
    "canonical_session_calendar",
    "canonical_roll_map",
)


def _sorted_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(rows, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))


def test_phase2a_dagster_materialization_executes_and_matches_data_plane_outputs(tmp_path: Path) -> None:
    dagster_report = materialize_phase2a_assets(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path / "dagster",
        whitelist_contracts=WHITELIST,
    )
    python_report = run_sample_backfill(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path / "python",
        whitelist_contracts=WHITELIST,
    )

    assert dagster_report["success"] is True
    assert set(dagster_report["selected_assets"]) == set(phase2a_assets_module.PHASE2A_TABLES)
    assert set(dagster_report["materialized_assets"]) == set(phase2a_assets_module.PHASE2A_TABLES)
    rows_by_table = dict(dagster_report["rows_by_table"])
    assert rows_by_table["raw_market_backfill"] == 2
    assert rows_by_table["canonical_bars"] == 2
    for table_name in COMPARE_TABLES:
        dagster_path = Path(str(dagster_report["output_paths"][table_name]))
        python_path = Path(str(python_report["output_paths"][table_name]))
        assert (dagster_path / "_delta_log").exists()
        assert _sorted_rows(read_delta_table_rows(dagster_path)) == _sorted_rows(read_delta_table_rows(python_path))


def test_phase2a_dagster_partial_selection_materializes_only_selected_assets(tmp_path: Path) -> None:
    dagster_report = materialize_phase2a_assets(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path / "dagster-partial",
        whitelist_contracts=WHITELIST,
        selection=("canonical_bars",),
    )
    python_report = run_sample_backfill(
        source_path=SOURCE_FIXTURE,
        output_dir=tmp_path / "python-partial",
        whitelist_contracts=WHITELIST,
    )

    assert dagster_report["success"] is True
    assert dagster_report["selected_assets"] == ["canonical_bars"]
    assert set(dagster_report["materialized_assets"]) == {"raw_market_backfill", "canonical_bars"}

    rows_by_table = dict(dagster_report["rows_by_table"])
    assert set(rows_by_table) == {"raw_market_backfill", "canonical_bars"}
    assert rows_by_table["raw_market_backfill"] == 2
    assert rows_by_table["canonical_bars"] == 2

    bars_path = Path(str(dagster_report["output_paths"]["canonical_bars"]))
    python_bars_path = Path(str(python_report["output_paths"]["canonical_bars"]))
    assert _sorted_rows(read_delta_table_rows(bars_path)) == _sorted_rows(read_delta_table_rows(python_bars_path))

    assert (Path(str(dagster_report["output_paths"]["raw_market_backfill"])) / "_delta_log").exists()
    assert (Path(str(dagster_report["output_paths"]["canonical_bars"])) / "_delta_log").exists()

    for table_name in (
        "canonical_instruments",
        "canonical_contracts",
        "canonical_session_calendar",
        "canonical_roll_map",
    ):
        table_path = Path(str(dagster_report["output_paths"][table_name]))
        assert not (table_path / "_delta_log").exists()


def test_phase2a_dagster_disprover_fails_on_missing_source_fixture(tmp_path: Path) -> None:
    missing_source = tmp_path / "missing.jsonl"
    try:
        materialize_phase2a_assets(
            source_path=missing_source,
            output_dir=tmp_path / "dagster-broken",
            whitelist_contracts=WHITELIST,
        )
    except Exception as exc:
        message = str(exc)
        assert "missing.jsonl" in message
        assert "No such file or directory" in message
    else:
        raise AssertionError("expected Dagster materialization failure for missing source fixture")


def test_phase2a_dagster_disprover_fails_for_metadata_only_definitions(monkeypatch, tmp_path: Path) -> None:
    metadata_only_defs = Definitions(
        assets=[
            phase2a_assets_module.AssetSpec(
                key=table_name,
                description="metadata-only placeholder",
                inputs=tuple(),
                outputs=(f"{table_name}_delta",),
            )
            for table_name in phase2a_assets_module.PHASE2A_TABLES
        ]
    )
    monkeypatch.setattr(phase2a_assets_module, "phase2a_definitions", metadata_only_defs)
    try:
        materialize_phase2a_assets(
            source_path=SOURCE_FIXTURE,
            output_dir=tmp_path / "dagster-metadata-only",
            whitelist_contracts=WHITELIST,
        )
    except RuntimeError as exc:
        message = str(exc)
        assert "metadata-only or incomplete" in message
        assert "phase2a_materialization_job" in message
    else:
        raise AssertionError("expected metadata-only Dagster definitions to fail closure proof")
