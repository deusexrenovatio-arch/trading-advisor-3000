from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    iter_delta_table_row_batches,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.datasets import (
    ResearchBarView,
    ResearchDatasetManifest,
    research_dataset_store_contract,
)
from trading_advisor_3000.spark_jobs import research_bar_views_job as spark_l0_job


def _view(*, contour_id: str, series_mode: str, series_id: str, contract_id: str) -> ResearchBarView:
    return ResearchBarView(
        dataset_version="dataset-v1",
        contour_id=contour_id,
        contract_id=contract_id,
        instrument_id="FUT_BR",
        timeframe="15m",
        ts="2026-04-01T10:00:00Z",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1_000,
        open_interest=2_000,
        session_date="2026-04-01",
        session_open_ts="2026-04-01T07:00:00Z",
        session_close_ts="2026-04-01T21:00:00Z",
        active_contract_id=contract_id,
        ret_1=None,
        log_ret_1=None,
        true_range=2.0,
        hl_range=2.0,
        oc_range=0.5,
        bar_index=0,
        slice_role="analysis",
        series_id=series_id,
        series_mode=series_mode,
    )


def test_research_bar_view_requires_contour_key() -> None:
    view = _view(
        contour_id="native_tradable",
        series_mode="contract",
        series_id="BR-6.26",
        contract_id="BR-6.26",
    )

    payload = view.to_dict()

    assert payload["contour_id"] == "native_tradable"
    assert ResearchBarView.from_dict(payload).contour_id == "native_tradable"


def test_research_dataset_contract_uses_contour_aware_l0_key() -> None:
    contract = research_dataset_store_contract()["research_bar_views"]

    assert "contour_id" in contract["columns"]
    assert contract["partition_by"] == ["dataset_version", "contour_id", "instrument_id", "timeframe"]
    assert (
        "unique(dataset_version, contour_id, series_mode, series_id, timeframe, ts)"
        in contract["constraints"]
    )


def test_legacy_python_bar_view_builder_is_removed_from_public_api() -> None:
    import trading_advisor_3000.product_plane.research.datasets as datasets

    assert not hasattr(datasets, "build_research_bar_views")


def test_inline_research_pipeline_route_is_removed(tmp_path) -> None:
    del tmp_path
    import importlib.util

    import trading_advisor_3000.product_plane.research as research

    assert not hasattr(research, "run_research_from_bars")
    assert importlib.util.find_spec("trading_advisor_3000.product_plane.research.pipeline") is None


def test_benchmark_python_l0_bootstrap_route_is_removed(tmp_path) -> None:
    del tmp_path
    import importlib.util

    import trading_advisor_3000.product_plane.research.jobs as research_jobs

    assert not hasattr(research_jobs, "run_benchmark_job")
    assert importlib.util.find_spec("trading_advisor_3000.product_plane.research.jobs.benchmark") is None


def test_spark_l0_writer_preserves_existing_delta_tables() -> None:
    source = Path("src/trading_advisor_3000/spark_jobs/research_bar_views_job.py").read_text(encoding="utf-8")

    assert "shutil.rmtree" not in source
    assert "_scoped_delete_condition(replace_scope)" in source
    assert "_research_l0_replace_filters(" in source
    assert ".merge(" in source
    assert ".whenMatchedUpdateAll()" in source
    assert ".whenNotMatchedInsertAll()" in source
    assert "\"total_rows_by_table\"" in source


def test_spark_l0_instrument_tree_preserves_business_identity_rules() -> None:
    source = Path("src/trading_advisor_3000/spark_jobs/research_bar_views_job.py").read_text(encoding="utf-8")

    assert "FUT_" in source
    assert "commodity" in source
    assert "index" in source
    assert "\"lineage_key\"" in source
    assert ".withColumn(\"universe_id\"" in source
    assert 'F.first("series_id").alias("internal_id")' not in source


def test_spark_l0_partial_replace_scope_tracks_request_filters() -> None:
    filters = spark_l0_job._research_l0_replace_filters(  # type: ignore[attr-defined]
        dataset_version="dataset-v1",
        contours=("native_tradable",),
        instrument_ids=("FUT_BR",),
        contract_ids=("BRM6@MOEX",),
        timeframes=("15m",),
        start_ts="2026-05-12T00:00:00Z",
        end_ts="2026-05-12T23:59:59Z",
    )

    condition = spark_l0_job._scoped_delete_condition(filters)  # type: ignore[attr-defined]

    assert condition == (
        "dataset_version = 'dataset-v1' AND contour_id IN ('native_tradable') "
        "AND instrument_id IN ('FUT_BR') AND contract_id IN ('BRM6@MOEX') "
        "AND timeframe IN ('15m') AND ts >= '2026-05-12T00:00:00Z' "
        "AND ts <= '2026-05-12T23:59:59Z'"
    )


def test_spark_l0_contract_id_filter_fails_closed_for_pit_contour() -> None:
    with pytest.raises(ValueError, match="contract_ids filter"):
        spark_l0_job._research_l0_replace_filters(  # type: ignore[attr-defined]
            dataset_version="dataset-v1",
            contours=("native_tradable", "pit_active_front"),
            instrument_ids=("FUT_BR",),
            contract_ids=("BRM6@MOEX",),
            timeframes=("15m",),
            start_ts=None,
            end_ts=None,
        )


def test_spark_l0_instrument_tree_scope_does_not_follow_time_slice() -> None:
    filters = spark_l0_job._research_instrument_tree_replace_filters(  # type: ignore[attr-defined]
        dataset_version="dataset-v1",
        contours=("pit_active_front",),
        instrument_ids=("FUT_BR",),
    )

    condition = spark_l0_job._scoped_delete_condition(filters)  # type: ignore[attr-defined]

    assert condition == (
        "dataset_version = 'dataset-v1' AND contour_id IN ('pit_active_front') "
        "AND instrument_id IN ('FUT_BR')"
    )
    assert "timeframe" not in condition
    assert "ts" not in condition


def test_hot_research_batch_iterator_requires_scope(tmp_path: Path) -> None:
    table_path = tmp_path / "research_bar_views.delta"
    columns = research_dataset_store_contract()["research_bar_views"]["columns"]
    write_delta_table_rows(
        table_path=table_path,
        columns=columns,
        rows=[
            _view(
                contour_id="native_tradable",
                series_mode="contract",
                series_id="BR-6.26",
                contract_id="BR-6.26",
            ).to_dict(),
            _view(
                contour_id="pit_active_front",
                series_mode="continuous_front",
                series_id="BR",
                contract_id="BR-6.26",
            ).to_dict(),
        ],
    )

    with pytest.raises(ValueError, match="batch iteration requires filters"):
        list(iter_delta_table_row_batches(table_path))

    rows = [
        row
        for batch in iter_delta_table_row_batches(
            table_path,
            filters=[
                ("dataset_version", "=", "dataset-v1"),
                ("contour_id", "=", "native_tradable"),
            ],
        )
        for row in batch
    ]

    assert len(rows) == 1
    assert rows[0]["contour_id"] == "native_tradable"


def test_dataset_manifest_records_stable_contour_metadata_without_daily_dataset_versions() -> None:
    manifest = ResearchDatasetManifest(
        dataset_version="dataset-v1",
        dataset_name="research materialized",
        universe_id="moex-futures",
        timeframes=("15m",),
        contour_id="native_tradable",
        series_mode="contract",
        source_delta_versions={"canonical_bars": 12},
        source_delta_hashes={"canonical_bars": "ABC123"},
        run_id="research-l0-001",
        as_of_ts="2026-04-01T21:00:00Z",
    )

    payload = manifest.to_dict()

    assert payload["dataset_version"] == "dataset-v1"
    assert payload["contour_id"] == "native_tradable"
    assert payload["run_id"] == "research-l0-001"
    assert payload["as_of_ts"] == "2026-04-01T21:00:00Z"
    assert payload["source_delta_versions_json"] == {"canonical_bars": 12}
    assert payload["source_delta_hashes_json"] == {"canonical_bars": "ABC123"}


def test_unknown_l0_contour_is_rejected() -> None:
    with pytest.raises(ValueError, match="contour_id"):
        ResearchDatasetManifest(
            dataset_version="dataset-v1",
            universe_id="moex-futures",
            timeframes=("15m",),
            contour_id="legacy_python_projection",
        )
