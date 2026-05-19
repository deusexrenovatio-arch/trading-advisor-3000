from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.research.datasets import (
    research_dataset_store_contract,
)
from trading_advisor_3000.spark_jobs.research_derived_source_frames_job import (
    run_research_derived_source_frames_spark_job,
)


def _write_empty_l0_l1_tables(
    root: Path, *, indicator_columns: dict[str, str]
) -> tuple[Path, Path]:
    bar_path = root / "research_bar_views.delta"
    indicator_path = root / "research_indicator_frames.delta"
    write_delta_table_rows(
        table_path=bar_path,
        rows=[],
        columns=research_dataset_store_contract()["research_bar_views"]["columns"],
    )
    write_delta_table_rows(
        table_path=indicator_path,
        rows=[],
        columns={
            "dataset_version": "string",
            "contour_id": "string",
            "series_mode": "string",
            "series_id": "string",
            "indicator_set_version": "string",
            "profile_version": "string",
            "contract_id": "string",
            "instrument_id": "string",
            "timeframe": "string",
            "ts": "timestamp",
            **indicator_columns,
            "source_bars_hash": "string",
            "source_dataset_bars_hash": "string",
            "row_count": "int",
            "warmup_span": "int",
            "null_warmup_span": "int",
            "created_at": "timestamp",
            "output_columns_hash": "string",
        },
    )
    return bar_path, indicator_path


def test_source_frame_job_rejects_missing_required_indicator_columns_before_spark(
    tmp_path: Path,
) -> None:
    bar_path, indicator_path = _write_empty_l0_l1_tables(
        tmp_path, indicator_columns={"atr_14": "double"}
    )

    with pytest.raises(ValueError, match="requires source indicator columns: rsi_14"):
        run_research_derived_source_frames_spark_job(
            bar_views_path=bar_path,
            indicator_frames_path=indicator_path,
            output_dir=tmp_path,
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            derived_profile_version="core_v1",
            source_indicator_columns=("atr_14", "rsi_14"),
            spark_session_factory=lambda _app_name, _master: pytest.fail(
                "Spark should not start when required L1 columns are missing"
            ),
        )


def test_source_frame_job_fails_closed_when_spark_runtime_is_unavailable(
    tmp_path: Path,
) -> None:
    bar_path, indicator_path = _write_empty_l0_l1_tables(
        tmp_path, indicator_columns={"atr_14": "double", "rsi_14": "double"}
    )

    def _missing_spark(_app_name: str, _master: str) -> object:
        raise RuntimeError("spark unavailable")

    with pytest.raises(RuntimeError, match="spark unavailable"):
        run_research_derived_source_frames_spark_job(
            bar_views_path=bar_path,
            indicator_frames_path=indicator_path,
            output_dir=tmp_path,
            dataset_version="dataset-v1",
            contour_id="native_tradable",
            indicator_set_version="indicators-v1",
            derived_profile_version="core_v1",
            source_indicator_columns=("atr_14", "rsi_14"),
            spark_session_factory=_missing_spark,
        )
