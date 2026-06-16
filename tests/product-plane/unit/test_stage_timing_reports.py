from __future__ import annotations

import importlib
import inspect

import pytest

from trading_advisor_3000.product_plane.runtime.stage_timings import (
    StageTimings,
    record_skipped_stage,
    record_stage_timing,
    stage_timer,
)
from trading_advisor_3000.spark_jobs import (
    continuous_front_job,
    research_bar_views_job,
    research_derived_source_frames_job,
)

indicator_materialize = importlib.import_module(
    "trading_advisor_3000.product_plane.research.indicators.materialize"
)
derived_indicator_materialize = importlib.import_module(
    "trading_advisor_3000.product_plane.research.derived_indicators.materialize"
)


def test_stage_timing_payload_is_report_safe() -> None:
    stage_timings: StageTimings = {}

    started = stage_timer()
    record_stage_timing(stage_timings, "load_inputs", started, row_count=12)
    record_skipped_stage(stage_timings, "write_outputs", reason="all partitions reusable")

    assert stage_timings["load_inputs"]["status"] == "PASS"
    assert isinstance(stage_timings["load_inputs"]["elapsed_seconds"], float)
    assert stage_timings["load_inputs"]["row_count"] == 12
    assert stage_timings["write_outputs"] == {
        "status": "SKIPPED",
        "elapsed_seconds": 0.0,
        "reason": "all partitions reusable",
    }


@pytest.mark.parametrize(
    ("entrypoint", "expected_stages"),
    [
        (
            research_bar_views_job.run_research_bar_views_spark_job,
            (
                "build_usage_context",
                "build_pit_active_front_bar_views",
                "write_research_bar_views",
                "materialize_dataset_manifests",
            ),
        ),
        (
            continuous_front_job.run_continuous_front_spark_job,
            ("build_spark_native_tables", "write_staging_tables", "qc", "row_counts"),
        ),
        (
            research_derived_source_frames_job.run_research_derived_source_frames_spark_job,
            ("load_scoped_sources", "join_quality_counts", "write_source_frame", "row_counts"),
        ),
        (
            indicator_materialize._materialize_indicator_frames_unlocked,
            (
                "load_existing_metadata",
                "reuse_decision",
                "build_refresh_plan",
                "write_indicator_frames",
            ),
        ),
        (
            derived_indicator_materialize.materialize_derived_indicator_frames,
            ("build_source_frames_spark", "build_refresh_plan", "write_derived_indicator_frames"),
        ),
    ],
)
def test_downstream_reports_expose_named_stage_timings(
    entrypoint: object, expected_stages: tuple[str, ...]
) -> None:
    source = inspect.getsource(entrypoint)

    assert '"stage_timings": stage_timings' in source
    for stage in expected_stages:
        assert f'"{stage}"' in source
