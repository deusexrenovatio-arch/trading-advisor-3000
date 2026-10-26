"""Marked staging outputs cannot be reached by a bare native producer call."""

from __future__ import annotations

import importlib
import json

import pytest


@pytest.mark.parametrize(
    "module_name,function_name,options",
    [
        (
            "moex_raw_ingest_job",
            "run_moex_raw_ingest_spark_delta_job",
            {
                "table_path": "@raw.delta",
                "window_scopes": [],
                "initial_watermarks": {},
                "run_id": "bare",
                "ingest_till_utc": "2026-10-24T00:00:00Z",
                "refresh_overlap_minutes": 0,
                "progress_path": "@progress.json",
                "progress_latest_path": "@latest.json",
                "error_path": "@error.json",
                "error_latest_path": "@error-latest.json",
            },
        ),
        (
            "moex_canonicalization_job",
            "run_moex_canonicalization_spark_delta_job",
            {
                "raw_table_path": "@raw.delta",
                "changed_windows_path": "@windows.jsonl",
                "selected_source_intervals_path": "@intervals.jsonl",
                "session_intervals_path": None,
                "output_dir": "@canonical",
                "build_run_id": "bare",
                "built_at_utc": "2026-10-24T00:00:00Z",
            },
        ),
        (
            "moex_canonical_publish_job",
            "run_moex_canonical_publish_spark_delta_job",
            {
                "staged_bars_path": "@bars.delta",
                "staged_provenance_path": "@provenance.delta",
                "target_bars_path": "@target.delta",
                "target_provenance_path": "@target-prov.delta",
                "session_calendar_path": "@calendar.delta",
                "session_intervals_path": None,
                "roll_map_path": "@roll.delta",
                "output_dir": "@publish",
                "run_id": "bare",
            },
        ),
        (
            "moex_contract_economics_job",
            "run_moex_contract_economics_spark_job",
            {
                "raw_contract_specs_path": "@specs.delta",
                "raw_fx_rates_path": "@fx.delta",
                "output_dir": "@economics",
            },
        ),
        (
            "rebuild_storage_layout_job",
            "run_rebuild_storage_layout_spark_job",
            {
                "layout_manifest_path": "@layout.yaml",
                "source_root": "@source",
                "target_root": "@target",
                "report_path": "@layout.json",
                "run_id": "bare",
                "published_current_root": "@current",
            },
        ),
    ],
)
def test_bare_native_calls_refuse_marked_staging_before_spark(
    tmp_path,
    monkeypatch,
    module_name,
    function_name,
    options,
):
    root = tmp_path / "stage"
    root.mkdir()
    (root / ".ta3000-staging.json").write_text(
        json.dumps(
            {
                "schema_version": "staging_root.v1",
                "plan_path": str(tmp_path / "missing-plan.json"),
                "plan_digest": "0" * 64,
            }
        ),
        encoding="utf-8",
    )
    module = importlib.import_module("trading_advisor_3000.spark_jobs." + module_name)

    def forbidden(*args, **kwargs):
        pytest.fail("bare staging producer reached Spark")

    monkeypatch.setattr(module, "_ensure_spark_delta_runtime_available", forbidden, raising=False)
    monkeypatch.setattr(module, "_create_spark_session", forbidden, raising=False)
    values = {
        key: root / value[1:] if isinstance(value, str) and value.startswith("@") else value
        for key, value in options.items()
    }
    with pytest.raises((ValueError, RuntimeError), match="(?i)staging|protected|plan"):
        getattr(module, function_name)(**values)


@pytest.mark.parametrize("escaped_target", ["session_calendar_path", "roll_map_path"])
def test_canonical_publish_rejects_support_write_outside_staging_before_spark(
    tmp_path, monkeypatch, escaped_target
):
    from types import SimpleNamespace

    from trading_advisor_3000.product_plane.common import staging_execution as guard
    from trading_advisor_3000.spark_jobs import moex_canonical_publish_job as publish

    root = tmp_path / "staging"
    plan = {"data_root": str(root), "scope": {}}
    monkeypatch.setattr(guard, "_plan", lambda path: plan)
    # Isolate the publication boundary; run/quota ownership has separate tests.
    for name in ("require_staging_compute", "_require_permit", "_deadline", "_live_run"):
        monkeypatch.setattr(guard, name, lambda *args, **kwargs: None)

    def no_spark(*args, **kwargs):
        pytest.fail("escaped support target reached Spark")

    monkeypatch.setattr(publish, "_ensure_spark_delta_runtime_available", no_spark)
    options = {
        key: root / f"{key}.delta"
        for key in (
            "staged_bars_path",
            "staged_provenance_path",
            "target_bars_path",
            "target_provenance_path",
            "session_calendar_path",
            "roll_map_path",
        )
    }
    options[escaped_target] = tmp_path / "production" / f"{escaped_target}.delta"
    token = guard._ACTIVE.set(SimpleNamespace(plan=plan, reservation=None))
    try:
        with pytest.raises(guard.StagingExecutionError, match="write path outside"):
            publish.run_moex_canonical_publish_spark_delta_job(
                **options,
                session_intervals_path=None,
                output_dir=root / "publish",
                run_id="fixture",
            )
    finally:
        guard._ACTIVE.reset(token)
