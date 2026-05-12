from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import run_moex_canonical_publish_spark as publish_script
from trading_advisor_3000.product_plane.data_plane.delta_runtime import write_delta_table_rows
from trading_advisor_3000.product_plane.data_plane.moex import (
    historical_canonical_route as route_module,
)
from trading_advisor_3000.product_plane.data_plane.moex.foundation import RAW_COLUMNS
from trading_advisor_3000.product_plane.data_plane.moex.historical_route_contracts import (
    build_raw_ingest_run_report_v2,
)


def _changed_raw_report() -> dict[str, object]:
    return build_raw_ingest_run_report_v2(
        run_id="phase01-pass",
        ingest_till_utc="2026-04-02T10:05:00Z",
        source_rows=1,
        incremental_rows=1,
        deduplicated_rows=0,
        stale_rows=0,
        watermark_by_key={"FUT_BR|1m|BRM6": "2026-04-02T10:05:00Z"},
        raw_table_path="raw_moex_history.delta",
        raw_ingest_progress_path="raw-progress.jsonl",
        raw_ingest_error_path="raw-errors.jsonl",
        raw_ingest_error_latest_path="raw-error.latest.json",
        changed_windows=[
            {
                "internal_id": "FUT_BR",
                "source_timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "BRM6",
                "window_start_utc": "2026-04-02T10:00:00Z",
                "window_end_utc": "2026-04-02T10:05:00Z",
                "incremental_rows": 1,
            }
        ],
    )


def _noop_raw_report() -> dict[str, object]:
    return build_raw_ingest_run_report_v2(
        run_id="phase01-noop",
        ingest_till_utc="2026-04-02T10:05:00Z",
        source_rows=0,
        incremental_rows=0,
        deduplicated_rows=0,
        stale_rows=0,
        watermark_by_key={},
        raw_table_path="raw_moex_history.delta",
        raw_ingest_progress_path="raw-progress.jsonl",
        raw_ingest_error_path="raw-errors.jsonl",
        raw_ingest_error_latest_path="raw-error.latest.json",
        changed_windows=[],
    )


def test_default_route_uses_scoped_spark_delta_publish_without_python_delta_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)
    staged_bars_path = tmp_path / "staged" / "canonical_bars.delta"
    staged_provenance_path = tmp_path / "staged" / "canonical_bar_provenance.delta"
    captured_publish: dict[str, object] = {}

    def _fake_spark_canonicalization(**_kwargs: object) -> dict[str, object]:
        return {
            "engine": "spark",
            "input_mode": "raw_delta",
            "source_rows": 1,
            "source_providers": ["moex_iss"],
            "unmatched_window_rows": 0,
            "changed_window_rows": 1,
            "selected_source_interval_rows": 6,
            "canonical_rows": 6,
            "provenance_rows": 6,
            "output_paths": {
                "canonical_bars": staged_bars_path.as_posix(),
                "canonical_bar_provenance": staged_provenance_path.as_posix(),
            },
            "spark_profile": {"master": "local[2]", "delta_writer": "spark"},
        }

    def _fake_spark_publish(**kwargs: object) -> dict[str, object]:
        captured_publish.update(kwargs)
        target_bars_path = Path(str(kwargs["target_bars_path"]))
        target_provenance_path = Path(str(kwargs["target_provenance_path"]))
        session_calendar_path = Path(str(kwargs["session_calendar_path"]))
        roll_map_path = Path(str(kwargs["roll_map_path"]))
        return {
            "run_id": kwargs["run_id"],
            "runtime_owner": "spark_delta",
            "status": "PASS",
            "publish_decision": "publish",
            "mutation_applied": True,
            "scoped_canonical_rows": 6,
            "canonical_rows": 6,
            "provenance_rows": 6,
            "qc_report": {
                "run_id": kwargs["run_id"],
                "runtime_owner": "spark_delta",
                "status": "PASS",
                "publish_decision": "publish",
                "failed_gates": [],
                "gate_results": [],
            },
            "contract_compatibility_report": {
                "runtime_owner": "spark_delta",
                "status": "PASS",
                "checked_rows": 6,
                "errors": [],
            },
            "sidecar_refresh": {
                "mode": "scoped",
                "mutation_applied": True,
                "refreshed_session_calendar_rows": 2,
                "refreshed_roll_map_rows": 2,
                "affected_session_rows": 1,
                "overlap_session_rows": 3,
                "overlap_policy": "affected_sessions_plus_minus_1_day",
            },
            "output_paths": {
                "canonical_bars": target_bars_path.as_posix(),
                "canonical_bar_provenance": target_provenance_path.as_posix(),
                "canonical_session_calendar": session_calendar_path.as_posix(),
                "canonical_roll_map": roll_map_path.as_posix(),
            },
            "delta_log": {
                "canonical_bars": {"path": target_bars_path.as_posix(), "delta_log": True},
                "canonical_bar_provenance": {
                    "path": target_provenance_path.as_posix(),
                    "delta_log": True,
                },
                "canonical_session_calendar": {
                    "path": session_calendar_path.as_posix(),
                    "delta_log": True,
                },
                "canonical_roll_map": {"path": roll_map_path.as_posix(), "delta_log": True},
            },
            "spark_profile": {"master": "local[2]", "delta_writer": "spark"},
        }

    assert not hasattr(route_module, "read_delta_table_rows")
    monkeypatch.setattr(route_module, "_run_spark_canonicalization", _fake_spark_canonicalization)
    monkeypatch.setattr(route_module, "_run_spark_canonical_publish", _fake_spark_publish)

    report = route_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonical",
        run_id="phase02-scoped-default",
        raw_ingest_run_report=_changed_raw_report(),
        repo_root=Path.cwd(),
    )

    assert report["status"] == "PASS"
    assert report["canonical_merge_strategy"] == route_module.CANONICAL_MERGE_SCOPED_DELETE_INSERT
    assert report["canonical_publish_engine"] == "spark_delta"
    assert report["contract_compatibility_report"]["checked_rows"] == 6
    assert report["mutation_applied"] is True
    assert report["sidecar_refresh"]["overlap_policy"] == "affected_sessions_plus_minus_1_day"
    assert report["real_bindings"] == ["moex_iss"]
    assert captured_publish["staged_bars_path"] == staged_bars_path
    assert captured_publish["staged_provenance_path"] == staged_provenance_path
    assert (
        str(captured_publish["publish_scope_path"])
        .replace("\\", "/")
        .endswith(".spark-canonicalization/publish-scope.jsonl")
    )


def test_route_blocks_when_spark_publish_contract_report_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)
    staged_bars_path = tmp_path / "staged" / "canonical_bars.delta"
    staged_provenance_path = tmp_path / "staged" / "canonical_bar_provenance.delta"

    def _fake_spark_canonicalization(**_kwargs: object) -> dict[str, object]:
        return {
            "engine": "spark",
            "input_mode": "raw_delta",
            "source_rows": 1,
            "source_providers": ["moex_iss"],
            "unmatched_window_rows": 0,
            "changed_window_rows": 1,
            "selected_source_interval_rows": 6,
            "canonical_rows": 6,
            "provenance_rows": 6,
            "output_paths": {
                "canonical_bars": staged_bars_path.as_posix(),
                "canonical_bar_provenance": staged_provenance_path.as_posix(),
            },
            "spark_profile": {"master": "local[2]", "delta_writer": "spark"},
        }

    def _fake_spark_publish(**kwargs: object) -> dict[str, object]:
        return {
            "run_id": kwargs["run_id"],
            "runtime_owner": "spark_delta",
            "status": "PASS",
            "publish_decision": "publish",
            "mutation_applied": False,
            "canonical_rows": 6,
            "provenance_rows": 6,
            "qc_report": {
                "run_id": kwargs["run_id"],
                "runtime_owner": "spark_delta",
                "status": "PASS",
                "publish_decision": "publish",
                "failed_gates": [],
                "gate_results": [],
            },
        }

    monkeypatch.setattr(route_module, "_run_spark_canonicalization", _fake_spark_canonicalization)
    monkeypatch.setattr(route_module, "_run_spark_canonical_publish", _fake_spark_publish)

    report = route_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonical",
        run_id="phase02-missing-contract-report",
        raw_ingest_run_report=_changed_raw_report(),
        repo_root=Path.cwd(),
    )

    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert report["qc_report"]["status"] == "PASS"
    assert report["contract_compatibility_report"]["status"] == "FAIL"
    assert report["contract_compatibility_report"]["errors"] == [
        "spark publish report missing contract_compatibility_report"
    ]


def test_route_blocks_when_spark_publish_qc_report_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)
    staged_bars_path = tmp_path / "staged" / "canonical_bars.delta"
    staged_provenance_path = tmp_path / "staged" / "canonical_bar_provenance.delta"

    def _fake_spark_canonicalization(**_kwargs: object) -> dict[str, object]:
        return {
            "engine": "spark",
            "input_mode": "raw_delta",
            "source_rows": 1,
            "source_providers": ["moex_iss"],
            "unmatched_window_rows": 0,
            "changed_window_rows": 1,
            "selected_source_interval_rows": 6,
            "canonical_rows": 6,
            "provenance_rows": 6,
            "output_paths": {
                "canonical_bars": staged_bars_path.as_posix(),
                "canonical_bar_provenance": staged_provenance_path.as_posix(),
            },
            "spark_profile": {"master": "local[2]", "delta_writer": "spark"},
        }

    def _fake_spark_publish(**kwargs: object) -> dict[str, object]:
        return {
            "run_id": kwargs["run_id"],
            "runtime_owner": "spark_delta",
            "status": "PASS",
            "publish_decision": "publish",
            "mutation_applied": False,
            "canonical_rows": 6,
            "provenance_rows": 6,
            "qc_report": {
                "run_id": kwargs["run_id"],
                "runtime_owner": "spark_delta",
                "status": "FAIL",
                "publish_decision": "blocked",
                "failed_gates": ["row_count_match"],
                "gate_results": [],
            },
            "contract_compatibility_report": {
                "runtime_owner": "spark_delta",
                "status": "PASS",
                "checked_rows": 6,
                "errors": [],
            },
        }

    monkeypatch.setattr(route_module, "_run_spark_canonicalization", _fake_spark_canonicalization)
    monkeypatch.setattr(route_module, "_run_spark_canonical_publish", _fake_spark_publish)

    report = route_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonical",
        run_id="phase02-failing-qc-report",
        raw_ingest_run_report=_changed_raw_report(),
        repo_root=Path.cwd(),
    )

    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert report["qc_report"]["status"] == "FAIL"
    assert report["contract_compatibility_report"]["status"] == "PASS"


def test_noop_route_marks_contract_report_skipped_not_passed(tmp_path: Path) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)

    report = route_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonical",
        run_id="phase02-noop-contract",
        raw_ingest_run_report=_noop_raw_report(),
        repo_root=Path.cwd(),
    )

    contract_report = report["contract_compatibility_report"]
    assert report["status"] == "PASS-NOOP"
    assert report["publish_decision"] == "publish"
    assert contract_report["status"] == "SKIPPED"
    assert contract_report["checked_rows"] == 0
    assert contract_report["reason"] == "noop refresh did not produce Spark canonical rows"


def test_noop_route_blocks_when_runtime_decoupling_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)
    monkeypatch.setattr(
        route_module,
        "run_runtime_decoupling_check",
        lambda *, repo_root: {"status": "FAIL", "errors": ["runtime leak"]},
    )

    report = route_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonical",
        run_id="phase02-noop-runtime-blocked",
        raw_ingest_run_report=_noop_raw_report(),
        repo_root=Path.cwd(),
    )

    assert report["status"] == "BLOCKED"
    assert report["publish_decision"] == "blocked"
    assert report["qc_report"]["status"] == "FAIL"
    assert report["qc_report"]["failed_gates"] == ["runtime_decoupling"]
    assert report["contract_compatibility_report"]["status"] == "SKIPPED"
    assert report["runtime_decoupling_proof"]["status"] == "FAIL"


def test_route_rejects_blank_spark_output_paths_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)

    def _fake_spark_canonicalization(**_kwargs: object) -> dict[str, object]:
        return {
            "engine": "spark",
            "input_mode": "raw_delta",
            "source_rows": 1,
            "source_providers": ["moex_iss"],
            "unmatched_window_rows": 0,
            "changed_window_rows": 1,
            "selected_source_interval_rows": 6,
            "canonical_rows": 6,
            "provenance_rows": 6,
            "output_paths": {
                "canonical_bars": "   ",
                "canonical_bar_provenance": "",
            },
            "spark_profile": {"master": "local[2]", "delta_writer": "spark"},
        }

    def _publish_must_not_run(**_kwargs: object) -> dict[str, object]:
        raise AssertionError("publish must not run with blank Spark output paths")

    monkeypatch.setattr(route_module, "_run_spark_canonicalization", _fake_spark_canonicalization)
    monkeypatch.setattr(route_module, "_run_spark_canonical_publish", _publish_must_not_run)

    with pytest.raises(RuntimeError, match="Spark output paths"):
        route_module.run_historical_canonical_route(
            raw_table_path=raw_table_path,
            output_dir=tmp_path / "canonical",
            run_id="phase02-blank-spark-output",
            raw_ingest_run_report=_changed_raw_report(),
            repo_root=Path.cwd(),
        )


def test_route_keeps_contract_report_not_run_when_pre_publish_gate_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)
    staged_bars_path = tmp_path / "staged" / "canonical_bars.delta"
    staged_provenance_path = tmp_path / "staged" / "canonical_bar_provenance.delta"

    def _fake_spark_canonicalization(**_kwargs: object) -> dict[str, object]:
        return {
            "engine": "spark",
            "input_mode": "raw_delta",
            "source_rows": 1,
            "source_providers": ["moex_iss"],
            "unmatched_window_rows": 0,
            "changed_window_rows": 1,
            "selected_source_interval_rows": 6,
            "canonical_rows": 6,
            "provenance_rows": 6,
            "output_paths": {
                "canonical_bars": staged_bars_path.as_posix(),
                "canonical_bar_provenance": staged_provenance_path.as_posix(),
            },
            "spark_profile": {"master": "local[2]", "delta_writer": "spark"},
        }

    def _publish_must_not_run(**_kwargs: object) -> dict[str, object]:
        raise AssertionError("publish must not run when runtime decoupling fails")

    monkeypatch.setattr(route_module, "_run_spark_canonicalization", _fake_spark_canonicalization)
    monkeypatch.setattr(route_module, "_run_spark_canonical_publish", _publish_must_not_run)
    monkeypatch.setattr(
        route_module,
        "run_runtime_decoupling_check",
        lambda *, repo_root: {"status": "FAIL", "errors": ["runtime leak"]},
    )

    report = route_module.run_historical_canonical_route(
        raw_table_path=raw_table_path,
        output_dir=tmp_path / "canonical",
        run_id="phase02-pre-publish-contract-not-run",
        raw_ingest_run_report=_changed_raw_report(),
        repo_root=Path.cwd(),
    )

    assert report["publish_decision"] == "blocked"
    assert report["contract_compatibility_report"]["status"] == "NOT_RUN"
    assert report["contract_compatibility_report"]["checked_rows"] == 6


def test_full_overwrite_is_not_supported_even_with_legacy_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_table_path = tmp_path / "raw_moex_history.delta"
    write_delta_table_rows(table_path=raw_table_path, rows=[], columns=RAW_COLUMNS)
    monkeypatch.setenv("TA3000_MOEX_ALLOW_LEGACY_FULL_OVERWRITE_CANONICAL_ROUTE", "1")

    with pytest.raises(ValueError, match="only supports scoped_delete_insert"):
        route_module.run_historical_canonical_route(
            raw_table_path=raw_table_path,
            output_dir=tmp_path / "canonical",
            run_id="phase02-legacy-blocked",
            raw_ingest_run_report=_changed_raw_report(),
            canonical_merge_strategy="full_overwrite",
        )


def test_docker_publish_requires_output_json_before_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _unexpected_subprocess(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("docker container must not start without --output-json")

    monkeypatch.setattr(publish_script.subprocess, "run", _unexpected_subprocess)

    with pytest.raises(RuntimeError, match="requires --output-json"):
        publish_script._run_docker(SimpleNamespace(output_json=""))


def test_docker_report_hostifies_publish_protocol_paths() -> None:
    report = publish_script._hostify_report(
        {
            "publish_protocol": {
                "recovery_manifest_path": "/workspace/out/recovery.json",
                "publish_scope_path": "/workspace/out/publish-scope.jsonl",
                "target_paths": {"canonical_bars": "/workspace/data/canonical_bars.delta"},
                "staged_paths": {"canonical_bar_provenance": "/workspace/staged/provenance.delta"},
                "nested": {"path": "/workspace/nested/path.delta"},
            }
        }
    )

    protocol = report["publish_protocol"]
    assert isinstance(protocol, dict)
    assert str(protocol["recovery_manifest_path"]).replace("\\", "/").endswith("/out/recovery.json")
    assert (
        str(protocol["publish_scope_path"]).replace("\\", "/").endswith("/out/publish-scope.jsonl")
    )
    assert (
        str(protocol["target_paths"]["canonical_bars"])
        .replace("\\", "/")
        .endswith("/data/canonical_bars.delta")
    )
    assert (
        str(protocol["staged_paths"]["canonical_bar_provenance"])
        .replace("\\", "/")
        .endswith("/staged/provenance.delta")
    )
    assert str(protocol["nested"]["path"]).replace("\\", "/").endswith("/nested/path.delta")


def test_spark_publish_subprocess_timeout_is_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _timeout(*_args: object, **_kwargs: object) -> object:
        raise subprocess.TimeoutExpired(cmd=["publish"], timeout=1)

    monkeypatch.setenv("TA3000_MOEX_CANONICALIZATION_SPARK_PROFILE", "docker")
    monkeypatch.setattr(route_module.subprocess, "run", _timeout)

    with pytest.raises(RuntimeError, match="spark canonical publish failed: subprocess timed out"):
        route_module._run_spark_canonical_publish(
            staged_bars_path=tmp_path / "staged_bars.delta",
            staged_provenance_path=tmp_path / "staged_provenance.delta",
            publish_scope_path=tmp_path / "publish-scope.jsonl",
            target_bars_path=tmp_path / "target_bars.delta",
            target_provenance_path=tmp_path / "target_provenance.delta",
            session_calendar_path=tmp_path / "session_calendar.delta",
            roll_map_path=tmp_path / "roll_map.delta",
            output_dir=tmp_path / "out",
            run_id="phase02-publish-timeout",
            repo_root=Path.cwd(),
        )
