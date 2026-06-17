from __future__ import annotations

import inspect
from datetime import UTC

from trading_advisor_3000.product_plane.data_plane.moex import foundation
from trading_advisor_3000.spark_jobs import moex_raw_ingest_job


def test_raw_ingest_fingerprint_contract_covers_source_metadata_and_provenance() -> None:
    fingerprint_columns = set(moex_raw_ingest_job.RAW_FINGERPRINT_COLUMNS)
    fingerprint_source = inspect.getsource(moex_raw_ingest_job._raw_fingerprint_expr)

    assert moex_raw_ingest_job.KEY_SCOPE_COLUMNS == ("internal_id", "timeframe", "moex_secid")
    assert moex_raw_ingest_job.RAW_KEY_COLUMNS == (
        "internal_id",
        "timeframe",
        "moex_secid",
        "ts_open",
        "ts_close",
    )
    assert set(moex_raw_ingest_job.RAW_SOURCE_TIMESTAMP_COLUMNS) == {"ts_open", "ts_close"}
    for column in [
        "finam_symbol",
        "moex_engine",
        "moex_market",
        "moex_board",
        "asset_group",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest",
        "provenance_json",
    ]:
        assert column in fingerprint_columns

    assert "ingest_run_id" not in fingerprint_columns
    assert "ingested_at_utc" not in fingerprint_columns
    assert "requested_target_timeframes" in moex_raw_ingest_job._VOLATILE_PROVENANCE_KEYS
    assert "discovery_url" in moex_raw_ingest_job._VOLATILE_PROVENANCE_KEYS
    for fragment in ("map_filter(", "map_entries(", "array_sort(", "map_from_entries("):
        assert fragment in fingerprint_source


def test_raw_ingest_reconcile_uses_window_scoped_merge_transaction() -> None:
    source = inspect.getsource(moex_raw_ingest_job.run_moex_raw_ingest_spark_delta_job)
    scoped_filter = inspect.getsource(moex_raw_ingest_job._filtered_raw_by_scopes)

    assert "source_rows_path" in source
    assert "FileNotFoundError" in source
    assert "unmatched_source_count" in source
    assert "raw source rows did not match declared window scopes" in source
    assert "left_anti" in source
    assert '_raw_reconcile_action", functions.lit("upsert")' in source
    assert '_raw_reconcile_action", functions.lit("delete")' in source
    assert ".merge(" in source
    assert ".whenMatchedDelete(" in source
    assert ".whenMatchedUpdate(" in source
    assert ".whenNotMatchedInsert(" in source
    assert "_build_window_delete_condition" not in source
    assert "toLocalIterator()" not in source
    assert ".whenNotMatchedBySourceDelete(" not in source
    assert ".delete(" not in source
    assert "windows_to_reconcile_df.collect()" not in source
    assert "windows_to_reconcile_df.toLocalIterator()" not in source
    assert "_filtered_raw_by_scopes(" in source
    assert "raw_existing," in source
    assert "scope_windows_df," in source
    assert "scope_payload," in source
    scope_rows_param = inspect.signature(moex_raw_ingest_job._filtered_raw_by_scopes).parameters[
        "scope_rows"
    ]
    assert scope_rows_param.default is None
    assert "_scope_pushdown_condition" in scoped_filter


def test_raw_ingest_tail_catchup_uses_spark_append_without_target_reconcile() -> None:
    source = inspect.getsource(moex_raw_ingest_job.run_moex_raw_ingest_spark_delta_job)

    assert "tail_append_only" in source
    assert "refresh_overlap_minutes == 0" in source
    assert "and bool(scope_payload)" in source
    assert 'all(scope.get("watermark_utc") is not None for scope in scope_payload)' not in source
    assert "_storage_frame(" in source
    assert "include_layout=target_uses_layout" in source
    assert '.mode("append")' in source
    assert ".save(str(table_path))" in source
    assert "watermark_by_key = _collect_tail_append_watermarks(" in source
    assert "elif table_exists and not tail_append_only:" in source


def test_foundation_raw_delta_mutation_delegates_to_spark_job() -> None:
    source = inspect.getsource(foundation.run_moex_raw_ingest_spark_delta_job)
    baseline_source = inspect.getsource(foundation.ingest_moex_baseline_window)
    bootstrap_source = inspect.getsource(foundation.ingest_moex_bootstrap_window)

    assert "trading_advisor_3000.spark_jobs.moex_raw_ingest_job" in source
    assert "_run_moex_raw_ingest_spark_delta_job(**kwargs)" in source
    for inspected in (source, baseline_source, bootstrap_source):
        assert "deltalake" not in inspected
        assert "write_delta_table_rows" not in inspected
        assert "DeltaTable" not in inspected


def test_raw_ingest_scope_pushdown_uses_literal_window_predicates() -> None:
    condition = moex_raw_ingest_job._scope_pushdown_condition(
        [
            {
                "internal_id": "FUT_WHEAT",
                "timeframe": "1m",
                "source_interval": 1,
                "moex_secid": "W4M6",
                "window_start_utc": "2026-06-08T20:40:00Z",
                "window_end_utc": "2026-06-09T20:40:00Z",
            }
        ]
    )

    assert "internal_id = 'FUT_WHEAT'" in condition
    assert "timeframe = '1m'" in condition
    assert "source_interval" not in condition
    assert "moex_secid = 'W4M6'" in condition
    assert "ts_close >= '2026-06-08T20:40:00Z'" in condition
    assert "ts_close <= '2026-06-09T20:40:00Z'" in condition
    assert "TIMESTAMP" not in condition


def test_raw_ingest_watermark_keys_exclude_source_interval() -> None:
    source = inspect.getsource(moex_raw_ingest_job._collect_post_watermarks)
    watermark_source = inspect.getsource(moex_raw_ingest_job.compute_raw_watermarks_spark_delta)
    watermark_signature = inspect.signature(moex_raw_ingest_job.compute_raw_watermarks_spark_delta)

    assert "KEY_SCOPE_COLUMNS" in source
    assert 'row["source_interval"]' not in source
    assert "min_ts_close_utc" in watermark_signature.parameters
    assert watermark_signature.parameters["min_ts_close_utc"].default is None
    assert "ts_close >=" in watermark_source
    assert "ts_close_year" in watermark_source


def test_raw_ingest_timestamp_parser_keeps_utc_for_spark_scope_values() -> None:
    parsed = moex_raw_ingest_job._parse_iso_utc("2026-06-09T21:00:00Z")
    naive = moex_raw_ingest_job._parse_iso_utc("2026-06-09T21:00:00")
    scope = moex_raw_ingest_job._normalize_scope(
        {
            "internal_id": "FUT_WHEAT",
            "timeframe": "1d",
            "source_interval": 24,
            "moex_secid": "W4Z6",
            "window_start_utc": "2026-06-08T21:00:00Z",
            "window_end_utc": "2026-06-09T21:00:00Z",
            "watermark_utc": "",
        },
        {},
    )

    assert parsed is not None
    assert parsed.tzinfo is UTC
    assert naive is not None
    assert naive.tzinfo is UTC
    assert scope["window_start_utc"].tzinfo is UTC
    assert scope["window_end_utc"].tzinfo is UTC


def test_foundation_stages_raw_source_rows_without_per_row_file_reopen() -> None:
    assert not hasattr(foundation, "_append_raw_source_row")

    for target in [
        foundation.ingest_moex_baseline_window,
        foundation.ingest_moex_bootstrap_window,
    ]:
        source = inspect.getsource(target)
        assert 'with source_rows_path.open("a", encoding="utf-8") as source_rows_handle' in source
        assert "_write_raw_source_row(" in source
        assert "source_rows_handle" in source
        assert "_append_raw_source_row" not in source
