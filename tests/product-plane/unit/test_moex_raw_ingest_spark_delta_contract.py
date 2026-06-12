from __future__ import annotations

import inspect
from datetime import UTC

from trading_advisor_3000.product_plane.data_plane.moex import foundation
from trading_advisor_3000.spark_jobs import moex_raw_ingest_job


def test_raw_ingest_fingerprint_contract_covers_source_metadata_and_provenance() -> None:
    fingerprint_columns = set(moex_raw_ingest_job.RAW_FINGERPRINT_COLUMNS)

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


def test_raw_ingest_reconcile_uses_window_scoped_merge_transaction() -> None:
    source = inspect.getsource(moex_raw_ingest_job.run_moex_raw_ingest_spark_delta_job)

    assert "source_rows_path" in source
    assert "FileNotFoundError" in source
    assert "unmatched_source_count" in source
    assert "raw source rows did not match declared window scopes" in source
    assert "left_anti" in source
    assert "_build_window_delete_condition" in source
    assert "toLocalIterator()" in source
    assert ".merge(" in source
    assert ".whenMatchedUpdateAll()" in source
    assert ".whenNotMatchedInsertAll()" in source
    assert ".whenNotMatchedBySourceDelete(" in source
    assert ".delete(" not in source
    assert '.mode("append")' not in source
    assert "windows_to_reconcile_df.collect()" not in source


def test_raw_ingest_watermark_keys_include_source_interval() -> None:
    source = inspect.getsource(moex_raw_ingest_job._collect_post_watermarks)

    assert "KEY_SCOPE_COLUMNS" in source
    assert 'row["source_interval"]' in source


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
