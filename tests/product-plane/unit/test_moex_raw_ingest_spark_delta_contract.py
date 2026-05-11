from __future__ import annotations

import inspect

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


def test_raw_ingest_reconcile_uses_window_scoped_delete_before_append() -> None:
    source = inspect.getsource(moex_raw_ingest_job.run_moex_raw_ingest_spark_delta_job)

    assert "source_rows_path" in source
    assert "unmatched_source_count" in source
    assert "raw source rows did not match declared window scopes" in source
    assert "left_anti" in source
    assert "_iter_window_delete_conditions" in source
    assert "toLocalIterator()" in source
    assert ".delete(" in source
    assert "windows_to_reconcile_df.collect()" not in source
    assert ".whenMatchedUpdateAll()" not in source


def test_raw_ingest_watermark_keys_include_source_interval() -> None:
    source = inspect.getsource(moex_raw_ingest_job._collect_post_watermarks)

    assert "KEY_SCOPE_COLUMNS" in source
    assert 'row["source_interval"]' in source
