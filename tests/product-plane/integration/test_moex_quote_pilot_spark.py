"""Real Spark/Delta proof for typed quotes; no existing baseline inputs."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from trading_advisor_3000.product_plane.contracts.market_quotes import validate_quote_bar
from trading_advisor_3000.product_plane.data_plane.moex.quote_pilot import PILOT_ROOT
from trading_advisor_3000.spark_jobs import moex_quote_pilot_job as producer

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="real Spark/Delta proof runs in the Linux Docker profile"
)


def _descriptor(secid: str, asset: str = "equity") -> dict:
    engine, market, board = (
        ("futures", "forts", "RFUD")
        if asset == "futures"
        else ("stock", "index", "SNDX")
        if asset == "index"
        else ("stock", "shares", "TQBR")
    )
    index = asset == "index"
    return {
        "schema_version": "market_quote_series.v1",
        "series_id": f"MOEX:{engine}:{market}:{secid}:{board}:source_native",
        "security_id": f"MOEX:{engine}:{market}:{secid}",
        "source": "moex_iss",
        "asset_class": asset,
        "engine": engine,
        "market": market,
        "board": board,
        "secid": secid,
        "underlying_security_id": "MOEX:stock:shares:SBER" if asset == "futures" else None,
        "source_timezone": "Europe/Moscow",
        "price_basis": "source_native",
        "price_unit": "index_points" if index else "source_native",
        "volume_unit": "not_applicable" if index else "source_native",
        "turnover_unit": "not_applicable" if index else "source_native",
        "tradable": not index,
        "metadata_asof": "2026-06-02T20:00:00Z",
        "metadata_json": "{}",
        "aliases_json": "[]",
    }


def _acquisition(root: Path, run: str, specs: list[dict], *, close: float = 101) -> Path:
    path = root / run
    path.mkdir(parents=True)
    rows = [
        {
            "series_id": spec["series_id"],
            "timeframe": "1d",
            "source_run_id": run,
            "fetched_at": "2026-06-02T20:00:00Z",
            "payload_json": json.dumps(
                {
                    "open": 100,
                    "high": 105,
                    "low": 99,
                    "close": close,
                    "volume": 0 if spec["asset_class"] == "index" else 50,
                    "value": 0 if spec["asset_class"] == "index" else 5025,
                    "begin": "2026-06-01 00:00:00",
                    "end": "2026-06-01 23:59:59",
                }
            ),
        }
        for spec in specs
    ]
    (path / "series.jsonl").write_text("\n".join(map(json.dumps, specs)) + "\n", "utf-8")
    (path / "candles.jsonl").write_text("\n".join(map(json.dumps, rows)) + "\n", "utf-8")
    (path / "discovery.json").write_text("{}\n", "utf-8")
    _reseal(path, run)
    return path


def _reseal(path: Path, run: str) -> None:
    hashes = {
        name: hashlib.sha256((path / name).read_bytes()).hexdigest()
        for name in ("series.jsonl", "candles.jsonl", "discovery.json")
    }
    (path / "acquisition.json").write_text(
        json.dumps(
            {
                "schema_version": "moex_quote_pilot_acquisition.v1",
                "run_id": run,
                "date_from": "2026-06-01",
                "date_till": "2026-06-01",
                "evidence_hashes": hashes,
                "fetched_at": "2026-06-02T20:00:00Z",
                "source": "moex_iss",
                "timeframe": "1d",
                "series_count": len((path / "series.jsonl").read_text("utf-8").splitlines()),
                "source_row_count": len((path / "candles.jsonl").read_text("utf-8").splitlines()),
            }
        ),
        "utf-8",
    )


def _rows(report: dict, name: str) -> list[dict]:
    from deltalake import DeltaTable

    table = report["tables"][name]
    return DeltaTable(table["path"], version=table["version"]).to_pyarrow_table().to_pylist()


def test_real_spark_quotes_replay_correction_preserve_foreign_and_pinned_versions() -> None:
    root = PILOT_ROOT / "tests" / uuid4().hex
    specs = [
        _descriptor("SBER"),
        _descriptor("SRM6", "futures"),
        _descriptor("IMOEX", "index"),
        _descriptor("IMOEX2", "index"),
    ]
    source = _acquisition(root, "first", specs)
    output = root / "materialized"
    first = producer.run_moex_quote_pilot_spark_job(
        acquisition_dir=source, output_root=output, run_id="first"
    )
    assert first["tables"]["quote_bars"]["row_count"] == 4
    original = {row["series_id"]: row for row in _rows(first, "quote_bars")}
    for spec in specs:
        row = original[spec["series_id"]]
        validate_quote_bar(row)
        assert row["ts_open"] == "2026-05-31T21:00:00Z"
        assert row["ts_close"] == "2026-06-01T20:59:59Z"
        assert row["open_interest"] is None
        assert row["volume"] == (None if spec["asset_class"] == "index" else 50)
    raw_index = next(
        row
        for row in _rows(first, "raw_quote_candles")
        if row["series_id"] == specs[2]["series_id"]
    )
    assert json.loads(raw_index["payload_json"])["volume"] == 0
    replay = producer.run_moex_quote_pilot_spark_job(
        acquisition_dir=source, output_root=output, run_id="replay"
    )
    assert replay["tables"] == first["tables"]
    correction = _acquisition(root, "correction", specs[:1], close=103)
    corrected = producer.run_moex_quote_pilot_spark_job(
        acquisition_dir=correction, output_root=output, run_id="correction"
    )
    latest = {row["series_id"]: row for row in _rows(corrected, "quote_bars")}
    assert len(latest) == 4
    assert latest[specs[0]["series_id"]]["close"] == 103
    assert latest[specs[1]["series_id"]] == original[specs[1]["series_id"]]
    assert {row["series_id"]: row for row in _rows(first, "quote_bars")} == original
    assert corrected["tables"]["raw_quote_candles"]["row_count"] == 5
    with pytest.raises(FileExistsError, match="immutable"):
        producer.run_moex_quote_pilot_spark_job(
            acquisition_dir=source, output_root=output, run_id="first"
        )
    altered = json.loads((source / "candles.jsonl").read_text("utf-8").splitlines()[0])
    changed_payload = json.loads(altered["payload_json"])
    changed_payload["close"] = 102
    altered["payload_json"] = json.dumps(changed_payload)
    (source / "candles.jsonl").write_text(json.dumps(altered) + "\n", "utf-8")
    (source / "series.jsonl").write_text(json.dumps(specs[0]) + "\n", "utf-8")
    _reseal(source, "first")
    with pytest.raises(ValueError, match="immutable source candle"):
        producer.run_moex_quote_pilot_spark_job(
            acquisition_dir=source, output_root=output, run_id="source-id-reused"
        )
    from deltalake import DeltaTable

    for table in corrected["tables"].values():
        assert DeltaTable(table["path"]).version() == table["version"]


def test_invalid_batch_never_creates_delta_tables() -> None:
    root = PILOT_ROOT / "tests" / uuid4().hex
    source = _acquisition(root, "bad", [_descriptor("IMOEX", "index")])
    row = json.loads((source / "candles.jsonl").read_text("utf-8"))
    payload = json.loads(row["payload_json"])
    payload["volume"] = -1
    row["payload_json"] = json.dumps(payload)
    (source / "candles.jsonl").write_text(json.dumps(row) + "\n", "utf-8")
    _reseal(source, "bad")
    with pytest.raises(ValueError, match="numeric"):
        producer.run_moex_quote_pilot_spark_job(
            acquisition_dir=source, output_root=root / "materialized", run_id="bad"
        )
    assert not list((root / "materialized").glob("*.delta"))
    assert not (root / "materialized" / "reports" / "bad.json").exists()


def test_spark_rejects_conflicts_malformed_values_bounds_and_unmatched_series() -> None:
    root = PILOT_ROOT / "tests" / uuid4().hex
    spark = producer._create_spark_session("quote-invalid-source-proof", "local[2]")
    try:
        for case, changed, expected in (
            ("string-number", {"volume": "50"}, "numeric"),
            ("nonfinite", {"high": float("inf")}, "numeric"),
            ("ohlc", {"high": 100}, "OHLC"),
            ("dates", {"begin": "2026-06-02 00:00:00"}, "timestamp"),
            ("conflict", {"close": 103}, "conflicting raw"),
            ("unmatched", {}, "unmatched"),
        ):
            source = _acquisition(root, case, [_descriptor("SBER")])
            row = json.loads((source / "candles.jsonl").read_text("utf-8"))
            payload = json.loads(row["payload_json"])
            payload.update(changed)
            altered = {**row, "payload_json": json.dumps(payload)}
            if case == "unmatched":
                altered["series_id"] = "unknown"
            rows = [row, altered] if case == "conflict" else [altered]
            (source / "candles.jsonl").write_text("\n".join(map(json.dumps, rows)) + "\n", "utf-8")
            _reseal(source, case)
            with pytest.raises(ValueError, match=expected):
                producer._normalized_frames(
                    spark,
                    source,
                    {"run_id": case, "date_from": "2026-06-01", "date_till": "2026-06-01"},
                )
            spark.catalog.clearCache()
    finally:
        spark.stop()
