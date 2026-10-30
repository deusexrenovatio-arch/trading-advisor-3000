"""Reader proof with hand-authored rows and real Delta versions, independent of Spark."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.contracts.market_quotes import (
    validate_quote_bar,
    validate_quote_series,
)
from trading_advisor_3000.product_plane.data_plane.moex import quote_pilot as acquisition
from trading_advisor_3000.product_plane.data_plane.moex import quote_pilot_reader as reader

FIXTURES = Path(__file__).resolve().parents[3] / "tests/product-plane/fixtures/contracts"
SOURCE_RUN = "accepted-source"
SBER = "MOEX:stock:shares:SBER:TQBR:source_native"
IMOEX = "MOEX:stock:index:IMOEX:SNDX:source_native"
SOURCE_DAYS = {
    "2026-06-01": ("2026-05-31T21:00:00Z", "2026-06-01T20:59:59Z"),
    "2026-06-02": ("2026-06-01T21:00:00Z", "2026-06-02T20:59:59Z"),
    "2026-06-03": ("2026-06-02T21:00:00Z", "2026-06-03T20:59:59Z"),
}


def _fixture(name):
    return json.loads((FIXTURES / f"market_quote_{name}.v1.json").read_text("utf-8"))


def _series(index=False):
    record = _fixture("series")
    if index:
        record.update(
            series_id=IMOEX,
            security_id="MOEX:stock:index:IMOEX",
            asset_class="index",
            market="index",
            board="SNDX",
            secid="IMOEX",
            price_unit="index_points",
            volume_unit="not_applicable",
            turnover_unit="not_applicable",
            tradable=False,
        )
    validate_quote_series(record)
    return record


def _bar(day="2026-06-01", *, close=101.0, index=False, source_run=SOURCE_RUN):
    record = _fixture("bar")
    record.update(
        series_id=IMOEX if index else SBER,
        ts_open=SOURCE_DAYS[day][0],
        ts_close=SOURCE_DAYS[day][1],
        source_begin=f"{day} 00:00:00",
        source_end=f"{day} 23:59:59",
        open=100.0,
        high=110.0,
        low=99.0,
        close=close,
        volume=None if index else 10.0,
        turnover=None if index else 1010.0,
        volume_status="not_applicable" if index else "reported",
        source_run_id=source_run,
    )
    validate_quote_bar(record)
    return record


def _arrow(rows):
    table = pa.Table.from_pylist(rows)
    # Delta needs a physical nullable type even when the entire column is null.
    for name, dtype in (
        ("open_interest", pa.int64()),
        ("underlying_security_id", pa.string()),
        ("volume", pa.float64()),
        ("turnover", pa.float64()),
    ):
        if name in table.column_names:
            table = table.set_column(
                table.schema.get_field_index(name),
                name,
                pa.array([row[name] for row in rows], type=dtype),
            )
    return table


def _json(path, payload):
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", "utf-8")


def _seal_source(path):
    hashes = {
        name: hashlib.sha256((path / name).read_bytes()).hexdigest()
        for name in ("series.jsonl", "candles.jsonl", "discovery.json")
    }
    _json(
        path / "acquisition.json",
        {
            "schema_version": "moex_quote_pilot_acquisition.v1",
            "run_id": SOURCE_RUN,
            "date_from": "2026-06-01",
            "date_till": "2026-06-03",
            "evidence_hashes": hashes,
        },
    )


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    root = (tmp_path / "pilot").resolve()
    monkeypatch.setattr(acquisition, "PILOT_ROOT", root)
    monkeypatch.setattr(reader, "REPO_ROOT", tmp_path.resolve())
    source = root / "source"
    source.mkdir(parents=True)
    descriptors = [_series(), _series(index=True)]
    bars = [_bar(), _bar("2026-06-02", close=102.0), _bar(index=True)]
    raw = []
    for bar in bars:
        source_payload = {
            "begin": bar["source_begin"],
            "end": bar["source_end"],
            "open": 100.0,
            "high": 110.0,
            "low": 99.0,
            "close": bar["close"],
            "volume": 777.0 if bar["series_id"] == IMOEX else 10.0,
            "value": 8888.0 if bar["series_id"] == IMOEX else 1010.0,
        }
        raw.append(
            {
                "series_id": bar["series_id"],
                "timeframe": "1d",
                "source_run_id": SOURCE_RUN,
                "fetched_at": bar["fetched_at"],
                "source_begin": bar["source_begin"],
                "payload_json": json.dumps(source_payload),
            }
        )
    for name, rows in (("series.jsonl", descriptors), ("candles.jsonl", raw)):
        (source / name).write_text("\n".join(map(json.dumps, rows)) + "\n", "utf-8")
    _json(source / "discovery.json", {"fixture": "reader-only, no chain discovery claim"})
    _seal_source(source)
    output = root / "materialized"
    tables = {}
    for name, rows in (
        ("raw_quote_candles", raw),
        ("quote_series", descriptors),
        ("quote_bars", bars),
    ):
        path = output / f"{name}.delta"
        write_deltalake(str(path), _arrow(rows))
        tables[name] = {"path": str(path), "version": 0, "row_count": len(rows)}
    report_path = output / "reports" / "accepted.json"
    report_path.parent.mkdir()
    report = {
        "schema_version": "moex_quote_pilot_report.v1",
        "status": "PASS",
        "run_id": "accepted",
        "report_path": str(report_path),
        "acquisition_dir": str(source),
        "source_run_id": SOURCE_RUN,
        "date_from": "2026-06-01",
        "date_till": "2026-06-03",
        "source_evidence": {
            name: hashlib.sha256((source / name).read_bytes()).hexdigest()
            for name in acquisition.EVIDENCE_FILES
        },
        "tables": tables,
        "per_series_counts": dict(Counter(bar["series_id"] for bar in bars)),
    }
    _json(report_path, report)
    return {
        "report": report,
        "path": report_path,
        "source": source,
        "bars": bars,
        "raw": raw,
        "descriptors": descriptors,
    }


def _read(candidate, **overrides):
    kwargs = {
        "report_path": candidate["path"],
        "series_id": SBER,
        "date_from": date(2026, 6, 1),
        "date_till": date(2026, 6, 3),
    }
    return reader.read_quote_pilot_bars(**(kwargs | overrides))


def test_reader_keeps_accepted_version_after_a_real_later_correction(candidate):
    path = candidate["report"]["tables"]["quote_bars"]["path"]
    corrected = [{**row, "close": 104.0} for row in candidate["bars"]]
    write_deltalake(path, _arrow(corrected), mode="overwrite")
    latest = DeltaTable(path)
    assert latest.version() == 1
    assert {row["close"] for row in latest.to_pyarrow_table().to_pylist()} == {104.0}
    assert [(row["ts_open"], row["close"]) for row in _read(candidate)] == [
        ("2026-05-31T21:00:00Z", 101.0),
        ("2026-06-01T21:00:00Z", 102.0),
    ]


def test_reader_filters_stale_source_runs_and_unrelated_series(candidate):
    stale = _bar("2026-06-03", close=107.0, source_run="earlier-source")
    path = candidate["report"]["tables"]["quote_bars"]["path"]
    write_deltalake(path, _arrow([stale]), mode="append")
    candidate["report"]["tables"]["quote_bars"]["version"] = 1
    _json(candidate["path"], candidate["report"])
    result = _read(candidate)
    assert [(row["series_id"], row["source_run_id"], row["close"]) for row in result] == [
        (SBER, SOURCE_RUN, 101.0),
        (SBER, SOURCE_RUN, 102.0),
    ]


def test_reader_enforces_source_calendar_date_selection_and_bounded_limit(candidate):
    result = _read(candidate, date_from=date(2026, 6, 2), date_till=date(2026, 6, 2))
    assert [(row["source_begin"], row["ts_open"], row["close"]) for row in result] == [
        ("2026-06-02 00:00:00", "2026-06-01T21:00:00Z", 102.0),
    ]
    limited = _read(candidate, limit=1)
    assert len(limited) == 1
    assert limited[0]["series_id"] == SBER
    assert limited[0]["close"] in (101.0, 102.0)


@pytest.mark.parametrize(
    "overrides",
    [
        {"limit": 0},
        {"limit": True},
        {"limit": 1001},
        {"timeframe": "1h"},
        {"series_id": "MOEX:stock:shares:GAZP:TQBR:source_native"},
        {"date_from": date(2026, 5, 31)},
        {"date_till": date(2026, 6, 4)},
        {"date_from": date(2026, 6, 3), "date_till": date(2026, 6, 2)},
    ],
)
def test_reader_rejects_unaccepted_or_unbounded_scope(candidate, overrides):
    with pytest.raises(ValueError):
        _read(candidate, **overrides)


@pytest.mark.parametrize("reseal", [False, True])
def test_reader_rejects_tampered_source_and_changed_accepted_evidence(candidate, reseal):
    path = candidate["source"] / "candles.jsonl"
    path.write_text(path.read_text("utf-8") + "{}\n", "utf-8")
    if reseal:
        _seal_source(candidate["source"])
    with pytest.raises(ValueError, match="evidence"):
        _read(candidate)


def test_reader_rejects_failed_report(candidate):
    _json(candidate["path"], {**candidate["report"], "status": "FAIL"})
    with pytest.raises(ValueError, match="accepted pilot report"):
        _read(candidate)


def test_reader_rejects_unavailable_raw_delta_version_even_when_typed_pins_are_valid(candidate):
    candidate["report"]["tables"]["raw_quote_candles"]["version"] = 999999
    _json(candidate["path"], candidate["report"])
    with pytest.raises(ValueError, match="accepted Delta version.*unavailable: raw_quote_candles"):
        _read(candidate)


def test_report_cannot_admit_a_foreign_series_present_in_the_shared_tables(candidate):
    foreign_id = "MOEX:stock:shares:GAZP:TQBR:source_native"
    descriptor = {
        **_series(),
        "series_id": foreign_id,
        "security_id": "MOEX:stock:shares:GAZP",
        "secid": "GAZP",
    }
    validate_quote_series(descriptor)
    foreign_bar = {**_bar(close=109.0), "series_id": foreign_id}
    validate_quote_bar(foreign_bar)
    for name, row in (("quote_series", descriptor), ("quote_bars", foreign_bar)):
        pin = candidate["report"]["tables"][name]
        write_deltalake(pin["path"], _arrow([row]), mode="append")
        pin["version"] = 1
        pin["row_count"] += 1
    candidate["report"]["per_series_counts"][foreign_id] = 1
    _json(candidate["path"], candidate["report"])
    with pytest.raises(ValueError, match="series scope differs from acquired scope"):
        _read(candidate, series_id=foreign_id)


def test_reader_rejects_a_valid_pinned_descriptor_that_differs_from_verified_source(candidate):
    changed = {
        **candidate["descriptors"][0],
        "metadata_json": '{"SECID":"SBER","CURRENCYID":"USD"}',
    }
    validate_quote_series(changed)  # Shape validity cannot substitute for source equality.
    pin = candidate["report"]["tables"]["quote_series"]
    write_deltalake(
        pin["path"],
        _arrow([changed, candidate["descriptors"][1]]),
        mode="overwrite",
    )
    pin["version"] = 1
    _json(candidate["path"], candidate["report"])
    with pytest.raises(ValueError, match="descriptor differs from source evidence"):
        _read(candidate)


@pytest.mark.parametrize("target", ["report", "acquisition", "table"])
def test_reader_rejects_namespace_escape(candidate, tmp_path, target):
    outside = tmp_path / "outside"
    if target == "report":
        with pytest.raises(ValueError, match="inside"):
            _read(candidate, report_path=outside / "accepted.json")
        return
    report = candidate["report"]
    if target == "acquisition":
        report["acquisition_dir"] = str(outside)
    else:
        report["tables"]["quote_bars"]["path"] = str(outside / "quote_bars.delta")
    _json(candidate["path"], report)
    with pytest.raises(ValueError, match="inside"):
        _read(candidate)


def test_index_raw_aggregates_do_not_leak_as_typed_trading_volume(candidate):
    raw_path = candidate["report"]["tables"]["raw_quote_candles"]["path"]
    raw = DeltaTable(raw_path).to_pyarrow_table().to_pylist()
    index_payload = json.loads(
        next(row for row in raw if row["series_id"] == IMOEX)["payload_json"]
    )
    assert (index_payload["volume"], index_payload["value"]) == (777.0, 8888.0)
    result = _read(candidate, series_id=IMOEX)
    assert len(result) == 1
    assert (result[0]["volume"], result[0]["turnover"], result[0]["volume_status"]) == (
        None,
        None,
        "not_applicable",
    )
    bad = {
        **candidate["bars"][-1],
        "volume": 777.0,
        "turnover": 8888.0,
        "volume_status": "reported",
    }
    validate_quote_bar(bad)  # Individually valid shape; descriptor determines index meaning.
    bars_path = candidate["report"]["tables"]["quote_bars"]["path"]
    write_deltalake(bars_path, _arrow([bad]), mode="overwrite")
    candidate["report"]["tables"]["quote_bars"]["version"] = 1
    _json(candidate["path"], candidate["report"])
    with pytest.raises(ValueError, match="index source aggregates"):
        _read(candidate, series_id=IMOEX)
