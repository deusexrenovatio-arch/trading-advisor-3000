from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from trading_advisor_3000.product_plane.data_plane import delta_runtime
from trading_advisor_3000.product_plane.research.indicators import materialize as module

START = datetime(2025, 12, 31, 23, 55, tzinfo=UTC)


def _stamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _bar(start: int, end: int, instrument: str = "FUT_BR", contract: str = "BR1"):
    return SimpleNamespace(
        instrument_id=instrument,
        contract_id=contract,
        active_contract_id=contract,
        ts=_stamp(START + timedelta(minutes=start)),
        bar_end_ts=_stamp(START + timedelta(minutes=end)),
        session_close_ts=None,
        timeframe="15m",
    )


def _write_source(path, rows, *, instrument="instrument_id", contract="contract_id", ts="ts"):
    columns = {
        instrument: "string",
        contract: "string",
        "timeframe": "string",
        ts: "timestamp",
        "low": "double",
        "high": "double",
        "volume": "bigint",
        "unused_payload": "string",
    }
    if contract == "finam_symbol":
        columns["moex_secid"] = "string"
    delta_runtime.write_delta_table_rows(table_path=path, rows=rows, columns=columns)
    assert (path / "_delta_log" / "00000000000000000000.json").is_file()


def _minute(index, *, instrument="FUT_BR", contract="BR1", aliases=None, timeframe="1m"):
    i_col, c_col, t_col = aliases or ("instrument_id", "contract_id", "ts")
    row = {
        i_col: instrument,
        c_col: contract,
        "timeframe": timeframe,
        t_col: _stamp(START + timedelta(minutes=index)),
        "low": 10.0,
        "high": 11.0,
        "volume": index + 1,
        "unused_payload": "not projected",
    }
    if c_col == "finam_symbol":
        row["moex_secid"] = "different-secid"
    return row


@pytest.mark.parametrize(
    "aliases",
    [
        ("instrument_id", "contract_id", "ts"),
        ("internal_id", "finam_symbol", "ts_open"),
        ("internal_id", "moex_secid", "ts_open"),
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
def test_native_sparse_union_preserves_pairs_boundaries_source_fields_and_hash(
    tmp_path: Path, aliases, reverse: bool
) -> None:
    bars = [
        _bar(0, 4),
        _bar(2, 6),
        _bar(6, 8),  # Overlap and touch must not duplicate a minute/hash row.
        _bar(11, 14),
        _bar(8, 10, contract="BR2"),
        _bar(4, 7, instrument="FUT_SI", contract="SI1"),
    ]
    pairs = [("FUT_BR", "BR1"), ("FUT_BR", "BR2"), ("FUT_SI", "SI1")]
    rows = [
        _minute(index, instrument=instrument, contract=contract, aliases=aliases)
        for instrument, contract in [*pairs, ("FUT_SI", "BR1"), ("FUT_BR", "SI1")]
        for index in range(16)
    ]
    rows += [_minute(index, aliases=aliases, timeframe="15m") for index in range(16)]
    rows[2]["low"] = rows[2]["volume"] = None
    null_ts = _minute(0, aliases=aliases)
    null_ts[aliases[2]] = None
    rows.append(null_ts)
    path = tmp_path / "minute_source.delta"
    _write_source(
        path,
        list(reversed(rows)) if reverse else rows,
        instrument=aliases[0],
        contract=aliases[1],
        ts=aliases[2],
    )

    result = module._load_volume_profile_source_rows(
        raw_1m_table_path=path, local_series=list(reversed(bars)) if reverse else bars
    )
    expected_indices = {
        ("FUT_BR", "BR1"): [*range(8), 11, 12, 13],
        ("FUT_BR", "BR2"): [8, 9],
        ("FUT_SI", "SI1"): [4, 5, 6],
    }
    assert set(result) == set(expected_indices)
    expected = {}
    for key, indices in expected_indices.items():
        assert [row[aliases[2]] for row in result[key]] == [
            _stamp(START + timedelta(minutes=index)) for index in indices
        ]
        projected = {aliases[0], aliases[1], aliases[2], "timeframe", "low", "high", "volume"}
        assert all(set(row) == projected | {"instrument_id", "contract_id"} for row in result[key])
        expected[key] = []
        for index in indices:
            row = _minute(index, instrument=key[0], contract=key[1], aliases=aliases)
            row.pop("unused_payload")
            if aliases[1] == "finam_symbol":
                row.pop("moex_secid")
            if key == ("FUT_BR", "BR1") and index == 2:
                row["low"] = row["volume"] = None
            row.update(instrument_id=key[0], contract_id=key[1])
            expected[key].append(row)
        assert result[key] == expected[key]
    assert sum(map(len, result.values())) == 16
    kwargs = dict(
        target_bars_hash="fixed-target",
        local_series=bars,
        tick_size_by_instrument={"FUT_BR": 0.5, "FUT_SI": 1.0},
    )
    assert module._volume_profile_source_hash(source_rows_by_series=result, **kwargs) == (
        module._volume_profile_source_hash(source_rows_by_series=expected, **kwargs)
    )


def test_many_sparse_intervals_use_bounded_exact_pair_dnf_and_native_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bars = [
        _bar(2 * index, 2 * index + 1, contract=f"BR{number:02d}")
        for number in range(65)
        for index in range(40)
    ]
    rows = [
        _minute(index, contract=f"BR{number:02d}") for number in range(65) for index in range(80)
    ]
    path = tmp_path / "minute_source.delta"
    _write_source(path, list(reversed(rows)))
    calls, batch_lengths = [], []

    def observe(path, *, columns, filters, **kwargs):
        assert isinstance(filters[0], list)
        assert 1 <= len(filters) <= 32
        assert columns == [
            "contract_id",
            "instrument_id",
            "timeframe",
            "ts",
            "low",
            "high",
            "volume",
        ]
        for branch in filters:
            assert len(branch) == 5
            assert branch[1] == ("instrument_id", "=", "FUT_BR")
            assert branch[2][0:2] == ("contract_id", "=")
        calls.append(filters)
        # Exercise actual Arrow record-batch boundaries with a lower test bound.
        for batch in delta_runtime.iter_delta_table_row_batches(
            path, columns=columns, filters=filters, batch_size=17
        ):
            assert len(batch) <= 17
            batch_lengths.append(len(batch))
            yield batch

    monkeypatch.setattr(module, "iter_delta_table_row_batches", observe, raising=False)
    monkeypatch.setattr(
        module,
        "read_delta_table_rows",
        lambda *_a, **_kw: pytest.fail("whole minute row-list read is forbidden"),
    )
    result = module._load_volume_profile_source_rows(raw_1m_table_path=path, local_series=bars)
    assert [len(groups) for groups in calls] == [32, 32, 1]
    assert batch_lengths and max(batch_lengths) == 17
    assert len(result) == 65
    for values in result.values():
        assert [row["ts"] for row in values] == [
            _stamp(START + timedelta(minutes=index)) for index in range(0, 80, 2)
        ]
    assert sum(map(len, result.values())) == 2600


def test_empty_target_series_does_not_open_delta(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        module, "delta_table_columns", lambda _: pytest.fail("empty series must not open Delta")
    )
    assert (
        module._load_volume_profile_source_rows(
            raw_1m_table_path=tmp_path / "missing.delta", local_series=[]
        )
        == {}
    )


@pytest.mark.parametrize("null_rows", [False, True])
def test_empty_or_null_native_source_retains_requested_empty_keys(
    tmp_path: Path, null_rows
) -> None:
    rows = []
    if null_rows:
        for column in ("ts", "instrument_id", "contract_id"):
            row = _minute(0)
            row[column] = None
            rows.append(row)
    path = tmp_path / "minute_source.delta"
    _write_source(path, rows)
    result = module._load_volume_profile_source_rows(
        raw_1m_table_path=path, local_series=[_bar(0, 4), _bar(8, 10, contract="BR2")]
    )
    assert result == {("FUT_BR", "BR1"): [], ("FUT_BR", "BR2"): []}
