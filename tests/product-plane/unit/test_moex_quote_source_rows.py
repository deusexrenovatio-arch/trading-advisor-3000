from __future__ import annotations

from datetime import date

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexISSClient


class _SourceClient(MoexISSClient):
    def __init__(self, pages):
        super().__init__()
        self.pages = pages
        self.starts = []

    def _get_json(self, path, *, params, event_context=None):
        assert path.endswith("/boards/SNDX/securities/IMOEX/candles.json")
        assert event_context["operation"] == "candle_source_rows"
        self.starts.append(int(params["start"]))
        return {"candles": self.pages[len(self.starts) - 1]}


def _read(client, **kwargs):
    return list(
        client.iter_candle_source_rows(
            engine="stock",
            market="index",
            board="SNDX",
            secid="IMOEX",
            interval=24,
            date_from=date(2026, 10, 1),
            date_till=date(2026, 10, 26),
            **kwargs,
        )
    )


def test_source_rows_preserve_null_fractional_volume_and_unknown_columns():
    columns = ["begin", "volume", "value", "extra"]
    client = _SourceClient(
        [
            {
                "columns": columns,
                "data": [
                    ["day1", None, 12.5, {"original": True}],
                    ["day2", 1.25, None, "untouched"],
                ],
            },
            {"columns": columns, "data": [["day3", 0, 0, None]]},
            {"columns": columns, "data": []},
        ]
    )
    assert _read(client) == [
        {"begin": "day1", "volume": None, "value": 12.5, "extra": {"original": True}},
        {"begin": "day2", "volume": 1.25, "value": None, "extra": "untouched"},
        {"begin": "day3", "volume": 0, "value": 0, "extra": None},
    ]
    assert client.starts == [0, 2, 3]


@pytest.mark.parametrize(
    "block",
    [
        {},
        {"columns": ["begin"]},
        {"columns": "begin", "data": []},
        {"columns": ["begin", "begin"], "data": []},
        {"columns": [1], "data": []},
        {"columns": [], "data": []},
        {"columns": ["begin"], "data": {}},
        {"columns": ["begin"], "data": [["one", "two"]]},
        {"columns": ["begin"], "data": [{"begin": "one"}]},
    ],
)
def test_source_rows_reject_malformed_blocks_without_silent_row_loss(block):
    with pytest.raises(ValueError, match="candles"):
        _read(_SourceClient([block]))


def test_source_rows_reject_repeated_pages():
    page = {"columns": ["begin"], "data": [["day1"]]}
    client = _SourceClient([page, page])
    with pytest.raises(ValueError, match="repeat"):
        _read(client)
    assert client.starts == [0, 1]


def test_source_rows_stop_at_request_budget():
    client = _SourceClient([{"columns": ["begin"], "data": [["day1"]]}])
    with pytest.raises(ValueError, match="max_pages"):
        _read(client, max_pages=1)
    assert client.starts == [0]


@pytest.mark.parametrize("max_pages", [0, -1, True, 1.5])
def test_source_rows_reject_invalid_request_budget(max_pages):
    client = _SourceClient([])
    with pytest.raises(ValueError, match="max_pages"):
        _read(client, max_pages=max_pages)
    assert not client.starts
