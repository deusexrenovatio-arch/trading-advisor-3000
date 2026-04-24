from __future__ import annotations

import json
from urllib import parse
from urllib.error import URLError
from datetime import date

from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexISSClient, MoexRequestError


class _AdaptiveSplitClient(MoexISSClient):
    def __init__(self, events: list[dict[str, object]]) -> None:
        super().__init__(request_event_hook=events.append)
        self.requests: list[dict[str, str]] = []

    def _get_json(  # type: ignore[override]
        self,
        path: str,
        *,
        params: dict[str, str],
        event_context: dict[str, object] | None = None,
    ) -> dict[str, object]:
        del path, event_context
        self.requests.append(dict(params))
        if params["from"] == "2026-04-01" and params["till"] == "2026-04-04":
            raise MoexRequestError(
                url="https://iss.moex.com/example",
                params=params,
                attempts=3,
                last_error=TimeoutError("timed out"),
            )
        if params["start"] != "0":
            return {
                "candles": {
                    "columns": ["open", "close", "high", "low", "volume", "begin", "end"],
                    "data": [],
                }
            }
        return {
            "candles": {
                "columns": ["open", "close", "high", "low", "volume", "begin", "end"],
                "data": [
                    [100.0, 101.0, 101.5, 99.5, 10, f"{params['from']} 10:00:00", f"{params['till']} 10:09:59"],
                ],
            }
        }


def test_iter_candles_splits_failing_window_and_recovers() -> None:
    events: list[dict[str, object]] = []
    client = _AdaptiveSplitClient(events)

    candles = list(
        client.iter_candles(
            engine="futures",
            market="forts",
            board="RFUD",
            secid="BRQ6",
            interval=1,
            date_from=date(2026, 4, 1),
            date_till=date(2026, 4, 4),
        )
    )

    assert len(candles) == 2
    requested_windows = {(row["from"], row["till"]) for row in client.requests if row["start"] == "0"}
    assert ("2026-04-01", "2026-04-04") in requested_windows
    assert ("2026-04-01", "2026-04-02") in requested_windows
    assert ("2026-04-03", "2026-04-04") in requested_windows
    assert any(event.get("event") == "moex_candles_chunk_split" for event in events)


class _JsonResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = json.dumps(payload)

    def __enter__(self) -> "_JsonResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False

    def read(self, *_args, **_kwargs) -> str:
        return self._payload


def test_history_board_securities_uses_stronger_retry_budget(monkeypatch) -> None:
    events: list[dict[str, object]] = []
    client = MoexISSClient(
        max_retries=1,
        retry_backoff_seconds=0.0,
        history_board_max_retries=4,
        history_board_retry_backoff_seconds=0.0,
        request_event_hook=events.append,
    )
    calls: list[tuple[str, str]] = []

    def _fake_urlopen(req, timeout: float):  # noqa: ANN001 - urllib signature
        url = getattr(req, "full_url", str(req))
        params = parse.parse_qs(parse.urlparse(url).query)
        start = params.get("start", ["0"])[0]
        calls.append((start, str(timeout)))
        if start == "0" and len([item for item in calls if item[0] == "0"]) <= 4:
            raise URLError("flaky history page")
        payload = {
            "history": {
                "columns": ["BOARDID", "TRADEDATE", "SECID", "SHORTNAME", "ASSETCODE", "VOLUME", "NUMTRADES"],
                "data": (
                    [["RFUD", "2026-04-01", "BRQ6", "Brent", "BR", 10, 1]]
                    if start == "0"
                    else []
                ),
            }
        }
        return _JsonResponse(payload)

    monkeypatch.setattr(
        "trading_advisor_3000.product_plane.data_plane.moex.iss_client.request.urlopen",
        _fake_urlopen,
    )
    monkeypatch.setattr(
        "trading_advisor_3000.product_plane.data_plane.moex.iss_client.time.sleep",
        lambda _seconds: None,
    )

    rows = client.fetch_history_board_securities(
        engine="futures",
        market="forts",
        board="RFUD",
        trade_date=date(2026, 4, 1),
    )

    assert [row.secid for row in rows] == ["BRQ6"]
    start_zero_calls = [item for item in calls if item[0] == "0"]
    assert len(start_zero_calls) == 5
    assert any(event.get("event") == "moex_history_page" for event in events)
    retry_events = [
        event
        for event in events
        if event.get("event") == "moex_http" and event.get("operation") == "history_board_securities"
    ]
    assert retry_events
    assert max(int(event.get("attempt_limit", 0)) for event in retry_events) == 5
