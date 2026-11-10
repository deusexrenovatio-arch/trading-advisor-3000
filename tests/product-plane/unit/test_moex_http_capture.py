"""Strict source capture is a correctness boundary, unlike diagnostic hooks."""

import io
from urllib.error import HTTPError
from urllib.parse import urlencode

import pytest

from trading_advisor_3000.product_plane.data_plane.moex import iss_client
from trading_advisor_3000.product_plane.data_plane.moex import observation_acquisition as intake


class Response(io.BytesIO):
    status = 200
    headers = {"Content-Type": "application/json"}

    def geturl(self):
        return "https://iss.moex.com/iss/test.json"


def test_capture_retains_original_bytes_before_decode_and_ignores_observer_failure(monkeypatch):
    original = b'{"price":1.23000000001, "label":"\\u0430"}'
    captures = []
    monkeypatch.setattr(iss_client.request, "urlopen", lambda *a, **k: Response(original))

    def broken_observer(_event):
        raise RuntimeError("diagnostic sink failure")

    client = iss_client.MoexISSClient(
        response_capture=lambda meta, body: captures.append((meta, body)),
        request_event_hook=broken_observer,
    )
    assert client._get_json("/iss/test.json", params={})["price"] == 1.23000000001
    assert captures[0][1] == original
    assert captures[0][0]["status"] == 200
    assert captures[0][0]["final_url"] == Response.geturl(None)


def test_malformed_json_is_still_captured_as_original_response(monkeypatch):
    captures = []
    monkeypatch.setattr(iss_client.request, "urlopen", lambda *a, **k: Response(b"not-json"))
    client = iss_client.MoexISSClient(
        max_retries=0, response_capture=lambda meta, body: captures.append(body)
    )
    with pytest.raises(iss_client.MoexRequestError):
        client._get_json("/iss/test.json", params={})
    assert captures == [b"not-json"]


def test_capture_sink_failure_aborts_without_retry_or_success(monkeypatch):
    requests = []

    def open_response(*args, **kwargs):
        requests.append(1)
        return Response(b"{}")

    def fail_capture(_metadata, _body):
        raise OSError("capture disk full")

    monkeypatch.setattr(iss_client.request, "urlopen", open_response)
    client = iss_client.MoexISSClient(max_retries=3, response_capture=fail_capture)
    with pytest.raises(iss_client.MoexCaptureError):
        client._get_json("/iss/test.json", params={})
    assert len(requests) == 1


def test_http_error_body_is_retained_before_failure(monkeypatch):
    captures = []

    def fail(*args, **kwargs):
        raise HTTPError(
            "https://iss.moex.com/iss/test.json",
            503,
            "Unavailable",
            {"Content-Type": "application/json"},
            io.BytesIO(b'{"error":"temporary"}'),
        )

    monkeypatch.setattr(iss_client.request, "urlopen", fail)
    client = iss_client.MoexISSClient(
        max_retries=0, response_capture=lambda meta, body: captures.append((meta["status"], body))
    )
    with pytest.raises(iss_client.MoexRequestError):
        client._get_json("/iss/test.json", params={})
    assert captures == [(503, b'{"error":"temporary"}')]


def test_response_byte_budget_is_fail_closed(monkeypatch):
    monkeypatch.setattr(iss_client.request, "urlopen", lambda *a, **k: Response(b"12345"))
    client = iss_client.MoexISSClient(max_response_bytes=4)
    with pytest.raises(iss_client.MoexCaptureError):
        client._get_json("/iss/test.json", params={})


@pytest.mark.parametrize("selector", ["interval", "from", "till", "start"])
def test_source_redirect_must_preserve_every_acquired_selector(tmp_path, monkeypatch, selector):
    capture = intake.OriginalResponseCapture(tmp_path, max_requests=4)
    context = {"provider": "moex_iss", "venue": "MOEX", "market": "shares", "board": "TQBR"}
    assignments = {
        "bindings": [{"source_symbol": "SYNTHETIC", "engine": "stock", "context": context}]
    }
    inputs = {"instruments": [{"source_symbol": "SYNTHETIC"}]}
    monkeypatch.setattr(intake, "verify_identity_inputs", lambda *a: None)
    monkeypatch.setattr(intake, "verified_assignments", lambda *a: assignments)
    url = "https://iss.moex.com/iss/securities/SYNTHETIC.json"
    capture({"request_url": url, "final_url": url, "status": 200}, b"{}")
    query = {"interval": 1, "from": "2026-09-14", "till": "2026-09-15", "start": 0}
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SYNTHETIC/candles.json?"
    altered = {**query, selector: "different"}
    capture(
        {
            "request_url": url + urlencode(query),
            "final_url": url + urlencode(altered),
            "status": 200,
        },
        b'{"candles":{"columns":[],"data":[[]]}}',
    )
    inputs_path, assignments_path, manifest_path = [
        tmp_path / name for name in ["inputs", "assignments", "manifest"]
    ]
    inputs_path.write_bytes(intake.canonical_bytes(inputs))
    assignments_path.write_bytes(intake.canonical_bytes(assignments))
    manifest = {
        "status": "acquired",
        "input_mode": "native_observations.v1",
        "identity_inputs": intake.pin(tmp_path, inputs_path),
        "metadata": {"SYNTHETIC": capture.records[0]},
        "limits": {
            "requests": 100,
            "rows": 20000,
            "pages_per_slice": 16,
            "response_bytes": 4194304,
        },
        "source_rows": 1,
        "window": {"date_from": "2026-09-14", "date_till": "2026-09-15"},
        "pages": [
            {
                **capture.records[1],
                "source_symbol": "SYNTHETIC",
                "timeframe": "1m",
                "context": context,
                "engine": "stock",
                "page_start": 0,
                "interval": 1,
            }
        ],
    }
    manifest_path.write_bytes(intake.canonical_bytes(manifest))
    with pytest.raises(ValueError, match="source request selector mismatch"):
        intake.verify_source_bundle(
            tmp_path, intake.pin(tmp_path, manifest_path), intake.pin(tmp_path, assignments_path)
        )
