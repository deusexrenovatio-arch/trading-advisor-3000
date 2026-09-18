from dataclasses import replace
from threading import Barrier, Lock

import pytest

from trading_advisor_3000.product_plane.data_plane.moex import foundation
from trading_advisor_3000.product_plane.data_plane.moex.foundation import DiscoveryRecord
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexCandle
from trading_advisor_3000.product_plane.data_plane.moex.parallel_acquisition import (
    combine_sources,
    download_scopes,
    materialize_sources,
    plan_scopes,
)


def coverage():
    return DiscoveryRecord(
        "FUT_TEST",
        "TEST@MOEX",
        "futures",
        "forts",
        "RFUD",
        "TEST",
        "commodity",
        "1m,5m,15m",
        1,
        "1m",
        "2026-07-27T00:00:00Z",
        "2026-07-28T21:00:00Z",
        "2026-07-29T00:00:00Z",
        "https://iss.moex.com/test",
    )


class Client:
    calls = []
    fail = set()

    def __init__(self, **kwargs):
        pass

    def iter_candles(self, **kwargs):
        self.calls.append(kwargs["secid"])
        yield MoexCandle(1.0, 2.0, 3.0, 0.5, 10, "2026-07-27 12:00:00", "2026-07-27 12:00:59")
        if kwargs["secid"] in self.fail:
            raise RuntimeError("network failure after a partial download")
        yield MoexCandle(2.0, 3.0, 4.0, 1.5, 20, "2026-07-29 01:00:00", "2026-07-29 01:00:59")


@pytest.fixture(autouse=True)
def reset_client():
    Client.calls = []
    Client.fail = set()


def test_completed_parts_survive_partial_failure_and_are_not_downloaded_again(tmp_path):
    scopes = plan_scopes(
        [coverage(), replace(coverage(), moex_secid="FAIL")], "2026-07-28T21:00:00Z", 2376
    )
    Client.fail = {"FAIL"}
    first = download_scopes(scopes, tmp_path, "run", workers=2, client_factory=Client)
    assert first["status"] == "FAIL"
    assert first["completed_scopes"] == 1
    Client.fail.clear()
    second = download_scopes(scopes, tmp_path, "run", workers=2, client_factory=Client)
    assert second["status"] == "PASS"
    assert Client.calls.count("TEST") == 1
    assert Client.calls.count("FAIL") == 2
    combined = combine_sources(scopes, tmp_path)
    assert len(combined.read_text().splitlines()) == 2


def test_parallel_rows_match_existing_sequential_connector(tmp_path, monkeypatch):
    fixed = "2026-07-29T00:00:00Z"
    monkeypatch.setattr(foundation, "_utc_now_iso", lambda: fixed)
    monkeypatch.setattr(foundation, "compute_raw_watermarks_spark_delta", lambda **kwargs: {})
    captured = {}

    def capture(**kwargs):
        captured["bytes"] = kwargs["source_rows_path"].read_text().encode()
        return {"changed_windows": []}

    monkeypatch.setattr(foundation, "run_moex_raw_ingest_spark_delta_job", capture)
    foundation.ingest_moex_bootstrap_window(
        client=Client(),
        coverage=[coverage()],
        table_path=tmp_path / "old/delta/raw.delta",
        run_id="run",
        ingest_till_utc="2026-07-28T21:00:00Z",
        bootstrap_window_days=2376,
        stability_lag_minutes=0,
        refresh_overlap_minutes=0,
    )
    scopes = plan_scopes([coverage()], "2026-07-28T21:00:00Z", 2376)
    root = tmp_path / "parallel"
    report = download_scopes(
        scopes, root, "run", workers=2, client_factory=Client, ingested_at_utc=fixed
    )
    assert report["status"] == "PASS"
    assert combine_sources(scopes, root).read_bytes() == captured["bytes"]


def test_corruption_stops_resume_without_overwrite(tmp_path):
    scopes = plan_scopes([coverage()], "2026-07-28T21:00:00Z", 2376)
    assert (
        download_scopes(scopes, tmp_path, "run", workers=1, client_factory=Client)["status"]
        == "PASS"
    )
    part = next((tmp_path / "completed").glob("*/source.jsonl"))
    part.write_bytes(b"corrupted")
    report = download_scopes(scopes, tmp_path, "run", workers=1, client_factory=Client)
    assert report["status"] == "FAIL"
    assert part.read_bytes() == b"corrupted"
    assert Client.calls == ["TEST"]
    with pytest.raises(ValueError, match="checksum"):
        combine_sources(scopes, tmp_path)


def test_overlapping_scope_keys_and_invalid_worker_limits_are_rejected(tmp_path):
    with pytest.raises(ValueError, match="duplicate"):
        plan_scopes([coverage(), coverage()], "2026-07-28T21:00:00Z", 2376)
    with pytest.raises(ValueError, match="workers"):
        download_scopes([], tmp_path, "run", workers=0, client_factory=Client)


def test_workers_overlap_but_second_writer_is_rejected(tmp_path):
    barrier = Barrier(3, timeout=5)
    lock = Lock()
    active = peak = 0

    class ConcurrentClient(Client):
        def iter_candles(self, **kwargs):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            barrier.wait()
            with pytest.raises(FileExistsError):
                download_scopes([], tmp_path, "other", client_factory=Client)
            yield from super().iter_candles(**kwargs)
            with lock:
                active -= 1

    scopes = plan_scopes(
        [replace(coverage(), moex_secid=str(i)) for i in range(3)],
        "2026-07-28T21:00:00Z",
        2376,
    )
    report = download_scopes(scopes, tmp_path, "run", workers=3, client_factory=ConcurrentClient)
    assert report["status"] == "PASS"
    assert peak == 3
    assert not (tmp_path / "download.lock").exists()


def test_delta_failure_preserves_downloaded_parts_and_uses_new_attempt(tmp_path, monkeypatch):
    from trading_advisor_3000.spark_jobs import moex_raw_ingest_job

    scopes = plan_scopes([coverage()], "2026-07-28T21:00:00Z", 2376)
    download_scopes(scopes, tmp_path, "run", workers=1, client_factory=Client)
    paths = []

    def fail_write(**kwargs):
        paths.append(kwargs["table_path"])
        raise RuntimeError("native write failed")

    monkeypatch.setattr(moex_raw_ingest_job, "run_moex_raw_ingest_spark_delta_job", fail_write)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="native write failed"):
            materialize_sources(scopes, tmp_path, "run", "2026-07-28T21:00:00Z")
    assert paths[0] != paths[1]
    assert download_scopes(scopes, tmp_path, "run", client_factory=Client)["status"] == "PASS"
    assert Client.calls == ["TEST"]
