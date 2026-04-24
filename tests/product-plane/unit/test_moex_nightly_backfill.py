from __future__ import annotations

import json
from pathlib import Path

import scripts.run_moex_nightly_backfill as nightly
from trading_advisor_3000.product_plane.data_plane.moex.foundation import DiscoveryRecord, load_universe


def _coverage_record(*, internal_id: str, secid: str, source_interval: int, source_timeframe: str) -> DiscoveryRecord:
    return DiscoveryRecord(
        internal_id=internal_id,
        finam_symbol=internal_id if secid == "BRQ6" else f"{secid}@MOEX",
        moex_engine="futures",
        moex_market="forts",
        moex_board="RFUD",
        moex_secid=secid,
        asset_group="commodity",
        requested_target_timeframes="5m,15m" if source_interval == 1 else "1h,4h",
        source_interval=source_interval,
        source_timeframe=source_timeframe,
        coverage_begin_utc="2026-03-01T07:00:00Z",
        coverage_end_utc="2026-04-02T12:00:00Z",
        discovered_at_utc="2026-04-02T12:05:00Z",
        discovery_url=(
            "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/"
            f"securities/{secid}/candleborders.json"
        ),
    )


def test_build_jobs_scopes_shared_discovery_by_internal_id(tmp_path: Path) -> None:
    universe = load_universe(Path("configs/moex_foundation/universe/moex-futures-priority.v1.yaml"))
    active_symbols = [item for item in universe if item.is_active][:2]
    assert len(active_symbols) == 2

    coverage = [
        _coverage_record(
            internal_id=active_symbols[0].internal_id,
            secid=active_symbols[0].moex_secid,
            source_interval=1,
            source_timeframe="1m",
        ),
        _coverage_record(
            internal_id=active_symbols[1].internal_id,
            secid=active_symbols[1].moex_secid,
            source_interval=60,
            source_timeframe="1h",
        ),
    ]

    jobs = nightly._build_jobs(
        run_id="nightly-shared-discovery",
        phase01_run_dir=tmp_path / "phase01",
        shards_dir=tmp_path / "nightly" / "shards",
        shards=[[active_symbols[0]], [active_symbols[1]]],
        coverage=coverage,
        timeframes={"5m", "15m", "1h", "4h"},
        bootstrap_window_days=180,
        ingest_till_utc="2026-04-02T12:00:00Z",
        stability_lag_minutes=20,
        expand_contract_chain=True,
        contract_discovery_step_days=14,
        contract_discovery_lookback_days=180,
        refresh_overlap_minutes=180,
    )

    assert len(jobs) == 2
    assert {row["internal_id"] for row in jobs[0]["coverage"]} == {active_symbols[0].internal_id}
    assert {row["internal_id"] for row in jobs[1]["coverage"]} == {active_symbols[1].internal_id}
    assert (Path(str(jobs[0]["output_dir"])) / "coverage-report.json").exists()
    assert (Path(str(jobs[1]["output_dir"])) / "coverage-report.csv").exists()


def test_run_shard_job_reuses_cached_success(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    def _fake_ingest_moex_bootstrap_window(**kwargs):  # noqa: ANN003 - test double keeps loose signature
        run_id = str(kwargs["run_id"])
        calls.append(run_id)

        table_path = Path(str(kwargs["table_path"]))
        (table_path / "_delta_log").mkdir(parents=True, exist_ok=True)

        progress_path = Path(str(kwargs["progress_path"]))
        progress_latest_path = Path(str(kwargs["progress_latest_path"]))
        error_path = Path(str(kwargs["error_path"]))
        error_latest_path = Path(str(kwargs["error_latest_path"]))
        for path in (progress_path, progress_latest_path, error_path, error_latest_path):
            path.parent.mkdir(parents=True, exist_ok=True)

        progress_payload = {
            "run_id": run_id,
            "internal_id": "FUT_BR",
            "moex_secid": "BRQ6",
            "source_timeframe": "1m",
            "source_interval": 1,
            "window_start_utc": "2026-04-01T07:00:00Z",
            "window_end_utc": "2026-04-02T11:40:00Z",
            "source_rows": 10,
            "incremental_rows": 10,
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "processed_at_utc": "2026-04-02T12:06:00Z",
        }
        progress_path.write_text(json.dumps(progress_payload, ensure_ascii=False) + "\n", encoding="utf-8")
        progress_latest_path.write_text(
            json.dumps(progress_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        error_path.write_text("", encoding="utf-8")
        error_latest_path.write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "status": "PASS",
                    "reported_at_utc": "2026-04-02T12:06:01Z",
                    "message": "no raw ingest errors recorded",
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        return nightly.build_raw_ingest_run_report_v2(
            run_id=run_id,
            ingest_till_utc="2026-04-02T12:00:00Z",
            source_rows=10,
            incremental_rows=10,
            deduplicated_rows=0,
            stale_rows=0,
            watermark_by_key={"FUT_BR|1m|BRQ6": "2026-04-02T11:39:59Z"},
            raw_table_path=table_path.as_posix(),
            raw_ingest_progress_path=progress_path.as_posix(),
            raw_ingest_error_path=error_path.as_posix(),
            raw_ingest_error_latest_path=error_latest_path.as_posix(),
            changed_windows=[
                {
                    "internal_id": "FUT_BR",
                    "source_timeframe": "1m",
                    "source_interval": 1,
                    "moex_secid": "BRQ6",
                    "window_start_utc": "2026-04-01T07:00:00Z",
                    "window_end_utc": "2026-04-02T11:40:00Z",
                    "incremental_rows": 10,
                }
            ],
            generated_at_utc="2026-04-02T12:06:02Z",
        )

    monkeypatch.setattr(nightly, "ingest_moex_bootstrap_window", _fake_ingest_moex_bootstrap_window)

    job = {
        "shard_id": "shard-01",
        "internal_ids": ["FUT_BR"],
        "coverage": [
            _coverage_record(
                internal_id="FUT_BR",
                secid="BRQ6",
                source_interval=1,
                source_timeframe="1m",
            ).to_dict()
        ],
        "output_dir": (tmp_path / "phase01" / "shards" / "shard-01").as_posix(),
        "run_id": "nightly-shard-01",
        "timeframes": ["5m", "15m"],
        "bootstrap_window_days": 30,
        "ingest_till_utc": "2026-04-02T12:00:00Z",
        "stability_lag_minutes": 20,
        "expand_contract_chain": True,
        "contract_discovery_step_days": 14,
        "contract_discovery_lookback_days": 30,
        "refresh_overlap_minutes": 180,
    }

    first = nightly._run_shard_job(job)
    second = nightly._run_shard_job(job)

    assert calls == ["nightly-shard-01"]
    assert first["status"] == "PASS"
    assert second["status"] == "PASS"
    assert second["resume_mode"] == "reused_success"
    success_payload = json.loads(
        (Path(str(job["output_dir"])) / "shard-success.json").read_text(encoding="utf-8")
    )
    assert success_payload["status"] == "PASS"
