from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_update_script():
    script_path = Path(__file__).parents[3] / "scripts" / "run_moex_contract_economics_update.py"
    spec = importlib.util.spec_from_file_location("run_moex_contract_economics_update", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_money_math_update_date_window_is_inclusive() -> None:
    module = _load_update_script()

    assert [
        item.isoformat()
        for item in module._date_window(  # type: ignore[attr-defined]
            trade_date="2026-06-10",
            date_from="2026-06-10",
            date_till="2026-06-12",
        )
    ] == ["2026-06-10", "2026-06-11", "2026-06-12"]


def test_money_math_update_exposes_moex_request_policy() -> None:
    module = _load_update_script()

    assert module._moex_client_kwargs(  # type: ignore[attr-defined]
        SimpleNamespace(
            moex_timeout_seconds=45.0,
            moex_max_retries=5,
            moex_retry_backoff_seconds=1.25,
            moex_retry_jitter_ratio=0.2,
        )
    ) == {
        "timeout_seconds": 45.0,
        "max_retries": 5,
        "retry_backoff_seconds": 1.25,
        "retry_jitter_ratio": 0.2,
    }


def test_money_math_update_rejects_invalid_moex_request_policy() -> None:
    module = _load_update_script()

    with pytest.raises(SystemExit, match="--moex-timeout-seconds must be > 0"):
        module._moex_client_kwargs(  # type: ignore[attr-defined]
            SimpleNamespace(
                moex_timeout_seconds=0,
                moex_max_retries=2,
                moex_retry_backoff_seconds=0.8,
                moex_retry_jitter_ratio=0.0,
            )
        )


def test_money_math_update_persists_moex_request_events(tmp_path: Path) -> None:
    module = _load_update_script()
    jsonl_path = tmp_path / "evidence" / "moex-request-log.jsonl"
    latest_path = tmp_path / "evidence" / "moex-request.latest.json"

    module._append_request_event(  # type: ignore[attr-defined]
        jsonl_path=jsonl_path,
        latest_path=latest_path,
        payload={
            "event": "moex_http",
            "status": "fail",
            "operation": "futures_contract_securities",
            "attempt": 6,
        },
    )

    lines = jsonl_path.read_text(encoding="utf-8").splitlines()
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert len(lines) == 1
    assert json.loads(lines[0])["status"] == "fail"
    assert latest["operation"] == "futures_contract_securities"
    assert latest["attempt"] == 6
    assert latest["emitted_at_utc"]


def test_money_math_update_rejects_multi_day_current_contract_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_update_script()
    external_root = Path(tempfile.gettempdir()) / "ta3000-money-math-script-guard"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_moex_contract_economics_update.py",
            "--mode",
            "update",
            "--date-from",
            "2026-06-10",
            "--date-till",
            "2026-06-11",
            "--raw-economics-root",
            str(external_root / "raw" / "economics"),
            "--canonical-economics-root",
            str(external_root / "canonical" / "economics"),
            "--evidence-root",
            str(external_root / "evidence"),
        ],
    )

    with pytest.raises(SystemExit, match="current-snapshot endpoint"):
        module.main()


def test_money_math_update_filters_historical_contract_jsonl_by_trade_date() -> None:
    module = _load_update_script()
    payloads = [
        {"trade_date": "2026-06-10", "SECID": "BRM6", "ASSETCODE": "BR"},
        {"trade_date": "2026-06-11", "SECID": "BRN6", "ASSETCODE": "BR"},
    ]

    rows = []
    for trade_date in module._date_window(  # type: ignore[attr-defined]
        trade_date="2026-06-10",
        date_from="2026-06-10",
        date_till="2026-06-11",
    ):
        rows.extend(
            module._raw_contract_rows(  # type: ignore[attr-defined]
                payload_rows=module._payloads_for_trade_date(  # type: ignore[attr-defined]
                    payloads,
                    trade_date=trade_date,
                    source_label="contracts",
                    allow_undated_single_day=False,
                ),
                trade_date=trade_date,
                fetched_at_utc="2026-06-11T00:00:00Z",
            )
        )

    assert [(row["trade_date"], row["moex_secid"]) for row in rows] == [
        ("2026-06-10", "BRM6"),
        ("2026-06-11", "BRN6"),
    ]


def test_money_math_update_filters_historical_fx_jsonl_by_trade_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_update_script()

    def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
        path.write_text(
            "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
            encoding="utf-8",
        )

    contracts_jsonl = tmp_path / "contracts.jsonl"
    fx_jsonl = tmp_path / "fx.jsonl"
    limits_jsonl = tmp_path / "limits.jsonl"
    staticparams_jsonl = tmp_path / "staticparams.jsonl"
    _write_jsonl(
        contracts_jsonl,
        [
            {"trade_date": "2026-06-10", "SECID": "BRM6", "ASSETCODE": "BR"},
            {"trade_date": "2026-06-11", "SECID": "BRN6", "ASSETCODE": "BR"},
        ],
    )
    _write_jsonl(
        fx_jsonl,
        [
            {"tradedate": "2026-06-09", "secid": "USD/RUB", "clearing": "tc", "rate": 70.0},
            {"tradedate": "2026-06-10", "secid": "USD/RUB", "clearing": "tc", "rate": 71.0},
            {"tradedate": "2026-06-11", "secid": "USD/RUB", "clearing": "tc", "rate": 72.0},
            {"tradedate": "2026-06-12", "secid": "USD/RUB", "clearing": "tc", "rate": 73.0},
        ],
    )
    _write_jsonl(
        limits_jsonl,
        [
            {"trade_date": "2026-06-10", "assetcode": "BR", "mr1": 0.12},
            {"trade_date": "2026-06-11", "assetcode": "BR", "mr1": 0.13},
        ],
    )
    _write_jsonl(
        staticparams_jsonl,
        [
            {"trade_date": "2026-06-10", "assetcode": "BR", "radius_pct": 15.0},
            {"trade_date": "2026-06-11", "assetcode": "BR", "radius_pct": 16.0},
        ],
    )
    captured_rows: dict[str, list[dict[str, object]]] = {}

    def _fake_write_raw_table_for_mode(**kwargs: object) -> str:
        table_name = str(kwargs["table_name"])
        captured_rows[table_name] = list(kwargs["rows"])  # type: ignore[arg-type]
        return "captured"

    def _fake_spark_job(**_kwargs: object) -> dict[str, object]:
        return {"status": "PASS"}

    monkeypatch.setattr(module, "_write_raw_table_for_mode", _fake_write_raw_table_for_mode)
    monkeypatch.setattr(module, "run_moex_contract_economics_spark_job", _fake_spark_job)
    external_root = Path(tempfile.gettempdir()) / f"ta3000-money-math-fx-jsonl-{tmp_path.name}"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_moex_contract_economics_update.py",
            "--mode",
            "update",
            "--date-from",
            "2026-06-10",
            "--date-till",
            "2026-06-11",
            "--contracts-jsonl",
            str(contracts_jsonl),
            "--fx-jsonl",
            str(fx_jsonl),
            "--rms-source",
            "jsonl",
            "--rms-limits-jsonl",
            str(limits_jsonl),
            "--rms-staticparams-jsonl",
            str(staticparams_jsonl),
            "--raw-economics-root",
            str(external_root / "raw" / "economics"),
            "--canonical-economics-root",
            str(external_root / "canonical" / "economics"),
            "--evidence-root",
            str(external_root / "evidence"),
        ],
    )

    module.main()

    assert [row["trade_date"] for row in captured_rows["raw_moex_indicative_fx_rates"]] == [
        "2026-06-10",
        "2026-06-11",
    ]


@pytest.mark.parametrize("historical_mode", [False, True])
def test_money_math_update_forwards_explicit_margin_calibration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, historical_mode: bool
) -> None:
    from datetime import date

    module = _load_update_script()
    captured: dict[str, object] = {}
    writes: list[str] = []
    monkeypatch.setattr(module, "MoexISSClient", lambda **_kwargs: SimpleNamespace())
    monkeypatch.setattr(module, "_read_jsonl", lambda _path: [{"trade_date": "2026-10-05"}])
    monkeypatch.setattr(module, "resolve_external_root", lambda value, **_kwargs: Path(value))
    for name in ("_raw_contract_rows", "_raw_fx_rows", "_risk_rows_from_jsonl"):
        monkeypatch.setattr(module, name, lambda **_kwargs: [])
    monkeypatch.setattr(
        module,
        "_write_raw_table_for_mode",
        lambda **kwargs: writes.append(str(kwargs["table_name"])) or "captured",
    )

    def capture_runner(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {"status": "PASS"}

    monkeypatch.setattr(module, "run_moex_contract_economics_spark_job", capture_runner)
    arguments = [
        "run_moex_contract_economics_update.py",
        "--trade-date",
        "2026-10-05",
        "--contracts-jsonl",
        "contracts.jsonl",
        "--fx-jsonl",
        "fx.jsonl",
        "--rms-source",
        "jsonl",
        "--rms-limits-jsonl",
        "limits.jsonl",
        "--rms-staticparams-jsonl",
        "staticparams.jsonl",
        "--raw-economics-root",
        str(tmp_path / "raw"),
        "--canonical-economics-root",
        str(tmp_path / "canonical"),
        "--evidence-root",
        str(tmp_path / "evidence"),
    ]
    if historical_mode:
        arguments += [
            "--allow-calibrated-latest-margin",
            "--margin-calibration-as-of-date",
            "2026-08-14",
        ]
    monkeypatch.setattr(sys, "argv", arguments)
    module.main()

    assert len(writes) == 4
    assert captured.get("allow_calibrated_latest_margin", False) is historical_mode
    assert captured.get("margin_calibration_as_of_date") == (
        date(2026, 8, 14) if historical_mode else None
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["--allow-calibrated-latest-margin"],
        ["--margin-calibration-as-of-date", "2026-08-14"],
    ],
)
def test_money_math_update_requires_paired_calibration_before_acquisition(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    module = _load_update_script()

    def forbidden_acquisition(**_kwargs: object) -> None:
        pytest.fail("invalid calibration configuration reached acquisition/write")

    monkeypatch.setattr(module, "MoexISSClient", forbidden_acquisition)
    monkeypatch.setattr(module, "_write_raw_table_for_mode", forbidden_acquisition)
    monkeypatch.setattr(sys, "argv", ["run_moex_contract_economics_update.py", *arguments])
    with pytest.raises(SystemExit, match="2"):
        module.main()
    assert "must be supplied together" in capsys.readouterr().err


@pytest.mark.parametrize("calibration_date", ["2026-02-30", "14.08.2026"])
def test_money_math_update_requires_typed_iso_calibration_before_acquisition(
    monkeypatch: pytest.MonkeyPatch, calibration_date: str, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _load_update_script()

    def forbidden_acquisition(**_kwargs: object) -> None:
        pytest.fail("invalid calibration date reached acquisition/write")

    monkeypatch.setattr(module, "MoexISSClient", forbidden_acquisition)
    monkeypatch.setattr(module, "_write_raw_table_for_mode", forbidden_acquisition)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_moex_contract_economics_update.py",
            "--allow-calibrated-latest-margin",
            "--margin-calibration-as-of-date",
            calibration_date,
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        module.main()
    assert "invalid fromisoformat value" in capsys.readouterr().err


@pytest.mark.parametrize(
    "allow,calibration,expected_error",
    [
        (True, None, ValueError),
        (False, "2026-08-14", TypeError),
        (True, "2026-08-14", TypeError),
        ("true", None, TypeError),
        (1, None, TypeError),
    ],
)
def test_economics_direct_run_rejects_invalid_calibration_before_spark(
    tmp_path, allow, calibration, expected_error
):
    from trading_advisor_3000.spark_jobs.moex_contract_economics_job import (
        run_moex_contract_economics_spark_job,
    )

    def forbidden_factory(*args):
        pytest.fail("invalid calibration reached Spark acquisition")

    output = tmp_path / "canonical"
    with pytest.raises(expected_error):
        run_moex_contract_economics_spark_job(
            raw_contract_specs_path=tmp_path / "contracts",
            raw_fx_rates_path=tmp_path / "fx",
            raw_rms_limits_path=tmp_path / "limits",
            raw_rms_staticparams_path=tmp_path / "staticparams",
            output_dir=output,
            allow_calibrated_latest_margin=allow,
            margin_calibration_as_of_date=calibration,
            spark_session_factory=forbidden_factory,
        )
    assert not output.exists()


def test_economics_direct_run_requires_paired_date_and_rejects_datetime(tmp_path):
    from datetime import date, datetime

    from trading_advisor_3000.spark_jobs.moex_contract_economics_job import (
        run_moex_contract_economics_spark_job,
    )

    def forbidden_factory(*args):
        pytest.fail("invalid calibration reached Spark acquisition")

    output = tmp_path / "canonical"
    kwargs = {
        "raw_contract_specs_path": tmp_path / "contracts",
        "raw_fx_rates_path": tmp_path / "fx",
        "raw_rms_limits_path": tmp_path / "limits",
        "raw_rms_staticparams_path": tmp_path / "staticparams",
        "output_dir": output,
        "spark_session_factory": forbidden_factory,
    }
    with pytest.raises(ValueError, match="must be supplied together"):
        run_moex_contract_economics_spark_job(
            **kwargs, margin_calibration_as_of_date=date(2026, 8, 14)
        )
    with pytest.raises(TypeError, match="excluding datetime"):
        run_moex_contract_economics_spark_job(
            **kwargs,
            allow_calibrated_latest_margin=True,
            margin_calibration_as_of_date=datetime(2026, 8, 14),
        )
    assert not output.exists()
