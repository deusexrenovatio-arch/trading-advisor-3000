from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.data_plane.moex import (
    contract_parameter_continuation as continuation,
)
from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES,
    MoexContractParameterRegime,
    validate_contract_parameter_regimes,
)
from trading_advisor_3000.spark_jobs import moex_contract_economics_job as job


@pytest.mark.parametrize("method", ["turnover", "dated_official_tc"])
def test_profile_report_preserves_legacy_keys_and_marks_dated_method(monkeypatch, method):
    from pyspark.sql import Row
    from pyspark.sql import functions as F

    for name in ("col", "lit", "count", "percentile_approx", "max"):
        monkeypatch.setattr(F, name, MagicMock())
    profile = {
        "rule_id": "BR-test",
        "assetcode": "BR",
        "quote_currency": "USD",
        "min_step": 0.01,
        "lot_volume": 10.0,
        "tick_value_quote": 0.1,
        "rule_rows": 1,
        "validation_rows": 1,
        "validation_p95_relative_error": 0.0,
        "validation_max_relative_error": 0.0,
    }
    raw = {
        "__step_price_rule_" + key: value
        for key, value in profile.items()
        if key
        in {"id", "assetcode", "quote_currency", "min_step", "lot_volume", "tick_value_quote"}
    }
    raw["__step_price_rule_id"] = profile["rule_id"]
    raw["__step_price_rule_validation_method"] = method
    raw.update(
        {
            key: value
            for key, value in profile.items()
            if key.startswith(("rule_rows", "validation_"))
        }
    )
    economics = MagicMock()
    aggregate = economics.where.return_value.groupBy.return_value.agg.return_value
    aggregate.orderBy.return_value.collect.return_value = [Row(**raw)]
    if method == "dated_official_tc":
        profile["validation_method"] = method
    assert job._fail_closed_on_step_price_rule_validation(economics) == [profile]


@pytest.mark.parametrize("value", [True, float("nan"), float("inf")])
def test_frame_rejects_invalid_registry_before_spark(monkeypatch, value):
    rules = job.MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES
    monkeypatch.setattr(
        job,
        "MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES",
        (replace(rules[0], min_step=value), *rules[1:]),
    )

    class ForbiddenSpark:
        def sql(self, *args):
            pytest.fail("invalid registry reached Spark SQL")

        def createDataFrame(self, *args, **kwargs):
            pytest.fail("invalid registry reached Spark metadata frame")

    with pytest.raises(ValueError, match="finite positive"):
        job._contract_parameter_regimes_frame(ForbiddenSpark())


def test_runner_rejects_additional_list_before_source_checks(tmp_path, monkeypatch):
    def forbidden_source(*args):
        pytest.fail("invalid configuration reached Delta source")

    monkeypatch.setattr(job, "has_delta_log", forbidden_source)
    output = tmp_path / "canonical"
    with pytest.raises(TypeError, match="must be tuple"):
        job.run_moex_contract_economics_spark_job(
            raw_contract_specs_path=tmp_path / "raw",
            raw_fx_rates_path=tmp_path / "fx",
            output_dir=output,
            additional_contract_parameter_regimes=[],
        )
    assert not output.exists()


def _metadata_fixture(tmp_path, monkeypatch):
    counts = {
        "BR": 67,
        "GOLD": 36,
        "SILV": 36,
        "PLD": 36,
        "PLT": 36,
        "NG": 64,
        "SPYF": 36,
        "NASD": 36,
        "RTS": 36,
        "MIX": 72,
        "MXI": 72,
        "RGBI": 36,
        "WHEAT": 111,
    }
    dated = {
        "status": "PASS",
        "validation_method": "dated_official_tc",
        "snapshot_version": 134,
        "dated_official_rows": 674,
        "extrapolation_scope": ["2026-07-29", "2026-10-05"],
        "inference_not_daily_official": True,
        "turnover_proxy_rejected_due_to_nonidentical_trade_baskets": True,
        "profiles": [
            {"assetcode": asset, "rows": rows, "p95": 0.0, "maximum": 0.0}
            for asset, rows in counts.items()
        ],
    }
    evidence_path = tmp_path / "dated.json"
    evidence_path.write_text(json.dumps(dated), encoding="utf-8")
    evidence_sha = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    additional = tuple(
        replace(
            rule,
            rule_id=f"{rule.assetcode}-bounded-tail-20261005-v1",
            effective_from="2026-07-29",
            effective_to="2026-10-05",
            validation_method="dated_official_tc",
            source_document_id=f"{rule.source_document_id};sha256:{evidence_sha}",
        )
        for rule in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES[:13]
    )
    rules_path = tmp_path / "regimes.json"
    rules_path.write_text(json.dumps([asdict(rule) for rule in additional]), encoding="utf-8")
    monkeypatch.setattr(
        continuation,
        "_ADMITTED_BUNDLE",
        replace(
            continuation._ADMITTED_BUNDLE,
            regimes_sha256=hashlib.sha256(rules_path.read_bytes()).hexdigest(),
            evidence_sha256=evidence_sha,
        ),
    )
    return additional, continuation.MoexContractParameterEvidence(
        "bounded-tail-20261005-v1", rules_path, evidence_path
    )


def _runner_kwargs(tmp_path, **extras):
    return dict(
        raw_contract_specs_path=tmp_path / "raw",
        raw_fx_rates_path=tmp_path / "fx",
        raw_rms_limits_path=tmp_path / "limits",
        raw_rms_staticparams_path=tmp_path / "static",
        output_dir=tmp_path / "output",
        report_path=tmp_path / "report.json",
        **extras,
    )


def _forbid_runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid admission reached Delta/Spark/output")

    monkeypatch.setattr(continuation, "DeltaTable", forbidden)
    monkeypatch.setattr(job, "has_delta_log", forbidden)
    return forbidden


def test_empty_default_and_old_positional_contract_preserved():
    assert validate_contract_parameter_regimes() == MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES
    assert len(MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES) == 32
    assert continuation.validate_contract_parameter_evidence((), None) is None
    first = MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES[0]
    assert MoexContractParameterRegime(*list(asdict(first).values())[:9]) == first
    assert all(
        rule.validation_method == "turnover" for rule in MOEX_HISTORICAL_CONTRACT_PARAMETER_REGIMES
    )


@pytest.mark.parametrize("additional", [None, [], iter(()), (object(),)])
def test_tuple_and_item_types_fail_before_runtime(tmp_path, monkeypatch, additional):
    forbidden = _forbid_runtime(monkeypatch)
    with pytest.raises(TypeError):
        job.run_moex_contract_economics_spark_job(
            **_runner_kwargs(
                tmp_path,
                additional_contract_parameter_regimes=additional,
                spark_session_factory=forbidden,
            )
        )
    assert not (tmp_path / "output").exists()
    assert not (tmp_path / "report.json").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("assetcode", "br"),
        ("assetcode", "UNKNOWN"),
        ("rule_id", " "),
        ("rule_id", "bad\n"),
        ("quote_currency", "usd"),
        ("validation_method", "proxy"),
        ("source_document_id", " "),
        ("effective_from", "20260729"),
        ("effective_from", "2026-7-29"),
        ("effective_from", date(2026, 7, 29)),
        ("effective_from", datetime(2026, 7, 29)),
        ("effective_to", "2026-07-28"),
        ("min_step", False),
        ("lot_volume", 0),
        ("tick_value_quote", -1),
        ("min_step", "0.01"),
        ("min_step", Decimal("0.01")),
        ("min_step", float("nan")),
        ("lot_volume", float("inf")),
        ("tick_value_quote", 1j),
    ],
)
def test_invalid_fields_fail_before_runtime(tmp_path, monkeypatch, field, value):
    additional, evidence = _metadata_fixture(tmp_path, monkeypatch)
    forbidden = _forbid_runtime(monkeypatch)
    invalid = (replace(additional[0], **{field: value}), *additional[1:])
    with pytest.raises(ValueError):
        job.run_moex_contract_economics_spark_job(
            **_runner_kwargs(
                tmp_path,
                additional_contract_parameter_regimes=invalid,
                contract_parameter_evidence=evidence,
                spark_session_factory=forbidden,
            )
        )
    assert not (tmp_path / "output").exists()
    assert not (tmp_path / "report.json").exists()


def test_duplicate_and_inclusive_overlaps_rejected(tmp_path, monkeypatch):
    additional, _ = _metadata_fixture(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="duplicate"):
        validate_contract_parameter_regimes((*additional, additional[0]))
    for start, end in [("2026-06-16", "2026-07-29"), ("2026-07-29", "2026-07-29")]:
        other = replace(additional[0], rule_id="overlap", effective_from=start, effective_to=end)
        with pytest.raises(ValueError, match="overlapping"):
            validate_contract_parameter_regimes((other, *additional))
    following = replace(
        additional[0], rule_id="next-day", effective_from="2026-10-06", effective_to="2026-10-06"
    )
    assert len(validate_contract_parameter_regimes((*additional, following))) == 46


def test_verified_bundle_binds_exact_thirteen_and_native_units(tmp_path, monkeypatch):
    additional, evidence = _metadata_fixture(tmp_path, monkeypatch)
    result = continuation.validate_contract_parameter_evidence(additional, evidence)
    assert result["receipt_dated_official_rows"] == 674
    assert result["inference_not_daily_official"] is True
    assert result["prepared_input_version"] == 0
    values = {r.assetcode: r for r in additional}
    assert values["NASD"].tick_value_quote == 0.01
    assert values["RTS"].tick_value_quote == 0.2
    assert values["MXI"].tick_value_quote == 0.5


@pytest.mark.parametrize(
    "kind",
    ["unknown", "missing", "extra_empty", "tuple", "tamper_rules", "tamper_evidence", "oversized"],
)
def test_bad_evidence_fails_before_delta(tmp_path, monkeypatch, kind):
    additional, evidence = _metadata_fixture(tmp_path, monkeypatch)
    if kind == "unknown":
        evidence = replace(evidence, bundle_id="renewed")
    if kind == "missing":
        evidence.dated_official_tc_path.unlink()
    if kind == "extra_empty":
        additional = ()
    if kind == "tuple":
        additional = additional[:-1]
    if kind == "tamper_rules":
        evidence.regimes_path.write_bytes(b"[]")
    if kind == "tamper_evidence":
        evidence.dated_official_tc_path.write_bytes(b"{}")
    if kind == "oversized":
        evidence.regimes_path.write_bytes(b" " * (64 * 1024 + 1))
    forbidden = _forbid_runtime(monkeypatch)
    with pytest.raises((ValueError, FileNotFoundError)):
        job.run_moex_contract_economics_spark_job(
            **_runner_kwargs(
                tmp_path,
                additional_contract_parameter_regimes=additional,
                contract_parameter_evidence=evidence,
                spark_session_factory=forbidden,
            )
        )
    assert not (tmp_path / "output").exists()


def test_arbitrary_file_hash_is_not_self_admitted(tmp_path):
    rules = tmp_path / "rules.json"
    rules.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="SHA256"):
        continuation._read_verified_json(rules, continuation._ADMITTED_BUNDLE.regimes_sha256)


@pytest.mark.parametrize(
    "payload,match",
    [(b'{"a":1,"a":2}', "duplicate"), (b'{"a":NaN}', "nonfinite"), (b"\xff", "decode")],
)
def test_json_integrity_uses_verified_same_bytes(tmp_path, payload, match):
    path = tmp_path / "evidence.json"
    path.write_bytes(payload)
    with pytest.raises((ValueError, UnicodeDecodeError), match=match):
        continuation._read_verified_json(path, hashlib.sha256(payload).hexdigest())


def test_invalid_provenance_and_scope_cannot_use_valid_receipt(tmp_path, monkeypatch):
    additional, evidence = _metadata_fixture(tmp_path, monkeypatch)
    changed = tuple(replace(r, effective_to="2026-10-06") for r in additional)
    with pytest.raises(ValueError, match="tuple differs"):
        continuation.validate_contract_parameter_evidence(changed, evidence)
    altered = (
        replace(additional[0], source_document_id=additional[0].source_document_id + ";sha256:bad"),
        *additional[1:],
    )
    evidence.regimes_path.write_text(json.dumps([asdict(r) for r in altered]), encoding="utf-8")
    monkeypatch.setattr(
        continuation,
        "_ADMITTED_BUNDLE",
        replace(
            continuation._ADMITTED_BUNDLE,
            regimes_sha256=hashlib.sha256(evidence.regimes_path.read_bytes()).hexdigest(),
        ),
    )
    with pytest.raises(ValueError, match="source_document_id"):
        continuation.validate_contract_parameter_evidence(altered, evidence)


@pytest.mark.parametrize(
    "field,value",
    [
        ("inference_not_daily_official", False),
        ("snapshot_version", True),
        ("dated_official_rows", 673),
        ("extrapolation_scope", ["2026-07-29", "2026-10-06"]),
        ("profiles", []),
    ],
)
def test_receipt_semantics_checked_after_integrity(tmp_path, monkeypatch, field, value):
    additional, evidence = _metadata_fixture(tmp_path, monkeypatch)
    dated = json.loads(evidence.dated_official_tc_path.read_text())
    dated[field] = value
    evidence.dated_official_tc_path.write_text(json.dumps(dated), encoding="utf-8")
    sha = hashlib.sha256(evidence.dated_official_tc_path.read_bytes()).hexdigest()
    changed = tuple(
        replace(
            r, source_document_id=r.source_document_id.rpartition(";sha256:")[0] + ";sha256:" + sha
        )
        for r in additional
    )
    evidence.regimes_path.write_text(json.dumps([asdict(r) for r in changed]), encoding="utf-8")
    monkeypatch.setattr(
        continuation,
        "_ADMITTED_BUNDLE",
        replace(
            continuation._ADMITTED_BUNDLE,
            regimes_sha256=hashlib.sha256(evidence.regimes_path.read_bytes()).hexdigest(),
            evidence_sha256=sha,
        ),
    )
    with pytest.raises(ValueError):
        continuation.validate_contract_parameter_evidence(changed, evidence)


def test_native_metadata_binds_original_snapshot_even_when_current_advances(tmp_path):
    table = tmp_path / "prepared.delta"
    write_deltalake(str(table), pa.table({"value": [1]}))
    original = DeltaTable(str(table))
    verified = {"prepared_input_table_id": original.metadata().id, "prepared_input_version": 0}
    write_deltalake(str(table), pa.table({"value": [2]}), mode="append")
    assert DeltaTable(str(table)).version() == 1
    assert continuation.bind_contract_parameter_input(table, verified) == 0
    with pytest.raises(ValueError, match="identity"):
        continuation.bind_contract_parameter_input(
            table, {**verified, "prepared_input_table_id": "different"}
        )


def test_wrong_prepared_identity_fails_before_spark_and_output(tmp_path, monkeypatch):
    additional, evidence = _metadata_fixture(tmp_path, monkeypatch)
    write_deltalake(str(tmp_path / "raw"), pa.table({"value": [1]}))
    with pytest.raises(ValueError, match="identity"):
        job.run_moex_contract_economics_spark_job(
            **_runner_kwargs(
                tmp_path,
                additional_contract_parameter_regimes=additional,
                contract_parameter_evidence=evidence,
                spark_session_factory=lambda *args: pytest.fail("wrong identity reached Spark"),
            )
        )
    assert not (tmp_path / "output").exists()
    assert not (tmp_path / "report.json").exists()


def test_calibration_guards_still_precede_new_evidence(tmp_path, monkeypatch):
    forbidden = _forbid_runtime(monkeypatch)
    with pytest.raises(ValueError, match="supplied together"):
        job.run_moex_contract_economics_spark_job(
            **_runner_kwargs(
                tmp_path,
                allow_calibrated_latest_margin=True,
                additional_contract_parameter_regimes=[],
                spark_session_factory=forbidden,
            )
        )


def test_relative_prepared_path_rejected_before_delta(tmp_path, monkeypatch):
    additional, evidence = _metadata_fixture(tmp_path, monkeypatch)
    forbidden = _forbid_runtime(monkeypatch)
    kwargs = _runner_kwargs(
        tmp_path,
        additional_contract_parameter_regimes=additional,
        contract_parameter_evidence=evidence,
        spark_session_factory=forbidden,
    )
    kwargs["raw_contract_specs_path"] = Path("relative/prepared.delta")
    with pytest.raises(ValueError, match="must be absolute"):
        job.run_moex_contract_economics_spark_job(**kwargs)
    assert not (tmp_path / "output").exists()
    assert not (tmp_path / "report.json").exists()
