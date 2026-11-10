"""Synthetic control-branch tests; the native trial proves actual durability."""

import hashlib
import json
from contextlib import nullcontext
from decimal import Decimal
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.moex import observation_materialization as owner
from trading_advisor_3000.product_plane.data_plane.moex.observation_acquisition import (
    canonical_bytes,
)

FIXTURES = Path(__file__).parents[1] / "fixtures/contracts"


@pytest.mark.parametrize("value", ["1.00000000001", "1E28", "NaN", "Infinity"])
def test_decimal_intake_rejects_rounding_overflow_and_nonfinite(value):
    with pytest.raises(ValueError):
        owner.exact_decimal_lexeme(Decimal(value))


def test_decimal_intake_accepts_boundary_without_float_or_rounding():
    value = Decimal("9999999999999999999999999999.9999999999")
    assert Decimal(owner.exact_decimal_lexeme(value)) == value


@pytest.mark.parametrize(
    "begin", ["2026-09-14 9:05:00", "2026-9-14 09:05:00", "2026-09-14 09:05:0"]
)
def test_noncanonical_source_keys_quarantine_before_lexical_selection(tmp_path, begin):
    from trading_advisor_3000.product_plane.data_plane.moex.observation_validation import (
        _source_valid,
    )

    columns = ["open", "high", "low", "close", "volume", "begin", "end"]
    values = [1, 2, 1, 2, 3, begin, "2026-09-14 09:05:59"]
    body = tmp_path / "body.json"
    body.write_bytes(canonical_bytes({"candles": {"columns": columns, "data": [values]}}))
    page = {
        "body": owner.pin(tmp_path, body),
        "receipt": {"sha256": "0" * 64},
        "source_symbol": "SYNTHETIC",
        "timeframe": "1m",
        "page_start": 0,
    }
    source = {"pages": [page], "window": {"date_from": "2026-09-14", "date_till": "2026-09-15"}}
    assignments = {
        "namespace": {"namespace": "synthetic-only"},
        "bindings": [
            {
                "source_symbol": "SYNTHETIC",
                "asset_id": "ASSET:fixture",
                "instrument_id": "INSTR:fixture",
                "engine": "stock",
                "context": {
                    "provider": "moex_iss",
                    "venue": "MOEX",
                    "market": "shares",
                    "board": "TQBR",
                },
            }
        ],
    }
    assert (
        next(owner.intake_records(tmp_path, source, assignments))["rejection_reason"]
        == "source_key"
    )
    parsed = [Decimal(value) if isinstance(value, int) else value for value in values]
    assert not _source_valid(parsed, columns, source["window"])


def test_completed_command_replays_before_current_writer_version_checks(tmp_path, monkeypatch):
    metadata = json.loads((FIXTURES / "native_observation_snapshot.v1.json").read_bytes())
    intent = json.loads((FIXTURES / "native_observation_intent.v1.json").read_bytes())
    (tmp_path / "scope.json").write_text("{}", encoding="utf-8")
    command_dir = tmp_path / "commands" / hashlib.sha256(intent["command_id"].encode()).hexdigest()
    command_dir.mkdir(parents=True)
    (command_dir / "intent.json").write_bytes(canonical_bytes(intent) + b"\n")
    (command_dir / "completion.json").write_text('{"synthetic_only":true}', encoding="utf-8")
    real_path = Path
    monkeypatch.setattr(
        owner, "Path", lambda value: tmp_path if value == "/trial" else real_path(value)
    )
    monkeypatch.setattr(
        owner, "verify_source_bundle", lambda *a: ({}, {"namespace": metadata["namespace"]})
    )
    monkeypatch.setattr(owner, "pinned_bytes", lambda *a: canonical_bytes(metadata))
    monkeypatch.setattr(owner, "producer_code_sha256", lambda: "0" * 64)
    monkeypatch.setattr(owner, "_command_lock", lambda *a: nullcontext())
    monkeypatch.setattr(
        owner, "build_observation_command", lambda *a, **k: {**intent, "code_sha256": "0" * 64}
    )
    monkeypatch.setattr(
        owner,
        "completed_candidate",
        lambda *a: {
            "completion": {"input_sha256": hashlib.sha256(canonical_bytes(intent)).hexdigest()}
        },
    )
    monkeypatch.setattr(
        owner,
        "_frames",
        lambda *a: (_ for _ in ()).throw(
            AssertionError("completed replay reached materialization")
        ),
    )
    result = owner.materialize_observation_command(tmp_path, intent)
    assert result["replayed"] is True
    assert result["completion"]["path"].endswith("/completion.json")
