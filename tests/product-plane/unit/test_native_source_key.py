from decimal import Decimal

import pyarrow as pa
import pytest

from trading_advisor_3000.product_plane.contracts.schema_validation import SchemaValidationError
from trading_advisor_3000.product_plane.research.native_source_key import _align_tables


def _lower(labels):
    return pa.table(
        {
            "source_begin": pa.array(labels, type=pa.string()),
            "source_end": pa.array(labels, type=pa.string()),
            "close": pa.array([Decimal("9.5000000000")] * len(labels), type=pa.decimal128(38, 10)),
        }
    )


def _higher(labels, values):
    return pa.Table.from_arrays(
        [
            pa.array(labels, type=pa.string()),
            pa.array(labels, type=pa.string()),
            pa.array(values, type=pa.decimal128(38, 10)),
        ],
        schema=pa.schema(
            [
                pa.field("source_begin", pa.string()),
                pa.field("source_end", pa.string()),
                pa.field("close", pa.decimal128(38, 10), metadata={b"meaning": b"source price"}),
            ]
        ),
    )


def test_strict_configured_boundary_null_match_and_original_order():
    lower = _lower(
        [
            "2026-09-15 10:20:01",
            "2026-09-15 09:20:00",
            "2026-09-15 09:19:59",
            "2026-09-15 09:20:01",
        ]
    )
    higher = _higher(["2026-09-15 09:00:00", "2026-09-15 10:00:00"], [Decimal("1"), None])
    result = _align_tables(lower, higher, ["close"])
    assert result.select(lower.column_names).equals(lower, check_metadata=True)
    assert result["higher__close"].to_pylist() == [None, None, None, Decimal("1")]
    assert result["higher__source_begin"].to_pylist() == [
        "2026-09-15 10:00:00",
        None,
        None,
        "2026-09-15 09:00:00",
    ]
    assert result["higher__join_key"].to_pylist() == [
        "2026-09-15 10:20:00",
        None,
        None,
        "2026-09-15 09:20:00",
    ]
    assert result.schema.field("higher__close").equals(
        higher.schema.field("close").with_name("higher__close"), check_metadata=True
    )


def test_future_high_key_mutation_cannot_change_earlier_targets():
    lower = _lower(["2026-09-15 09:21:00", "2026-09-15 10:19:59"])
    before = _higher(["2026-09-15 09:00:00", "2026-09-15 10:00:00"], [Decimal("1"), Decimal("2")])
    after = _higher(["2026-09-15 09:00:00", "2026-09-15 12:00:00"], [Decimal("1"), Decimal("999")])
    assert _align_tables(lower, before, ["close"]).equals(
        _align_tables(lower, after, ["close"]), check_metadata=True
    )


def test_duplicate_matching_key_rejects_even_when_prices_equal():
    higher = _higher(["2026-09-15 09:00:00"] * 2, [Decimal("1")] * 2)
    with pytest.raises(SchemaValidationError, match="duplicate higher matching key"):
        _align_tables(_lower(["2026-09-15 10:00:00"]), higher, ["close"])


@pytest.mark.parametrize("labels", [[], ["2026-09-15 10:00:00"]])
def test_empty_higher_preserves_typed_null_columns(labels):
    lower = _lower(labels)
    result = _align_tables(lower, _higher([], []), ["close"])
    assert result.select(lower.column_names).equals(lower, check_metadata=True)
    assert result["higher__close"].type == pa.decimal128(38, 10)
    assert result["higher__close"].null_count == len(labels)
    assert result["higher__source_begin"].null_count == len(labels)


def test_source_key_parser_rejects_utc_and_invalid_calendar_dates():
    with pytest.raises(SchemaValidationError, match="source-local key time"):
        _align_tables(_lower(["2026-09-15T10:00:00Z"]), _higher([], []), ["close"])
    with pytest.raises(SchemaValidationError, match="invalid source-local key time"):
        _align_tables(_lower(["2026-02-30 10:00:00"]), _higher([], []), ["close"])


def test_temporal_mode_has_no_source_key_fallback():
    from trading_advisor_3000.product_plane.research.native_source_key import align_source_key

    class NoReads:
        def read(self, *args, **kwargs):
            raise AssertionError("temporal rejection must precede input reads")

    with pytest.raises(SchemaValidationError, match="^temporal_mode_unverified$"):
        align_source_key(NoReads(), {"mode": "TEMPORAL"})


@pytest.mark.parametrize("payload", [None, {"mode": "SOURCE_KEY"}])
def test_invalid_alignment_request_rejects_before_graph_io(payload):
    from trading_advisor_3000.product_plane.research.native_source_key import align_source_key

    class NoReads:
        def read(self, *args, **kwargs):
            raise AssertionError("invalid wire request must precede graph reads")

    with pytest.raises(SchemaValidationError):
        align_source_key(NoReads(), payload)
