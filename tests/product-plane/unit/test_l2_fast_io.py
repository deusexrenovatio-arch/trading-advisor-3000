"""Native snapshot/cache/codec regressions for the L2 preparation path."""

import math

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from trading_advisor_3000.product_plane.data_plane.delta_runtime import DeltaArrowReader
from trading_advisor_3000.product_plane.research.derived_indicators.fingerprints import (
    columnar_fingerprint,
)
from trading_advisor_3000.product_plane.research.derived_indicators.read_cache import (
    PartitionReadCache,
)


def test_hash_golden_and_canonical_equivalences():
    assert columnar_fingerprint(pa.table({}), domain="") == (
        "columnar-v1:bc5b7bb3311052d7487aef3e37e8c211f4237ce89571320771c321745fa25f8c"
    )
    a = pa.table({"x": [None, -0.0, math.inf, -math.inf], "s": ["", None, "Я", "a\0b"]})
    b = pa.table({"s": a["s"], "x": [math.nan, 0.0, math.inf, -math.inf]})
    digest = columnar_fingerprint(a, domain="test")
    assert digest == columnar_fingerprint(b, domain="test")
    chunks = pa.concat_tables([a.slice(0, 1), a.slice(1, 2), a.slice(3)])
    assert digest == columnar_fingerprint(chunks, domain="test")
    assert digest != columnar_fingerprint(a, domain="other")
    assert digest != columnar_fingerprint(
        a.set_column(0, "x", pa.array([0.0, 0.0, math.inf, -math.inf])), domain="test"
    )
    assert columnar_fingerprint(
        pa.table({"a": ["ab"], "bc": ["c"]}), domain="x"
    ) != columnar_fingerprint(pa.table({"a": ["a"], "bc": ["bc"]}), domain="x")


def test_hash_dictionary_list_slice_and_types():
    strings = pa.array(["padding", "Я", None, "", "tail"])
    lists = pa.array([[9], [1, None], None, [], [8]], type=pa.list_(pa.int32()))
    plain = pa.table({"s": strings.slice(1, 3), "list": lists.slice(1, 3)})
    encoded = plain.set_column(0, "s", plain["s"].dictionary_encode())
    assert columnar_fingerprint(plain, domain="x") == columnar_fingerprint(encoded, domain="x")
    assert columnar_fingerprint(
        pa.table({"v": pa.array([1], type=pa.int32())}), domain="x"
    ) != columnar_fingerprint(pa.table({"v": pa.array([1], type=pa.int64())}), domain="x")
    with pytest.raises(TypeError):
        columnar_fingerprint(
            pa.table({"unsupported": pa.array([1], type=pa.timestamp("us"))}), domain="x"
        )


@pytest.mark.parametrize(
    "name,values",
    [
        ("n", pa.array([None, 1], type=pa.int32())),
        ("u", pa.array([0, 2], type=pa.uint64())),
        ("b", pa.array([None, True], type=pa.bool_())),
        ("s", pa.array(["", "Я"])),
        ("binary", pa.array([b"", b"\0"])),
        ("list", pa.array([[], [None]], type=pa.list_(pa.int32()))),
    ],
)
def test_non_equivalent_semantic_changes_change_hash(name, values):
    left = pa.table({name: values.slice(0, 1)})
    right = pa.table({name: values.slice(1, 1)})
    assert columnar_fingerprint(left, domain="x") != columnar_fingerprint(right, domain="x")


def test_native_cache_one_scan_full_scope_and_pinned_snapshot(tmp_path, monkeypatch):
    path = tmp_path / "probe.delta"
    table = pa.table(
        {
            "instrument_id": ["A", "A", "B"],
            "timeframe": ["15m"] * 3,
            "series_id": ["x", "y", "x"],
            "contract_id": ["one", "two", "one"],
            "value": [1.0, None, 3.0],
        }
    )
    write_deltalake(str(path), table)
    reader = DeltaArrowReader(path)
    write_deltalake(str(path), table.slice(0, 1), mode="append")
    calls = []
    original = reader.batches
    monkeypatch.setattr(reader, "batches", lambda **kw: (calls.append(kw), original(**kw))[1])
    cache = PartitionReadCache(reader, columns=table.column_names, filters=[], enabled=True)
    assert cache.directory is None
    with cache:
        filters = [
            ("instrument_id", "=", "A"),
            ("series_id", "=", "y"),
            ("contract_id", "=", "two"),
        ]
        actual = cache.read(columns=table.column_names, filters=filters)
        assert actual.to_pylist() == table.slice(1, 1).to_pylist()
        assert actual.schema == table.schema
        assert len(cache.read(filters=[("instrument_id", "=", "A")])) == 2
        assert len(calls) == 1
        directory = cache.directory
        assert directory.exists()
        direct = reader.read(columns=table.column_names, filters=filters)
        assert columnar_fingerprint(actual, domain="x") == columnar_fingerprint(direct, domain="x")
    assert not directory.exists()
    with pytest.raises(RuntimeError):
        cache.read(filters=[])


def test_cache_lazy_empty_failure_cleanup(tmp_path):
    path = tmp_path / "probe.delta"
    write_deltalake(
        str(path), pa.table({"instrument_id": ["A"], "timeframe": ["1h"], "value": [1.0]})
    )
    reader = DeltaArrowReader(path)
    with PartitionReadCache(
        reader, columns=reader.schema.names, filters=[], enabled=True
    ) as unused:
        assert unused.directory is None
    with PartitionReadCache(
        reader, columns=reader.schema.names, filters=[("value", ">", 2.0)], enabled=True
    ) as empty:
        assert len(empty.read()) == 0
        assert empty.read().schema == reader.schema
    failed = PartitionReadCache(reader, columns=reader.schema.names, filters=[], enabled=True)
    with pytest.raises(ValueError), failed:
        failed.read()
        directory = failed.directory
        raise ValueError("consumer failed")
    assert not directory.exists()


@pytest.mark.parametrize("enabled", [True, False])
def test_cache_direct_and_spill_keep_global_filter_and_projection(tmp_path, enabled):
    path = tmp_path / "scope.delta"
    write_deltalake(
        str(path),
        pa.table(
            {
                "instrument_id": ["A", "B"],
                "timeframe": ["1h"] * 2,
                "value": [1.0, 2.0],
                "unrequested": [3, 4],
            }
        ),
    )
    with PartitionReadCache(
        DeltaArrowReader(path),
        columns=["instrument_id", "timeframe", "value"],
        filters=[("instrument_id", "=", "A")],
        enabled=enabled,
    ) as cache:
        assert cache.read().to_pylist() == [{"instrument_id": "A", "timeframe": "1h", "value": 1.0}]
        assert cache.read(columns=["value", "unrequested"]).column_names == ["value"]
        with pytest.raises(ValueError, match="outside the cache projection"):
            cache.read(filters=[("unrequested", "=", 3)])
        assert len(cache.read(filters=[[("value", "=", 2.0)], [("timeframe", "=", "15m")]])) == 0
