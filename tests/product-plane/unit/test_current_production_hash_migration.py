"""Actual post-226 native proof migration; tiny real Delta inputs and outputs."""

import hashlib
import json
from dataclasses import replace

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from test_derived_indicator_tails import native_materializer as native_materializer

from trading_advisor_3000.product_plane.research import hash_compatibility as bridge
from trading_advisor_3000.product_plane.research.derived_indicators import materialize as m
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    load_derived_indicator_frames,
    write_derived_indicator_frame_batches,
)


def _old_compute(ladder=()):
    # Independent reconstruction from exact79257fa3 CRLF files, in old order.
    implementation = [
        "3183b255524a646acbf791cad4a12b31bd5017d410a7c8ff444ef7c95a40fb15",
        "03e55fd245842af90f697ad9a30cdd5005e88038060bf56e44bf62685dcc3f9c",
        "78630475a3a79745d9116a6e00b888e1bdc258240a7f839fb280c495c29a0ee1",
        "b017e27e3ed51f21b8b0799bbfa06ebf252306c895707b07ed00c29205204796",
        "2f4efdbb0a62db05f963b92daad11bcc2d7a1e83ef847fb8537706280ecec376",
        "f003b68ccafe64d2bd063dd9ea94d9eebdf2b5462619b46d697f4c3301ba059f",
    ]
    payload = dict(
        contract="derived-native-v1",
        implementation=implementation,
        numpy=np.__version__,
        pandas=pd.__version__,
        bar_usage=m.BAR_USAGE_POLICY_ID,
        indicator_usage=m.INDICATOR_USAGE_POLICY_ID,
        ladder=sorted(ladder, key=lambda row: json.dumps(row, sort_keys=True, default=str)),
    )
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _rows(root):
    return sorted(
        DeltaTable(str(root / "research_derived_indicator_frames.delta"))
        .to_pyarrow_table()
        .to_pylist(),
        key=lambda row: (row["instrument_id"], row["ts"]),
    )


@pytest.fixture
def old_native(native_materializer, monkeypatch):
    inputs, run, root, profile = native_materializer
    run()  # real Delta source frame; no production data mounted
    columns = m._source_indicator_columns_for_profile(profile)
    legacy = []
    partitions = []
    for asset in ("A", "B"):
        partition = m.DerivedSourceFramePartitionKey(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            timeframe="15m",
            instrument_id=asset,
            contract_id=None,
            contour_id="native_tradable",
            series_mode="continuous_front",
            series_id=asset,
        )
        source = m.load_derived_source_frame_partition_rows(
            output_dir=root, partition=partition, source_indicator_columns=columns
        )
        bars, indicators, frame = m._source_frame_rows_to_inputs(
            rows=source, source_indicator_columns=columns
        )
        frame = m._with_continuous_front_projection(frame, adjustment_ladder_rows=())
        # Genuine old unchanged builder/formulas/JSON recipe, not relabelled
        # columnar metadata. AST identity to old Git is attested separately.
        built = m._build_partition_rows(
            dataset_version="dataset-v5",
            indicator_set_version="indicators-v1",
            derived_indicator_set_version="derived-v1",
            series_mode="continuous_front",
            profile=profile,
            local_series=bars,
            local_indicator_rows=indicators,
            base_frame=frame,
            compute_contract_hash=_old_compute(),
            source_snapshot_hash="old-accepted-snapshot",
        )
        assert any(value is not None for row in built for value in row.values.values())
        # Mixed historical provenance is valid; newest complete receipt wins.
        built[0] = replace(
            built[0],
            compute_contract_hash="earlier-native-compute",
            source_bars_hash="earlier-native-bars",
            created_at="2020-01-01T00:00:00Z",
        )
        legacy.append(built)
        partitions.append(built[0].partition_key(series_mode="continuous_front"))
    write_derived_indicator_frame_batches(
        output_dir=root,
        row_batches=legacy,
        replace_partitions=tuple(partitions),
        profile=profile,
        atomic_partition_batches=True,
    )
    monkeypatch.setattr(bridge, "reviewed_current_source", lambda: True)
    return inputs, run, root, profile


def test_unchanged_old_native_migrates_only_complete_carriers(old_native, monkeypatch):
    _, run, root, _ = old_native
    before = _rows(root)
    monkeypatch.setattr(
        m, "_compute_derived_frame", lambda **_: pytest.fail("hash-only numerical recompute")
    )
    with bridge._verified_migration_scope(bridge.NATIVE_LEGACY_SOURCE):
        result = run()
    assert result["hash_migrated_partition_count"] == 2
    assert result["recomputed_partition_count"] == 0
    assert result["reused_partition_count"] == 2
    assert result["refreshed_row_count"] == 2
    after = _rows(root)
    for asset in ("A", "B"):
        old = [row for row in before if row["instrument_id"] == asset]
        new = [row for row in after if row["instrument_id"] == asset]
        assert new[:-1] == old[:-1]
        changed = {key for key in old[-1] if new[-1][key] != old[-1][key]}
        assert changed <= {
            "source_bars_hash",
            "source_indicators_hash",
            "compute_contract_hash",
            "source_snapshot_hash",
            "source_dataset_bars_hash",
            "created_at",
        }
        assert new[-1]["compute_contract_hash"] == m._derived_compute_contract_hash(())
        assert new[-1]["source_bars_hash"].startswith("columnar-v1:")
    assert run()["hash_migrated_partition_count"] == 0
    assert run()["refreshed_row_count"] == 0


@pytest.mark.parametrize("change", ["value", "null", "key", "recipe", "warmup"])
def test_changed_native_input_or_recipe_cannot_migrate(old_native, change):
    inputs, run, root, profile = old_native
    path = root / "research_derived_indicator_frames.delta"
    if change in {"value", "null"}:
        inputs["A"][5]["ema_20"] = 999.0 if change == "value" else None
    else:
        table = DeltaTable(str(path))
        if change == "key":
            table.update({"ts": "ts + INTERVAL '1 second'"}, predicate="instrument_id = 'A'")
        elif change == "recipe":
            table.update(
                {"compute_contract_hash": "'unknown-compute'"}, predicate="instrument_id = 'A'"
            )
        else:
            table.update({"warmup_span": "1"}, predicate="instrument_id = 'A'")
    with bridge._verified_migration_scope(bridge.NATIVE_LEGACY_SOURCE):
        result = run()
    assert result["hash_migrated_partition_count"] == 1
    assert result["refreshed_partition_count"] == 1
    assert result["recomputed_partition_count"] == 1
    actual = load_derived_indicator_frames(
        output_dir=root,
        dataset_version="dataset-v5",
        indicator_set_version="indicators-v1",
        value_columns=profile.output_columns,
    )
    expected = []
    for asset in ("A", "B"):
        bars, indicators, frame = m._source_frame_rows_to_inputs(
            rows=inputs[asset],
            source_indicator_columns=m._source_indicator_columns_for_profile(profile),
        )
        expected.extend(
            m._build_partition_rows(
                dataset_version="dataset-v5",
                indicator_set_version="indicators-v1",
                derived_indicator_set_version="derived-v1",
                series_mode="continuous_front",
                profile=profile,
                local_series=bars,
                local_indicator_rows=indicators,
                base_frame=m._with_continuous_front_projection(frame, adjustment_ladder_rows=()),
            )
        )
    assert {(row.instrument_id, row.ts): row.values for row in actual} == {
        (row.instrument_id, row.ts): row.values for row in expected
    }


def test_explicit_force_never_enters_migration(old_native, monkeypatch):
    _, run, _, _ = old_native
    monkeypatch.setattr(
        m, "_reviewed_native_partition_metadata", lambda **_: pytest.fail("force migrated")
    )
    with bridge._verified_migration_scope(bridge.NATIVE_LEGACY_SOURCE):
        result = run(force_recompute=True)
    assert result["hash_migrated_partition_count"] == 0
    assert result["recomputed_partition_count"] == 2
    assert result["refreshed_row_count"] == 48


def test_native_migration_source_race_cannot_commit(old_native, monkeypatch):
    _, run, root, _ = old_native
    path = root / "research_derived_indicator_frames.delta"
    before = DeltaTable(str(path)).version()
    native = m._reviewed_native_partition_metadata

    def race(**kwargs):
        result = native(**kwargs)
        write_deltalake(
            str(root / "research_indicator_frames.delta"), pa.table({"probe": [2]}), mode="append"
        )
        return result

    monkeypatch.setattr(m, "_reviewed_native_partition_metadata", race)
    with bridge._verified_migration_scope(bridge.NATIVE_LEGACY_SOURCE):
        with pytest.raises(ValueError, match="inputs changed"):
            run()
    assert DeltaTable(str(path)).version() == before


@pytest.mark.parametrize("damage", ["missing", "duplicate", "duplicate_and_missing"])
def test_damaged_old_output_never_migrates(old_native, damage):
    _, run, root, _ = old_native
    path = root / "research_derived_indicator_frames.delta"
    table = DeltaTable(str(path))
    original = table.to_pyarrow_table(filters=[("instrument_id", "=", "A")]).sort_by(
        [("ts", "ascending")]
    )
    if damage in {"missing", "duplicate_and_missing"}:
        table.delete("instrument_id = 'A' AND ts = '" + original["ts"][0].as_py().isoformat() + "'")
    if damage in {"duplicate", "duplicate_and_missing"}:
        write_deltalake(str(path), original.slice(1, 1), mode="append")
    with bridge._verified_migration_scope(bridge.NATIVE_LEGACY_SOURCE):
        if damage == "duplicate_and_missing":
            with pytest.raises(ValueError, match="unique"):
                run()
        else:
            result = run()
            assert result["hash_migrated_partition_count"] == 0
            assert result["recomputed_partition_count"] == 2
            assert len(_rows(root)) == 48


def test_accepted_source_scope_is_required_for_native_adoption(old_native):
    _, run, _, _ = old_native
    result = run()
    assert result["hash_migrated_partition_count"] == 0
    assert result["recomputed_partition_count"] == 2


def test_old_producer_helpers_are_the_exact_reviewed_old_functions():
    import ast
    import inspect

    expected = {
        "_source_frame_rows_to_inputs": (
            "eea2248dc21a3d807aa2f67198f0b5350754e7d501d436adb07685258f5e6c7f"
        ),
        "_bars_hash": "a927d8db8ad195b22eb784e4498dbf957d4365693273db5954170a7e46c7f3d2",
        "_indicator_hash": "b273d8afee60115d41915b6abb994c281bbad82819ef1a593cb464d0ed693932",
        "_prepare_base_frame": "9da401bb4de74606397a3710514086720eb3a45178c1b693c4139886b39762a0",
        "_with_continuous_front_projection": (
            "4e77092aca6e536895dc136dd42a6af651bb2424c6c9313ee3100efd491f3747"
        ),
        "_compute_derived_frame_unmasked": (
            "148ffafe7d097154bc9b13c145a9310d47f49317402dfa82e7e9fa0881ba6ea0"
        ),
        "_compute_derived_frame": (
            "d423f66046ead1fa0abfb7ab72ca45334c6e039b82a905a94ece9db62a6dbaf9"
        ),
        "_source_inputs_hash": "b14ee5b40aba35ffd7b24cc060da24f71514ff16e8a8ad29e86194e18c7ce722",
        "_build_partition_rows": "287806877554da573cb11d342c8cec58206738214e1f8820b0fe05741266bc73",
    }
    for name, digest in expected.items():
        node = ast.parse(inspect.getsource(getattr(m, name))).body[0]
        assert hashlib.sha256(ast.dump(node).encode()).hexdigest() == digest


def test_equal_null_contract_keys_never_grant_native_adoption(old_native, monkeypatch):
    from types import SimpleNamespace

    _, run, _, _ = old_native
    native = m._reviewed_native_partition_metadata
    seen = []

    def check_null_keys(**kwargs):
        table = kwargs["table"]
        table = table.set_column(
            table.schema.get_field_index("contract_id"),
            "contract_id",
            pa.nulls(len(table), type=pa.string()),
        )
        old_bars, old_indicators = m._columnar_source_hashes(
            table, m._source_indicator_columns_for_profile(kwargs["profile"])
        )
        head = {
            **kwargs["head"],
            "source_bars_hash": old_bars,
            "source_indicators_hash": old_indicators,
        }
        # All other valid recipe/profile/count/input guards match. Equal NULL
        # source/output contract keys must still reject metadata-only adoption.
        reader = SimpleNamespace(read=lambda **_: table.select(["ts", "contract_id"]))
        assert native(**{**kwargs, "table": table, "head": head, "reader": reader}) is None
        seen.append(kwargs["partition"])
        return native(**kwargs)

    monkeypatch.setattr(m, "_reviewed_native_partition_metadata", check_null_keys)
    with bridge._verified_migration_scope(bridge.NATIVE_LEGACY_SOURCE):
        assert run()["hash_migrated_partition_count"] == 2
    assert len(seen) == 2
