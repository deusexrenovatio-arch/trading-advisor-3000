from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from trading_advisor_3000.product_plane.research.derived_indicators.registry import (
    current_derived_indicator_profile,
)
from trading_advisor_3000.product_plane.research.derived_indicators.retire_mtf import (
    RETIRED_PROJECTIONS,
    apply_retirement,
    plan_retirement,
    rollback_retirement,
)
from trading_advisor_3000.product_plane.research.derived_indicators.store import (
    derived_indicator_output_columns_hash,
)
from trading_advisor_3000.spark_jobs.continuous_front_indicator_sidecar_job import (
    ROW_HASH_VERSION,
    _sidecar_row_hash_from_payload,
)

WRITER_ROOT = Path(__file__).resolve().parents[3]


def _legacy_tables(root):
    outputs = current_derived_indicator_profile().output_columns
    old_outputs = (*outputs, *RETIRED_PROJECTIONS)
    values = {c: pa.array([1.0, None]) for c in outputs}
    # Native integer outputs retain their physical types as well as values.
    for c in outputs:
        if c.endswith(("_code", "_flag")):
            values[c] = pa.array([1, None], type=pa.int32())
    values.update(
        dataset_version=["d", "d"],
        instrument_id=["BR", "BR"],
        timeframe=["15m", "15m"],
        ts=["2026-01-01T09:00:00Z", "2026-01-01T09:15:00Z"],
        **{column: [9.0, 9.0] for column in RETIRED_PROJECTIONS},
    )
    research = pa.table(
        {
            **values,
            "source_mtf_hash": ["old", "old"],
            "mtf_source_family_hash": ["production", "production"],
            "output_columns_hash": [derived_indicator_output_columns_hash(old_outputs)] * 2,
        }
    )
    write_deltalake(root / "research_derived_indicator_frames.delta", research)
    sidecar = pa.table(
        {
            **values,
            "derived_row_hash": ["old", "old"],
            "derived_row_hash_version": [ROW_HASH_VERSION] * 2,
            "adapter_bundle_hash": ["actual-original-producer"] * 2,
        }
    )
    write_deltalake(root / "continuous_front_derived_indicator_frames.delta", sidecar)
    write_deltalake(
        root / "indicator_roll_rules.delta",
        pa.table(
            {
                "output_column": [outputs[0], "mtf_1h_to_15m_ema_20"],
                "rule_hash": ["native", "retired"],
                "adapter_input_columns": [["open", "close"], ["source"]],
            }
        ),
    )


def test_native_projection_rewrite_preserves_rows_values_and_rollback(tmp_path):
    _legacy_tables(tmp_path)
    plan = plan_retirement(tmp_path)
    assert len(plan["tables"]) == 3
    before = DeltaTable(str(tmp_path / "research_derived_indicator_frames.delta"))
    outputs = list(current_derived_indicator_profile().output_columns)
    native_before = before.to_pyarrow_table(columns=outputs)
    receipt = apply_retirement(plan, writer_root=WRITER_ROOT)
    assert len(receipt["tables"]) == 3
    after = DeltaTable(str(tmp_path / "research_derived_indicator_frames.delta"))
    assert after.to_pyarrow_table(columns=outputs).equals(native_before)
    assert (
        after.to_pyarrow_table(columns=["source_inputs_hash"])["source_inputs_hash"].null_count == 2
    )
    sidecar = DeltaTable(
        str(tmp_path / "continuous_front_derived_indicator_frames.delta")
    ).to_pyarrow_table()
    for row in sidecar.to_pylist():
        assert row["derived_row_hash"] == _sidecar_row_hash_from_payload(row, tuple(outputs))
        assert row["adapter_bundle_hash"] == "actual-original-producer"
    assert not plan_retirement(tmp_path)["tables"]
    assert not apply_retirement(plan_retirement(tmp_path), writer_root=WRITER_ROOT)["tables"]
    rules = DeltaTable(str(tmp_path / "indicator_roll_rules.delta")).to_pyarrow_table()
    assert rules["output_column"].to_pylist() == [outputs[0]]
    # Native Delta restore retains recoverability; no filesystem surgery.
    rollback_retirement(receipt)
    restored = DeltaTable(str(tmp_path / "research_derived_indicator_frames.delta"))
    assert "source_mtf_hash" in restored.to_pyarrow_table().column_names
    assert restored.to_pyarrow_table(columns=outputs).equals(native_before)


def test_drift_rejects_every_table_before_any_write(tmp_path):
    _legacy_tables(tmp_path)
    plan = plan_retirement(tmp_path)
    path = tmp_path / "research_derived_indicator_frames.delta"
    table = DeltaTable(str(path))
    write_deltalake(path, table.to_pyarrow_table(), mode="append")
    with pytest.raises(ValueError, match="drift"):
        apply_retirement(plan, writer_root=WRITER_ROOT)
    assert (
        DeltaTable(str(tmp_path / "continuous_front_derived_indicator_frames.delta")).version() == 0
    )


def test_old_writer_and_unknown_profile_are_rejected(tmp_path):
    _legacy_tables(tmp_path)
    old = tmp_path / "writer/src/trading_advisor_3000/product_plane/research/derived_indicators"
    old.mkdir(parents=True)
    (old / "registry.py").write_text("MTF_MAPPINGS = ()")
    (old / "materialize.py").write_text("def _compute_mtf_overlay(): pass")
    with pytest.raises(ValueError, match="still produces"):
        apply_retirement(plan_retirement(tmp_path), writer_root=tmp_path / "writer")
    path = tmp_path / "research_derived_indicator_frames.delta"
    table = DeltaTable(str(path)).to_pyarrow_table()
    table = table.set_column(
        table.schema.get_field_index("output_columns_hash"),
        "output_columns_hash",
        pa.array(["unknown"] * len(table)),
    )
    write_deltalake(path, table, mode="overwrite")
    with pytest.raises(ValueError, match="unknown output profile"):
        apply_retirement(plan_retirement(tmp_path), writer_root=WRITER_ROOT)


def test_empty_tables_keep_native_types_and_null_rule_keys(tmp_path):
    _legacy_tables(tmp_path)
    for name in ("research_derived_indicator_frames", "continuous_front_derived_indicator_frames"):
        path = tmp_path / f"{name}.delta"
        write_deltalake(
            path, DeltaTable(str(path)).to_pyarrow_table().slice(0, 0), mode="overwrite"
        )
    path = tmp_path / "indicator_roll_rules.delta"
    original = DeltaTable(str(path)).to_pyarrow_table()
    nullable = pa.table(
        {
            "output_column": pa.array([None], type=pa.string()),
            "rule_hash": ["unknown"],
            "adapter_input_columns": [["close"]],
        }
    )
    write_deltalake(path, pa.concat_tables([original, nullable]), mode="overwrite")
    apply_retirement(plan_retirement(tmp_path), writer_root=WRITER_ROOT)
    schema = (
        DeltaTable(str(tmp_path / "research_derived_indicator_frames.delta"))
        .to_pyarrow_dataset()
        .schema
    )
    assert schema.field("output_columns_hash").type == pa.string()
    assert schema.field("source_inputs_hash").type == pa.string()
    sidecar = DeltaTable(
        str(tmp_path / "continuous_front_derived_indicator_frames.delta")
    ).to_pyarrow_dataset()
    assert sidecar.schema.field("derived_row_hash").type == pa.string()
    assert sidecar.count_rows() == 0
    assert DeltaTable(str(path)).to_pyarrow_table()["output_column"].to_pylist() == [
        current_derived_indicator_profile().output_columns[0],
        None,
    ]
