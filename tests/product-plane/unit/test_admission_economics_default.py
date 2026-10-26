from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake
from test_research_admission import admitted_chain as admitted_chain
from test_research_admission import window

from trading_advisor_3000.dagster_defs.research_assets import _canonical_contract_economics_path
from trading_advisor_3000.product_plane.common import data_snapshots as storage
from trading_advisor_3000.product_plane.research import admission as owner


def test_daily_economics_default_binds_pinned_shared_table_and_rejects_override(admitted_chain):
    config, _, _ = admitted_chain
    canonical = Path(config["canonical_output_dir"])
    economics = canonical / "economics/canonical_contract_economics.delta"
    write_deltalake(str(economics), pa.table({"ts": ["2026-01-04T12:00:00Z"], "value": [1]}))
    root = canonical / ".data_snapshots"
    previous = storage.current_snapshot(root)
    tables = {
        **previous["tables"],
        "canonical_contract_economics": storage.capture_table(economics),
    }
    child = storage.create_snapshot(
        tables=tables,
        required_tables=list(tables),
        dataset_version="data",
        run_id="native-producer",
        code_version="fixture",
        quality_checks={"fixture": "PASS"},
        parents=[previous["snapshot_id"]],
        parameters={
            "downstream_scope": {
                "version": 2,
                "full_refresh": False,
                "covered_tables": tables,
                "windows": [window()],
                "effective_intervals": [],
            }
        },
    )
    path = storage.save_snapshot(root, child)
    storage.publish_snapshot(root, path, expected_current_id=previous["snapshot_id"])
    config["data_snapshot_manifest_path"] = path.as_posix()
    value = owner.issue_admission(config, operation="daily")
    assert value["outcome"] == "SCOPED"
    native_path = _canonical_contract_economics_path(config)
    assert native_path == economics.resolve()
    calls = []

    @owner.admitted_compute(output="output_dir", sources=("canonical_contract_economics_path",))
    def native(*, output_dir, dataset_version, canonical_contract_economics_path):
        calls.append(canonical_contract_economics_path)

    options = dict(
        output_dir=Path(config["materialized_output_dir"]),
        dataset_version=config["dataset_version"],
        canonical_contract_economics_path=native_path,
    )
    with owner._owner_scope(value):
        native(**options)
        with pytest.raises(ValueError, match="native input path changed"):
            native(
                **{
                    **options,
                    "canonical_contract_economics_path": canonical
                    / "canonical_contract_economics.delta",
                }
            )
    assert calls == [native_path]
    configured = {
        **config,
        "operation": "daily",
        "changed_windows": value["effective_windows"],
        "hash_compatibility": value["hash_compatibility"],
        "refresh_admission": value,
    }
    owner.validate_admission(value, config=configured)
    for explicit in ("", native_path.as_posix()):
        owner.validate_admission(
            value,
            config={**configured, "canonical_contract_economics_path": explicit},
            resolve=False,
        )
    with pytest.raises(ValueError, match="configuration changed"):
        owner.validate_admission(
            value,
            config={
                **configured,
                "canonical_contract_economics_path": (canonical / "wrong.delta").as_posix(),
            },
        )
