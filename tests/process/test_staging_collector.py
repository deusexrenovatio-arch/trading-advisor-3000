"""A producer counter/new manifest ID cannot replace native mutation evidence."""

import sys
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from collect_staging_acceptance import observed_mutations  # noqa: E402

from trading_advisor_3000.product_plane.common.data_snapshots import capture_table  # noqa: E402


def test_new_snapshot_id_without_native_mutation_is_not_storage_change(tmp_path):
    path = tmp_path / "raw.delta"
    write_deltalake(str(path), pa.table({"close": [1.0]}))
    tables = {"raw_moex_history": capture_table(path)}
    assert not observed_mutations(
        {"snapshot_id": "before", "tables": tables},
        {"snapshot_id": "different-id", "changed_windows": 999, "tables": tables},
    )


def test_native_append_supplies_pinned_version_files_and_row_change(tmp_path):
    path = tmp_path / "raw.delta"
    write_deltalake(str(path), pa.table({"close": [1.0]}))
    before = {"tables": {"raw_moex_history": capture_table(path)}}
    write_deltalake(str(path), pa.table({"close": [2.0]}), mode="append")
    result = observed_mutations(before, {"tables": {"raw_moex_history": capture_table(path)}})
    assert len(result) == 1
    mutation = result[0]
    assert mutation["from_version"] == 0 and mutation["to_version"] == 1
    assert mutation["before_rows"] == 1 and mutation["after_rows"] == 2
    assert mutation["before_files_sha256"] != mutation["after_files_sha256"]
