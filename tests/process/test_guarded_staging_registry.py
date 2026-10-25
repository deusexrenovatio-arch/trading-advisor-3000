from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.runtime_instances import (
    build_moex_baseline_run_config_for_instance,
    load_moex_runtime_instances_registry,
)

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "instance_id,port", [("moex_branch_staging", 3011), ("moex_acceptance_staging", 3012)]
)
def test_guarded_registry_owns_dagster_and_preserves_logical_paths(instance_id, port):
    registry = load_moex_runtime_instances_registry(repo_root=ROOT)
    instance = registry.instance(instance_id)
    assert registry.dagster_owner(instance) is instance
    assert f":{port}/graphql" in instance.dagster["graphql_url"]
    assert instance.mutation_policy["require_protected_plan"] is True
    config = build_moex_baseline_run_config_for_instance(
        instance, run_id="test", ingest_till_utc="2026-10-24T00:00:00Z"
    )
    assert config["ops"]["moex_baseline_update"]["config"]["raw_table_path"].startswith(
        "/ta3000-data/moex-historical/"
    )
