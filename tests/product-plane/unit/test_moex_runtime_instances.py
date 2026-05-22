from __future__ import annotations

from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.data_plane.moex.runtime_instances import (
    PRODUCT_RUNTIME_ROLE,
    VERIFICATION_RUNTIME_ROLE,
    build_moex_baseline_run_config_for_instance,
    load_moex_runtime_instances_registry,
    render_moex_runtime_instance_paths,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_moex_runtime_instances_registry_declares_product_and_test_staging() -> None:
    registry = load_moex_runtime_instances_registry(repo_root=REPO_ROOT)

    product = registry.default_product_runtime()
    verification = registry.default_verification_runtime()

    assert product.instance_id == "moex_product_staging"
    assert product.role == PRODUCT_RUNTIME_ROLE
    assert product.paths["data_root"] == "/ta3000-data/moex-historical"
    assert (
        product.docker["preferred_container_data_root"]
        == "/ta3000-data/staging/product/moex-historical"
    )
    assert product.dagster["graphql_url"] == "http://127.0.0.1:3000/graphql"
    assert product.dagster["graphql_port_env"] == "TA3000_DAGSTER_PORT"
    assert product.dagster["job"] == "moex_baseline_update_job"
    assert product.mutation_policy["require_explicit_product_write"] is True
    assert verification.instance_id == "moex_test_staging_on_demand"
    assert verification.role == VERIFICATION_RUNTIME_ROLE
    assert verification.paths["seed_from_instance"] == "moex_product_staging"
    assert "{run_id}" in str(verification.paths["run_root_template"])
    assert verification.mutation_policy["require_seed"] is True


def test_moex_runtime_instances_render_product_and_test_paths() -> None:
    registry = load_moex_runtime_instances_registry(repo_root=REPO_ROOT)

    product_paths = render_moex_runtime_instance_paths(
        registry.default_product_runtime(),
        run_id="ignored-for-product",
    )
    test_paths = render_moex_runtime_instance_paths(
        registry.default_verification_runtime(),
        run_id="proof-run-1",
    )

    assert product_paths["raw_table"] == (
        "/ta3000-data/moex-historical/raw/moex/baseline-4y-current/raw_moex_history.delta"
    )
    assert test_paths["data_root"] == "/ta3000-data/staging/test/moex-verification/proof-run-1"
    assert test_paths["canonical_bars"] == (
        "/ta3000-data/staging/test/moex-verification/proof-run-1/"
        "canonical/moex/baseline-4y-current/canonical_bars.delta"
    )


def test_moex_runtime_instances_build_baseline_run_config() -> None:
    registry = load_moex_runtime_instances_registry(repo_root=REPO_ROOT)
    instance = registry.default_product_runtime()

    run_config = build_moex_baseline_run_config_for_instance(
        instance,
        run_id="manual-proof",
        ingest_till_utc="2026-05-08T12:39:47Z",
    )

    config = run_config["ops"]["moex_baseline_update"]["config"]
    assert config["run_id"] == "manual-proof"
    assert config["ingest_till_utc"] == "2026-05-08T12:39:47Z"
    assert config["timeframes"] == "1m,5m,15m,1h,4h,1d,1w"
    assert config["raw_table_path"].startswith("/ta3000-data/moex-historical/")


def test_moex_runtime_instances_reject_unknown_instance() -> None:
    registry = load_moex_runtime_instances_registry(repo_root=REPO_ROOT)

    with pytest.raises(KeyError, match="unknown MOEX runtime instance"):
        registry.instance("missing")


def test_moex_runtime_instances_reject_path_like_verification_run_id() -> None:
    registry = load_moex_runtime_instances_registry(repo_root=REPO_ROOT)

    with pytest.raises(ValueError, match="single safe path segment"):
        render_moex_runtime_instance_paths(
            registry.default_verification_runtime(),
            run_id="../escape",
        )


def test_moex_runtime_instances_require_product_seed_instance(tmp_path: Path) -> None:
    registry_path = (
        REPO_ROOT / "deployment" / "runtime-instances" / "moex-runtime-instances.v1.yaml"
    )
    bad_registry_path = tmp_path / "moex-runtime-instances.v1.yaml"
    bad_registry_path.write_text(
        registry_path.read_text(encoding="utf-8").replace(
            "seed_from_instance: moex_product_staging",
            "seed_from_instance: moex_test_staging_on_demand",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="seed_from_instance.*product_runtime_staging"):
        load_moex_runtime_instances_registry(bad_registry_path, repo_root=REPO_ROOT)


def test_moex_runtime_instances_reject_string_boolean_policy(tmp_path: Path) -> None:
    registry_path = (
        REPO_ROOT / "deployment" / "runtime-instances" / "moex-runtime-instances.v1.yaml"
    )
    bad_registry_path = tmp_path / "moex-runtime-instances.v1.yaml"
    bad_registry_path.write_text(
        registry_path.read_text(encoding="utf-8").replace(
            "manual_launch_allowed: true",
            'manual_launch_allowed: "false"',
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="manual_launch_allowed.*boolean"):
        load_moex_runtime_instances_registry(bad_registry_path, repo_root=REPO_ROOT)


def test_moex_runtime_instances_reject_string_boolean_launch_default(
    tmp_path: Path,
) -> None:
    registry_path = (
        REPO_ROOT / "deployment" / "runtime-instances" / "moex-runtime-instances.v1.yaml"
    )
    bad_registry_path = tmp_path / "moex-runtime-instances.v1.yaml"
    bad_registry_path.write_text(
        registry_path.read_text(encoding="utf-8").replace(
            "expand_contract_chain: true",
            'expand_contract_chain: "false"',
            1,
        ),
        encoding="utf-8",
    )
    registry = load_moex_runtime_instances_registry(bad_registry_path, repo_root=REPO_ROOT)

    with pytest.raises(ValueError, match="expand_contract_chain.*boolean"):
        build_moex_baseline_run_config_for_instance(
            registry.default_product_runtime(),
            run_id="manual-proof",
            ingest_till_utc="2026-05-08T12:39:47Z",
        )
