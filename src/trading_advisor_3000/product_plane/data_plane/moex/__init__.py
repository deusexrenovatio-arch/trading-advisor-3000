from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .baseline_update import run_moex_baseline_update
    from .foundation import (
        DiscoveryRecord,
        FoundationRunReport,
        ingest_moex_baseline_window,
        ingest_moex_bootstrap_window,
        load_mapping_registry,
        load_universe,
        run_moex_foundation,
    )
    from .historical_route_contracts import (
        acquire_technical_route_lease,
        build_parity_manifest_v1,
        build_raw_ingest_run_report_v2,
        heartbeat_technical_route_lease,
        record_technical_route_blocked_conflict,
        read_technical_route_run_ledger,
        release_technical_route_lease,
        takeover_technical_route_lease,
    )
    from .iss_client import MoexISSClient
    from .historical_canonical_route import (
        run_contract_compatibility_check,
        run_historical_canonical_route,
        run_qc_gates,
        run_runtime_decoupling_check,
    )
    from .phase03_reconciliation import (
        ingest_finam_archive_snapshots,
        load_phase03_threshold_policy,
        run_phase03_reconciliation,
    )
    from .historical_dagster_cutover import run_historical_dagster_cutover
    from .route_staging_binding import build_route_staging_binding_report
    from .phase04_operations import (
        load_phase04_monitoring_policy,
        load_phase04_scheduler_policy,
        run_phase04_production_hardening,
    )


_MODULE_EXPORTS = {
    "run_moex_baseline_update": (".baseline_update", "run_moex_baseline_update"),
    "DiscoveryRecord": (".foundation", "DiscoveryRecord"),
    "FoundationRunReport": (".foundation", "FoundationRunReport"),
    "MoexISSClient": (".iss_client", "MoexISSClient"),
    "acquire_technical_route_lease": (".historical_route_contracts", "acquire_technical_route_lease"),
    "build_parity_manifest_v1": (".historical_route_contracts", "build_parity_manifest_v1"),
    "build_route_staging_binding_report": (".route_staging_binding", "build_route_staging_binding_report"),
    "build_raw_ingest_run_report_v2": (".historical_route_contracts", "build_raw_ingest_run_report_v2"),
    "heartbeat_technical_route_lease": (".historical_route_contracts", "heartbeat_technical_route_lease"),
    "ingest_moex_baseline_window": (".foundation", "ingest_moex_baseline_window"),
    "ingest_moex_bootstrap_window": (".foundation", "ingest_moex_bootstrap_window"),
    "ingest_finam_archive_snapshots": (".phase03_reconciliation", "ingest_finam_archive_snapshots"),
    "load_mapping_registry": (".foundation", "load_mapping_registry"),
    "load_phase03_threshold_policy": (".phase03_reconciliation", "load_phase03_threshold_policy"),
    "load_phase04_monitoring_policy": (".phase04_operations", "load_phase04_monitoring_policy"),
    "load_phase04_scheduler_policy": (".phase04_operations", "load_phase04_scheduler_policy"),
    "load_universe": (".foundation", "load_universe"),
    "record_technical_route_blocked_conflict": (
        ".historical_route_contracts",
        "record_technical_route_blocked_conflict",
    ),
    "read_technical_route_run_ledger": (".historical_route_contracts", "read_technical_route_run_ledger"),
    "release_technical_route_lease": (".historical_route_contracts", "release_technical_route_lease"),
    "run_contract_compatibility_check": (".historical_canonical_route", "run_contract_compatibility_check"),
    "run_moex_foundation": (".foundation", "run_moex_foundation"),
    "run_historical_canonical_route": (".historical_canonical_route", "run_historical_canonical_route"),
    "run_historical_dagster_cutover": (".historical_dagster_cutover", "run_historical_dagster_cutover"),
    "run_phase03_reconciliation": (".phase03_reconciliation", "run_phase03_reconciliation"),
    "run_phase04_production_hardening": (".phase04_operations", "run_phase04_production_hardening"),
    "run_qc_gates": (".historical_canonical_route", "run_qc_gates"),
    "run_runtime_decoupling_check": (".historical_canonical_route", "run_runtime_decoupling_check"),
    "takeover_technical_route_lease": (".historical_route_contracts", "takeover_technical_route_lease"),
}


def __getattr__(name: str) -> object:
    target = _MODULE_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = target
    import importlib

    module = importlib.import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


__all__ = sorted(_MODULE_EXPORTS)
