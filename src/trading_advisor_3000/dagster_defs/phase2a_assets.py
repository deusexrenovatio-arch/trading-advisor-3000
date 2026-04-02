from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from dagster import AssetSelection, Config, Definitions, asset, define_asset_job, materialize

from trading_advisor_3000.product_plane.data_plane.canonical import build_canonical_dataset, run_data_quality_checks
from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    read_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.ingestion import ingest_raw_backfill
from trading_advisor_3000.product_plane.data_plane.schemas import phase2a_delta_schema_manifest


@dataclass(frozen=True)
class AssetSpec:
    key: str
    description: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


PHASE2A_TABLES = (
    "raw_market_backfill",
    "canonical_bars",
    "canonical_instruments",
    "canonical_contracts",
    "canonical_session_calendar",
    "canonical_roll_map",
)

PHASE2A_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "raw_market_backfill": tuple(),
    "canonical_bars": ("raw_market_backfill",),
    "canonical_instruments": ("raw_market_backfill",),
    "canonical_contracts": ("raw_market_backfill",),
    "canonical_session_calendar": ("raw_market_backfill",),
    "canonical_roll_map": ("raw_market_backfill",),
}


def phase2a_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="raw_market_backfill",
            description="Ingest raw backfill rows for whitelist contracts.",
            inputs=("raw_backfill_source",),
            outputs=("raw_market_backfill_delta",),
        ),
        AssetSpec(
            key="canonical_bars",
            description="Build canonical OHLCV bars from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_bars_delta",),
        ),
        AssetSpec(
            key="canonical_instruments",
            description="Build canonical instruments table from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_instruments_delta",),
        ),
        AssetSpec(
            key="canonical_contracts",
            description="Build canonical contracts table from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_contracts_delta",),
        ),
        AssetSpec(
            key="canonical_session_calendar",
            description="Build canonical session calendar from raw backfill.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_session_calendar_delta",),
        ),
        AssetSpec(
            key="canonical_roll_map",
            description="Build canonical roll map by instrument/session.",
            inputs=("raw_market_backfill_delta",),
            outputs=("canonical_roll_map_delta",),
        ),
    ]


class Phase2aMaterializationConfig(Config):
    source_path: str
    output_dir: str
    whitelist_contracts: list[str]


def _normalize_whitelist(raw_values: list[str]) -> set[str]:
    values = {item.strip() for item in raw_values if item.strip()}
    if not values:
        raise ValueError("whitelist_contracts must contain at least one contract id")
    return values


def _table_path(report: dict[str, object], table_name: str) -> Path:
    output_paths = report.get("output_paths")
    if not isinstance(output_paths, dict):
        raise RuntimeError("phase2a report is missing `output_paths` mapping")
    path_value = output_paths.get(table_name)
    if not path_value:
        raise RuntimeError(f"phase2a report is missing output path for `{table_name}`")
    return Path(str(path_value))


def _schema_columns(report: dict[str, object], table_name: str) -> dict[str, str]:
    manifest = report.get("delta_schema_manifest")
    if not isinstance(manifest, dict):
        raise RuntimeError("phase2a report is missing `delta_schema_manifest` mapping")
    table_manifest = manifest.get(table_name)
    if not isinstance(table_manifest, dict):
        raise RuntimeError(f"phase2a report is missing schema manifest entry for `{table_name}`")
    columns = table_manifest.get("columns")
    if not isinstance(columns, dict):
        raise RuntimeError(f"phase2a schema manifest is missing `columns` for `{table_name}`")
    normalized: dict[str, str] = {}
    for column_name, type_name in columns.items():
        if not isinstance(column_name, str) or not isinstance(type_name, str):
            raise RuntimeError(f"phase2a schema manifest has non-string column contract for `{table_name}`")
        normalized[column_name] = type_name
    return normalized


def _load_rows(report: dict[str, object], table_name: str) -> list[dict[str, object]]:
    table_path = _table_path(report, table_name)
    if not has_delta_log(table_path):
        raise RuntimeError(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
    return read_delta_table_rows(table_path)


def _load_dataset(report: dict[str, object]):
    raw_rows = _load_rows(report, "raw_market_backfill")
    whitelist_values = report.get("whitelist_contracts")
    if not isinstance(whitelist_values, list):
        raise RuntimeError("phase2a report is missing `whitelist_contracts` list")
    whitelist_contracts = _normalize_whitelist([str(item) for item in whitelist_values])
    dataset = build_canonical_dataset(raw_rows)
    quality_errors = run_data_quality_checks(dataset.bars, whitelist_contracts=whitelist_contracts)
    if quality_errors:
        raise ValueError("data quality failed: " + "; ".join(quality_errors))
    return dataset


def _write_table(
    report: dict[str, object],
    *,
    table_name: str,
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    table_path = _table_path(report, table_name)
    write_delta_table_rows(
        table_path=table_path,
        rows=rows,
        columns=_schema_columns(report, table_name),
    )
    if not has_delta_log(table_path):
        raise RuntimeError(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
    return read_delta_table_rows(table_path)


@asset(group_name="phase2a")
def raw_market_backfill(config: Phase2aMaterializationConfig) -> dict[str, object]:
    output_dir = Path(config.output_dir).resolve()
    output_paths = phase2a_output_paths(output_dir)
    delta_schema_manifest = phase2a_delta_schema_manifest()
    whitelist_contracts = _normalize_whitelist(config.whitelist_contracts)

    raw_output_path = Path(output_paths["raw_market_backfill"])
    existing_raw_rows = read_delta_table_rows(raw_output_path) if has_delta_log(raw_output_path) else []
    ingestion_batch = ingest_raw_backfill(
        Path(config.source_path),
        whitelist_contracts=whitelist_contracts,
        existing_rows=existing_raw_rows,
    )
    write_delta_table_rows(
        table_path=raw_output_path,
        rows=ingestion_batch.rows,
        columns=dict(delta_schema_manifest["raw_market_backfill"]["columns"]),
    )

    return {
        "source_path": Path(config.source_path).resolve().as_posix(),
        "output_dir": output_dir.as_posix(),
        "output_paths": output_paths,
        "delta_schema_manifest": delta_schema_manifest,
        "whitelist_contracts": sorted(whitelist_contracts),
        "source_rows": ingestion_batch.source_rows,
        "whitelisted_rows": ingestion_batch.whitelisted_rows,
        "incremental_rows": ingestion_batch.incremental_rows,
        "deduplicated_rows": ingestion_batch.deduplicated_rows,
        "stale_rows": ingestion_batch.stale_rows,
        "watermark_by_key": ingestion_batch.watermark_by_key,
    }


@asset(group_name="phase2a")
def canonical_bars(raw_market_backfill: dict[str, object]) -> list[dict[str, object]]:
    dataset = _load_dataset(raw_market_backfill)
    rows = [item.to_dict() for item in dataset.bars]
    return _write_table(raw_market_backfill, table_name="canonical_bars", rows=rows)


@asset(group_name="phase2a")
def canonical_instruments(raw_market_backfill: dict[str, object]) -> list[dict[str, object]]:
    dataset = _load_dataset(raw_market_backfill)
    rows = [item.to_dict() for item in dataset.instruments]
    return _write_table(raw_market_backfill, table_name="canonical_instruments", rows=rows)


@asset(group_name="phase2a")
def canonical_contracts(raw_market_backfill: dict[str, object]) -> list[dict[str, object]]:
    dataset = _load_dataset(raw_market_backfill)
    rows = [item.to_dict() for item in dataset.contracts]
    return _write_table(raw_market_backfill, table_name="canonical_contracts", rows=rows)


@asset(group_name="phase2a")
def canonical_session_calendar(raw_market_backfill: dict[str, object]) -> list[dict[str, object]]:
    dataset = _load_dataset(raw_market_backfill)
    rows = [item.to_dict() for item in dataset.session_calendar]
    return _write_table(raw_market_backfill, table_name="canonical_session_calendar", rows=rows)


@asset(group_name="phase2a")
def canonical_roll_map(raw_market_backfill: dict[str, object]) -> list[dict[str, object]]:
    dataset = _load_dataset(raw_market_backfill)
    rows = [item.to_dict() for item in dataset.roll_map]
    return _write_table(raw_market_backfill, table_name="canonical_roll_map", rows=rows)


PHASE2A_ASSETS = (
    raw_market_backfill,
    canonical_bars,
    canonical_instruments,
    canonical_contracts,
    canonical_session_calendar,
    canonical_roll_map,
)


phase2a_materialization_job = define_asset_job(
    name="phase2a_materialization_job",
    selection=AssetSelection.groups("phase2a"),
)


phase2a_definitions = Definitions(
    assets=list(PHASE2A_ASSETS),
    jobs=[phase2a_materialization_job],
)


def assert_phase2a_definitions_executable(definitions: Definitions | None = None) -> None:
    defs = definitions or phase2a_definitions
    try:
        repository = defs.get_repository_def()
        job = repository.get_job("phase2a_materialization_job")
    except Exception as exc:  # pragma: no cover - exercised by metadata-only disprover path
        raise RuntimeError(
            "phase2a Dagster definitions are metadata-only or incomplete: "
            "missing executable `phase2a_materialization_job`."
        ) from exc

    expected_nodes = set(PHASE2A_TABLES)
    actual_nodes = set(job.graph.node_dict.keys())
    missing_nodes = sorted(expected_nodes - actual_nodes)
    if missing_nodes:
        missing_text = ", ".join(missing_nodes)
        raise RuntimeError(
            "phase2a Dagster definitions are metadata-only or incomplete: "
            f"missing executable asset nodes: {missing_text}"
        )


def build_phase2a_definitions() -> Definitions:
    assert_phase2a_definitions_executable(phase2a_definitions)
    return phase2a_definitions


def phase2a_output_paths(output_dir: Path) -> dict[str, str]:
    return {
        table_name: (output_dir / f"{table_name}.delta").as_posix()
        for table_name in PHASE2A_TABLES
    }


PHASE2A_ASSET_BY_NAME = {
    asset_def.key.path[-1]: asset_def
    for asset_def in PHASE2A_ASSETS
}


def _resolve_selected_assets(selection: Sequence[str] | None) -> list[str]:
    if selection is None:
        return list(PHASE2A_TABLES)
    normalized = sorted({item.strip() for item in selection if item.strip()})
    if not normalized:
        raise ValueError("selection must include at least one asset key")
    unknown = [item for item in normalized if item not in PHASE2A_ASSET_BY_NAME]
    if unknown:
        unknown_text = ", ".join(sorted(unknown))
        raise ValueError(f"unknown phase2a asset selection: {unknown_text}")
    return normalized


def _resolve_expected_materialization(selection: Sequence[str]) -> list[str]:
    resolved: set[str] = set()

    def _visit(asset_name: str) -> None:
        if asset_name in resolved:
            return
        for dependency in PHASE2A_DEPENDENCIES.get(asset_name, tuple()):
            _visit(dependency)
        resolved.add(asset_name)

    for asset_name in selection:
        _visit(asset_name)
    return [table_name for table_name in PHASE2A_TABLES if table_name in resolved]


def materialize_phase2a_assets(
    *,
    source_path: Path,
    output_dir: Path,
    whitelist_contracts: set[str],
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    assert_phase2a_definitions_executable()
    selected_assets = _resolve_selected_assets(selection)
    expected_materialized_assets = _resolve_expected_materialization(selected_assets)
    normalized_contracts = sorted(_normalize_whitelist(list(whitelist_contracts)))
    output_dir = output_dir.resolve()
    output_paths = phase2a_output_paths(output_dir)

    result = materialize(
        assets=list(PHASE2A_ASSETS),
        selection=expected_materialized_assets,
        run_config={
            "ops": {
                "raw_market_backfill": {
                    "config": {
                        "source_path": source_path.resolve().as_posix(),
                        "output_dir": output_dir.as_posix(),
                        "whitelist_contracts": normalized_contracts,
                    }
                }
            }
        },
        raise_on_error=raise_on_error,
    )

    report: dict[str, object] = {
        "success": bool(result.success),
        "output_paths": output_paths,
        "selected_assets": selected_assets,
        "materialized_assets": expected_materialized_assets,
    }
    if not result.success:
        report["rows_by_table"] = {}
        return report

    rows_by_table: dict[str, int] = {}
    for table_name in expected_materialized_assets:
        path_text = output_paths[table_name]
        table_path = Path(path_text)
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
        rows_by_table[table_name] = len(read_delta_table_rows(table_path))

    unexpected_tables = [
        table_name
        for table_name in PHASE2A_TABLES
        if table_name not in expected_materialized_assets and has_delta_log(Path(output_paths[table_name]))
    ]
    if unexpected_tables:
        unexpected_text = ", ".join(unexpected_tables)
        raise RuntimeError(
            "phase2a Dagster materialization produced hidden side effects for non-selected assets: "
            f"{unexpected_text}"
        )

    report["rows_by_table"] = rows_by_table
    return report
