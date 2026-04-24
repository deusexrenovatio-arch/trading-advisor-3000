from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import json
from pathlib import Path

from dagster import AssetSelection, Definitions, Field as DagsterField, asset, define_asset_job, materialize

from trading_advisor_3000.product_plane.contracts import CanonicalBar
from trading_advisor_3000.product_plane.data_plane.delta_runtime import has_delta_log, read_delta_table_rows, write_delta_table_rows
from trading_advisor_3000.product_plane.research.backtest import run_backtest
from trading_advisor_3000.product_plane.research.features import (
    FeatureSnapshot,
    TechnicalIndicatorSnapshot,
    build_feature_snapshots_from_indicators,
    build_indicator_snapshots,
    research_feature_store_contract,
    run_indicator_feature_quality_gates,
)
from trading_advisor_3000.product_plane.research.scoring import (
    build_strategy_promotion_decisions,
    build_strategy_scorecards,
    load_strategy_scoring_profile,
)
from trading_advisor_3000.product_plane.research.strategies import sample_strategy_ids

from .historical_data_proof_assets import AssetSpec


DEFAULT_INDICATOR_SET_VERSION = "pandas-ta-v1"
DEFAULT_FEATURE_SET_VERSION = "gold"
RESEARCH_GOVERNED_GOVERNED_TABLES = (
    "technical_indicator_snapshot",
    "gold_feature_snapshot",
    "research_runtime_candidate_projection",
    "strategy_scorecard",
    "strategy_promotion_decision",
    "promoted_strategy_registry",
)

RESEARCH_GOVERNED_GOVERNED_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "technical_indicator_snapshot": tuple(),
    "gold_feature_snapshot": ("technical_indicator_snapshot",),
    "research_runtime_candidate_projection": ("gold_feature_snapshot",),
    "strategy_scorecard": ("research_runtime_candidate_projection",),
    "strategy_promotion_decision": ("strategy_scorecard",),
    "promoted_strategy_registry": ("strategy_promotion_decision",),
}

RESEARCH_GOVERNED_OP_CONFIG_SCHEMA = {
    "canonical_bars_path": str,
    "output_dir": str,
    "indicator_output_dir": DagsterField(str, is_required=False),
    "strategy_ids": [str],
    "dataset_version": str,
    "indicator_set_version": str,
    "feature_set_version": str,
    "canonical_timeframes": DagsterField([str], is_required=False),
    "canonical_start_ts": DagsterField(str, is_required=False),
    "canonical_end_ts": DagsterField(str, is_required=False),
    "canonical_max_rows": DagsterField(int, is_required=False),
    "source_storage_binding_json": DagsterField(str, is_required=False),
    "scoring_profile_path": str,
    "walk_forward_windows": int,
    "commission_per_trade": float,
    "slippage_bps": float,
    "session_start_hour_utc": int,
    "session_end_hour_utc": int,
    "capital_rub": int,
}


def research_governed_asset_specs() -> list[AssetSpec]:
    return [
        AssetSpec(
            key="technical_indicator_snapshot",
            description="Build PandasTA technical indicator snapshots from canonical bars.",
            inputs=("canonical_bars_delta",),
            outputs=("technical_indicator_snapshot_delta",),
        ),
        AssetSpec(
            key="gold_feature_snapshot",
            description="Build governed feature snapshots from PandasTA indicator snapshots.",
            inputs=("technical_indicator_snapshot_delta",),
            outputs=("gold_feature_snapshot_delta", "feature_snapshots_delta", "indicator_feature_qc_report"),
        ),
        AssetSpec(
            key="research_runtime_candidate_projection",
            description="Run strategy batches and materialize governed candidate projection.",
            inputs=("gold_feature_snapshot_delta",),
            outputs=("research_runtime_candidate_projection_delta",),
        ),
        AssetSpec(
            key="strategy_scorecard",
            description="Score strategy candidates with versioned profile thresholds.",
            inputs=("research_runtime_candidate_projection_delta",),
            outputs=("strategy_scorecard_delta",),
        ),
        AssetSpec(
            key="strategy_promotion_decision",
            description="Build promotion decisions and promoted strategy registry.",
            inputs=("strategy_scorecard_delta",),
            outputs=("strategy_promotion_decision_delta", "promoted_strategy_registry_delta"),
        ),
        AssetSpec(
            key="promoted_strategy_registry",
            description="Expose promoted strategy registry rows from promotion decision step.",
            inputs=("strategy_promotion_decision_delta",),
            outputs=("promoted_strategy_registry_delta",),
        ),
    ]


def _normalize_strategy_ids(raw_values: list[str]) -> list[str]:
    values = sorted({item.strip() for item in raw_values if item.strip()})
    if not values:
        return list(sample_strategy_ids())
    return values


def _config_from_context(context) -> dict[str, object]:
    execution_context = getattr(context, "op_execution_context", None)
    op_config = getattr(execution_context, "op_config", None)
    if not isinstance(op_config, dict):
        op_config = getattr(context, "op_config", None)
    if not isinstance(op_config, dict):
        raise RuntimeError("research-governed asset context is missing op_config mapping")

    config = dict(op_config)
    config.setdefault("dataset_version", "bars-whitelist-v1")
    config.setdefault("indicator_set_version", DEFAULT_INDICATOR_SET_VERSION)
    config.setdefault("feature_set_version", DEFAULT_FEATURE_SET_VERSION)
    config.setdefault("scoring_profile_path", "configs/moex_canonicalization/strategy_scoring_profile.v1.yaml")
    config.setdefault("walk_forward_windows", 2)
    config.setdefault("commission_per_trade", 0.25)
    config.setdefault("slippage_bps", 5.0)
    config.setdefault("session_start_hour_utc", 9)
    config.setdefault("session_end_hour_utc", 23)
    config.setdefault("capital_rub", 1_000_000)

    strategy_ids_raw = config.get("strategy_ids")
    if not isinstance(strategy_ids_raw, list):
        raise RuntimeError("strategy_ids must be list")
    config["strategy_ids"] = _normalize_strategy_ids([str(item) for item in strategy_ids_raw])

    if int(config["capital_rub"]) <= 0:
        raise RuntimeError("capital_rub must be positive")
    max_rows = config.get("canonical_max_rows")
    if max_rows is not None and max_rows != "" and int(max_rows) <= 0:
        raise RuntimeError("canonical_max_rows must be positive when provided")
    return config


def _table_path(output_dir: Path, table_name: str, *, indicator_output_dir: Path | None = None) -> Path:
    indicator_dir = indicator_output_dir or output_dir
    mapping = {
        "technical_indicator_snapshot": indicator_dir / "technical.indicator_snapshot.delta",
        "gold_feature_snapshot": output_dir / "gold.feature_snapshot.delta",
        "research_runtime_candidate_projection": output_dir / "research.runtime_candidate_projection.delta",
        "strategy_scorecard": output_dir / "strategy.scorecard.delta",
        "strategy_promotion_decision": output_dir / "strategy.promotion_decision.delta",
        "promoted_strategy_registry": output_dir / "strategy.promoted_registry.delta",
        "feature_snapshots": output_dir / "feature.snapshots.delta",
        "research_backtest_runs": output_dir / "research.backtest_runs.delta",
        "research_signal_candidates": output_dir / "research.signal_candidates.delta",
        "research_strategy_metrics": output_dir / "research.strategy_metrics.delta",
    }
    if table_name not in mapping:
        raise KeyError(f"unknown research-governed table path: {table_name}")
    return mapping[table_name]


def _indicator_feature_qc_report_path(output_dir: Path) -> Path:
    return output_dir / "indicator-feature-qc-report.json"


def _indicator_output_dir_from_config(config: dict[str, object]) -> Path | None:
    text = str(config.get("indicator_output_dir", "")).strip()
    return Path(text).resolve() if text else None


def _canonical_read_filters(config: dict[str, object]) -> list[tuple[str, str, object]] | list[list[tuple[str, str, object]]] | None:
    raw_timeframes = config.get("canonical_timeframes")
    timeframes: list[str] = []
    if isinstance(raw_timeframes, list):
        timeframes = sorted({str(item).strip() for item in raw_timeframes if str(item).strip()})

    start_ts = str(config.get("canonical_start_ts", "")).strip()
    end_ts = str(config.get("canonical_end_ts", "")).strip()
    if not timeframes and not start_ts and not end_ts:
        return None

    def _clause(timeframe: str | None = None) -> list[tuple[str, str, object]]:
        filters: list[tuple[str, str, object]] = []
        if timeframe is not None:
            filters.append(("timeframe", "=", timeframe))
        if start_ts:
            filters.append(("ts", ">=", start_ts))
        if end_ts:
            filters.append(("ts", "<", end_ts))
        return filters

    if timeframes:
        return [_clause(timeframe) for timeframe in timeframes]
    return _clause()


def research_governed_output_paths(output_dir: Path, *, indicator_output_dir: Path | None = None) -> dict[str, str]:
    paths = {
        table_name: _table_path(output_dir, table_name, indicator_output_dir=indicator_output_dir).as_posix()
        for table_name in (
            *RESEARCH_GOVERNED_GOVERNED_TABLES,
            "feature_snapshots",
            "research_backtest_runs",
            "research_signal_candidates",
            "research_strategy_metrics",
        )
    }
    paths["indicator_feature_qc_report"] = _indicator_feature_qc_report_path(output_dir).as_posix()
    return paths


def _load_canonical_bars(
    canonical_bars_path: Path,
    *,
    filters: list[tuple[str, str, object]] | list[list[tuple[str, str, object]]] | None = None,
    max_rows: int | None = None,
) -> list[CanonicalBar]:
    if not has_delta_log(canonical_bars_path):
        raise RuntimeError(f"canonical bars delta is missing `_delta_log`: {canonical_bars_path.as_posix()}")
    rows = read_delta_table_rows(canonical_bars_path, filters=filters)
    if max_rows is not None and len(rows) > max_rows:
        raise RuntimeError(f"canonical bars read exceeded canonical_max_rows={max_rows}: {len(rows)} rows")
    bars: list[CanonicalBar] = []
    for row in rows:
        try:
            bars.append(CanonicalBar.from_dict(row))
        except Exception:
            continue
    if not bars:
        raise RuntimeError("canonical bars path did not yield valid CanonicalBar rows")
    return bars


def _to_feature_snapshots(rows: list[dict[str, object]]) -> list[FeatureSnapshot]:
    snapshots: list[FeatureSnapshot] = []
    for row in rows:
        snapshots.append(FeatureSnapshot.from_dict(row))
    return snapshots


def _to_indicator_snapshots(rows: list[dict[str, object]]) -> list[TechnicalIndicatorSnapshot]:
    snapshots: list[TechnicalIndicatorSnapshot] = []
    for row in rows:
        snapshots.append(TechnicalIndicatorSnapshot.from_dict(row))
    return snapshots


def _resolve_selected_assets(selection: Sequence[str] | None) -> list[str]:
    if selection is None:
        return list(RESEARCH_GOVERNED_GOVERNED_TABLES)
    normalized = sorted({item.strip() for item in selection if item.strip()})
    if not normalized:
        raise ValueError("selection must include at least one research-governed asset")
    unknown = [item for item in normalized if item not in RESEARCH_GOVERNED_GOVERNED_DEPENDENCIES]
    if unknown:
        unknown_text = ", ".join(sorted(unknown))
        raise ValueError(f"unknown research-governed selection: {unknown_text}")
    return normalized


def _resolve_expected_materialization(selection: Sequence[str]) -> list[str]:
    resolved: set[str] = set()

    def _visit(asset_name: str) -> None:
        if asset_name in resolved:
            return
        for dependency in RESEARCH_GOVERNED_GOVERNED_DEPENDENCIES.get(asset_name, tuple()):
            _visit(dependency)
        resolved.add(asset_name)

    for asset_name in selection:
        _visit(asset_name)
    return [table_name for table_name in RESEARCH_GOVERNED_GOVERNED_TABLES if table_name in resolved]


@asset(group_name="research_governed", config_schema=RESEARCH_GOVERNED_OP_CONFIG_SCHEMA)
def technical_indicator_snapshot(context) -> dict[str, object]:
    config = _config_from_context(context)
    output_dir = Path(str(config["output_dir"])).resolve()
    indicator_output_dir = _indicator_output_dir_from_config(config)
    canonical_bars_path = Path(str(config["canonical_bars_path"])).resolve()
    canonical_filters = _canonical_read_filters(config)
    max_rows_raw = config.get("canonical_max_rows")
    max_rows = int(max_rows_raw) if max_rows_raw not in (None, "") else None
    bars = _load_canonical_bars(canonical_bars_path, filters=canonical_filters, max_rows=max_rows)
    indicators = build_indicator_snapshots(
        bars,
        indicator_set_version=str(config["indicator_set_version"]),
    )
    rows = [item.to_dict() for item in indicators]

    manifest = research_feature_store_contract()
    indicator_path = _table_path(output_dir, "technical_indicator_snapshot", indicator_output_dir=indicator_output_dir)
    write_delta_table_rows(
        table_path=indicator_path,
        rows=rows,
        columns=manifest["technical_indicator_snapshot"]["columns"],
    )

    return {
        "output_dir": output_dir.as_posix(),
        "indicator_output_dir": indicator_output_dir.as_posix() if indicator_output_dir is not None else output_dir.as_posix(),
        "output_paths": research_governed_output_paths(output_dir, indicator_output_dir=indicator_output_dir),
        "canonical_bars_path": canonical_bars_path.as_posix(),
        "canonical_filters": canonical_filters,
        "source_storage_binding_json": str(config.get("source_storage_binding_json", "")).strip(),
        "indicator_rows": len(rows),
        "rows": rows,
    }


@asset(group_name="research_governed", config_schema=RESEARCH_GOVERNED_OP_CONFIG_SCHEMA)
def gold_feature_snapshot(context, technical_indicator_snapshot: dict[str, object]) -> dict[str, object]:
    config = _config_from_context(context)
    output_dir = Path(str(config["output_dir"])).resolve()
    indicator_output_dir = _indicator_output_dir_from_config(config)

    indicator_rows_raw = technical_indicator_snapshot.get("rows")
    if not isinstance(indicator_rows_raw, list):
        raise RuntimeError("technical indicator asset output is missing `rows`")

    indicators = _to_indicator_snapshots([dict(item) for item in indicator_rows_raw if isinstance(item, dict)])
    snapshots = build_feature_snapshots_from_indicators(
        indicators,
        feature_set_version=str(config["feature_set_version"]),
    )
    rows = [item.to_dict() for item in snapshots]
    qc_report = run_indicator_feature_quality_gates(indicators=indicators, features=snapshots)
    if qc_report["status"] != "PASS":
        raise RuntimeError("indicator-feature quality gates failed")

    manifest = research_feature_store_contract()
    gold_path = _table_path(output_dir, "gold_feature_snapshot")
    feature_path = _table_path(output_dir, "feature_snapshots")
    qc_report_path = _indicator_feature_qc_report_path(output_dir)
    write_delta_table_rows(
        table_path=gold_path,
        rows=rows,
        columns=manifest["gold_feature_snapshot"]["columns"],
    )
    write_delta_table_rows(
        table_path=feature_path,
        rows=rows,
        columns=manifest["feature_snapshots"]["columns"],
    )
    qc_report_path.parent.mkdir(parents=True, exist_ok=True)
    qc_report_path.write_text(json.dumps(qc_report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return {
        "output_dir": output_dir.as_posix(),
        "indicator_output_dir": indicator_output_dir.as_posix() if indicator_output_dir is not None else output_dir.as_posix(),
        "output_paths": research_governed_output_paths(output_dir, indicator_output_dir=indicator_output_dir),
        "indicator_rows": len(indicators),
        "gold_rows": len(rows),
        "indicator_feature_qc_report": qc_report,
        "rows": rows,
    }


@asset(group_name="research_governed", config_schema=RESEARCH_GOVERNED_OP_CONFIG_SCHEMA)
def research_runtime_candidate_projection(
    context,
    gold_feature_snapshot: dict[str, object],
) -> dict[str, object]:
    config = _config_from_context(context)
    output_dir = Path(str(config["output_dir"])).resolve()
    indicator_output_dir = _indicator_output_dir_from_config(config)
    manifest = research_feature_store_contract()

    rows_raw = gold_feature_snapshot.get("rows")
    if not isinstance(rows_raw, list):
        raise RuntimeError("gold feature asset output is missing `rows`")
    snapshots = _to_feature_snapshots([dict(item) for item in rows_raw if isinstance(item, dict)])

    strategy_ids = list(config["strategy_ids"])
    projection_rows: list[dict[str, object]] = []
    signal_rows: list[dict[str, object]] = []
    backtest_rows: list[dict[str, object]] = []
    metrics_rows: list[dict[str, object]] = []

    for strategy_version_id in strategy_ids:
        report = run_backtest(
            snapshots,
            strategy_version_id=strategy_version_id,
            dataset_version=str(config["dataset_version"]),
            output_dir=None,
            walk_forward_windows=int(config["walk_forward_windows"]),
            commission_per_trade=float(config["commission_per_trade"]),
            slippage_bps=float(config["slippage_bps"]),
            session_hours_utc=(int(config["session_start_hour_utc"]), int(config["session_end_hour_utc"])),
            capital_rub=int(config["capital_rub"]),
        )
        projection_rows.extend(report["candidate_projection"])
        signal_rows.extend(report["research_candidates"])
        backtest_rows.append(report["backtest_run"])
        metrics_rows.append(report["strategy_metrics"])

    projection_path = _table_path(output_dir, "research_runtime_candidate_projection")
    signal_path = _table_path(output_dir, "research_signal_candidates")
    runs_path = _table_path(output_dir, "research_backtest_runs")
    metrics_path = _table_path(output_dir, "research_strategy_metrics")

    write_delta_table_rows(
        table_path=projection_path,
        rows=projection_rows,
        columns=manifest["research_runtime_candidate_projection"]["columns"],
    )
    write_delta_table_rows(
        table_path=signal_path,
        rows=signal_rows,
        columns=manifest["research_signal_candidates"]["columns"],
    )
    write_delta_table_rows(
        table_path=runs_path,
        rows=backtest_rows,
        columns=manifest["research_backtest_runs"]["columns"],
    )
    write_delta_table_rows(
        table_path=metrics_path,
        rows=metrics_rows,
        columns=manifest["research_strategy_metrics"]["columns"],
    )

    return {
        "output_dir": output_dir.as_posix(),
        "output_paths": research_governed_output_paths(output_dir, indicator_output_dir=indicator_output_dir),
        "strategy_ids": strategy_ids,
        "projection_rows": projection_rows,
    }


@asset(group_name="research_governed", config_schema=RESEARCH_GOVERNED_OP_CONFIG_SCHEMA)
def strategy_scorecard(
    context,
    research_runtime_candidate_projection: dict[str, object],
) -> dict[str, object]:
    config = _config_from_context(context)
    output_dir = Path(str(config["output_dir"])).resolve()
    indicator_output_dir = _indicator_output_dir_from_config(config)
    scoring_profile_path = Path(str(config["scoring_profile_path"])).resolve()
    profile = load_strategy_scoring_profile(scoring_profile_path)

    projection_rows_raw = research_runtime_candidate_projection.get("projection_rows")
    if not isinstance(projection_rows_raw, list):
        raise RuntimeError("candidate projection asset output is missing `projection_rows`")

    scorecards = build_strategy_scorecards(
        projection_rows=[dict(item) for item in projection_rows_raw if isinstance(item, dict)],
        profile=profile,
        repeatable=True,
    )

    manifest = research_feature_store_contract()
    scorecard_path = _table_path(output_dir, "strategy_scorecard")
    write_delta_table_rows(
        table_path=scorecard_path,
        rows=[
            {
                **row,
                "repeatable": "true" if row["repeatable"] else "false",
                "criteria_json": row["criteria"],
                "blocked_reasons_json": row["blocked_reasons"],
            }
            for row in scorecards
        ],
        columns=manifest["strategy_scorecard"]["columns"],
    )

    return {
        "output_dir": output_dir.as_posix(),
        "output_paths": research_governed_output_paths(output_dir, indicator_output_dir=indicator_output_dir),
        "profile_version": profile.profile_version,
        "scorecards": scorecards,
    }


@asset(group_name="research_governed", config_schema=RESEARCH_GOVERNED_OP_CONFIG_SCHEMA)
def strategy_promotion_decision(
    context,
    strategy_scorecard: dict[str, object],
) -> dict[str, object]:
    config = _config_from_context(context)
    output_dir = Path(str(config["output_dir"])).resolve()
    indicator_output_dir = _indicator_output_dir_from_config(config)

    scorecards_raw = strategy_scorecard.get("scorecards")
    if not isinstance(scorecards_raw, list):
        raise RuntimeError("strategy scorecard asset output is missing `scorecards`")

    scorecards = [dict(item) for item in scorecards_raw if isinstance(item, dict)]
    decisions = build_strategy_promotion_decisions(scorecards)
    scorecard_by_strategy = {
        str(item.get("strategy_version_id", "")).strip(): item
        for item in scorecards
        if str(item.get("strategy_version_id", "")).strip()
    }

    promoted_registry = []
    for row in decisions:
        if row["verdict"] != "PASS":
            continue
        scorecard = scorecard_by_strategy.get(row["strategy_version_id"], {})
        promoted_registry.append(
            {
                "strategy_version_id": row["strategy_version_id"],
                "strategy_family": str(scorecard.get("strategy_family", "unknown")),
                "profile_version": row["profile_version"],
                "capital_rub": row["capital_rub"],
                "promoted_at": row["effective_from"],
                "reproducibility_fingerprint": row["reproducibility_fingerprint"],
            }
        )

    manifest = research_feature_store_contract()
    decision_path = _table_path(output_dir, "strategy_promotion_decision")
    promoted_path = _table_path(output_dir, "promoted_strategy_registry")
    write_delta_table_rows(
        table_path=decision_path,
        rows=[{**row, "blocked_reasons_json": row["blocked_reasons"]} for row in decisions],
        columns=manifest["strategy_promotion_decision"]["columns"],
    )
    write_delta_table_rows(
        table_path=promoted_path,
        rows=promoted_registry,
        columns=manifest["promoted_strategy_registry"]["columns"],
    )

    return {
        "output_dir": output_dir.as_posix(),
        "output_paths": research_governed_output_paths(output_dir, indicator_output_dir=indicator_output_dir),
        "decisions": decisions,
        "promoted_registry": promoted_registry,
    }


@asset(group_name="research_governed")
def promoted_strategy_registry(
    strategy_promotion_decision: dict[str, object],
) -> list[dict[str, object]]:
    promoted_raw = strategy_promotion_decision.get("promoted_registry")
    if not isinstance(promoted_raw, list):
        raise RuntimeError("strategy promotion decision output is missing `promoted_registry`")
    return [dict(item) for item in promoted_raw if isinstance(item, dict)]


RESEARCH_GOVERNED_GOVERNED_ASSETS = (
    technical_indicator_snapshot,
    gold_feature_snapshot,
    research_runtime_candidate_projection,
    strategy_scorecard,
    strategy_promotion_decision,
    promoted_strategy_registry,
)


research_governed_materialization_job = define_asset_job(
    name="research_governed_materialization_job",
    selection=AssetSelection.groups("research_governed"),
)


research_governed_definitions = Definitions(
    assets=list(RESEARCH_GOVERNED_GOVERNED_ASSETS),
    jobs=[research_governed_materialization_job],
)


def assert_research_governed_definitions_executable(definitions: Definitions | None = None) -> None:
    defs = definitions or research_governed_definitions
    try:
        repository = defs.get_repository_def()
        job = repository.get_job("research_governed_materialization_job")
    except Exception as exc:
        raise RuntimeError(
            "research-governed Dagster definitions are metadata-only or incomplete: "
            "missing executable `research_governed_materialization_job`."
        ) from exc

    expected_nodes = set(RESEARCH_GOVERNED_GOVERNED_TABLES)
    actual_nodes = set(job.graph.node_dict.keys())
    missing_nodes = sorted(expected_nodes - actual_nodes)
    if missing_nodes:
        missing_text = ", ".join(missing_nodes)
        raise RuntimeError(
            "research-governed Dagster definitions are metadata-only or incomplete: "
            f"missing executable asset nodes: {missing_text}"
        )


def build_research_governed_definitions() -> Definitions:
    assert_research_governed_definitions_executable(research_governed_definitions)
    return research_governed_definitions


def materialize_research_governed_assets(
    *,
    canonical_bars_path: Path,
    output_dir: Path,
    indicator_output_dir: Path | None = None,
    strategy_ids: list[str] | None = None,
    dataset_version: str = "bars-whitelist-v1",
    indicator_set_version: str = DEFAULT_INDICATOR_SET_VERSION,
    feature_set_version: str = DEFAULT_FEATURE_SET_VERSION,
    canonical_timeframes: Sequence[str] | None = None,
    canonical_start_ts: str | None = None,
    canonical_end_ts: str | None = None,
    canonical_max_rows: int | None = None,
    source_storage_binding: dict[str, object] | None = None,
    scoring_profile_path: Path | None = None,
    selection: Sequence[str] | None = None,
    raise_on_error: bool = True,
) -> dict[str, object]:
    assert_research_governed_definitions_executable()
    selected_assets = _resolve_selected_assets(selection)
    expected_materialized_assets = _resolve_expected_materialization(selected_assets)

    output_dir = output_dir.resolve()
    resolved_indicator_output_dir = indicator_output_dir.resolve() if indicator_output_dir is not None else None
    scoring_path = (scoring_profile_path or Path("configs/moex_canonicalization/strategy_scoring_profile.v1.yaml")).resolve()
    resolved_strategy_ids = _normalize_strategy_ids(strategy_ids or [])

    op_config = {
        "canonical_bars_path": canonical_bars_path.resolve().as_posix(),
        "output_dir": output_dir.as_posix(),
        "indicator_output_dir": resolved_indicator_output_dir.as_posix() if resolved_indicator_output_dir is not None else "",
        "strategy_ids": resolved_strategy_ids,
        "dataset_version": dataset_version,
        "indicator_set_version": indicator_set_version,
        "feature_set_version": feature_set_version,
        "canonical_timeframes": [str(item) for item in (canonical_timeframes or [])],
        "canonical_start_ts": str(canonical_start_ts or ""),
        "canonical_end_ts": str(canonical_end_ts or ""),
        "source_storage_binding_json": json.dumps(source_storage_binding or {}, ensure_ascii=False, sort_keys=True),
        "scoring_profile_path": scoring_path.as_posix(),
        "walk_forward_windows": 2,
        "commission_per_trade": 0.25,
        "slippage_bps": 5.0,
        "session_start_hour_utc": 9,
        "session_end_hour_utc": 23,
        "capital_rub": 1_000_000,
    }
    if canonical_max_rows is not None:
        op_config["canonical_max_rows"] = int(canonical_max_rows)

    configurable_assets = {
        "technical_indicator_snapshot",
        "gold_feature_snapshot",
        "research_runtime_candidate_projection",
        "strategy_scorecard",
        "strategy_promotion_decision",
    }
    ops_config = {
        asset_name: {"config": op_config}
        for asset_name in expected_materialized_assets
        if asset_name in configurable_assets
    }

    result = materialize(
        assets=list(RESEARCH_GOVERNED_GOVERNED_ASSETS),
        selection=expected_materialized_assets,
        run_config={"ops": ops_config},
        raise_on_error=raise_on_error,
    )

    report: dict[str, object] = {
        "success": bool(result.success),
        "output_paths": research_governed_output_paths(output_dir, indicator_output_dir=resolved_indicator_output_dir),
        "selected_assets": selected_assets,
        "materialized_assets": expected_materialized_assets,
        "source_storage_binding": source_storage_binding or {},
    }
    if not result.success:
        report["rows_by_table"] = {}
        return report

    rows_by_table: dict[str, int] = {}
    for table_name in expected_materialized_assets:
        table_path = _table_path(output_dir, table_name, indicator_output_dir=resolved_indicator_output_dir)
        if not has_delta_log(table_path):
            raise RuntimeError(f"missing `_delta_log` for `{table_name}` at {table_path.as_posix()}")
        rows_by_table[table_name] = len(read_delta_table_rows(table_path))

    report["rows_by_table"] = rows_by_table
    return report
