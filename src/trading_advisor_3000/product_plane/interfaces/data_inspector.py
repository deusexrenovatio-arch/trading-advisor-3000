from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from deltalake import DeltaTable
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    _normalize_loaded_rows,
    count_delta_table_rows,
    delta_table_columns,
    has_delta_log,
    read_delta_table_arrow,
)
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    MOEX_HISTORICAL_DATA_ROOT_ENV,
)

DATA_INSPECTOR_ROOT_ENV = "TA3000_DATA_INSPECTOR_ROOT"
DEFAULT_DATA_ROOT = Path("D:/TA3000-data/trading-advisor-3000-nightly")
DATA_INSPECTOR_HTML = Path(__file__).with_name("static") / "data_inspector.html"
RESEARCH_INSTRUMENT_TREE_RELATIVE_PATH = (
    Path("research") / "gold" / "current" / "research_instrument_tree.delta"
)


@dataclass(frozen=True)
class DataInspectorLayer:
    id: str
    label: str
    description: str
    relative_path: Path
    instrument_column: str
    timeframe_column: str
    timestamp_column: str
    series_column: str | None = None
    series_label: str = "Series"
    version_columns: tuple[str, ...] = ()
    default_columns: tuple[str, ...] = ()
    options_relative_path: Path | None = None
    options_instrument_column: str | None = None
    options_timeframes_json_column: str | None = None
    options_series_json_column: str | None = None
    static_timeframes: tuple[str, ...] = ()
    scan_table_timeframes: bool = False


DATA_INSPECTOR_LAYERS: dict[str, DataInspectorLayer] = {
    "raw": DataInspectorLayer(
        id="raw",
        label="Raw",
        description="MOEX source candles before canonicalization.",
        relative_path=Path("raw") / "moex" / "baseline-4y-current" / "raw_moex_history.delta",
        instrument_column="internal_id",
        timeframe_column="timeframe",
        timestamp_column="ts_open",
        series_column="finam_symbol",
        series_label="Contract",
        default_columns=(
            "internal_id",
            "finam_symbol",
            "timeframe",
            "source_interval",
            "ts_open",
            "ts_close",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
            "ingest_run_id",
        ),
        options_relative_path=RESEARCH_INSTRUMENT_TREE_RELATIVE_PATH,
        options_instrument_column="internal_id",
        options_series_json_column="contract_ids_json",
        scan_table_timeframes=True,
    ),
    "canonical": DataInspectorLayer(
        id="canonical",
        label="Canonical",
        description="Canonical bars after baseline normalization.",
        relative_path=Path("canonical") / "moex" / "baseline-4y-current" / "canonical_bars.delta",
        instrument_column="instrument_id",
        timeframe_column="timeframe",
        timestamp_column="ts",
        series_column="contract_id",
        series_label="Contract",
        default_columns=(
            "contract_id",
            "instrument_id",
            "timeframe",
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
        ),
        options_relative_path=RESEARCH_INSTRUMENT_TREE_RELATIVE_PATH,
        options_instrument_column="instrument_id",
        options_series_json_column="contract_ids_json",
        scan_table_timeframes=True,
    ),
    "indicators": DataInspectorLayer(
        id="indicators",
        label="Indicators",
        description="Materialized base indicator frames.",
        relative_path=Path("research") / "gold" / "current" / "research_indicator_frames.delta",
        instrument_column="instrument_id",
        timeframe_column="timeframe",
        timestamp_column="ts",
        version_columns=(
            "dataset_version",
            "contour_id",
            "series_mode",
            "indicator_set_version",
            "profile_version",
        ),
        default_columns=(
            "dataset_version",
            "contour_id",
            "series_mode",
            "series_id",
            "indicator_set_version",
            "profile_version",
            "contract_id",
            "instrument_id",
            "timeframe",
            "ts",
            "sma_20",
            "ema_20",
            "ema_50",
            "rsi_14",
            "macd_12_26_9",
            "macd_signal_12_26_9",
            "atr_14",
            "vp_poc_price",
            "vp_vwap",
            "vp_value_area_low",
            "vp_value_area_high",
        ),
        options_relative_path=RESEARCH_INSTRUMENT_TREE_RELATIVE_PATH,
        options_instrument_column="instrument_id",
        options_timeframes_json_column="timeframes_json",
    ),
    "derived_indicators": DataInspectorLayer(
        id="derived_indicators",
        label="Derived Indicators",
        description="Materialized derived relationships built from bars and indicators.",
        relative_path=Path("research")
        / "gold"
        / "current"
        / "research_derived_indicator_frames.delta",
        instrument_column="instrument_id",
        timeframe_column="timeframe",
        timestamp_column="ts",
        version_columns=(
            "dataset_version",
            "contour_id",
            "series_mode",
            "indicator_set_version",
            "derived_indicator_set_version",
            "profile_version",
        ),
        default_columns=(
            "dataset_version",
            "contour_id",
            "series_mode",
            "series_id",
            "indicator_set_version",
            "derived_indicator_set_version",
            "profile_version",
            "contract_id",
            "instrument_id",
            "timeframe",
            "ts",
            "rolling_high_20",
            "rolling_low_20",
            "session_high",
            "session_low",
            "week_high",
            "week_low",
            "session_vwap",
            "distance_to_session_vwap",
            "distance_to_week_high",
            "distance_to_week_low",
            "source_indicators_hash",
        ),
        options_relative_path=RESEARCH_INSTRUMENT_TREE_RELATIVE_PATH,
        options_instrument_column="instrument_id",
        options_timeframes_json_column="timeframes_json",
    ),
}


def resolve_data_root(
    data_root: Path | str | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    if data_root is not None:
        return Path(data_root)
    source = env if env is not None else os.environ
    configured = (
        source.get(DATA_INSPECTOR_ROOT_ENV)
        or source.get(MOEX_HISTORICAL_DATA_ROOT_ENV)
        or DEFAULT_DATA_ROOT.as_posix()
    )
    return Path(configured)


def _layer_or_404(layer_id: str) -> DataInspectorLayer:
    layer = DATA_INSPECTOR_LAYERS.get(layer_id)
    if layer is None:
        raise HTTPException(status_code=404, detail=f"unknown data inspector layer: {layer_id}")
    return layer


def _table_path(data_root: Path, layer: DataInspectorLayer) -> Path:
    return data_root / layer.relative_path


def _utc_mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def _table_version(path: Path) -> int | None:
    if not has_delta_log(path):
        return None
    return int(DeltaTable(str(path)).version())


def _layer_summary(
    data_root: Path,
    layer: DataInspectorLayer,
    *,
    include_count: bool,
) -> dict[str, object]:
    path = _table_path(data_root, layer)
    has_log = has_delta_log(path)
    columns: list[str] = []
    row_count: int | None = None
    if has_log:
        columns = list(delta_table_columns(path))
        if include_count:
            row_count = count_delta_table_rows(path)
    return {
        "id": layer.id,
        "label": layer.label,
        "description": layer.description,
        "table_path": str(path),
        "exists": path.exists(),
        "has_delta_log": has_log,
        "version": _table_version(path),
        "updated_at_utc": _utc_mtime(path),
        "row_count": row_count,
        "instrument_column": layer.instrument_column,
        "timeframe_column": layer.timeframe_column,
        "timestamp_column": layer.timestamp_column,
        "series_column": layer.series_column,
        "series_label": layer.series_label,
        "version_columns": list(layer.version_columns),
        "default_columns": [column for column in layer.default_columns if column in columns],
        "columns": columns,
    }


def _string_values(values: object) -> list[str]:
    if values is None:
        return []
    if not isinstance(values, list):
        return []
    return sorted({str(value) for value in values if value is not None and str(value).strip()})


def _parse_json_list(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item) for item in raw if item is not None and str(item).strip()]
    if not isinstance(raw, str) or not raw.strip():
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if item is not None and str(item).strip()]


def _sorted_values_by_key(values_by_key: dict[str, set[str]]) -> dict[str, list[str]]:
    return {key: sorted(values) for key, values in sorted(values_by_key.items()) if values}


def _sorted_nested_values(
    nested_values: dict[str, dict[str, set[str]]],
) -> dict[str, dict[str, list[str]]]:
    return {
        column: _sorted_values_by_key(values_by_instrument)
        for column, values_by_instrument in sorted(nested_values.items())
        if values_by_instrument
    }


def _parameter_columns(layer: DataInspectorLayer, *, include_series: bool) -> tuple[str, ...]:
    columns: list[str] = []
    if include_series and layer.series_column:
        columns.append(layer.series_column)
    columns.extend(layer.version_columns)
    return tuple(dict.fromkeys(columns))


def _parameter_labels(layer: DataInspectorLayer) -> dict[str, str]:
    labels = {column: column.replace("_", " ") for column in layer.version_columns}
    if layer.series_column:
        labels[layer.series_column] = layer.series_label
    return labels


def _scan_distinct_columns(path: Path, columns: tuple[str, ...]) -> dict[str, list[str]]:
    if not columns or not has_delta_log(path):
        return {}
    dataset = DeltaTable(str(path)).to_pyarrow_dataset()
    available = set(dataset.schema.names)
    selected_columns = [column for column in columns if column in available]
    if not selected_columns:
        return {}
    values = {column: set() for column in selected_columns}
    scanner = dataset.scanner(columns=selected_columns)
    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            for column in selected_columns:
                value = row.get(column)
                if value is not None and str(value).strip():
                    values[column].add(str(value))
    return {column: sorted(column_values) for column, column_values in values.items()}


def _scan_timeframes_by_instrument(
    path: Path,
    *,
    instrument_column: str,
    timeframe_column: str,
) -> tuple[set[str], dict[str, set[str]]]:
    if not has_delta_log(path):
        return set(), {}
    dataset = DeltaTable(str(path)).to_pyarrow_dataset()
    available = set(dataset.schema.names)
    if instrument_column not in available or timeframe_column not in available:
        return set(), {}
    timeframes: set[str] = set()
    by_instrument: dict[str, set[str]] = {}
    scanner = dataset.scanner(columns=[instrument_column, timeframe_column])
    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            instrument = str(row.get(instrument_column) or "").strip()
            timeframe = str(row.get(timeframe_column) or "").strip()
            if timeframe:
                timeframes.add(timeframe)
            if instrument and timeframe:
                by_instrument.setdefault(instrument, set()).add(timeframe)
    return timeframes, by_instrument


def _metadata_options(data_root: Path, layer: DataInspectorLayer) -> dict[str, object] | None:
    if layer.options_relative_path is None or layer.options_instrument_column is None:
        return None
    options_path = data_root / layer.options_relative_path
    if not has_delta_log(options_path):
        return None
    available = set(delta_table_columns(options_path))
    if layer.options_instrument_column not in available:
        return None
    columns = [layer.options_instrument_column]
    if layer.options_timeframes_json_column and layer.options_timeframes_json_column in available:
        columns.append(layer.options_timeframes_json_column)
    if layer.options_series_json_column and layer.options_series_json_column in available:
        columns.append(layer.options_series_json_column)
    table = DeltaTable(str(options_path)).to_pyarrow_dataset().to_table(columns=columns)
    rows = table.to_pylist()
    instruments = {
        str(row[layer.options_instrument_column])
        for row in rows
        if row.get(layer.options_instrument_column)
    }
    timeframes = set(layer.static_timeframes)
    timeframes_by_instrument: dict[str, set[str]] = {}
    if layer.options_timeframes_json_column:
        for row in rows:
            instrument = str(row.get(layer.options_instrument_column) or "").strip()
            row_timeframes = set(_parse_json_list(row.get(layer.options_timeframes_json_column)))
            timeframes.update(row_timeframes)
            if instrument and row_timeframes:
                timeframes_by_instrument.setdefault(instrument, set()).update(row_timeframes)
    if layer.static_timeframes:
        for instrument in instruments:
            timeframes_by_instrument.setdefault(instrument, set()).update(layer.static_timeframes)
    if layer.scan_table_timeframes:
        table_timeframes, table_timeframes_by_instrument = _scan_timeframes_by_instrument(
            _table_path(data_root, layer),
            instrument_column=layer.instrument_column,
            timeframe_column=layer.timeframe_column,
        )
        timeframes.update(table_timeframes)
        instruments.update(table_timeframes_by_instrument)
        for instrument, values in table_timeframes_by_instrument.items():
            timeframes_by_instrument.setdefault(instrument, set()).update(values)
    parameter_values: dict[str, set[str]] = {}
    parameter_values_by_instrument: dict[str, dict[str, set[str]]] = {}
    if layer.series_column and layer.options_series_json_column:
        for row in rows:
            instrument = str(row.get(layer.options_instrument_column) or "").strip()
            series_values = set(_parse_json_list(row.get(layer.options_series_json_column)))
            if series_values:
                parameter_values.setdefault(layer.series_column, set()).update(series_values)
            if instrument and series_values:
                parameter_values_by_instrument.setdefault(layer.series_column, {}).setdefault(
                    instrument, set()
                ).update(series_values)
    return {
        "layer": layer.id,
        "instruments": sorted(instruments),
        "timeframes": sorted(timeframes),
        "timeframes_by_instrument": _sorted_values_by_key(timeframes_by_instrument),
        "version_values": {},
        "parameter_values": _sorted_values_by_key(parameter_values),
        "parameter_values_by_instrument": _sorted_nested_values(parameter_values_by_instrument),
        "parameter_labels": _parameter_labels(layer),
        "options_source": str(options_path),
    }


def _distinct_options(data_root: Path, layer: DataInspectorLayer) -> dict[str, object]:
    metadata_payload = _metadata_options(data_root, layer)
    if metadata_payload is not None:
        metadata_payload["version_values"] = _scan_distinct_columns(
            _table_path(data_root, layer),
            layer.version_columns,
        )
        parameter_values = dict(metadata_payload.get("parameter_values") or {})
        parameter_values.update(metadata_payload["version_values"])
        metadata_payload["parameter_values"] = parameter_values
        return metadata_payload
    path = _table_path(data_root, layer)
    if not has_delta_log(path):
        raise HTTPException(status_code=404, detail=f"delta table not found: {path}")
    available = set(delta_table_columns(path))
    parameter_columns = _parameter_columns(layer, include_series=True)
    option_columns = [
        column
        for column in (layer.instrument_column, layer.timeframe_column, *parameter_columns)
        if column in available
    ]
    table = DeltaTable(str(path)).to_pyarrow_dataset().to_table(columns=option_columns)
    payload: dict[str, object] = {
        "layer": layer.id,
        "instruments": [],
        "timeframes": [],
        "timeframes_by_instrument": {},
        "version_values": {},
        "parameter_values": {},
        "parameter_values_by_instrument": {},
        "parameter_labels": _parameter_labels(layer),
        "options_source": str(path),
    }
    values_by_column: dict[str, list[str]] = {}
    for column in option_columns:
        values_by_column[column] = _string_values(table.column(column).to_pylist())
    payload["instruments"] = values_by_column.get(layer.instrument_column, [])
    payload["timeframes"] = values_by_column.get(layer.timeframe_column, [])
    rows = table.to_pylist()
    timeframes_by_instrument: dict[str, set[str]] = {}
    parameter_values_by_instrument: dict[str, dict[str, set[str]]] = {}
    for row in rows:
        instrument = str(row.get(layer.instrument_column) or "").strip()
        timeframe = str(row.get(layer.timeframe_column) or "").strip()
        if instrument and timeframe:
            timeframes_by_instrument.setdefault(instrument, set()).add(timeframe)
        for column in parameter_columns:
            value = str(row.get(column) or "").strip()
            if instrument and value:
                parameter_values_by_instrument.setdefault(column, {}).setdefault(
                    instrument, set()
                ).add(value)
    payload["timeframes_by_instrument"] = _sorted_values_by_key(timeframes_by_instrument)
    payload["version_values"] = {
        column: values_by_column[column]
        for column in layer.version_columns
        if column in values_by_column
    }
    payload["parameter_values"] = {
        column: values_by_column[column]
        for column in parameter_columns
        if column in values_by_column
    }
    payload["parameter_values_by_instrument"] = _sorted_nested_values(
        parameter_values_by_instrument
    )
    return payload


def _requested_columns(path: Path, raw_columns: str | None) -> list[str] | None:
    if raw_columns is None or not raw_columns.strip():
        return None
    available = set(delta_table_columns(path))
    selected = [column.strip() for column in raw_columns.split(",") if column.strip()]
    missing = [column for column in selected if column not in available]
    if missing:
        raise HTTPException(status_code=400, detail=f"unknown columns: {', '.join(missing)}")
    return selected


def _sort_direction_or_400(raw_direction: str | None) -> str:
    value = (raw_direction or "desc").strip().lower()
    if value in {"asc", "ascending"}:
        return "asc"
    if value in {"desc", "descending"}:
        return "desc"
    raise HTTPException(status_code=400, detail="sort_direction must be one of: asc, desc")


def _requested_sort_column(
    path: Path,
    layer: DataInspectorLayer,
    raw_sort_by: str | None,
) -> str:
    sort_by = (raw_sort_by or layer.timestamp_column).strip()
    available = set(delta_table_columns(path))
    if sort_by not in available:
        raise HTTPException(status_code=400, detail=f"unknown sort column: {sort_by}")
    return sort_by


def _read_sorted_rows(
    path: Path,
    *,
    columns: list[str] | None,
    filters: list[tuple[str, str, object]],
    sort_by: str,
    sort_direction: str,
    limit: int,
) -> list[dict[str, object]]:
    response_columns = columns or list(delta_table_columns(path))
    read_columns = list(dict.fromkeys([*response_columns, sort_by]))
    arrow_table = read_delta_table_arrow(path, columns=read_columns, filters=filters)
    if arrow_table.num_rows:
        arrow_table = arrow_table.sort_by(
            [(sort_by, "ascending" if sort_direction == "asc" else "descending")]
        )
    arrow_table = arrow_table.slice(0, limit)
    rows = _normalize_loaded_rows(arrow_table.to_pylist())
    if sort_by not in response_columns:
        for row in rows:
            row.pop(sort_by, None)
    return rows


def _read_filters(
    layer: DataInspectorLayer,
    *,
    instrument: str,
    timeframe: str,
    series: str | None,
    start_ts: str | None,
    end_ts: str | None,
    dataset_version: str | None,
    contour_id: str | None,
    series_mode: str | None,
    indicator_set_version: str | None,
    derived_indicator_set_version: str | None,
    profile_version: str | None,
) -> list[tuple[str, str, object]]:
    filters: list[tuple[str, str, object]] = [
        (layer.instrument_column, "=", instrument),
        (layer.timeframe_column, "=", timeframe),
    ]
    if start_ts:
        filters.append((layer.timestamp_column, ">=", start_ts))
    if end_ts:
        filters.append((layer.timestamp_column, "<=", end_ts))
    if layer.series_column and series:
        filters.append((layer.series_column, "=", series))
    version_filters = {
        "dataset_version": dataset_version,
        "contour_id": contour_id,
        "series_mode": series_mode,
        "indicator_set_version": indicator_set_version,
        "derived_indicator_set_version": derived_indicator_set_version,
        "profile_version": profile_version,
    }
    for column in layer.version_columns:
        value = version_filters.get(column)
        if value:
            filters.append((column, "=", value))
    return filters


def create_app(
    *,
    data_root: Path | str | None = None,
    env: Mapping[str, str] | None = None,
) -> FastAPI:
    resolved_data_root = resolve_data_root(data_root, env=env)
    app = FastAPI(title="TA3000 Data Inspector", version="0.2.0")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        if not DATA_INSPECTOR_HTML.exists():
            raise HTTPException(status_code=500, detail="data inspector html asset is missing")
        return DATA_INSPECTOR_HTML.read_text(encoding="utf-8")

    @app.get("/api/data-inspector/layers")
    def list_layers(include_counts: bool = False) -> dict[str, object]:
        return {
            "data_root": str(resolved_data_root),
            "layers": [
                _layer_summary(resolved_data_root, layer, include_count=include_counts)
                for layer in DATA_INSPECTOR_LAYERS.values()
            ],
        }

    @app.get("/api/data-inspector/options")
    def list_options(layer: str = Query(min_length=1)) -> dict[str, object]:
        selected_layer = _layer_or_404(layer)
        return _distinct_options(resolved_data_root, selected_layer)

    @app.get("/api/data-inspector/rows")
    def read_rows(
        layer: str = Query(min_length=1),
        instrument: str = Query(min_length=1),
        timeframe: str = Query(min_length=1),
        limit: int = Query(default=100, ge=1, le=500),
        series: str | None = None,
        start_ts: str | None = None,
        end_ts: str | None = None,
        columns: str | None = None,
        sort_by: str | None = None,
        sort_direction: str | None = None,
        dataset_version: str | None = None,
        contour_id: str | None = None,
        series_mode: str | None = None,
        indicator_set_version: str | None = None,
        derived_indicator_set_version: str | None = None,
        profile_version: str | None = None,
    ) -> dict[str, object]:
        selected_layer = _layer_or_404(layer)
        path = _table_path(resolved_data_root, selected_layer)
        if not has_delta_log(path):
            raise HTTPException(status_code=404, detail=f"delta table not found: {path}")
        selected_columns = _requested_columns(path, columns)
        resolved_sort_by = _requested_sort_column(path, selected_layer, sort_by)
        resolved_sort_direction = _sort_direction_or_400(sort_direction)
        filters = _read_filters(
            selected_layer,
            instrument=instrument,
            timeframe=timeframe,
            series=series,
            start_ts=start_ts,
            end_ts=end_ts,
            dataset_version=dataset_version,
            contour_id=contour_id,
            series_mode=series_mode,
            indicator_set_version=indicator_set_version,
            derived_indicator_set_version=derived_indicator_set_version,
            profile_version=profile_version,
        )
        rows = _read_sorted_rows(
            path,
            columns=selected_columns,
            filters=filters,
            sort_by=resolved_sort_by,
            sort_direction=resolved_sort_direction,
            limit=limit,
        )
        return {
            "layer": selected_layer.id,
            "table_path": str(path),
            "columns": selected_columns or list(delta_table_columns(path)),
            "sort": {"column": resolved_sort_by, "direction": resolved_sort_direction},
            "rows": rows,
        }

    return app


app = create_app()
