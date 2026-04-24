from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pandas_ta_classic as ta

from trading_advisor_3000.product_plane.research.datasets import ResearchBarView, load_materialized_research_dataset

from .registry import (
    IndicatorProfile,
    IndicatorProfileRegistry,
    IndicatorSpec,
    build_indicator_profile_registry,
    default_indicator_profile,
)
from .store import (
    IndicatorFramePartitionKey,
    IndicatorFrameRow,
    load_indicator_frames,
    indicator_store_contract,
    write_indicator_frames,
)


def _bars_hash(rows: list[ResearchBarView]) -> str:
    payload = [
        {
            "contract_id": row.contract_id,
            "instrument_id": row.instrument_id,
            "timeframe": row.timeframe,
            "ts": row.ts,
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.volume,
            "open_interest": row.open_interest,
        }
        for row in rows
    ]
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _format_library_number(value: object) -> str:
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value)}.0"
        return str(value)
    return str(value)


def _series_group_key(row: ResearchBarView, *, dataset_version: str, indicator_set_version: str, series_mode: str) -> IndicatorFramePartitionKey:
    return IndicatorFramePartitionKey(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        timeframe=row.timeframe,
        instrument_id=row.instrument_id,
        contract_id=None if series_mode == "continuous_front" else row.contract_id,
    )


def _group_bar_views(
    *,
    dataset_version: str,
    indicator_set_version: str,
    bar_views: list[ResearchBarView],
    series_mode: str,
) -> dict[IndicatorFramePartitionKey, list[ResearchBarView]]:
    grouped: dict[IndicatorFramePartitionKey, list[ResearchBarView]] = {}
    for row in sorted(bar_views, key=lambda item: (item.instrument_id, item.timeframe, item.ts, item.contract_id)):
        key = _series_group_key(
            row,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            series_mode=series_mode,
        )
        grouped.setdefault(key, []).append(row)
    return grouped


def _group_existing_rows(
    *,
    rows: list[IndicatorFrameRow],
    series_mode: str,
) -> dict[IndicatorFramePartitionKey, list[IndicatorFrameRow]]:
    grouped: dict[IndicatorFramePartitionKey, list[IndicatorFrameRow]] = {}
    for row in sorted(rows, key=lambda item: (item.instrument_id, item.timeframe, item.ts, item.contract_id)):
        grouped.setdefault(row.partition_key(series_mode=series_mode), []).append(row)
    return grouped


def _validate_required_inputs(frame: pd.DataFrame, spec: IndicatorSpec) -> None:
    missing = [column for column in spec.required_input_columns if column not in frame.columns]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"indicator `{spec.indicator_id}` missing required input columns: {joined}")


def _compute_spec(frame: pd.DataFrame, spec: IndicatorSpec) -> dict[str, pd.Series]:
    _validate_required_inputs(frame, spec)
    params = spec.params_dict()
    close = frame["close"]
    high = frame["high"] if "high" in frame.columns else None
    low = frame["low"] if "low" in frame.columns else None
    volume = frame["volume"] if "volume" in frame.columns else None

    def _none_outputs() -> dict[str, pd.Series]:
        return {
            column: pd.Series([None] * len(frame), index=frame.index, dtype="object")
            for column in spec.output_columns
        }

    if spec.operation_key == "sma":
        series = ta.sma(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "ema":
        series = ta.ema(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "hma":
        series = ta.hma(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "atr":
        series = ta.atr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "natr":
        series = ta.natr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "rsi":
        series = ta.rsi(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "roc":
        series = ta.roc(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "obv":
        series = ta.obv(close, volume)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "mfi":
        series = ta.mfi(high, low, close, volume, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "cmf":
        series = ta.cmf(high, low, close, volume, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "vwma":
        series = ta.vwma(close, volume, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "cci":
        series = ta.cci(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "willr":
        series = ta.willr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "volume_norm":
        length = int(params["length"])
        mean = volume.rolling(window=length, min_periods=length).mean()
        std = volume.rolling(window=length, min_periods=length).std(ddof=0).replace({0.0: pd.NA})
        return {
            spec.output_columns[0]: volume / mean,
            spec.output_columns[1]: (volume - mean) / std,
        }
    if spec.operation_key == "stoch":
        stoch = ta.stoch(
            high,
            low,
            close,
            k=int(params["k"]),
            d=int(params["d"]),
            smooth_k=int(params["smooth_k"]),
        )
        if stoch is None:
            return _none_outputs()
        suffix = f"{params['k']}_{params['smooth_k']}_{params['d']}"
        return {
            spec.output_columns[0]: stoch[f"STOCHk_{suffix}"],
            spec.output_columns[1]: stoch[f"STOCHd_{suffix}"],
        }
    if spec.operation_key == "macd":
        fast = int(params["fast"])
        slow = int(params["slow"])
        signal = int(params["signal"])
        macd = ta.macd(close, fast=fast, slow=slow, signal=signal)
        if macd is None:
            return _none_outputs()
        suffix = f"{fast}_{slow}_{signal}"
        return {
            spec.output_columns[0]: macd[f"MACD_{suffix}"],
            spec.output_columns[1]: macd[f"MACDs_{suffix}"],
            spec.output_columns[2]: macd[f"MACDh_{suffix}"],
        }
    if spec.operation_key == "adx":
        length = int(params["length"])
        adx = ta.adx(high, low, close, length=length)
        if adx is None:
            return _none_outputs()
        suffix = str(length)
        return {
            spec.output_columns[0]: adx[f"ADX_{suffix}"],
            spec.output_columns[1]: adx[f"DMP_{suffix}"],
            spec.output_columns[2]: adx[f"DMN_{suffix}"],
        }
    if spec.operation_key == "aroon":
        length = int(params["length"])
        aroon = ta.aroon(high, low, length=length)
        if aroon is None:
            return _none_outputs()
        suffix = str(length)
        return {
            spec.output_columns[0]: aroon[f"AROONU_{suffix}"],
            spec.output_columns[1]: aroon[f"AROOND_{suffix}"],
        }
    if spec.operation_key == "donchian":
        lower_length = int(params["lower_length"])
        upper_length = int(params["upper_length"])
        donchian = ta.donchian(high, low, lower_length=lower_length, upper_length=upper_length)
        if donchian is None:
            return _none_outputs()
        suffix = f"{lower_length}_{upper_length}"
        return {
            spec.output_columns[0]: donchian[f"DCU_{suffix}"],
            spec.output_columns[1]: donchian[f"DCL_{suffix}"],
        }
    if spec.operation_key == "bbands":
        length = int(params["length"])
        std = params["std"]
        bbands = ta.bbands(close, length=length, std=std)
        if bbands is None:
            return _none_outputs()
        suffix = f"{length}_{_format_library_number(float(std))}"
        return {
            spec.output_columns[0]: bbands[f"BBU_{suffix}"],
            spec.output_columns[1]: bbands[f"BBM_{suffix}"],
            spec.output_columns[2]: bbands[f"BBL_{suffix}"],
            spec.output_columns[3]: bbands[f"BBB_{suffix}"],
        }
    if spec.operation_key == "kc":
        length = int(params["length"])
        scalar = float(params["scalar"])
        kc = ta.kc(high, low, close, length=length, scalar=scalar)
        if kc is None:
            return _none_outputs()
        suffix = f"{length}_{_format_library_number(scalar)}"
        return {
            spec.output_columns[0]: kc[f"KCUe_{suffix}"],
            spec.output_columns[1]: kc[f"KCBe_{suffix}"],
            spec.output_columns[2]: kc[f"KCLe_{suffix}"],
        }
    if spec.operation_key == "ppo":
        fast = int(params["fast"])
        slow = int(params["slow"])
        signal = int(params["signal"])
        ppo = ta.ppo(close, fast=fast, slow=slow, signal=signal)
        if ppo is None:
            return _none_outputs()
        suffix = f"{fast}_{slow}_{signal}"
        return {spec.output_columns[0]: ppo[f"PPO_{suffix}"]}
    if spec.operation_key == "tsi":
        fast = int(params["fast"])
        slow = int(params["slow"])
        tsi = ta.tsi(close, fast=fast, slow=slow)
        if tsi is None:
            return _none_outputs()
        suffix = f"{fast}_{slow}_{fast}"
        return {spec.output_columns[0]: tsi[f"TSI_{suffix}"]}
    raise ValueError(f"unsupported indicator operation: {spec.operation_key}")


def _compute_profile_frame(frame: pd.DataFrame, profile: IndicatorProfile) -> pd.DataFrame:
    result = frame.copy()
    for spec in profile.indicators:
        for column, series in _compute_spec(result, spec).items():
            result[column] = series
    return result


def _null_warmup_span(payload_rows: list[dict[str, object]], output_columns: tuple[str, ...]) -> int:
    count = 0
    for row in payload_rows:
        if any(row.get(column) is None or pd.isna(row.get(column)) for column in output_columns):
            count += 1
            continue
        break
    return count


def _existing_partition_matches(
    existing_rows: list[IndicatorFrameRow],
    *,
    source_hash: str,
    profile_version: str,
    row_count: int,
) -> bool:
    if not existing_rows:
        return False
    head = existing_rows[0]
    return (
        head.source_bars_hash == source_hash
        and head.profile_version == profile_version
        and head.row_count == row_count
    )


def _build_partition_rows(
    *,
    dataset_version: str,
    indicator_set_version: str,
    profile: IndicatorProfile,
    series: list[ResearchBarView],
) -> list[IndicatorFrameRow]:
    frame = pd.DataFrame([row.to_dict() for row in series])
    computed = _compute_profile_frame(frame, profile)
    payload_rows = computed.to_dict("records")
    output_columns = profile.expected_output_columns()
    source_hash = _bars_hash(series)
    null_warmup_span = _null_warmup_span(payload_rows, output_columns)
    created_at = pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")

    rows: list[IndicatorFrameRow] = []
    for original, payload in zip(series, payload_rows, strict=True):
        values = {
            column: (None if payload.get(column) is None or pd.isna(payload.get(column)) else float(payload[column]))
            for column in output_columns
        }
        rows.append(
            IndicatorFrameRow(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                profile_version=profile.version,
                contract_id=original.contract_id,
                instrument_id=original.instrument_id,
                timeframe=original.timeframe,
                ts=original.ts,
                values=values,
                source_bars_hash=source_hash,
                row_count=len(series),
                warmup_span=profile.max_warmup_bars(),
                null_warmup_span=null_warmup_span,
                created_at=created_at,
            )
        )
    return rows


def build_indicator_frames(
    *,
    dataset_version: str,
    indicator_set_version: str,
    bar_views: list[ResearchBarView],
    series_mode: str = "contract",
    profile: IndicatorProfile | None = None,
) -> list[IndicatorFrameRow]:
    profile = profile or default_indicator_profile()
    grouped = _group_bar_views(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        bar_views=bar_views,
        series_mode=series_mode,
    )
    rows: list[IndicatorFrameRow] = []
    for _, series in sorted(grouped.items(), key=lambda item: item[0].partition_path()):
        rows.extend(
            _build_partition_rows(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                profile=profile,
                series=series,
            )
        )
    return rows


def materialize_indicator_frames(
    *,
    dataset_output_dir: Path,
    indicator_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    profile: IndicatorProfile | None = None,
    profile_version: str | None = None,
) -> dict[str, object]:
    loaded = load_materialized_research_dataset(output_dir=dataset_output_dir, dataset_version=dataset_version)
    series_mode = str(loaded["dataset_manifest"].get("series_mode", "contract"))
    registry: IndicatorProfileRegistry = build_indicator_profile_registry()
    resolved_profile = profile or registry.get(profile_version or "core_v1")
    grouped_current = _group_bar_views(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        bar_views=loaded["bar_views"],
        series_mode=series_mode,
    )
    existing_rows = load_indicator_frames(
        output_dir=indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    ) if (indicator_output_dir / "research_indicator_frames.delta" / "_delta_log").exists() else []
    existing_by_partition = _group_existing_rows(rows=existing_rows, series_mode=series_mode)

    refreshed_rows: list[IndicatorFrameRow] = []
    replace_partitions: list[IndicatorFramePartitionKey] = []
    reused_partitions = 0
    refreshed_partitions = 0

    for partition_key, series in grouped_current.items():
        source_hash = _bars_hash(series)
        existing_partition_rows = existing_by_partition.get(partition_key, [])
        if _existing_partition_matches(
            existing_partition_rows,
            source_hash=source_hash,
            profile_version=resolved_profile.version,
            row_count=len(series),
        ):
            reused_partitions += 1
            continue
        refreshed_rows.extend(
            _build_partition_rows(
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                profile=resolved_profile,
                series=series,
            )
        )
        replace_partitions.append(partition_key)
        refreshed_partitions += 1

    deleted_partitions = tuple(
        partition
        for partition in existing_by_partition
        if partition not in grouped_current
    )
    replace_partitions.extend(deleted_partitions)

    output_paths = {"research_indicator_frames": (indicator_output_dir / "research_indicator_frames.delta").as_posix()}
    if replace_partitions or not (indicator_output_dir / "research_indicator_frames.delta" / "_delta_log").exists():
        output_paths = write_indicator_frames(
            output_dir=indicator_output_dir,
            rows=refreshed_rows,
            replace_partitions=tuple(replace_partitions),
        )

    current_total_rows = sum(len(series) for series in grouped_current.values())
    return {
        "indicator_row_count": current_total_rows,
        "refreshed_partition_count": refreshed_partitions,
        "reused_partition_count": reused_partitions,
        "deleted_partition_count": len(deleted_partitions),
        "profile_version": resolved_profile.version,
        "output_paths": output_paths,
        "delta_manifest": indicator_store_contract(),
    }


def reload_indicator_frames(
    *,
    indicator_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
) -> list[IndicatorFrameRow]:
    return load_indicator_frames(
        output_dir=indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    )
