from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pandas_ta_classic as ta

from trading_advisor_3000.product_plane.data_plane.delta_runtime import iter_delta_table_row_batches, read_delta_table_rows
from trading_advisor_3000.product_plane.research.continuous_front_indicators.rules import (
    assert_rule_coverage,
    rules_for_indicator_profile,
)
from trading_advisor_3000.product_plane.research.datasets import ResearchBarView

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
    existing_indicator_value_columns,
    indicator_output_columns_hash,
    load_indicator_frames,
    load_indicator_partition_metadata,
    load_indicator_partition_rows,
    indicator_store_contract,
    write_indicator_frame_batches,
)


def _bars_hash(rows: list[ResearchBarView], *, adjustment_ladder_rows: tuple[dict[str, object], ...] = ()) -> str:
    payload = {
        "bars": [row.to_dict() for row in rows],
        "adjustment_ladder": _normalize_ladder_rows(adjustment_ladder_rows),
    }
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16].upper()


def _normalize_ladder_rows(rows: tuple[dict[str, object], ...]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append(
            {
                "instrument_id": str(row["instrument_id"]),
                "timeframe": str(row["timeframe"]),
                "roll_sequence": int(row["roll_sequence"]),
                "effective_ts": str(row["effective_ts"]),
                "additive_gap": float(row["additive_gap"]),
            }
        )
    return sorted(
        normalized,
        key=lambda row: (str(row["instrument_id"]), str(row["timeframe"]), int(row["roll_sequence"])),
    )


def _format_library_number(value: object) -> str:
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value)}.0"
        return str(value)
    return str(value)


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace({0.0: pd.NA})


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


def _metadata_partition_key(row: dict[str, object], *, series_mode: str) -> IndicatorFramePartitionKey:
    return IndicatorFramePartitionKey(
        dataset_version=str(row["dataset_version"]),
        indicator_set_version=str(row["indicator_set_version"]),
        timeframe=str(row["timeframe"]),
        instrument_id=str(row["instrument_id"]),
        contract_id=None if series_mode == "continuous_front" else str(row["contract_id"]),
    )


def _group_existing_partition_metadata(
    *,
    rows: list[dict[str, object]],
    series_mode: str,
) -> dict[IndicatorFramePartitionKey, dict[str, object]]:
    grouped: dict[IndicatorFramePartitionKey, dict[str, object]] = {}
    for row in rows:
        grouped.setdefault(_metadata_partition_key(row, series_mode=series_mode), row)
    return grouped


def _load_dataset_manifest(*, output_dir: Path, dataset_version: str) -> dict[str, object]:
    rows = read_delta_table_rows(
        output_dir / "research_datasets.delta",
        filters=[("dataset_version", "=", dataset_version)],
    )
    if not rows:
        raise KeyError(f"dataset_version not found: {dataset_version}")
    return rows[0]


def _bar_partition_key_from_row(
    row: dict[str, object],
    *,
    dataset_version: str,
    indicator_set_version: str,
    series_mode: str,
) -> IndicatorFramePartitionKey:
    return IndicatorFramePartitionKey(
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        timeframe=str(row["timeframe"]),
        instrument_id=str(row["instrument_id"]),
        contract_id=None if series_mode == "continuous_front" else str(row["contract_id"]),
    )


def _load_bar_partition_counts(
    *,
    dataset_output_dir: Path,
    dataset_version: str,
    indicator_set_version: str,
    series_mode: str,
) -> dict[IndicatorFramePartitionKey, int]:
    counts: dict[IndicatorFramePartitionKey, int] = {}
    for batch in iter_delta_table_row_batches(
        dataset_output_dir / "research_bar_views.delta",
        columns=["dataset_version", "contract_id", "instrument_id", "timeframe"],
    ):
        for row in batch:
            if row.get("dataset_version") != dataset_version:
                continue
            key = _bar_partition_key_from_row(
                row,
                dataset_version=dataset_version,
                indicator_set_version=indicator_set_version,
                series_mode=series_mode,
            )
            counts[key] = counts.get(key, 0) + 1
    return counts


def _load_bar_partition_rows(
    *,
    dataset_output_dir: Path,
    dataset_version: str,
    partition: IndicatorFramePartitionKey,
) -> list[ResearchBarView]:
    filters: list[tuple[str, str, object]] = [
        ("dataset_version", "=", dataset_version),
        ("instrument_id", "=", partition.instrument_id),
        ("timeframe", "=", partition.timeframe),
    ]
    if partition.contract_id is not None:
        filters.append(("contract_id", "=", partition.contract_id))
    rows = read_delta_table_rows(dataset_output_dir / "research_bar_views.delta", filters=filters)
    return [
        ResearchBarView.from_dict(row)
        for row in sorted(rows, key=lambda item: str(item["ts"]))
    ]


def _load_adjustment_ladder_rows(*, dataset_output_dir: Path, dataset_version: str) -> tuple[dict[str, object], ...]:
    table_path = dataset_output_dir / "continuous_front_adjustment_ladder.delta"
    if not (table_path / "_delta_log").exists():
        return ()
    rows = read_delta_table_rows(table_path, filters=[("dataset_version", "=", dataset_version)])
    return tuple(dict(row) for row in rows)


def _latest_delta_commit_timestamp(table_path: Path) -> int | None:
    log_dir = table_path / "_delta_log"
    if not log_dir.exists():
        return None
    log_files = sorted(log_dir.glob("*.json"))
    if not log_files:
        return None
    for line in reversed(log_files[-1].read_text(encoding="utf-8").splitlines()):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        commit_info = payload.get("commitInfo") if isinstance(payload, dict) else None
        if isinstance(commit_info, dict) and commit_info.get("timestamp") is not None:
            return int(commit_info["timestamp"])
    return None


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
    open_interest = frame["open_interest"] if "open_interest" in frame.columns else None

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
    if spec.operation_key == "slope":
        series = ta.slope(close, length=int(params["length"]), as_angle=True, to_degrees=True)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "atr":
        series = ta.atr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "natr":
        series = ta.natr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "chop":
        series = ta.chop(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "supertrend":
        length = int(params["length"])
        multiplier = float(params["multiplier"])
        supertrend = ta.supertrend(high, low, close, length=length, multiplier=multiplier)
        if supertrend is None:
            return _none_outputs()
        suffix = f"{length}_{_format_library_number(multiplier)}"
        return {
            spec.output_columns[0]: supertrend[f"SUPERT_{suffix}"],
            spec.output_columns[1]: supertrend[f"SUPERTd_{suffix}"],
        }
    if spec.operation_key == "rsi":
        series = ta.rsi(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "roc":
        series = ta.roc(close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "mom":
        series = ta.mom(close, length=int(params["length"]))
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
    if spec.operation_key == "ad":
        series = ta.ad(high, low, close, volume)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "adosc":
        fast = int(params["fast"])
        slow = int(params["slow"])
        series = ta.adosc(high, low, close, volume, fast=fast, slow=slow)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "force_index":
        series = ta.efi(close, volume, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "pvt":
        series = ta.pvt(close, volume)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "cci":
        series = ta.cci(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "willr":
        series = ta.willr(high, low, close, length=int(params["length"]))
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "stochrsi":
        length = int(params["length"])
        rsi_length = int(params["rsi_length"])
        k = int(params["k"])
        d = int(params["d"])
        stochrsi = ta.stochrsi(close, length=length, rsi_length=rsi_length, k=k, d=d)
        if stochrsi is None:
            return _none_outputs()
        suffix = f"{length}_{rsi_length}_{k}_{d}"
        return {
            spec.output_columns[0]: stochrsi[f"STOCHRSIk_{suffix}"],
            spec.output_columns[1]: stochrsi[f"STOCHRSId_{suffix}"],
        }
    if spec.operation_key == "ultimate_oscillator":
        fast = int(params["fast"])
        medium = int(params["medium"])
        slow = int(params["slow"])
        series = ta.uo(high, low, close, fast=fast, medium=medium, slow=slow)
        return _none_outputs() if series is None else {spec.output_columns[0]: series}
    if spec.operation_key == "true_range":
        return {spec.output_columns[0]: _numeric(frame["true_range"])}
    if spec.operation_key == "realized_volatility":
        length = int(params["length"])
        return {spec.output_columns[0]: _numeric(frame["log_ret_1"]).rolling(window=length, min_periods=length).std(ddof=0)}
    if spec.operation_key == "ulcer_index":
        series = ta.ui(close, length=int(params["length"]))
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
        upper = donchian[f"DCU_{suffix}"]
        lower = donchian[f"DCL_{suffix}"]
        return {
            spec.output_columns[0]: upper,
            spec.output_columns[1]: lower,
            spec.output_columns[2]: donchian[f"DCM_{suffix}"],
            spec.output_columns[3]: upper - lower,
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
            spec.output_columns[4]: bbands[f"BBP_{suffix}"],
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
        return {
            spec.output_columns[0]: ppo[f"PPO_{suffix}"],
            spec.output_columns[1]: ppo[f"PPOs_{suffix}"],
            spec.output_columns[2]: ppo[f"PPOh_{suffix}"],
        }
    if spec.operation_key == "tsi":
        fast = int(params["fast"])
        slow = int(params["slow"])
        tsi = ta.tsi(close, fast=fast, slow=slow)
        if tsi is None:
            return _none_outputs()
        return {spec.output_columns[0]: tsi.iloc[:, 0]}
    if spec.operation_key == "trix":
        length = int(params["length"])
        signal = int(params["signal"])
        trix = ta.trix(close, length=length, signal=signal)
        if trix is None:
            return _none_outputs()
        suffix = f"{length}_{signal}"
        return {
            spec.output_columns[0]: trix[f"TRIX_{suffix}"],
            spec.output_columns[1]: trix[f"TRIXs_{suffix}"],
        }
    if spec.operation_key == "kst":
        kst = ta.kst(close)
        if kst is None:
            return _none_outputs()
        return {
            spec.output_columns[0]: kst.iloc[:, 0],
            spec.output_columns[1]: kst.iloc[:, 1],
        }
    if spec.operation_key == "pvo":
        fast = int(params["fast"])
        slow = int(params["slow"])
        signal = int(params["signal"])
        pvo = ta.pvo(volume, fast=fast, slow=slow, signal=signal)
        if pvo is None:
            return _none_outputs()
        suffix = f"{fast}_{slow}_{signal}"
        return {
            spec.output_columns[0]: pvo[f"PVO_{suffix}"],
            spec.output_columns[1]: pvo[f"PVOs_{suffix}"],
            spec.output_columns[2]: pvo[f"PVOh_{suffix}"],
        }
    if spec.operation_key == "oi_change":
        return {spec.output_columns[0]: _numeric(open_interest).diff(int(params["length"]))}
    if spec.operation_key == "oi_roc":
        return {spec.output_columns[0]: _numeric(open_interest).pct_change(int(params["length"])) * 100.0}
    if spec.operation_key == "oi_z":
        length = int(params["length"])
        oi = _numeric(open_interest)
        mean = oi.rolling(window=length, min_periods=length).mean()
        std = oi.rolling(window=length, min_periods=length).std(ddof=0)
        return {spec.output_columns[0]: _safe_divide(oi - mean, std)}
    if spec.operation_key == "oi_relative_activity":
        length = int(params["length"])
        oi = _numeric(open_interest)
        mean = oi.rolling(window=length, min_periods=length).mean()
        return {spec.output_columns[0]: _safe_divide(oi, mean)}
    if spec.operation_key == "volume_oi_ratio":
        return {spec.output_columns[0]: _safe_divide(_numeric(volume), _numeric(open_interest))}
    raise ValueError(f"unsupported indicator operation: {spec.operation_key}")


def _compute_profile_frame(frame: pd.DataFrame, profile: IndicatorProfile) -> pd.DataFrame:
    result = frame.copy()
    for spec in profile.indicators:
        for column, series in _compute_spec(result, spec).items():
            result[column] = series
    return result


def _ladder_rows_for_series(
    adjustment_ladder_rows: tuple[dict[str, object], ...],
    *,
    instrument_id: str,
    timeframe: str,
) -> tuple[dict[str, object], ...]:
    return tuple(
        sorted(
            (
                row
                for row in adjustment_ladder_rows
                if str(row.get("instrument_id")) == instrument_id and str(row.get("timeframe")) == timeframe
            ),
            key=lambda row: int(row["roll_sequence"]),
        )
    )


def _offset_by_epoch(max_epoch: int, gap_by_sequence: dict[int, float]) -> dict[int, float]:
    return {
        epoch: sum(gap_by_sequence[sequence] for sequence in range(1, epoch + 1))
        for epoch in range(0, max_epoch + 1)
    }


def _recompute_price_inputs(frame: pd.DataFrame) -> pd.DataFrame:
    adjusted = frame.copy()
    close = _numeric(adjusted["close"])
    prev_close = close.shift(1)
    adjusted["ret_1"] = (close / prev_close) - 1.0
    adjusted.loc[prev_close.isna() | (prev_close == 0.0), "ret_1"] = pd.NA
    adjusted["log_ret_1"] = pd.NA
    valid_ret = prev_close.notna() & (prev_close > 0.0) & (close > 0.0)
    adjusted.loc[valid_ret, "log_ret_1"] = (close[valid_ret] / prev_close[valid_ret]).map(math.log)
    high = _numeric(adjusted["high"])
    low = _numeric(adjusted["low"])
    open_ = _numeric(adjusted["open"])
    adjusted["hl_range"] = high - low
    adjusted["oc_range"] = close - open_
    adjusted["true_range"] = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return adjusted


def _native_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    native = frame.copy()
    for column in ("open", "high", "low", "close"):
        native_column = f"native_{column}"
        if native_column in native.columns:
            native[column] = pd.to_numeric(native[native_column], errors="coerce")
    return _recompute_price_inputs(native)


def _zero_anchor_price_frame(frame: pd.DataFrame, *, gap_by_sequence: dict[int, float]) -> pd.DataFrame:
    zero_anchor = frame.copy()
    row_epochs = pd.to_numeric(zero_anchor["roll_epoch"], errors="coerce").fillna(0).astype(int)
    cumulative_offsets = row_epochs.map(lambda epoch: sum(gap_by_sequence[sequence] for sequence in range(1, int(epoch) + 1)))
    for column in ("open", "high", "low", "close"):
        native_column = f"native_{column}"
        source = zero_anchor[native_column] if native_column in zero_anchor.columns else zero_anchor[column]
        zero_anchor[column] = pd.to_numeric(source, errors="coerce") - cumulative_offsets
    return _recompute_price_inputs(zero_anchor)


def _asof_adjusted_price_frame(
    frame: pd.DataFrame,
    *,
    target_epoch: int,
    gap_by_sequence: dict[int, float],
) -> pd.DataFrame:
    adjusted = frame[frame["roll_epoch"] <= target_epoch].copy()
    row_epochs = pd.to_numeric(adjusted["roll_epoch"], errors="coerce").fillna(0).astype(int)
    offsets = row_epochs.map(
        lambda row_epoch: sum(gap_by_sequence[sequence] for sequence in range(row_epoch + 1, target_epoch + 1))
    )
    for column in ("open", "high", "low", "close"):
        native_column = f"native_{column}"
        source = adjusted[native_column] if native_column in adjusted.columns else adjusted[column]
        adjusted[column] = pd.to_numeric(source, errors="coerce") + offsets
    return _recompute_price_inputs(adjusted)


def _output_groups_for_spec(
    spec: IndicatorSpec,
    rules_by_output: dict[str, object],
) -> dict[str, tuple[str, ...]]:
    groups: dict[str, list[str]] = {}
    for output_column in spec.output_columns:
        rule = rules_by_output[output_column]
        group_id = rule.calculation_group_id
        groups.setdefault(group_id, []).append(output_column)
    return {group_id: tuple(columns) for group_id, columns in groups.items()}


def _frame_for_calculation_group(
    group_id: str,
    *,
    zero_anchor_frame: pd.DataFrame,
    target_anchor_frame: pd.DataFrame,
    native_frame: pd.DataFrame,
) -> pd.DataFrame:
    if group_id in {"price_level_post_transform", "price_range_on_p0", "oscillator_on_p0"}:
        return zero_anchor_frame
    if group_id in {"anchor_sensitive_roll_aware", "price_volume_roll_aware"}:
        return target_anchor_frame
    if group_id in {"native_volume_oi_roll_aware", "grid_locked_native"}:
        return native_frame
    return target_anchor_frame


def _compute_continuous_front_profile_frame(
    frame: pd.DataFrame,
    profile: IndicatorProfile,
    *,
    adjustment_ladder_rows: tuple[dict[str, object], ...],
) -> pd.DataFrame:
    if frame.empty:
        return _compute_profile_frame(frame, profile)
    rules = rules_for_indicator_profile(profile)
    assert_rule_coverage(output_columns=set(profile.expected_output_columns()), rules=rules)
    if "roll_epoch" not in frame.columns:
        raise ValueError("continuous_front indicator materialization requires roll_epoch on research_bar_views")
    instrument_id = str(frame["instrument_id"].iloc[0])
    timeframe = str(frame["timeframe"].iloc[0])
    ladder_rows = _ladder_rows_for_series(
        adjustment_ladder_rows,
        instrument_id=instrument_id,
        timeframe=timeframe,
    )
    frame = frame.copy()
    frame["roll_epoch"] = pd.to_numeric(frame["roll_epoch"], errors="coerce").fillna(0).astype(int)
    max_epoch = int(frame["roll_epoch"].max() or 0)
    if not ladder_rows:
        if max_epoch > 0:
            raise ValueError(
                "continuous_front indicator materialization requires adjustment ladder rows for rolled series "
                f"{instrument_id}|{timeframe}"
            )
        gap_by_sequence: dict[int, float] = {}
    else:
        gap_by_sequence = {int(row["roll_sequence"]): float(row["additive_gap"]) for row in ladder_rows}
    missing_sequences = [sequence for sequence in range(1, max_epoch + 1) if sequence not in gap_by_sequence]
    if missing_sequences:
        joined = ", ".join(str(sequence) for sequence in missing_sequences)
        raise ValueError(
            "continuous_front indicator materialization missing adjustment ladder roll_sequence "
            f"{joined} for {instrument_id}|{timeframe}"
        )

    rules_by_output = {rule.output_column: rule for rule in rules}
    target_offset_by_epoch = _offset_by_epoch(max_epoch=max_epoch, gap_by_sequence=gap_by_sequence)
    computed_segments: list[pd.DataFrame] = []
    for target_epoch in sorted(frame["roll_epoch"].unique()):
        epoch = int(target_epoch)
        target_index = frame.index[frame["roll_epoch"] == epoch]
        source_frame = frame[frame["roll_epoch"] <= epoch].copy()
        zero_anchor_frame = _zero_anchor_price_frame(source_frame, gap_by_sequence=gap_by_sequence)
        target_anchor_frame = _asof_adjusted_price_frame(frame, target_epoch=epoch, gap_by_sequence=gap_by_sequence)
        native_frame = _native_price_frame(source_frame)
        segment = target_anchor_frame.loc[target_index].copy()
        target_offset = float(target_offset_by_epoch[epoch])
        compute_cache: dict[tuple[str, str], dict[str, pd.Series]] = {}

        for spec in profile.indicators:
            for group_id, output_columns in _output_groups_for_spec(spec, rules_by_output).items():
                cache_key = (spec.indicator_id, group_id)
                if cache_key not in compute_cache:
                    group_frame = _frame_for_calculation_group(
                        group_id,
                        zero_anchor_frame=zero_anchor_frame,
                        target_anchor_frame=target_anchor_frame,
                        native_frame=native_frame,
                    )
                    compute_cache[cache_key] = _compute_spec(group_frame, spec)
                computed_outputs = compute_cache[cache_key]
                for output_column in output_columns:
                    series = computed_outputs[output_column].reindex(target_anchor_frame.index)
                    if group_id == "price_level_post_transform":
                        series = series + target_offset
                    segment[output_column] = series.loc[target_index]
        computed_segments.append(segment)
    computed = pd.concat(computed_segments).sort_index()
    return _enforce_continuous_front_native_boundaries(computed, profile)


def _enforce_continuous_front_native_boundaries(frame: pd.DataFrame, profile: IndicatorProfile) -> pd.DataFrame:
    result = frame.copy()
    roll_epoch = pd.to_numeric(result["roll_epoch"], errors="coerce").fillna(0).astype(int)
    specs_by_output = {
        output_column: spec
        for spec in profile.indicators
        for output_column in spec.output_columns
    }
    rules_by_output = {rule.output_column: rule for rule in rules_for_indicator_profile(profile)}

    reset_operation_keys = {"obv", "ad", "adosc", "pvt", "pvo"}
    for spec in profile.indicators:
        spec_rules = [rules_by_output[column] for column in spec.output_columns]
        if spec.operation_key not in reset_operation_keys or not any(rule.group.reset_on_roll for rule in spec_rules):
            continue
        recomputed_outputs = pd.DataFrame(index=result.index, columns=list(spec.output_columns), dtype="object")
        for _, segment in result.groupby(roll_epoch, sort=False):
            recomputed = _compute_profile_frame(segment.copy(), _profile_for_specs(profile, (spec,)))
            segment_outputs = recomputed[list(spec.output_columns)]
            recomputed_outputs.loc[segment_outputs.index, list(spec.output_columns)] = segment_outputs
        for column in spec.output_columns:
            result[column] = recomputed_outputs[column]

    for column, rule in rules_by_output.items():
        if column not in result.columns or rule.group.allow_cross_contract_window:
            continue
        spec = specs_by_output[column]
        if spec.operation_key in {"oi_change", "oi_roc"}:
            lag = max(1, int(spec.params_dict().get("length", 1)))
            same_epoch_lag = roll_epoch == roll_epoch.shift(lag)
            result.loc[~same_epoch_lag.fillna(False), column] = pd.NA
            continue
        if spec.operation_key == "volume_oi_ratio":
            continue
        length = max(1, int(spec.params_dict().get("length", spec.warmup_bars)))
        window_crosses = roll_epoch.rolling(window=length, min_periods=length).apply(
            lambda values: 0.0 if len(set(int(value) for value in values)) == 1 else 1.0,
            raw=False,
        )
        result.loc[window_crosses.fillna(1.0).astype(bool), column] = pd.NA
    return result


def _compute_profile_frame_for_series(
    frame: pd.DataFrame,
    profile: IndicatorProfile,
    *,
    series_mode: str,
    adjustment_ladder_rows: tuple[dict[str, object], ...],
) -> pd.DataFrame:
    if series_mode != "continuous_front":
        return _compute_profile_frame(frame, profile)
    return _compute_continuous_front_profile_frame(
        frame,
        profile,
        adjustment_ladder_rows=adjustment_ladder_rows,
    )


def _null_warmup_span(payload_rows: list[dict[str, object]], output_columns: tuple[str, ...]) -> int:
    count = 0
    for row in payload_rows:
        if any(row.get(column) is None or pd.isna(row.get(column)) for column in output_columns):
            count += 1
            continue
        break
    return count


def _profile_for_specs(profile: IndicatorProfile, specs: tuple[IndicatorSpec, ...]) -> IndicatorProfile:
    return IndicatorProfile(
        version=profile.version,
        description=profile.description,
        indicators=specs,
    )


def _specs_for_columns(profile: IndicatorProfile, columns: set[str]) -> tuple[IndicatorSpec, ...]:
    return tuple(
        spec
        for spec in profile.indicators
        if any(column in columns for column in spec.output_columns)
    )


def _existing_partition_matches(
    existing_metadata: dict[str, object] | None,
    *,
    source_hash: str,
    profile_version: str,
    row_count: int,
    output_columns_hash: str,
    legacy_output_columns_match: bool = False,
) -> bool:
    if not existing_metadata:
        return False
    existing_output_columns_hash = str(existing_metadata.get("output_columns_hash") or "")
    output_hash_matches = (
        existing_output_columns_hash == output_columns_hash
        or (not existing_output_columns_hash and legacy_output_columns_match)
    )
    return (
        existing_metadata.get("source_bars_hash") == source_hash
        and existing_metadata.get("profile_version") == profile_version
        and int(existing_metadata.get("row_count", -1) or -1) == row_count
        and output_hash_matches
    )


def _timestamp_after(left: object, right: object) -> bool:
    if not left or not right:
        return False
    try:
        return pd.Timestamp(left) > pd.Timestamp(right)
    except (TypeError, ValueError):
        return str(left) > str(right)


def _build_partition_rows(
    *,
    dataset_version: str,
    indicator_set_version: str,
    profile: IndicatorProfile,
    series: list[ResearchBarView],
    series_mode: str = "contract",
    source_dataset_bars_hash: str = "",
    output_columns_hash: str | None = None,
    compute_profile: IndicatorProfile | None = None,
    existing_rows: list[IndicatorFrameRow] | None = None,
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
) -> list[IndicatorFrameRow]:
    frame = pd.DataFrame([row.to_dict() for row in series])
    compute_profile = compute_profile or profile
    series_ladder_rows = ()
    if series_mode == "continuous_front" and series:
        series_ladder_rows = _ladder_rows_for_series(
            adjustment_ladder_rows,
            instrument_id=series[0].instrument_id,
            timeframe=series[0].timeframe,
        )
    computed = _compute_profile_frame_for_series(
        frame,
        compute_profile,
        series_mode=series_mode,
        adjustment_ladder_rows=series_ladder_rows,
    )
    payload_rows = computed.to_dict("records")
    output_columns = profile.expected_output_columns()
    computed_columns = set(compute_profile.expected_output_columns())
    existing_by_ts = {row.ts: row for row in existing_rows or []}
    source_hash = _bars_hash(series, adjustment_ladder_rows=series_ladder_rows)
    output_columns_hash = output_columns_hash or indicator_output_columns_hash(output_columns)
    prepared_values: list[tuple[ResearchBarView, dict[str, float | None], IndicatorFrameRow | None]] = []
    merged_payload_rows: list[dict[str, object]] = []

    for original, payload in zip(series, payload_rows, strict=True):
        existing = existing_by_ts.get(original.ts)
        values: dict[str, float | None] = {}
        for column in output_columns:
            if column in computed_columns:
                value = payload.get(column)
            elif existing is not None and column in existing.values:
                value = existing.values[column]
            else:
                value = None
            values[column] = None if value is None or pd.isna(value) else float(value)
        prepared_values.append((original, values, existing))
        merged_payload_rows.append(values)

    null_warmup_span = _null_warmup_span(merged_payload_rows, output_columns)
    materialized_at = pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")

    rows: list[IndicatorFrameRow] = []
    for original, values, existing in prepared_values:
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
                created_at=existing.created_at if existing is not None else materialized_at,
                output_columns_hash=output_columns_hash,
                source_dataset_bars_hash=source_dataset_bars_hash,
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
    adjustment_ladder_rows: tuple[dict[str, object], ...] = (),
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
                series_mode=series_mode,
                adjustment_ladder_rows=adjustment_ladder_rows,
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
    dataset_manifest = _load_dataset_manifest(output_dir=dataset_output_dir, dataset_version=dataset_version)
    series_mode = str(dataset_manifest.get("series_mode", "contract"))
    adjustment_ladder_rows = (
        _load_adjustment_ladder_rows(dataset_output_dir=dataset_output_dir, dataset_version=dataset_version)
        if series_mode == "continuous_front"
        else ()
    )
    current_dataset_bars_hash = str(dataset_manifest.get("bars_hash") or "")
    current_dataset_created_at = str(dataset_manifest.get("created_at") or "")
    registry: IndicatorProfileRegistry = build_indicator_profile_registry()
    resolved_profile = profile or registry.get(profile_version or "core_v1")
    partition_counts = _load_bar_partition_counts(
        dataset_output_dir=dataset_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
        series_mode=series_mode,
    )
    table_exists = (indicator_output_dir / "research_indicator_frames.delta" / "_delta_log").exists()
    source_table_commit_ts = _latest_delta_commit_timestamp(dataset_output_dir / "research_bar_views.delta")
    indicator_table_commit_ts = (
        _latest_delta_commit_timestamp(indicator_output_dir / "research_indicator_frames.delta")
        if table_exists else None
    )
    legacy_table_covers_source_commit = bool(
        source_table_commit_ts is not None
        and indicator_table_commit_ts is not None
        and indicator_table_commit_ts >= source_table_commit_ts
    )
    existing_metadata = load_indicator_partition_metadata(
        output_dir=indicator_output_dir,
        dataset_version=dataset_version,
        indicator_set_version=indicator_set_version,
    ) if table_exists else []
    existing_by_partition = _group_existing_partition_metadata(rows=existing_metadata, series_mode=series_mode)
    target_output_columns = resolved_profile.expected_output_columns()
    target_output_columns_hash = indicator_output_columns_hash(target_output_columns)
    existing_output_columns = set(
        existing_indicator_value_columns(output_dir=indicator_output_dir)
    ) if table_exists else set()
    reusable_existing_columns = tuple(
        column for column in target_output_columns if column in existing_output_columns
    )

    current_partitions: set[IndicatorFramePartitionKey] = set()
    replace_partitions: list[IndicatorFramePartitionKey] = []
    refresh_plan: list[
        tuple[
            IndicatorFramePartitionKey,
            list[ResearchBarView],
            IndicatorProfile,
            list[IndicatorFrameRow] | None,
        ]
    ] = []
    reused_partitions = 0
    refreshed_partitions = 0
    extended_partitions = 0
    recomputed_partitions = 0

    for partition_key, row_count in sorted(
        partition_counts.items(),
        key=lambda item: (item[0].instrument_id, item[0].contract_id or "", item[0].timeframe),
    ):
        current_partitions.add(partition_key)
        existing_partition_metadata = existing_by_partition.get(partition_key)
        source_hash = str(existing_partition_metadata.get("source_bars_hash") or "") if existing_partition_metadata else ""
        source_row_count = int(existing_partition_metadata.get("row_count", -1) or -1) if existing_partition_metadata else -1
        existing_dataset_bars_hash = (
            str(existing_partition_metadata.get("source_dataset_bars_hash") or "")
            if existing_partition_metadata else ""
        )
        existing_materialized_at = (
            str(existing_partition_metadata.get("created_at") or "")
            if existing_partition_metadata else ""
        )
        series: list[ResearchBarView] | None = None
        dataset_may_have_changed = (
            bool(current_dataset_bars_hash and existing_dataset_bars_hash and current_dataset_bars_hash != existing_dataset_bars_hash)
            or bool(
                current_dataset_bars_hash
                and not existing_dataset_bars_hash
                and not legacy_table_covers_source_commit
                and _timestamp_after(current_dataset_created_at, existing_materialized_at)
            )
        )
        source_changed = source_row_count != row_count or not source_hash or dataset_may_have_changed or series_mode == "continuous_front"
        if source_changed:
            series = _load_bar_partition_rows(
                dataset_output_dir=dataset_output_dir,
                dataset_version=dataset_version,
                partition=partition_key,
            )
            series_ladder_rows = _ladder_rows_for_series(
                adjustment_ladder_rows,
                instrument_id=partition_key.instrument_id,
                timeframe=partition_key.timeframe,
            )
            source_hash = _bars_hash(series, adjustment_ladder_rows=series_ladder_rows)
        if _existing_partition_matches(
            existing_partition_metadata,
            source_hash=source_hash,
            profile_version=resolved_profile.version,
            row_count=row_count,
            output_columns_hash=target_output_columns_hash,
            legacy_output_columns_match=set(target_output_columns).issubset(existing_output_columns),
        ):
            reused_partitions += 1
            continue

        source_unchanged = bool(existing_partition_metadata) and (
            existing_partition_metadata.get("source_bars_hash") == source_hash
            and int(existing_partition_metadata.get("row_count", -1) or -1) == row_count
        )
        missing_columns = set(target_output_columns) - existing_output_columns
        can_extend_from_existing = bool(source_unchanged and missing_columns and reusable_existing_columns)
        if series is None:
            series = _load_bar_partition_rows(
                dataset_output_dir=dataset_output_dir,
                dataset_version=dataset_version,
                partition=partition_key,
            )
        if can_extend_from_existing:
            missing_specs = _specs_for_columns(resolved_profile, missing_columns)
            compute_profile = _profile_for_specs(resolved_profile, missing_specs)
            existing_rows = load_indicator_partition_rows(
                output_dir=indicator_output_dir,
                partition=partition_key,
                value_columns=reusable_existing_columns,
            )
            extended_partitions += 1
        else:
            compute_profile = resolved_profile
            existing_rows = None
            recomputed_partitions += 1

        refresh_plan.append((partition_key, series, compute_profile, existing_rows))
        replace_partitions.append(partition_key)
        refreshed_partitions += 1

    deleted_partitions = tuple(
        partition
        for partition in existing_by_partition
        if partition not in current_partitions
    )
    replace_partitions.extend(deleted_partitions)

    output_paths = {"research_indicator_frames": (indicator_output_dir / "research_indicator_frames.delta").as_posix()}
    refreshed_row_count = 0
    batch_count = 0
    if replace_partitions or not table_exists:
        def _row_batches() -> Iterator[list[IndicatorFrameRow]]:
            for _, series, compute_profile, existing_rows in refresh_plan:
                yield _build_partition_rows(
                    dataset_version=dataset_version,
                    indicator_set_version=indicator_set_version,
                    profile=resolved_profile,
                    series=series,
                    series_mode=series_mode,
                    source_dataset_bars_hash=current_dataset_bars_hash,
                    output_columns_hash=target_output_columns_hash,
                    compute_profile=compute_profile,
                    existing_rows=existing_rows,
                    adjustment_ladder_rows=adjustment_ladder_rows,
                )

        output_paths, refreshed_row_count, batch_count = write_indicator_frame_batches(
            output_dir=indicator_output_dir,
            row_batches=_row_batches(),
            replace_partitions=tuple(replace_partitions),
            profile=resolved_profile,
        )

    current_total_rows = sum(partition_counts.values())
    return {
        "indicator_row_count": current_total_rows,
        "refreshed_row_count": refreshed_row_count,
        "refreshed_partition_count": refreshed_partitions,
        "reused_partition_count": reused_partitions,
        "extended_partition_count": extended_partitions,
        "recomputed_partition_count": recomputed_partitions,
        "deleted_partition_count": len(deleted_partitions),
        "write_batch_count": batch_count,
        "profile_version": resolved_profile.version,
        "output_columns_hash": target_output_columns_hash,
        "output_paths": output_paths,
        "delta_manifest": indicator_store_contract(profile=resolved_profile),
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
