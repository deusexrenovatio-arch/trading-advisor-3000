from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from itertools import pairwise

VOLUME_PROFILE_INDICATOR_COLUMNS: tuple[str, ...] = (
    "vp_poc_price",
    "vp_poc_volume_share",
    "vp_vwap",
    "vp_value_area_low",
    "vp_value_area_high",
    "vp_value_area_width",
    "vp_cluster_count",
    "vp_primary_cluster_low",
    "vp_primary_cluster_high",
    "vp_primary_cluster_volume_share",
    "vp_secondary_cluster_low",
    "vp_secondary_cluster_high",
    "vp_secondary_cluster_volume_share",
    "vp_cluster_separation_ticks",
    "vp_low_volume_gap_count",
    "vp_shape_code",
    "vp_source_1m_coverage_ratio",
    "vp_volume_conservation_ratio",
    "vp_quality_code",
)

VOLUME_PROFILE_INT_COLUMNS: frozenset[str] = frozenset(
    {
        "vp_cluster_count",
        "vp_low_volume_gap_count",
        "vp_shape_code",
        "vp_quality_code",
    }
)

VP_QUALITY_OK = 0
VP_QUALITY_NO_SOURCE = 1
VP_QUALITY_LOW_COVERAGE = 2
VP_QUALITY_VOLUME_MISMATCH = 3
_VOLUME_RATIO_EPSILON = 1e-12


@dataclass(frozen=True)
class VolumeProfileSettings:
    value_area_ratio: float = 0.70
    min_coverage_ratio: float = 0.95
    volume_conservation_tolerance: float = 0.001
    weak_peak_share: float = 0.08
    merge_distance_ticks: int = 2
    low_volume_gap_share: float = 0.01


@dataclass(frozen=True)
class _Cluster:
    peak_position: int
    start_position: int
    end_position: int
    volume: float
    peak_volume: float


def compute_volume_profile_features(
    minute_bars: Iterable[Mapping[str, object]],
    *,
    tick_size: float,
    target_volume: float | None = None,
    expected_source_bars: int | None = None,
    settings: VolumeProfileSettings | None = None,
) -> dict[str, float | int | None]:
    settings = settings or VolumeProfileSettings()
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    if not 0 < settings.value_area_ratio <= 1:
        raise ValueError("value_area_ratio must be in (0, 1]")
    if not 0 <= settings.min_coverage_ratio <= 1:
        raise ValueError("min_coverage_ratio must be in [0, 1]")

    histogram: dict[int, float] = {}
    source_volume = 0.0
    valid_source_rows = 0
    for bar in minute_bars:
        low = _float_value(bar.get("low"))
        high = _float_value(bar.get("high"))
        volume = _float_value(bar.get("volume"))
        if low is None or high is None or volume is None or volume < 0:
            continue
        valid_source_rows += 1
        source_volume += volume
        if high < low:
            low, high = high, low
        low_tick = _price_to_tick_floor(low, tick_size)
        if low == high:
            high_tick = low_tick
        else:
            high_tick = max(
                low_tick,
                _price_to_tick_ceil(high - tick_size * 1e-12, tick_size),
            )
        if low_tick == high_tick:
            histogram[low_tick] = histogram.get(low_tick, 0.0) + volume
            continue
        touched_ticks = high_tick - low_tick + 1
        volume_per_tick = volume / float(touched_ticks)
        for tick in range(low_tick, high_tick + 1):
            histogram[tick] = histogram.get(tick, 0.0) + volume_per_tick

    coverage_ratio = _coverage_ratio(valid_source_rows, expected_source_bars)
    conservation_ratio = _volume_conservation_ratio(source_volume, target_volume)
    if valid_source_rows == 0 or source_volume <= 0:
        return _empty_features(
            quality_code=VP_QUALITY_NO_SOURCE,
            coverage_ratio=coverage_ratio,
            conservation_ratio=conservation_ratio,
        )
    if coverage_ratio < settings.min_coverage_ratio:
        return _empty_features(
            quality_code=VP_QUALITY_LOW_COVERAGE,
            coverage_ratio=coverage_ratio,
            conservation_ratio=conservation_ratio,
        )
    if (
        target_volume is not None
        and abs(1.0 - conservation_ratio) > settings.volume_conservation_tolerance
    ):
        return _empty_features(
            quality_code=VP_QUALITY_VOLUME_MISMATCH,
            coverage_ratio=coverage_ratio,
            conservation_ratio=conservation_ratio,
        )

    ticks = list(range(min(histogram), max(histogram) + 1))
    volumes = [histogram.get(tick, 0.0) for tick in ticks]
    total_volume = sum(volumes)
    poc_position = max(
        range(len(ticks)), key=lambda position: (volumes[position], -ticks[position])
    )
    poc_price = _tick_to_price(ticks[poc_position], tick_size)
    value_area_low, value_area_high = _value_area_bounds(
        ticks=ticks,
        volumes=volumes,
        poc_position=poc_position,
        tick_size=tick_size,
        target_volume=total_volume * settings.value_area_ratio,
    )
    clusters = _detect_clusters(ticks=ticks, volumes=volumes, settings=settings)
    primary = clusters[0] if clusters else None
    secondary = clusters[1] if len(clusters) > 1 else None
    low_volume_gap_count = sum(
        1
        for volume in volumes
        if total_volume > 0 and 0 <= volume / total_volume <= settings.low_volume_gap_share
    )
    shape_code = 1 if len(clusters) <= 1 else 2 if len(clusters) == 2 else 3

    return {
        "vp_poc_price": poc_price,
        "vp_poc_volume_share": volumes[poc_position] / total_volume,
        "vp_vwap": sum(
            _tick_to_price(tick, tick_size) * volume
            for tick, volume in zip(ticks, volumes, strict=True)
        )
        / total_volume,
        "vp_value_area_low": value_area_low,
        "vp_value_area_high": value_area_high,
        "vp_value_area_width": value_area_high - value_area_low,
        "vp_cluster_count": len(clusters),
        "vp_primary_cluster_low": _cluster_bound_price(primary, ticks, tick_size, "low"),
        "vp_primary_cluster_high": _cluster_bound_price(primary, ticks, tick_size, "high"),
        "vp_primary_cluster_volume_share": _cluster_share(primary, total_volume),
        "vp_secondary_cluster_low": _cluster_bound_price(secondary, ticks, tick_size, "low"),
        "vp_secondary_cluster_high": _cluster_bound_price(secondary, ticks, tick_size, "high"),
        "vp_secondary_cluster_volume_share": _cluster_share(secondary, total_volume),
        "vp_cluster_separation_ticks": (
            abs(ticks[primary.peak_position] - ticks[secondary.peak_position])
            if primary is not None and secondary is not None
            else 0
        ),
        "vp_low_volume_gap_count": low_volume_gap_count,
        "vp_shape_code": shape_code,
        "vp_source_1m_coverage_ratio": coverage_ratio,
        "vp_volume_conservation_ratio": conservation_ratio,
        "vp_quality_code": VP_QUALITY_OK,
    }


def _float_value(value: object) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _price_to_tick_floor(price: float, tick_size: float) -> int:
    return math.floor((price + tick_size * 1e-12) / tick_size)


def _price_to_tick_ceil(price: float, tick_size: float) -> int:
    return math.ceil(price / tick_size)


def _tick_to_price(tick: int, tick_size: float) -> float:
    return float(tick) * tick_size


def _coverage_ratio(valid_source_rows: int, expected_source_bars: int | None) -> float:
    if expected_source_bars is None:
        return 1.0 if valid_source_rows else 0.0
    if expected_source_bars <= 0:
        return 1.0
    return min(1.0, valid_source_rows / float(expected_source_bars))


def _volume_conservation_ratio(source_volume: float, target_volume: float | None) -> float:
    if target_volume is None:
        return 1.0
    if target_volume == 0:
        return 1.0 if source_volume == 0 else source_volume / _VOLUME_RATIO_EPSILON
    return source_volume / float(target_volume)


def _empty_features(
    *,
    quality_code: int,
    coverage_ratio: float,
    conservation_ratio: float,
) -> dict[str, float | int | None]:
    return {
        column: (
            0
            if column == "vp_shape_code"
            else coverage_ratio
            if column == "vp_source_1m_coverage_ratio"
            else conservation_ratio
            if column == "vp_volume_conservation_ratio"
            else quality_code
            if column == "vp_quality_code"
            else None
        )
        for column in VOLUME_PROFILE_INDICATOR_COLUMNS
    }


def _value_area_bounds(
    *,
    ticks: list[int],
    volumes: list[float],
    poc_position: int,
    tick_size: float,
    target_volume: float,
) -> tuple[float, float]:
    left = poc_position - 1
    right = poc_position + 1
    area_low = poc_position
    area_high = poc_position
    accumulated = volumes[poc_position]
    while accumulated < target_volume and (left >= 0 or right < len(volumes)):
        left_volume = volumes[left] if left >= 0 else -1.0
        right_volume = volumes[right] if right < len(volumes) else -1.0
        if left_volume >= right_volume:
            accumulated += max(left_volume, 0.0)
            area_low = left
            left -= 1
        else:
            accumulated += max(right_volume, 0.0)
            area_high = right
            right += 1
    return _tick_to_price(ticks[area_low], tick_size), _tick_to_price(ticks[area_high], tick_size)


def _detect_clusters(
    *,
    ticks: list[int],
    volumes: list[float],
    settings: VolumeProfileSettings,
) -> list[_Cluster]:
    total_volume = sum(volumes)
    if not volumes or total_volume <= 0:
        return []
    smoothed = _smooth(volumes)
    peak_positions = [
        position
        for position, volume in enumerate(smoothed)
        if _is_peak(
            position=position,
            volume=volume,
            smoothed=smoothed,
            total_volume=total_volume,
            weak_peak_share=settings.weak_peak_share,
        )
    ]
    if not peak_positions:
        peak_positions = [
            max(range(len(volumes)), key=lambda position: (volumes[position], -ticks[position]))
        ]
    merged_peaks: list[int] = []
    for position in peak_positions:
        if not merged_peaks:
            merged_peaks.append(position)
            continue
        previous = merged_peaks[-1]
        if ticks[position] - ticks[previous] <= settings.merge_distance_ticks:
            if smoothed[position] > smoothed[previous]:
                merged_peaks[-1] = position
        else:
            merged_peaks.append(position)

    split_positions = [
        min(range(left_peak + 1, right_peak), key=lambda position: (smoothed[position], position))
        for left_peak, right_peak in pairwise(merged_peaks)
        if right_peak - left_peak > 1
    ]
    clusters: list[_Cluster] = []
    start = 0
    for index, peak_position in enumerate(merged_peaks):
        end = split_positions[index] if index < len(split_positions) else len(volumes) - 1
        if start > end:
            start = end
        volume = sum(volumes[start : end + 1])
        clusters.append(
            _Cluster(
                peak_position=peak_position,
                start_position=start,
                end_position=end,
                volume=volume,
                peak_volume=volumes[peak_position],
            )
        )
        start = end + 1
    return sorted(
        clusters,
        key=lambda item: (item.volume, item.peak_volume, -ticks[item.peak_position]),
        reverse=True,
    )


def _smooth(volumes: list[float]) -> list[float]:
    if len(volumes) <= 2:
        return list(volumes)
    smoothed: list[float] = []
    for index, volume in enumerate(volumes):
        left = volumes[index - 1] if index > 0 else volume
        right = volumes[index + 1] if index + 1 < len(volumes) else volume
        smoothed.append((left + 2.0 * volume + right) / 4.0)
    return smoothed


def _is_peak(
    *,
    position: int,
    volume: float,
    smoothed: list[float],
    total_volume: float,
    weak_peak_share: float,
) -> bool:
    if volume <= 0 or volume / total_volume < weak_peak_share:
        return False
    left_ok = position == 0 or volume >= smoothed[position - 1]
    right_ok = position + 1 == len(smoothed) or volume >= smoothed[position + 1]
    return left_ok and right_ok


def _cluster_bound_price(
    cluster: _Cluster | None,
    ticks: list[int],
    tick_size: float,
    bound: str,
) -> float | None:
    if cluster is None:
        return None
    position = cluster.start_position if bound == "low" else cluster.end_position
    return _tick_to_price(ticks[position], tick_size)


def _cluster_share(cluster: _Cluster | None, total_volume: float) -> float | None:
    if cluster is None:
        return None
    return cluster.volume / total_volume
