"""Exact old792 native producer -> current semantic metadata adoption."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from trading_advisor_3000.product_plane.research.indicators import materialize as m

# Exact Git79257fa3 function; helpers/recipes are independently proved against
# the original complete module in semantic-native-parity-20261020 evidence.
OLD_SOURCE_SHA256 = "30af2e1e24af3908e5a17b5e81a4851446ef4229278858157bd5baa177902a17"
OLD_NATIVE_PRODUCER = r"""
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
    volume_profile_source_rows: Mapping[tuple[str, str], Sequence[Mapping[str, object]]]
    | None = None,
    volume_profile_raw_1m_table_path: Path | None = None,
    volume_profile_tick_size_by_instrument: Mapping[str, float] | None = None,
    compute_diagnostics: dict[str, object] | None = None,
    precomputed_source_hash: str | None = None,
    native_full_compute: bool = False,
    volume_profile_refresh_ts: set[str] | None = None,
    source_snapshot_hash: str = "",
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
    resolved_volume_profile_source_rows = volume_profile_source_rows
    existing_by_ts = {row.ts: row for row in existing_rows or []}
    volume_profile_series = [
        row
        for row in series
        if volume_profile_refresh_ts is None or row.ts in volume_profile_refresh_ts
    ]
    if (
        _profile_has_volume_profile_spec(compute_profile)
        and resolved_volume_profile_source_rows is None
        and volume_profile_raw_1m_table_path is not None
    ):
        resolved_volume_profile_source_rows = _load_volume_profile_source_rows(
            raw_1m_table_path=volume_profile_raw_1m_table_path,
            local_series=volume_profile_series,
        )
    cached_volume_profile_hashes = {
        ts: row.volume_profile_input_hash
        for ts, row in existing_by_ts.items()
        if volume_profile_refresh_ts is not None
        and ts not in volume_profile_refresh_ts
        and row.volume_profile_input_hash
    }
    volume_profile_hashes: dict[str, str] = {}
    compute_contract_hash = _indicator_compute_contract_hash(profile)
    input_row_hashes = _indicator_input_row_hashes(
        series,
        compute_contract_hash=compute_contract_hash,
        volume_profile_source_rows=resolved_volume_profile_source_rows,
        tick_size_by_instrument=volume_profile_tick_size_by_instrument,
        has_volume_profile=_profile_has_volume_profile_spec(profile),
        required_columns=profile.required_input_columns(),
        series_mode=series_mode,
        cached_volume_profile_hashes=cached_volume_profile_hashes,
        volume_profile_hashes=volume_profile_hashes,
    )
    cached_rows = sorted(existing_rows or [], key=lambda row: row.ts)
    prefix_length = 0
    # Native source-hash format is independent of whether a verified prefix
    # can continue computation. Unsupported specs still use full history.
    if series_mode != "continuous_front" and compute_profile == profile:
        for original, cached, row_hash in zip(series, cached_rows, input_row_hashes):
            if (
                cached.ts != original.ts
                or cached.input_row_hash != row_hash
                or cached.compute_contract_hash != compute_contract_hash
                or not set(profile.expected_output_columns()).issubset(cached.values)
            ):
                break
            prefix_length += 1
    diagnostics = compute_diagnostics if compute_diagnostics is not None else {}
    diagnostics["reused_prefix_row_count"] = (
        int(diagnostics.get("reused_prefix_row_count", 0)) + prefix_length
    )
    if prefix_length:
        existing_frame = pd.DataFrame([row.values for row in cached_rows[:prefix_length]])
        computed = _compute_cached_profile_frame(
            frame,
            compute_profile,
            existing=existing_frame,
            prefix_length=prefix_length,
            diagnostics=diagnostics,
        )
    else:
        diagnostics["full_history_spec_count"] = int(
            diagnostics.get("full_history_spec_count", 0)
        ) + len(compute_profile.indicators)
        computed = _compute_profile_frame_for_series(
            frame,
            compute_profile,
            series_mode=series_mode,
            adjustment_ladder_rows=series_ladder_rows,
        )
    if _profile_has_volume_profile_spec(compute_profile):
        if volume_profile_refresh_ts is not None:
            cached_indices = [
                index for index, bar in enumerate(series) if bar.ts not in volume_profile_refresh_ts
            ]
            for column in VOLUME_PROFILE_INDICATOR_COLUMNS:
                computed[column] = computed[column].astype(object)
                computed.loc[cached_indices, column] = pd.Series(
                    [
                        existing_by_ts[series[index].ts].values.get(column)
                        for index in cached_indices
                    ],
                    index=cached_indices,
                    dtype="object",
                )
        volume_indices = [
            index
            for index, bar in enumerate(series)
            if index >= prefix_length
            and (volume_profile_refresh_ts is None or bar.ts in volume_profile_refresh_ts)
        ]
        for column, series_values in _compute_volume_profile_columns(
            local_series=[series[index] for index in volume_indices],
            index=computed.index[volume_indices],
            source_rows_by_series=resolved_volume_profile_source_rows,
            tick_size_by_instrument=volume_profile_tick_size_by_instrument,
        ).items():
            if prefix_length or volume_profile_refresh_ts is not None:
                computed[column] = computed[column].astype(object)
                computed.loc[series_values.index, column] = series_values
            else:
                computed[column] = series_values
        volume_profile_columns = tuple(
            column for column in VOLUME_PROFILE_INDICATOR_COLUMNS if column in computed.columns
        )
        for rule, output_columns in indicator_bar_usage_groups_for_outputs(volume_profile_columns):
            eligible = _bar_usage_eligible_mask(frame, rule.required_flags)
            projected = _project_bar_usage_computed_columns(
                frame=frame,
                computed=computed.loc[eligible, list(output_columns)],
                output_columns=output_columns,
                rule=rule,
            )
            for column, series_values in projected.items():
                computed[column] = series_values
    payload_rows = computed.to_dict("records")
    output_columns = profile.expected_output_columns()
    computed_columns = set(compute_profile.expected_output_columns())
    source_hash = precomputed_source_hash or _bars_hash(
        series, adjustment_ladder_rows=series_ladder_rows
    )
    if (
        _profile_requires_volume_profile(profile)
        and resolved_volume_profile_source_rows is not None
        and precomputed_source_hash is None
        and not native_full_compute
    ):
        source_hash = _volume_profile_source_hash(
            target_bars_hash=source_hash,
            local_series=series,
            source_rows_by_series=resolved_volume_profile_source_rows,
            tick_size_by_instrument=volume_profile_tick_size_by_instrument,
        )
    if native_full_compute:
        source_hash = hashlib.sha256(
            _canonical_json_bytes(
                {
                    "bars": _bars_hash(series, adjustment_ladder_rows=series_ladder_rows),
                    "inputs": input_row_hashes,
                    "ladder": _normalize_ladder_rows(series_ladder_rows),
                }
            )
        ).hexdigest()
    refresh_context_hash = _refresh_context_hash(
        profile,
        instrument_id=series[0].instrument_id,
        ladder_rows=series_ladder_rows,
        raw_1m_path=volume_profile_raw_1m_table_path,
        source_rows=volume_profile_source_rows,
        tick_sizes=volume_profile_tick_size_by_instrument,
    )
    output_columns_hash = output_columns_hash or indicator_output_columns_hash(output_columns)
    prepared_values: list[
        tuple[ResearchBarView, dict[str, float | int | None], IndicatorFrameRow | None]
    ] = []
    merged_payload_rows: list[dict[str, object]] = []

    for original, payload in zip(series, payload_rows, strict=True):
        existing = existing_by_ts.get(original.ts)
        values: dict[str, float | int | None] = {}
        for column in output_columns:
            if column in computed_columns:
                value = payload.get(column)
            elif existing is not None and column in existing.values:
                value = existing.values[column]
            else:
                value = None
            if value is None or pd.isna(value):
                values[column] = None
            elif column in VOLUME_PROFILE_INT_COLUMNS:
                values[column] = int(value)
            else:
                values[column] = float(value)
        prepared_values.append((original, values, existing))
        merged_payload_rows.append(values)

    null_warmup_span = _null_warmup_span(merged_payload_rows, output_columns)
    materialized_at = pd.Timestamp.utcnow().isoformat().replace("+00:00", "Z")

    rows: list[IndicatorFrameRow] = []
    for index, (original, values, existing) in enumerate(prepared_values):
        series_key = _series_group_key(
            original,
            dataset_version=dataset_version,
            indicator_set_version=indicator_set_version,
            series_mode=series_mode,
        )
        rows.append(
            IndicatorFrameRow(
                dataset_version=dataset_version,
                contour_id=original.contour_id,
                series_mode=series_key.series_mode,
                series_id=series_key.series_id,
                indicator_set_version=indicator_set_version,
                profile_version=profile.version,
                contract_id=original.contract_id,
                instrument_id=original.instrument_id,
                timeframe=original.timeframe,
                ts=original.ts,
                bar_end_ts=original.bar_end_ts,
                values=values,
                source_bars_hash=source_hash,
                row_count=len(series),
                warmup_span=profile.max_warmup_bars(),
                null_warmup_span=null_warmup_span,
                created_at=existing.created_at
                if existing is not None and (index < prefix_length or compute_profile != profile)
                else materialized_at,
                output_columns_hash=output_columns_hash,
                source_dataset_bars_hash=source_dataset_bars_hash,
                compute_contract_hash=compute_contract_hash,
                input_row_hash=input_row_hashes[index],
                refresh_context_hash=refresh_context_hash,
                source_snapshot_hash=source_snapshot_hash,
                volume_profile_input_hash=volume_profile_hashes.get(original.ts, ""),
            )
        )
    return rows
""".lstrip("\n")


@pytest.mark.parametrize("change", ["unchanged", "mixed_prefix", "changed_input"])
def test_exact_old_native_l1_receipt_preserves_values_without_recompute(
    tmp_path, monkeypatch, change
):
    import importlib.util
    from dataclasses import replace

    from deltalake import DeltaTable

    from trading_advisor_3000.product_plane.research import hash_compatibility as bridge
    from trading_advisor_3000.product_plane.research.indicators.store import (
        write_indicator_frame_partition_batches,
    )

    # Reuse the repository's real tiny L0/minute fixture; L2 is irrelevant here.
    path = (
        Path(__file__).resolve().parents[1]
        / "integration/test_research_indicator_materialization.py"
    )
    spec = importlib.util.spec_from_file_location("_semantic_native_l1_fixture", path)
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    monkeypatch.setattr(fixture, "materialize_derived_indicator_frames", lambda **_: {})
    l1, *_ = fixture._vp_partition_fixture(tmp_path, monkeypatch)
    heads = m.load_indicator_partition_metadata(
        output_dir=l1["indicator_output_dir"],
        dataset_version="vp-scope",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
    )
    namespace = dict(m.__dict__)
    namespace["_indicator_compute_contract_hash"] = m._legacy_indicator_compute_contract_hash
    namespace["_refresh_context_hash"] = lambda *args, **kwargs: m._refresh_context_hash(
        *args, **kwargs, _legacy_compute=True
    )
    exec(OLD_NATIVE_PRODUCER, namespace)
    old = namespace["_build_partition_rows"]
    expected = {}
    for head in heads:
        partition = m._metadata_partition_key(head, series_mode="continuous_front")
        bars = m._load_bar_partition_rows(
            dataset_output_dir=l1["dataset_output_dir"],
            dataset_version="vp-scope",
            contour_id="pit_active_front",
            partition=partition,
        )
        rows = old(
            dataset_version="vp-scope",
            indicator_set_version="indicators-v1",
            profile=l1["profile"],
            series=bars,
            series_mode="continuous_front",
            native_full_compute=True,
            volume_profile_raw_1m_table_path=l1["volume_profile_raw_1m_table_path"],
            volume_profile_tick_size_by_instrument=l1["volume_profile_tick_size_by_instrument"],
            source_snapshot_hash=m._source_snapshot_hash(
                l1["dataset_output_dir"] / "research_bar_views.delta",
                l1["volume_profile_raw_1m_table_path"],
            ),
        )
        assert len(rows[-1].source_bars_hash) == 64
        expected.update({(row.instrument_id, row.timeframe, row.ts): row.values for row in rows})
        if change == "mixed_prefix":
            rows = [
                replace(
                    row,
                    source_bars_hash="historical-prefix",
                    row_count=i + 1,
                    created_at="2020-01-01T00:00:00Z",
                )
                if i < len(rows) - 1
                else row
                for i, row in enumerate(rows)
            ]
        write_indicator_frame_partition_batches(
            output_dir=l1["indicator_output_dir"],
            partition_row_batches=((partition, rows),),
            profile=l1["profile"],
        )
    if change == "changed_input":
        DeltaTable(str(l1["dataset_output_dir"] / "research_bar_views.delta")).update(
            updates={"close": "close + 0.2", "native_close": "native_close + 0.2"},
            predicate="instrument_id = 'BR' AND timeframe = '15m' AND ts = '2026-03-01T09:00:00Z'",
        )
    builder = m._build_partition_rows
    called = []

    def compute(**kwargs):
        key = kwargs["series"][0]
        called.append((key.instrument_id, key.timeframe))
        assert change == "changed_input" and called == [("BR", "15m")], (
            "hash-only change triggered numerical recomputation"
        )
        return builder(**kwargs)

    monkeypatch.setattr(m, "_build_partition_rows", compute)
    monkeypatch.setattr(bridge, "reviewed_current_source", lambda: True)
    with bridge._verified_migration_scope(bridge.NATIVE_LEGACY_SOURCE):
        report = m.materialize_indicator_frames(operation="rebuild", **l1)
    assert report["hash_migrated_partition_count"] == (3 if change == "changed_input" else 4)
    actual = fixture.reload_indicator_frames(
        indicator_output_dir=l1["indicator_output_dir"],
        dataset_version="vp-scope",
        indicator_set_version="indicators-v1",
        contour_id="pit_active_front",
    )
    values = {(row.instrument_id, row.timeframe, row.ts): row.values for row in actual}
    if change != "changed_input":
        assert values == expected and called == []
    else:
        assert called == [("BR", "15m")]
        for key, value in expected.items():
            if key[:2] != ("BR", "15m"):
                assert values[key] == value
    monkeypatch.setattr(m, "_build_partition_rows", builder)
    assert (
        m.materialize_indicator_frames(operation="rebuild", **l1)["refreshed_partition_count"] == 0
    )


def test_old_native_producer_is_immutable_historical_code():
    assert hashlib.sha256(OLD_NATIVE_PRODUCER.encode()).hexdigest() == OLD_SOURCE_SHA256
