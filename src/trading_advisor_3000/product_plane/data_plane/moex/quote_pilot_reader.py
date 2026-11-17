"""Bounded quote reads from an explicitly accepted, version-pinned pilot report."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from trading_advisor_3000.product_plane.contracts.market_quotes import (
    validate_quote_bar,
    validate_quote_series,
)
from trading_advisor_3000.product_plane.data_plane.moex.quote_pilot import (
    REPO_ROOT,
    evidence_hashes,
    load_acquisition,
    validate_pilot_path,
    validate_run_id,
)


def _path(value: str) -> Path:
    # The supported Docker proof mounts this candidate at /workspace.
    # Mapping permits the same immutable report to be inspected from its host.
    candidate = (
        REPO_ROOT / value[len("/workspace/") :] if value.startswith("/workspace/") else Path(value)
    )
    return validate_pilot_path(candidate)


def _accepted_report(report_path: Path) -> dict[str, Any]:
    from deltalake import DeltaTable

    path = validate_pilot_path(report_path)
    report = json.loads(path.read_text("utf-8"))
    if (
        report.get("schema_version") != "moex_quote_pilot_report.v1"
        or report.get("status") != "PASS"
    ):
        raise ValueError("an accepted pilot report is required")
    validate_run_id(report["run_id"])
    if _path(report["report_path"]) != path:
        raise ValueError("accepted report path mismatch")
    acquisition = _path(report["acquisition_dir"])
    metadata = load_acquisition(acquisition)
    if evidence_hashes(acquisition) != report["source_evidence"]:
        raise ValueError("accepted source evidence no longer matches")
    if any(report.get(key) != metadata.get(key) for key in ("date_from", "date_till")):
        raise ValueError("accepted report scope differs from acquired scope")
    if report.get("source_run_id") != metadata["run_id"]:
        raise ValueError("accepted report source identity mismatch")
    acquired_series = [
        json.loads(line)
        for line in (acquisition / "series.jsonl").read_text("utf-8").splitlines()
        if line.strip()
    ]
    source_ids = {record["series_id"] for record in acquired_series}
    if len(source_ids) != len(acquired_series) or not set(report["per_series_counts"]).issubset(
        source_ids
    ):
        raise ValueError("accepted report series scope differs from acquired scope")
    if set(report["tables"]) != {"raw_quote_candles", "quote_series", "quote_bars"}:
        raise ValueError("accepted report is missing required table pins")
    for name, pin in report["tables"].items():
        table_path = _path(pin["path"])
        expected_path = validate_pilot_path(path.parent.parent / f"{name}.delta")
        if table_path != expected_path or not (table_path / "_delta_log").is_dir():
            raise ValueError("accepted Delta table path mismatch or missing table")
        if type(pin["version"]) is not int or pin["version"] < 0:
            raise ValueError("accepted report requires nonnegative Delta versions")
        try:
            pinned = DeltaTable(str(table_path), version=pin["version"])
            if pinned.version() != pin["version"]:
                raise ValueError("Delta returned a different version")
        except Exception as exc:
            raise ValueError(f"accepted Delta version is unavailable: {name}") from exc
    return report


def read_quote_pilot_bars(
    *,
    report_path: Path,
    series_id: str,
    date_from: date,
    date_till: date,
    timeframe: str = "1d",
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Read only selected source-run rows, applying predicates/limit in Arrow."""
    import pyarrow.dataset as ds
    from deltalake import DeltaTable

    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("pilot read limit must be between 1 and 1000")
    if timeframe != "1d" or not series_id or date_from > date_till:
        raise ValueError("pilot reads require one daily series and an ordered date range")
    report = _accepted_report(report_path)
    if (
        series_id not in report["per_series_counts"]
        or date_from < date.fromisoformat(report["date_from"])
        or date_till > date.fromisoformat(report["date_till"])
    ):
        raise ValueError("read scope is outside the accepted acquisition")

    def dataset(name: str):
        pin = report["tables"][name]
        return DeltaTable(str(_path(pin["path"])), version=pin["version"]).to_pyarrow_dataset()

    descriptors = (
        dataset("quote_series")
        .scanner(
            filter=ds.field("series_id") == series_id,
        )
        .head(2)
        .to_pylist()
    )
    if len(descriptors) != 1:
        raise ValueError("accepted series descriptor is missing or duplicated")
    descriptor = descriptors[0]
    validate_quote_series(descriptor)
    source = _path(report["acquisition_dir"])
    original_descriptor = next(
        json.loads(line)
        for line in (source / "series.jsonl").read_text("utf-8").splitlines()
        if line.strip() and json.loads(line)["series_id"] == series_id
    )
    if descriptor != original_descriptor:
        raise ValueError("accepted series descriptor differs from source evidence")
    expression = (
        (ds.field("series_id") == series_id)
        & (ds.field("timeframe") == timeframe)
        & (ds.field("source_run_id") == report["source_run_id"])
        & (ds.field("source_begin") >= f"{date_from.isoformat()} 00:00:00")
        & (ds.field("source_begin") <= f"{date_till.isoformat()} 23:59:59")
    )
    bars = dataset("quote_bars").scanner(filter=expression).head(limit).to_pylist()
    for bar in bars:
        validate_quote_bar(bar)
        if descriptor["asset_class"] == "index" and (
            bar["volume_status"] != "not_applicable" or bar["turnover"] is not None
        ):
            raise ValueError("index source aggregates cannot be exposed as trading volume/turnover")
        if descriptor["asset_class"] != "index" and bar["volume_status"] == "not_applicable":
            raise ValueError("tradable source volume availability is inconsistent")
    return sorted(bars, key=lambda bar: bar["ts_open"])
