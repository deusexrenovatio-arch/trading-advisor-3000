from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.moex.foundation import (  # noqa: E402
    load_mapping_registry,
    validate_mapping_registry,
)
from trading_advisor_3000.product_plane.data_plane.moex.session_schedule import (  # noqa: E402
    DEFAULT_PUBLIC_RULE_CATALOG_PATH,
    materialize_reconstructed_session_schedule_for_changed_windows,
)

DEFAULT_MAPPING_REGISTRY = (
    ROOT / "configs" / "moex_foundation" / "instrument_mapping_registry.v1.yaml"
)


def _split_csv(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def _changed_windows_for_backfill(
    *,
    date_from: date,
    date_till: date,
    mappings,
    internal_ids: set[str],
) -> list[dict[str, object]]:
    windows: list[dict[str, object]] = []
    moscow_tz = ZoneInfo("Europe/Moscow")
    window_start_utc = datetime.combine(
        date_from,
        time.min,
        tzinfo=moscow_tz,
    ).astimezone(UTC)
    window_end_utc = datetime.combine(
        date_till + timedelta(days=1),
        time.min,
        tzinfo=moscow_tz,
    ).astimezone(UTC)
    window_start_text = window_start_utc.isoformat().replace("+00:00", "Z")
    window_end_text = window_end_utc.isoformat().replace("+00:00", "Z")
    for mapping in mappings:
        if not getattr(mapping, "is_active", True):
            continue
        if internal_ids and mapping.internal_id not in internal_ids:
            continue
        windows.append(
            {
                "internal_id": mapping.internal_id,
                "source_timeframe": "1m",
                "source_interval": 1,
                "moex_engine": mapping.moex_engine,
                "moex_market": mapping.moex_market,
                "moex_board": mapping.moex_board,
                "moex_secid": mapping.moex_secid,
                "window_start_utc": window_start_text,
                "window_end_utc": window_end_text,
                "incremental_rows": 0,
            }
        )
    return windows


def _parse_utc_timestamp_date(value: object) -> date:
    if isinstance(value, datetime):
        timestamp = value
    elif isinstance(value, date):
        return value
    else:
        text = str(value).strip()
        if not text:
            raise ValueError("empty timestamp value in current data table")
        timestamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(ZoneInfo("Europe/Moscow")).date()


def _resolve_timestamp_column(table: DeltaTable, timestamp_column: str | None) -> str:
    available = set(table.to_pyarrow_dataset().schema.names)
    if timestamp_column:
        if timestamp_column not in available:
            raise ValueError(f"current data table has no timestamp column: {timestamp_column}")
        return timestamp_column
    for candidate in ("ts", "ts_open"):
        if candidate in available:
            return candidate
    raise ValueError("current data table must contain `ts` or `ts_open` timestamp column")


def _validate_backfill_covers_current_data(
    *,
    date_from: date,
    date_till: date,
    current_data_path: Path,
    timestamp_column: str | None,
) -> dict[str, object]:
    if not (current_data_path / "_delta_log").exists():
        raise FileNotFoundError(
            f"current data delta table is missing `_delta_log`: {current_data_path.as_posix()}"
        )
    table = DeltaTable(str(current_data_path))
    resolved_timestamp_column = _resolve_timestamp_column(table, timestamp_column)
    scanner = table.to_pyarrow_dataset().scanner(
        columns=[resolved_timestamp_column],
        batch_size=65_536,
    )
    current_min = None
    current_max = None
    timestamp_count = 0
    for batch in scanner.to_batches():
        for value in batch.column(0).to_pylist():
            if value is None:
                continue
            timestamp_count += 1
            current_min = value if current_min is None or value < current_min else current_min
            current_max = value if current_max is None or value > current_max else current_max
    if timestamp_count == 0 or current_min is None or current_max is None:
        raise ValueError(
            f"current data table has no timestamp values: {current_data_path.as_posix()}"
        )

    current_date_from = _parse_utc_timestamp_date(current_min)
    current_date_till = _parse_utc_timestamp_date(current_max)
    coverage = {
        "current_data_path": current_data_path.as_posix(),
        "timestamp_column": resolved_timestamp_column,
        "current_data_date_from": current_date_from.isoformat(),
        "current_data_date_till": current_date_till.isoformat(),
        "requested_date_from": date_from.isoformat(),
        "requested_date_till": date_till.isoformat(),
        "current_data_rows_scanned": timestamp_count,
    }
    if date_from > current_date_from or date_till < current_date_till:
        raise ValueError(
            "session schedule backfill date range does not cover current data range: "
            f"requested {date_from.isoformat()}..{date_till.isoformat()}, "
            f"current data {current_date_from.isoformat()}..{current_date_till.isoformat()} "
            f"from {current_data_path.as_posix()}"
        )
    return coverage


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill MOEX futures session schedule tables from static public rules."
    )
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-till", required=True)
    parser.add_argument("--mapping-registry-path", default=DEFAULT_MAPPING_REGISTRY.as_posix())
    parser.add_argument("--rule-catalog-path", default=DEFAULT_PUBLIC_RULE_CATALOG_PATH.as_posix())
    parser.add_argument("--raw-table-path")
    parser.add_argument("--raw-schedule-path", required=True)
    parser.add_argument("--canonical-session-intervals-path", required=True)
    parser.add_argument("--internal-ids", default="")
    parser.add_argument("--report-path")
    parser.add_argument("--no-candle-inference", action="store_true")
    parser.add_argument(
        "--require-current-data-path",
        help="Delta table whose timestamp range must be covered by --date-from/--date-till.",
    )
    parser.add_argument(
        "--current-data-timestamp-column",
        help="Timestamp column in --require-current-data-path; defaults to ts, then ts_open.",
    )
    args = parser.parse_args()

    date_from = date.fromisoformat(args.date_from)
    date_till = date.fromisoformat(args.date_till)
    if date_till < date_from:
        raise ValueError("--date-till must be >= --date-from")

    current_data_coverage = None
    if args.require_current_data_path:
        current_data_coverage = _validate_backfill_covers_current_data(
            date_from=date_from,
            date_till=date_till,
            current_data_path=Path(args.require_current_data_path).resolve(),
            timestamp_column=args.current_data_timestamp_column,
        )

    mappings = load_mapping_registry(Path(args.mapping_registry_path))
    validate_mapping_registry(mappings)
    changed_windows = _changed_windows_for_backfill(
        date_from=date_from,
        date_till=date_till,
        mappings=mappings,
        internal_ids=_split_csv(args.internal_ids),
    )
    if not changed_windows:
        raise RuntimeError("session schedule backfill resolved no active mappings")

    raw_table_path = Path(args.raw_table_path).resolve() if args.raw_table_path else None
    report = materialize_reconstructed_session_schedule_for_changed_windows(
        changed_windows=changed_windows,
        mappings=mappings,
        raw_table_path=raw_table_path,
        raw_schedule_path=Path(args.raw_schedule_path).resolve(),
        canonical_session_intervals_path=Path(args.canonical_session_intervals_path).resolve(),
        rule_catalog_path=Path(args.rule_catalog_path).resolve(),
        allow_candle_inference=not args.no_candle_inference,
    )
    if current_data_coverage is not None:
        report["current_data_coverage"] = current_data_coverage
    if args.report_path:
        report_path = Path(args.report_path).resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
