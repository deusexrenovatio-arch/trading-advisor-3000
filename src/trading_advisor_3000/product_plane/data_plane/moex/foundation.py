from __future__ import annotations

# ruff: noqa: E501
import csv
import hashlib
import json
import re
import traceback
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from deltalake import DeltaTable

from ..delta_runtime import (
    has_delta_log,
    read_filtered_delta_table_rows,
    write_delta_table_rows,
)
from .historical_route_contracts import build_raw_ingest_run_report_v2
from .iss_client import MoexISSClient

MOEX_TIMEZONE = ZoneInfo("Europe/Moscow")
ALLOWED_ASSET_GROUPS = {"commodity", "index"}
SOURCE_INTERVAL_BY_TARGET_TIMEFRAME = {
    "1m": 1,
    "5m": 1,
    "15m": 1,
    "1h": 60,
    "4h": 60,
    "1d": 24,
    "1w": 7,
}
SOURCE_TIMEFRAME_LABEL_BY_INTERVAL = {
    1: "1m",
    10: "10m",
    60: "1h",
    24: "1d",
    7: "1w",
    31: "1M",
    4: "quarter",
}
DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS = 14
DEFAULT_REFRESH_OVERLAP_MINUTES = 180
DEFAULT_TAIL_MISSING_CANDLE_GRACE_MINUTES = 5
FUTURES_MONTH_CODES = frozenset("FGHJKMNQUVXZ")
FUTURES_CONTRACT_SECID_RE = re.compile(r"^[A-Z0-9]{1,6}[FGHJKMNQUVXZ][0-9]$")
TARGET_TIMEFRAME_ORDER = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1w": 10080,
}
RAW_COLUMNS: dict[str, str] = {
    "internal_id": "string",
    "finam_symbol": "string",
    "moex_engine": "string",
    "moex_market": "string",
    "moex_board": "string",
    "moex_secid": "string",
    "asset_group": "string",
    "timeframe": "string",
    "source_interval": "int",
    "ts_open": "timestamp",
    "ts_close": "timestamp",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "volume": "bigint",
    "open_interest": "bigint",
    "ingest_run_id": "string",
    "ingested_at_utc": "timestamp",
    "provenance_json": "json",
}
RAW_KEY_COLUMNS = ("internal_id", "timeframe", "source_interval", "moex_secid")
RAW_ROW_KEY_COLUMNS = RAW_KEY_COLUMNS + ("ts_open", "ts_close")
RAW_PROVIDER_METADATA_COLUMNS = (
    "finam_symbol",
    "moex_engine",
    "moex_market",
    "moex_board",
    "asset_group",
    "provenance_json",
)
RAW_VALUE_COLUMNS = ("open", "high", "low", "close", "volume", "open_interest")
RAW_FINGERPRINT_COLUMNS = RAW_ROW_KEY_COLUMNS + RAW_PROVIDER_METADATA_COLUMNS + RAW_VALUE_COLUMNS
RAW_VOLATILE_PROVENANCE_KEYS = frozenset(
    (
        "run_id",
        "ingest_run_id",
        "ingested_at_utc",
        "window_start_utc",
        "window_end_utc",
        "stability_lag_minutes",
        "refresh_overlap_minutes",
        "discovery_url",
        "requested_target_timeframes",
    )
)
RAW_NULL_FINGERPRINT_TOKEN = "__TA3000_NULL__"


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_progress_print(message: str) -> None:
    try:
        print(message, flush=True)
    except OSError:
        # Progress is already persisted to JSONL files; stdout must not be able to abort the run.
        return


def _require_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"`{field}` must be non-empty string")
    return value.strip()


def _require_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"`{field}` must be boolean")
    return value


def _require_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"`{field}` must be integer")
    return value


def _parse_moex_datetime(text: str) -> datetime:
    naive = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return naive.replace(tzinfo=MOEX_TIMEZONE).astimezone(UTC)


def _parse_iso_utc(text: str) -> datetime:
    normalized = text.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(UTC)


def _to_iso_utc(value: datetime | str) -> str:
    parsed = value if isinstance(value, datetime) else _parse_iso_utc(str(value))
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _source_timeframe_label(interval: int) -> str:
    label = SOURCE_TIMEFRAME_LABEL_BY_INTERVAL.get(interval)
    if label is None:
        raise ValueError(f"unsupported MOEX source interval: {interval}")
    return label


def _source_interval_delta(interval: int) -> timedelta:
    normalized = int(interval)
    if normalized in {1, 10, 60}:
        return timedelta(minutes=normalized)
    if normalized == 24:
        return timedelta(days=1)
    if normalized == 7:
        return timedelta(days=7)
    if normalized == 31:
        return timedelta(days=31)
    return timedelta(minutes=normalized)


def _tail_watermark_covers_window(
    *,
    watermark_dt: datetime,
    window_end: datetime,
    source_interval: int,
) -> bool:
    missing_candle_grace = max(
        _source_interval_delta(source_interval),
        timedelta(minutes=DEFAULT_TAIL_MISSING_CANDLE_GRACE_MINUTES),
    )
    return watermark_dt >= window_end or watermark_dt + missing_candle_grace >= window_end


def _sorted_target_timeframes(timeframes: set[str]) -> list[str]:
    return sorted(timeframes, key=lambda item: (TARGET_TIMEFRAME_ORDER[item], item))


@dataclass(frozen=True)
class UniverseSymbol:
    internal_id: str
    asset_class: str
    asset_group: str
    status: str
    finam_symbol: str
    moex_engine: str
    moex_market: str
    moex_board: str
    moex_secid: str
    moex_asset_codes: tuple[str, ...]

    @property
    def is_active(self) -> bool:
        return self.status == "active"


@dataclass(frozen=True)
class MappingRecord:
    internal_id: str
    finam_symbol: str
    moex_engine: str
    moex_market: str
    moex_board: str
    moex_secid: str
    asset_class: str
    asset_group: str
    mapping_version: int
    is_active: bool
    activated_at_utc: str
    deactivated_at_utc: str | None
    change_reason: str


@dataclass(frozen=True)
class DiscoveryRecord:
    internal_id: str
    finam_symbol: str
    moex_engine: str
    moex_market: str
    moex_board: str
    moex_secid: str
    asset_group: str
    requested_target_timeframes: str
    source_interval: int
    source_timeframe: str
    coverage_begin_utc: str
    coverage_end_utc: str
    discovered_at_utc: str
    discovery_url: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "internal_id": self.internal_id,
            "finam_symbol": self.finam_symbol,
            "moex_engine": self.moex_engine,
            "moex_market": self.moex_market,
            "moex_board": self.moex_board,
            "moex_secid": self.moex_secid,
            "asset_group": self.asset_group,
            "requested_target_timeframes": self.requested_target_timeframes,
            "source_interval": self.source_interval,
            "source_timeframe": self.source_timeframe,
            "coverage_begin_utc": self.coverage_begin_utc,
            "coverage_end_utc": self.coverage_end_utc,
            "discovered_at_utc": self.discovered_at_utc,
            "discovery_url": self.discovery_url,
        }


@dataclass(frozen=True)
class FoundationRunReport:
    run_id: str
    route_signal: str
    timeframe_set: list[str]
    source_interval_set: list[int]
    source_timeframe_set: list[str]
    expand_contract_chain: bool
    contract_discovery_step_days: int
    contract_discovery_lookback_days: int
    refresh_overlap_minutes: int
    universe_size: int
    coverage_rows: int
    coverage_report_path: str
    coverage_table_path: str
    moex_request_log_path: str
    moex_request_latest_path: str
    raw_ingest_report_path: str
    raw_ingest_progress_path: str
    raw_ingest_error_path: str
    raw_ingest_error_latest_path: str
    raw_table_path: str
    source_rows: int
    incremental_rows: int
    deduplicated_rows: int
    stale_rows: int
    watermark_by_key: dict[str, str]
    real_bindings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "route_signal": self.route_signal,
            "timeframe_set": self.timeframe_set,
            "source_interval_set": self.source_interval_set,
            "source_timeframe_set": self.source_timeframe_set,
            "expand_contract_chain": self.expand_contract_chain,
            "contract_discovery_step_days": self.contract_discovery_step_days,
            "contract_discovery_lookback_days": self.contract_discovery_lookback_days,
            "refresh_overlap_minutes": self.refresh_overlap_minutes,
            "universe_size": self.universe_size,
            "coverage_rows": self.coverage_rows,
            "coverage_report_path": self.coverage_report_path,
            "coverage_table_path": self.coverage_table_path,
            "moex_request_log_path": self.moex_request_log_path,
            "moex_request_latest_path": self.moex_request_latest_path,
            "raw_ingest_report_path": self.raw_ingest_report_path,
            "raw_ingest_progress_path": self.raw_ingest_progress_path,
            "raw_ingest_error_path": self.raw_ingest_error_path,
            "raw_ingest_error_latest_path": self.raw_ingest_error_latest_path,
            "raw_table_path": self.raw_table_path,
            "source_rows": self.source_rows,
            "incremental_rows": self.incremental_rows,
            "deduplicated_rows": self.deduplicated_rows,
            "stale_rows": self.stale_rows,
            "watermark_by_key": self.watermark_by_key,
            "real_bindings": self.real_bindings,
        }


def load_universe(path: Path) -> list[UniverseSymbol]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"universe payload must be object: {path.as_posix()}")
    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise ValueError(f"universe symbols must be non-empty list: {path.as_posix()}")

    rows: list[UniverseSymbol] = []
    for idx, row in enumerate(symbols):
        if not isinstance(row, dict):
            raise ValueError(f"universe symbols[{idx}] must be object")
        moex = row.get("moex")
        if not isinstance(moex, dict):
            raise ValueError(f"universe symbols[{idx}].moex must be object")
        asset_codes_raw = moex.get("asset_codes", [])
        if asset_codes_raw is None:
            asset_codes_raw = []
        if not isinstance(asset_codes_raw, list):
            raise ValueError(f"universe symbols[{idx}].moex.asset_codes must be list when provided")
        asset_codes = tuple(
            sorted(
                {
                    _require_text(value, f"symbols[{idx}].moex.asset_codes[]").upper()
                    for value in asset_codes_raw
                }
            )
        )
        symbol = UniverseSymbol(
            internal_id=_require_text(row.get("internal_id"), f"symbols[{idx}].internal_id"),
            asset_class=_require_text(row.get("asset_class"), f"symbols[{idx}].asset_class"),
            asset_group=_require_text(row.get("asset_group"), f"symbols[{idx}].asset_group"),
            status=_require_text(row.get("status"), f"symbols[{idx}].status"),
            finam_symbol=_require_text(row.get("finam_symbol"), f"symbols[{idx}].finam_symbol"),
            moex_engine=_require_text(moex.get("engine"), f"symbols[{idx}].moex.engine"),
            moex_market=_require_text(moex.get("market"), f"symbols[{idx}].moex.market"),
            moex_board=_require_text(moex.get("board"), f"symbols[{idx}].moex.board"),
            moex_secid=_require_text(moex.get("secid"), f"symbols[{idx}].moex.secid"),
            moex_asset_codes=asset_codes,
        )
        if symbol.asset_class != "futures":
            raise ValueError(f"universe symbol `{symbol.internal_id}` must use asset_class=futures")
        if symbol.asset_group not in ALLOWED_ASSET_GROUPS:
            allowed = ", ".join(sorted(ALLOWED_ASSET_GROUPS))
            raise ValueError(
                f"universe symbol `{symbol.internal_id}` asset_group must be one of: {allowed}"
            )
        if symbol.status not in {"active", "inactive"}:
            raise ValueError(
                f"universe symbol `{symbol.internal_id}` status must be active|inactive"
            )
        rows.append(symbol)
    return rows


def load_mapping_registry(path: Path) -> list[MappingRecord]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"mapping payload must be object: {path.as_posix()}")
    mappings = payload.get("mappings")
    if not isinstance(mappings, list) or not mappings:
        raise ValueError(
            f"mapping registry must contain non-empty mappings list: {path.as_posix()}"
        )

    rows: list[MappingRecord] = []
    for idx, row in enumerate(mappings):
        if not isinstance(row, dict):
            raise ValueError(f"mappings[{idx}] must be object")
        deactivated_raw = row.get("deactivated_at_utc")
        rows.append(
            MappingRecord(
                internal_id=_require_text(row.get("internal_id"), f"mappings[{idx}].internal_id"),
                finam_symbol=_require_text(
                    row.get("finam_symbol"), f"mappings[{idx}].finam_symbol"
                ),
                moex_engine=_require_text(row.get("moex_engine"), f"mappings[{idx}].moex_engine"),
                moex_market=_require_text(row.get("moex_market"), f"mappings[{idx}].moex_market"),
                moex_board=_require_text(row.get("moex_board"), f"mappings[{idx}].moex_board"),
                moex_secid=_require_text(row.get("moex_secid"), f"mappings[{idx}].moex_secid"),
                asset_class=_require_text(row.get("asset_class"), f"mappings[{idx}].asset_class"),
                asset_group=_require_text(row.get("asset_group"), f"mappings[{idx}].asset_group"),
                mapping_version=_require_int(
                    row.get("mapping_version"), f"mappings[{idx}].mapping_version"
                ),
                is_active=_require_bool(row.get("is_active"), f"mappings[{idx}].is_active"),
                activated_at_utc=_require_text(
                    row.get("activated_at_utc"), f"mappings[{idx}].activated_at_utc"
                ),
                deactivated_at_utc=deactivated_raw.strip()
                if isinstance(deactivated_raw, str) and deactivated_raw.strip()
                else None,
                change_reason=_require_text(
                    row.get("change_reason"), f"mappings[{idx}].change_reason"
                ),
            )
        )
    return rows


def validate_mapping_registry(mappings: list[MappingRecord]) -> None:
    active = [row for row in mappings if row.is_active]
    if not active:
        raise ValueError("mapping registry must contain at least one active row")

    seen_internal: dict[str, str] = {}
    seen_finam: dict[str, str] = {}
    seen_moex: dict[tuple[str, str, str, str], str] = {}

    for row in mappings:
        if row.asset_class != "futures":
            raise ValueError(f"mapping `{row.internal_id}` must use asset_class=futures")
        if row.asset_group not in ALLOWED_ASSET_GROUPS:
            allowed = ", ".join(sorted(ALLOWED_ASSET_GROUPS))
            raise ValueError(f"mapping `{row.internal_id}` asset_group must be one of: {allowed}")
        if row.mapping_version <= 0:
            raise ValueError(f"mapping `{row.internal_id}` must use mapping_version > 0")

        if not row.is_active:
            continue
        internal_key = row.internal_id
        if internal_key in seen_internal:
            raise ValueError(
                f"duplicate active mapping for internal_id `{internal_key}`: "
                f"{seen_internal[internal_key]} and {row.moex_secid}"
            )
        seen_internal[internal_key] = row.moex_secid

        if row.finam_symbol in seen_finam:
            raise ValueError(
                f"duplicate active mapping for finam_symbol `{row.finam_symbol}`: "
                f"{seen_finam[row.finam_symbol]} and {row.moex_secid}"
            )
        seen_finam[row.finam_symbol] = row.moex_secid

        moex_key = (row.moex_engine, row.moex_market, row.moex_board, row.moex_secid)
        if moex_key in seen_moex:
            raise ValueError(f"duplicate active mapping for MOEX key `{moex_key}`")
        seen_moex[moex_key] = row.internal_id


def validate_universe_mapping_alignment(
    universe: list[UniverseSymbol], mappings: list[MappingRecord]
) -> None:
    active_mappings = {row.internal_id: row for row in mappings if row.is_active}
    active_universe = [row for row in universe if row.is_active]
    for symbol in active_universe:
        mapping = active_mappings.get(symbol.internal_id)
        if mapping is None:
            raise ValueError(f"active universe symbol `{symbol.internal_id}` has no active mapping")
        if mapping.finam_symbol != symbol.finam_symbol:
            raise ValueError(f"active mapping mismatch for `{symbol.internal_id}`: finam_symbol")
        if mapping.moex_engine != symbol.moex_engine:
            raise ValueError(f"active mapping mismatch for `{symbol.internal_id}`: moex_engine")
        if mapping.moex_market != symbol.moex_market:
            raise ValueError(f"active mapping mismatch for `{symbol.internal_id}`: moex_market")
        if mapping.moex_board != symbol.moex_board:
            raise ValueError(f"active mapping mismatch for `{symbol.internal_id}`: moex_board")
        if mapping.moex_secid != symbol.moex_secid:
            raise ValueError(f"active mapping mismatch for `{symbol.internal_id}`: moex_secid")


def _select_active_mappings_for_universe(
    universe: list[UniverseSymbol], mappings: list[MappingRecord]
) -> list[MappingRecord]:
    active_universe_ids = {row.internal_id for row in universe if row.is_active}
    return sorted(
        (row for row in mappings if row.is_active and row.internal_id in active_universe_ids),
        key=lambda item: item.internal_id,
    )


def _is_futures_contract_secid(value: str) -> bool:
    if not FUTURES_CONTRACT_SECID_RE.match(value):
        return False
    return value[-2] in FUTURES_MONTH_CODES and value[-1].isdigit()


def _normalize_snapshot_trade_date(*, candidate: date, end_date: date) -> date:
    normalized = candidate
    while normalized.weekday() >= 5:
        normalized += timedelta(days=1)
    if normalized > end_date:
        normalized = candidate
        while normalized.weekday() >= 5:
            normalized -= timedelta(days=1)
    return normalized


def _iter_snapshot_dates(*, start_date: date, end_date: date, step_days: int) -> list[date]:
    if step_days <= 0:
        raise ValueError("contract discovery step_days must be positive integer")
    if end_date < start_date:
        return []
    candidates: list[date] = []
    current = start_date
    while current <= end_date:
        candidates.append(current)
        current += timedelta(days=step_days)
    if not candidates or candidates[-1] != end_date:
        candidates.append(end_date)

    snapshots = {
        _normalize_snapshot_trade_date(candidate=item, end_date=end_date) for item in candidates
    }
    return sorted(snapshots)


def _discover_contract_secids(
    *,
    client: MoexISSClient,
    universe: list[UniverseSymbol],
    active_mappings: list[MappingRecord],
    ingest_till_utc: str,
    contract_discovery_lookback_days: int,
    contract_discovery_step_days: int,
) -> dict[str, list[str]]:
    ingest_till = _parse_iso_utc(ingest_till_utc)
    window_start = ingest_till - timedelta(days=contract_discovery_lookback_days)
    if window_start > ingest_till:
        return {}

    active_symbols = {item.internal_id: item for item in universe if item.is_active}
    mapping_by_internal = {item.internal_id: item for item in active_mappings}

    asset_code_to_internal: dict[str, set[str]] = {}
    for internal_id, symbol in active_symbols.items():
        if not symbol.moex_asset_codes:
            continue
        for asset_code in symbol.moex_asset_codes:
            asset_code_to_internal.setdefault(asset_code, set()).add(internal_id)

    secids_by_internal: dict[str, set[str]] = {
        mapping.internal_id: {mapping.moex_secid} for mapping in active_mappings
    }
    if not asset_code_to_internal:
        return {key: sorted(value) for key, value in secids_by_internal.items()}

    board_keys = {
        (mapping.moex_engine, mapping.moex_market, mapping.moex_board)
        for mapping in active_mappings
    }
    snapshot_dates = _iter_snapshot_dates(
        start_date=window_start.date(),
        end_date=ingest_till.date(),
        step_days=contract_discovery_step_days,
    )
    for engine, market, board in sorted(board_keys):
        for snapshot_date in snapshot_dates:
            history_rows = client.fetch_history_board_securities(
                engine=engine,
                market=market,
                board=board,
                trade_date=snapshot_date,
            )
            for row in history_rows:
                if not row.assetcode:
                    continue
                internal_ids = asset_code_to_internal.get(row.assetcode.upper())
                if not internal_ids:
                    continue
                if row.numtrades <= 0 and row.volume <= 0:
                    continue
                secid = row.secid.strip().upper()
                if not _is_futures_contract_secid(secid):
                    continue
                for internal_id in internal_ids:
                    mapping = mapping_by_internal.get(internal_id)
                    if mapping is None:
                        continue
                    if (
                        mapping.moex_engine == engine
                        and mapping.moex_market == market
                        and mapping.moex_board == board
                    ):
                        secids_by_internal.setdefault(internal_id, set()).add(secid)

    return {internal_id: sorted(secids) for internal_id, secids in secids_by_internal.items()}


def discover_coverage(
    *,
    client: MoexISSClient,
    universe: list[UniverseSymbol],
    mappings: list[MappingRecord],
    timeframes: set[str],
    discovered_at_utc: str,
    ingest_till_utc: str,
    bootstrap_window_days: int,
    expand_contract_chain: bool,
    contract_discovery_step_days: int,
    contract_discovery_lookback_days: int | None = None,
) -> list[DiscoveryRecord]:
    ordered_timeframes = _sorted_target_timeframes(timeframes)
    required_intervals = {SOURCE_INTERVAL_BY_TARGET_TIMEFRAME[item] for item in ordered_timeframes}
    target_timeframes_by_interval: dict[int, list[str]] = {}
    for target_timeframe in ordered_timeframes:
        source_interval = SOURCE_INTERVAL_BY_TARGET_TIMEFRAME[target_timeframe]
        target_timeframes_by_interval.setdefault(source_interval, []).append(target_timeframe)
    records: list[DiscoveryRecord] = []
    active_mappings = _select_active_mappings_for_universe(universe, mappings)
    if not active_mappings:
        raise ValueError(
            "discover_coverage requires at least one active mapping for the selected universe"
        )
    effective_contract_discovery_lookback_days = (
        bootstrap_window_days
        if contract_discovery_lookback_days is None
        else contract_discovery_lookback_days
    )
    if effective_contract_discovery_lookback_days <= 0:
        raise ValueError("contract_discovery_lookback_days must be > 0")
    secids_by_internal: dict[str, list[str]]
    if expand_contract_chain:
        secids_by_internal = _discover_contract_secids(
            client=client,
            universe=universe,
            active_mappings=active_mappings,
            ingest_till_utc=ingest_till_utc,
            contract_discovery_lookback_days=effective_contract_discovery_lookback_days,
            contract_discovery_step_days=contract_discovery_step_days,
        )
    else:
        secids_by_internal = {row.internal_id: [row.moex_secid] for row in active_mappings}
    if expand_contract_chain and effective_contract_discovery_lookback_days >= 365:
        expanded_contracts = sum(max(len(secids) - 1, 0) for secids in secids_by_internal.values())
        if expanded_contracts == 0:
            raise RuntimeError(
                "contract chain expansion returned only seed contracts for long-window backfill; "
                "verify discovery snapshots resolve to trading days and asset_codes "
                "match MOEX ASSETCODE values"
            )

    for mapping in active_mappings:
        secids = secids_by_internal.get(mapping.internal_id) or [mapping.moex_secid]
        for secid in secids:
            finam_symbol = mapping.finam_symbol if secid == mapping.moex_secid else f"{secid}@MOEX"
            try:
                borders = client.fetch_candleborders(
                    engine=mapping.moex_engine,
                    market=mapping.moex_market,
                    board=mapping.moex_board,
                    secid=secid,
                    required_intervals=required_intervals,
                )
            except Exception:  # noqa: BLE001 - unknown/expired contracts are filtered out in chain expansion mode
                if secid == mapping.moex_secid:
                    raise
                continue

            for source_interval in sorted(required_intervals):
                border = borders[source_interval]
                begin_utc = _to_iso_utc(_parse_moex_datetime(border.begin))
                end_utc = _to_iso_utc(_parse_moex_datetime(border.end))
                records.append(
                    DiscoveryRecord(
                        internal_id=mapping.internal_id,
                        finam_symbol=finam_symbol,
                        moex_engine=mapping.moex_engine,
                        moex_market=mapping.moex_market,
                        moex_board=mapping.moex_board,
                        moex_secid=secid,
                        asset_group=mapping.asset_group,
                        requested_target_timeframes=",".join(
                            target_timeframes_by_interval[source_interval]
                        ),
                        source_interval=source_interval,
                        source_timeframe=_source_timeframe_label(source_interval),
                        coverage_begin_utc=begin_utc,
                        coverage_end_utc=end_utc,
                        discovered_at_utc=discovered_at_utc,
                        discovery_url=(
                            f"https://iss.moex.com/iss/engines/{mapping.moex_engine}/markets/{mapping.moex_market}"
                            f"/boards/{mapping.moex_board}/securities/{secid}/candleborders.json"
                        ),
                    )
                )
    return sorted(
        records,
        key=lambda row: (
            row.internal_id,
            row.source_interval,
            row.coverage_begin_utc,
            row.coverage_end_utc,
            row.moex_secid,
        ),
    )


def _compute_watermarks(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], str]:
    watermarks: dict[tuple[str, str, str], str] = {}
    for row in rows:
        internal_id = str(row.get("internal_id", ""))
        timeframe = str(row.get("timeframe", ""))
        moex_secid = str(row.get("moex_secid", ""))
        ts_close = str(row.get("ts_close", ""))
        if not internal_id or not timeframe or not moex_secid or not ts_close:
            continue
        key = (internal_id, timeframe, moex_secid)
        current = watermarks.get(key)
        if current is None or ts_close > current:
            watermarks[key] = ts_close
    return watermarks


def _watermark_key_for_discovery(row: DiscoveryRecord) -> tuple[str, str, int, str]:
    return (row.internal_id, row.source_timeframe, row.source_interval, row.moex_secid)


def _raw_scope_key(row: Mapping[str, Any]) -> tuple[str, str, int, str]:
    return (
        str(row["internal_id"]),
        str(row["timeframe"]),
        int(row["source_interval"]),
        str(row["moex_secid"]),
    )


def _raw_row_key(row: Mapping[str, Any]) -> tuple[str, str, int, str, str, str]:
    return (
        str(row["internal_id"]),
        str(row["timeframe"]),
        int(row["source_interval"]),
        str(row["moex_secid"]),
        _to_iso_utc(row["ts_open"]),
        _to_iso_utc(row["ts_close"]),
    )


def _raw_watermark_filters(
    keys: set[tuple[str, str, int, str]],
    *,
    min_ts_close_utc: str | None = None,
) -> list[tuple[str, str, object]]:
    if not keys:
        return []
    filters: list[tuple[str, str, object]] = [
        ("internal_id", "in", sorted({item[0] for item in keys})),
        ("timeframe", "in", sorted({item[1] for item in keys})),
        ("source_interval", "in", sorted({int(item[2]) for item in keys})),
        ("moex_secid", "in", sorted({item[3] for item in keys})),
    ]
    if min_ts_close_utc:
        filters.append(("ts_close", ">=", min_ts_close_utc))
    return filters


def _delta_log_stats_may_contain(
    row: Mapping[str, object],
    column: str,
    values: set[object],
) -> bool:
    if not values:
        return False
    minimum = row.get(f"min.{column}")
    maximum = row.get(f"max.{column}")
    if minimum is None or maximum is None:
        return True
    return any(minimum <= value <= maximum for value in values)


def _raw_watermark_candidate_files_from_delta_log(
    *,
    table_path: Path,
    keys: set[tuple[str, str, int, str]],
    min_ts_close_utc: str,
    max_ts_close_utc: str | None = None,
) -> list[Path] | None:
    try:
        add_rows = DeltaTable(str(table_path)).get_add_actions(flatten=True).to_pylist()
    except Exception:
        return None

    internal_ids = {item[0] for item in keys}
    timeframes = {item[1] for item in keys}
    source_intervals = {int(item[2]) for item in keys}
    moex_secids = {item[3] for item in keys}
    candidate_files: list[Path] = []
    for row in add_rows:
        max_ts_close = row.get("max.ts_close")
        relative_path = row.get("path")
        if max_ts_close is None or not relative_path:
            return None
        try:
            file_max_ts_close_utc = _to_iso_utc(max_ts_close)
        except (TypeError, ValueError):
            return None
        if not file_max_ts_close_utc or file_max_ts_close_utc < min_ts_close_utc:
            continue
        if max_ts_close_utc is not None:
            min_ts_close = row.get("min.ts_close")
            if min_ts_close is None:
                return None
            try:
                file_min_ts_close_utc = _to_iso_utc(min_ts_close)
            except (TypeError, ValueError):
                return None
            if file_min_ts_close_utc and file_min_ts_close_utc > max_ts_close_utc:
                continue
        if not _delta_log_stats_may_contain(row, "internal_id", set(internal_ids)):
            continue
        if not _delta_log_stats_may_contain(row, "timeframe", set(timeframes)):
            continue
        if not _delta_log_stats_may_contain(row, "source_interval", set(source_intervals)):
            continue
        if not _delta_log_stats_may_contain(row, "moex_secid", set(moex_secids)):
            continue
        candidate_files.append(table_path / str(relative_path))
    return candidate_files


def _compute_raw_watermarks_delta_log_tail(
    *,
    table_path: Path,
    keys: set[tuple[str, str, int, str]],
    min_ts_close_utc: str | None,
) -> dict[tuple[str, str, int, str], str] | None:
    if not min_ts_close_utc:
        return None
    candidate_files = _raw_watermark_candidate_files_from_delta_log(
        table_path=table_path,
        keys=keys,
        min_ts_close_utc=min_ts_close_utc,
    )
    if candidate_files is None:
        return None
    if not candidate_files:
        return {}

    watermarks: dict[tuple[str, str, int, str], str] = {}
    columns = ["internal_id", "timeframe", "source_interval", "moex_secid", "ts_close"]
    try:
        for candidate_file in candidate_files:
            arrow_table = pq.read_table(candidate_file, columns=columns)
            for row in arrow_table.to_pylist():
                try:
                    key = _raw_scope_key(row)
                    ts_close = _to_iso_utc(row["ts_close"])
                except (KeyError, TypeError, ValueError):
                    continue
                if key not in keys or not ts_close or ts_close < min_ts_close_utc:
                    continue
                current = watermarks.get(key)
                if current is None or ts_close > current:
                    watermarks[key] = ts_close
    except Exception:
        return None
    return watermarks


def _load_scoped_raw_rows_delta_log_tail(
    *,
    table_path: Path,
    scopes: list[dict[str, object]],
) -> list[dict[str, object]] | None:
    keys = {_raw_scope_key(scope) for scope in scopes}
    min_ts_close_utc = min(str(scope["window_start_utc"]) for scope in scopes)
    max_ts_close_utc = max(str(scope["window_end_utc"]) for scope in scopes)
    candidate_files = _raw_watermark_candidate_files_from_delta_log(
        table_path=table_path,
        keys=keys,
        min_ts_close_utc=min_ts_close_utc,
        max_ts_close_utc=max_ts_close_utc,
    )
    if candidate_files is None:
        return None
    if not candidate_files:
        return []

    scopes_by_key = _raw_scopes_by_key(scopes)
    scoped_rows: list[dict[str, object]] = []
    try:
        for candidate_file in candidate_files:
            arrow_table = pq.read_table(candidate_file, columns=list(RAW_COLUMNS))
            for row in arrow_table.to_pylist():
                normalized = _normalize_raw_delta_row(row)
                scope = _matching_raw_scope(normalized, scopes_by_key)
                if scope is not None:
                    scoped_rows.append(_raw_row_with_scope(normalized, scope))
    except Exception:
        return None
    return scoped_rows


def _compute_raw_watermarks_delta_rs(
    *,
    table_path: Path,
    keys: set[tuple[str, str, int, str]],
    min_ts_close_utc: str | None = None,
) -> dict[tuple[str, str, int, str], str]:
    if not keys or not has_delta_log(table_path):
        return {}
    tail_watermarks = _compute_raw_watermarks_delta_log_tail(
        table_path=table_path,
        keys=keys,
        min_ts_close_utc=min_ts_close_utc,
    )
    if tail_watermarks is not None:
        return tail_watermarks
    rows = read_filtered_delta_table_rows(
        table_path,
        columns=["internal_id", "timeframe", "source_interval", "moex_secid", "ts_close"],
        filters=_raw_watermark_filters(keys, min_ts_close_utc=min_ts_close_utc),
    )
    watermarks: dict[tuple[str, str, int, str], str] = {}
    for row in rows:
        try:
            key = _raw_scope_key(row)
            ts_close = _to_iso_utc(row["ts_close"])
        except (KeyError, TypeError, ValueError):
            continue
        if key not in keys or not ts_close:
            continue
        current = watermarks.get(key)
        if current is None or ts_close > current:
            watermarks[key] = ts_close
    return watermarks


def _raw_json_source_rows(source_rows_path: Path) -> list[dict[str, Any]]:
    if not source_rows_path.exists():
        raise FileNotFoundError(
            f"raw source rows staging path does not exist: {source_rows_path.as_posix()}"
        )
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        source_rows_path.read_text(encoding="utf-8").splitlines(), 1
    ):
        text = line.strip()
        if not text:
            continue
        row = json.loads(text)
        if not isinstance(row, dict):
            raise ValueError(f"raw source row {line_number} is not a JSON object")
        rows.append(row)
    return rows


def _normalize_raw_delta_row(row: Mapping[str, Any]) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for column, type_name in RAW_COLUMNS.items():
        value = row.get(column)
        normalized_type = type_name.strip().lower()
        if value is None:
            normalized[column] = None
        elif normalized_type == "timestamp":
            normalized[column] = _to_iso_utc(value)
        elif normalized_type in {"int", "integer", "bigint", "long"}:
            normalized[column] = int(value)
        elif normalized_type in {"double", "float"}:
            normalized[column] = float(value)
        elif normalized_type == "json" and not isinstance(value, str):
            normalized[column] = json.dumps(value, ensure_ascii=False, sort_keys=True)
        else:
            normalized[column] = str(value)
    return normalized


def _normalize_raw_window_scope(
    scope: Mapping[str, Any],
    initial_watermarks: Mapping[tuple[str, str, int, str], str],
) -> dict[str, object]:
    key = (
        str(scope["internal_id"]),
        str(scope["timeframe"]),
        int(scope["source_interval"]),
        str(scope["moex_secid"]),
    )
    watermark = scope.get("watermark_utc") or initial_watermarks.get(key)
    return {
        "internal_id": key[0],
        "timeframe": key[1],
        "source_interval": key[2],
        "moex_secid": key[3],
        "window_start_utc": _to_iso_utc(scope["window_start_utc"]),
        "window_end_utc": _to_iso_utc(scope["window_end_utc"]),
        "watermark_utc": _to_iso_utc(watermark) if watermark else "",
    }


def _raw_scopes_by_key(
    scopes: list[dict[str, object]],
) -> dict[tuple[str, str, int, str], list[dict[str, object]]]:
    by_key: dict[tuple[str, str, int, str], list[dict[str, object]]] = {}
    for scope in scopes:
        by_key.setdefault(_raw_scope_key(scope), []).append(scope)
    return by_key


def _matching_raw_scope(
    row: Mapping[str, Any],
    scopes_by_key: dict[tuple[str, str, int, str], list[dict[str, object]]],
) -> dict[str, object] | None:
    scopes = scopes_by_key.get(_raw_scope_key(row), [])
    if not scopes:
        return None
    ts_close = _parse_iso_utc(row["ts_close"])
    for scope in scopes:
        if (
            _parse_iso_utc(scope["window_start_utc"])
            <= ts_close
            <= _parse_iso_utc(scope["window_end_utc"])
        ):
            return scope
    return None


def _raw_row_with_scope(row: Mapping[str, Any], scope: Mapping[str, object]) -> dict[str, object]:
    payload = _normalize_raw_delta_row(row)
    payload["_window_start_utc"] = str(scope["window_start_utc"])
    payload["_window_end_utc"] = str(scope["window_end_utc"])
    return payload


def _raw_row_is_eligible(
    row: Mapping[str, Any],
    scope: Mapping[str, object],
    *,
    refresh_overlap_minutes: int,
) -> bool:
    watermark_raw = str(scope.get("watermark_utc", "") or "")
    if not watermark_raw:
        return True
    ts_close = _parse_iso_utc(row["ts_close"])
    watermark = _parse_iso_utc(watermark_raw)
    if ts_close > watermark:
        return True
    if refresh_overlap_minutes <= 0:
        return False
    return watermark - timedelta(minutes=refresh_overlap_minutes) <= ts_close <= watermark


def _stable_raw_provenance(value: object) -> str:
    if value is None:
        return RAW_NULL_FINGERPRINT_TOKEN
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return RAW_NULL_FINGERPRINT_TOKEN
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return RAW_NULL_FINGERPRINT_TOKEN
    else:
        payload = value
    if not isinstance(payload, dict):
        return RAW_NULL_FINGERPRINT_TOKEN
    stable = {
        str(key): item
        for key, item in payload.items()
        if str(key) not in RAW_VOLATILE_PROVENANCE_KEYS
    }
    return json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _raw_fingerprint_value(row: Mapping[str, Any], column: str) -> str:
    value = row.get(column)
    if value is None:
        return RAW_NULL_FINGERPRINT_TOKEN
    if column in {"ts_open", "ts_close", "ingested_at_utc"}:
        return _to_iso_utc(value)
    return str(value)


def _raw_fingerprint(row: Mapping[str, Any]) -> str:
    payload = {
        column: (
            _stable_raw_provenance(row.get(column))
            if column == "provenance_json"
            else _raw_fingerprint_value(row, column)
        )
        for column in RAW_FINGERPRINT_COLUMNS
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _load_scoped_raw_rows_delta_rs(
    *,
    table_path: Path,
    scopes: list[dict[str, object]],
) -> list[dict[str, object]]:
    if not scopes or not has_delta_log(table_path):
        return []
    scoped_tail_rows = _load_scoped_raw_rows_delta_log_tail(
        table_path=table_path,
        scopes=scopes,
    )
    if scoped_tail_rows is not None:
        return scoped_tail_rows
    keys = {_raw_scope_key(scope) for scope in scopes}
    filters = _raw_watermark_filters(keys)
    filters.extend(
        [
            ("ts_close", ">=", min(str(scope["window_start_utc"]) for scope in scopes)),
            ("ts_close", "<=", max(str(scope["window_end_utc"]) for scope in scopes)),
        ]
    )
    rows = read_filtered_delta_table_rows(table_path, columns=list(RAW_COLUMNS), filters=filters)
    scopes_by_key = _raw_scopes_by_key(scopes)
    scoped_rows: list[dict[str, object]] = []
    for row in rows:
        normalized = _normalize_raw_delta_row(row)
        scope = _matching_raw_scope(normalized, scopes_by_key)
        if scope is not None:
            scoped_rows.append(_raw_row_with_scope(normalized, scope))
    return scoped_rows


def _raw_window_key(row: Mapping[str, Any]) -> tuple[str, str, int, str, str, str]:
    return (
        str(row["internal_id"]),
        str(row["timeframe"]),
        int(row["source_interval"]),
        str(row["moex_secid"]),
        str(row["_window_start_utc"]),
        str(row["_window_end_utc"]),
    )


def _collect_changed_windows_delta_rs(rows: list[Mapping[str, Any]]) -> list[dict[str, object]]:
    counts: dict[tuple[str, str, int, str, str, str], int] = {}
    for row in rows:
        key = _raw_window_key(row)
        counts[key] = counts.get(key, 0) + 1
    return [
        {
            "internal_id": internal_id,
            "source_timeframe": timeframe,
            "source_interval": source_interval,
            "moex_secid": moex_secid,
            "window_start_utc": window_start,
            "window_end_utc": window_end,
            "incremental_rows": incremental_rows,
        }
        for (
            internal_id,
            timeframe,
            source_interval,
            moex_secid,
            window_start,
            window_end,
        ), incremental_rows in sorted(counts.items())
        if incremental_rows > 0
    ]


def _raw_report_watermarks(
    watermarks: Mapping[tuple[str, str, int, str], str],
) -> dict[str, str]:
    return {
        "|".join((internal_id, timeframe, str(source_interval), moex_secid)): watermark
        for (internal_id, timeframe, source_interval, moex_secid), watermark in sorted(
            watermarks.items()
        )
    }


def _strip_raw_internal_columns(row: Mapping[str, Any]) -> dict[str, object]:
    return {column: row[column] for column in RAW_COLUMNS}


def _raw_reconcile_arrow_type(type_name: str) -> pa.DataType:
    normalized = type_name.strip().lower()
    if normalized in {"string", "json"}:
        return pa.string()
    if normalized == "timestamp":
        return pa.timestamp("us", tz="UTC")
    if normalized in {"double", "float"}:
        return pa.float64()
    if normalized in {"int", "integer"}:
        return pa.int32()
    if normalized in {"bigint", "long"}:
        return pa.int64()
    return pa.string()


def _raw_reconcile_arrow_value(value: object, type_name: str) -> object:
    if value is None:
        return None
    normalized = type_name.strip().lower()
    if normalized == "timestamp":
        parsed = value if isinstance(value, datetime) else _parse_iso_utc(str(value))
        return parsed.astimezone(UTC)
    if normalized in {"int", "integer", "bigint", "long"}:
        return int(value)
    if normalized in {"double", "float"}:
        return float(value)
    if normalized == "json" and not isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _raw_reconcile_action_table(rows: list[Mapping[str, object]]) -> pa.Table:
    schema = pa.schema(
        [
            *[
                pa.field(column, _raw_reconcile_arrow_type(type_name))
                for column, type_name in RAW_COLUMNS.items()
            ],
            pa.field("_raw_reconcile_action", pa.string()),
        ]
    )
    normalized_rows: list[dict[str, object]] = []
    for row in rows:
        action = str(row.get("_raw_reconcile_action", "")).strip()
        if action not in {"upsert", "delete"}:
            raise ValueError("raw reconcile action must be `upsert` or `delete`")
        for key_column in RAW_ROW_KEY_COLUMNS:
            value = row.get(key_column)
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"raw reconcile key column `{key_column}` must be non-empty")
        normalized = {
            column: _raw_reconcile_arrow_value(row.get(column), type_name)
            for column, type_name in RAW_COLUMNS.items()
        }
        normalized["_raw_reconcile_action"] = action
        normalized_rows.append(normalized)
    if normalized_rows:
        return pa.Table.from_pylist(normalized_rows, schema=schema)
    return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)


def _merge_raw_reconcile_delta_rs(
    *,
    table_path: Path,
    upsert_rows: list[Mapping[str, object]],
    delete_rows: list[Mapping[str, object]],
) -> None:
    action_rows = [
        {**_strip_raw_internal_columns(row), "_raw_reconcile_action": "upsert"}
        for row in upsert_rows
    ] + [
        {**_strip_raw_internal_columns(row), "_raw_reconcile_action": "delete"}
        for row in delete_rows
    ]
    if not action_rows:
        return

    action_table = _raw_reconcile_action_table(action_rows)
    merge_condition = " AND ".join(
        f"target.{column} = source.{column}" for column in RAW_ROW_KEY_COLUMNS
    )
    upsert_condition = "source._raw_reconcile_action = 'upsert'"
    (
        DeltaTable(str(table_path))
        .merge(
            action_table,
            merge_condition,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_delete(predicate="source._raw_reconcile_action = 'delete'")
        .when_matched_update(
            updates={column: f"source.{column}" for column in RAW_COLUMNS},
            predicate=upsert_condition,
        )
        .when_not_matched_insert(
            updates={column: f"source.{column}" for column in RAW_COLUMNS},
            predicate=upsert_condition,
        )
        .execute()
    )


def run_moex_raw_ingest_delta_rs_job(
    *,
    table_path: Path,
    source_rows: list[Mapping[str, Any]] | None = None,
    source_rows_path: Path | None = None,
    window_scopes: list[Mapping[str, Any]],
    initial_watermarks: Mapping[tuple[str, str, int, str], str],
    run_id: str,
    ingest_till_utc: str,
    refresh_overlap_minutes: int,
    progress_path: Path,
    progress_latest_path: Path,
    error_path: Path,
    error_latest_path: Path,
    spark_master: str | None = None,
    spark_session_factory: Any | None = None,
    emit_progress_event: bool = True,
) -> dict[str, Any]:
    del spark_master, spark_session_factory
    if refresh_overlap_minutes < 0:
        raise ValueError("refresh_overlap_minutes must be >= 0")
    if source_rows_path is not None and source_rows:
        raise ValueError("provide either source_rows_path or source_rows, not both")

    source_payload = (
        _raw_json_source_rows(source_rows_path)
        if source_rows_path is not None
        else [dict(row) for row in (source_rows or [])]
    )
    source_rows_count = len(source_payload)
    scopes = [_normalize_raw_window_scope(scope, initial_watermarks) for scope in window_scopes]
    scopes_by_key = _raw_scopes_by_key(scopes)
    scope_keys = {_raw_scope_key(scope) for scope in scopes}

    table_exists = has_delta_log(table_path)
    if source_rows_count == 0 and not table_exists:
        write_delta_table_rows(table_path=table_path, rows=[], columns=RAW_COLUMNS)
        table_exists = True

    source_by_row_key: dict[tuple[str, str, int, str, str, str], tuple[int, dict[str, object]]] = {}
    for source_order, source_row in enumerate(source_payload, 1):
        normalized = _normalize_raw_delta_row(source_row)
        order = int(source_row.get("_source_order", source_order) or source_order)
        row_key = _raw_row_key(normalized)
        current = source_by_row_key.get(row_key)
        if current is None or order >= current[0]:
            source_by_row_key[row_key] = (order, normalized)
    unique_source = [item[1] for item in source_by_row_key.values()]

    source_with_scope: list[dict[str, object]] = []
    unmatched_source_count = 0
    for row in unique_source:
        scope = _matching_raw_scope(row, scopes_by_key)
        if scope is None:
            unmatched_source_count += 1
            continue
        source_with_scope.append(_raw_row_with_scope(row, scope))
    if unmatched_source_count > 0:
        raise ValueError(
            f"{unmatched_source_count} raw source rows did not match declared window scopes"
        )

    eligible_source = [
        row
        for row in source_with_scope
        if _raw_row_is_eligible(row, row, refresh_overlap_minutes=refresh_overlap_minutes)
    ]
    stale_rows = len(unique_source) - len(eligible_source)
    eligible_source_keys = {_raw_row_key(row) for row in eligible_source}

    existing_scoped = _load_scoped_raw_rows_delta_rs(table_path=table_path, scopes=scopes)
    existing_by_row_key = {_raw_row_key(row): row for row in existing_scoped}

    changed_source_rows = [
        row
        for row in eligible_source
        if _raw_row_key(row) not in existing_by_row_key
        or _raw_fingerprint(row) != _raw_fingerprint(existing_by_row_key[_raw_row_key(row)])
    ]
    target_missing_rows = [
        row for row in existing_scoped if _raw_row_key(row) not in eligible_source_keys
    ]
    changed_events = changed_source_rows + target_missing_rows
    changed_windows = _collect_changed_windows_delta_rs(changed_events)
    reconcile_windows = {_raw_window_key(row) for row in changed_events}
    replacement_source_rows = [
        _strip_raw_internal_columns(row)
        for row in eligible_source
        if _raw_window_key(row) in reconcile_windows
    ]

    incremental_rows = len(changed_source_rows) + len(target_missing_rows)
    deduplicated_rows = max(0, source_rows_count - len(changed_source_rows) - stale_rows)

    if incremental_rows > 0:
        table_path.parent.mkdir(parents=True, exist_ok=True)
        if table_exists:
            _merge_raw_reconcile_delta_rs(
                table_path=table_path,
                upsert_rows=replacement_source_rows,
                delete_rows=target_missing_rows,
            )
        else:
            write_delta_table_rows(
                table_path=table_path,
                rows=replacement_source_rows,
                columns=RAW_COLUMNS,
            )
            table_exists = True

    watermark_keys = scope_keys or set(initial_watermarks)
    post_watermark_min_ts_close_utc = (
        min(str(scope["window_start_utc"]) for scope in scopes) if scopes else None
    )
    post_watermarks = (
        _compute_raw_watermarks_delta_rs(
            table_path=table_path,
            keys=watermark_keys,
            min_ts_close_utc=post_watermark_min_ts_close_utc,
        )
        if table_exists and watermark_keys
        else {}
    )
    if not post_watermarks and initial_watermarks:
        post_watermarks = dict(initial_watermarks)

    report = build_raw_ingest_run_report_v2(
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
        source_rows=source_rows_count,
        incremental_rows=incremental_rows,
        deduplicated_rows=deduplicated_rows,
        stale_rows=stale_rows,
        watermark_by_key=_raw_report_watermarks(post_watermarks),
        raw_table_path=table_path.as_posix(),
        raw_ingest_progress_path=progress_path.as_posix(),
        raw_ingest_error_path=error_path.as_posix(),
        raw_ingest_error_latest_path=error_latest_path.as_posix(),
        changed_windows=changed_windows,
    )
    if emit_progress_event:
        _append_progress_event(
            jsonl_path=progress_path,
            latest_path=progress_latest_path,
            payload={
                "run_id": run_id,
                "runtime_owner": "delta_rs",
                "source_rows": source_rows_count,
                "incremental_rows": incremental_rows,
                "deduplicated_rows": deduplicated_rows,
                "stale_rows": stale_rows,
                "deleted_rows": len(target_missing_rows),
                "changed_windows": len(changed_windows),
                "processed_at_utc": _utc_now_iso(),
            },
        )
    return report


def compute_raw_watermarks_spark_delta(
    *,
    table_path: Path,
    keys: set[tuple[str, str, int, str]],
    min_ts_close_utc: str | None = None,
) -> dict[tuple[str, str, int, str], str]:
    return _compute_raw_watermarks_delta_rs(
        table_path=table_path,
        keys=keys,
        min_ts_close_utc=min_ts_close_utc,
    )


def run_moex_raw_ingest_spark_delta_job(**kwargs: Any) -> dict[str, Any]:
    return run_moex_raw_ingest_delta_rs_job(**kwargs)


def _raw_source_rows_stage_path(*, table_path: Path, run_id: str) -> Path:
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id.strip() or "run")
    return table_path.parent.parent / "raw-source-rows" / f"{safe_run_id}-source-rows.jsonl"


def _json_ready_raw_source_row(row: dict[str, Any], *, source_order: int) -> dict[str, Any]:
    payload = dict(row)
    provenance = payload.get("provenance_json")
    if provenance is not None and not isinstance(provenance, str):
        payload["provenance_json"] = json.dumps(provenance, ensure_ascii=False, sort_keys=True)
    payload["_source_order"] = source_order
    return payload


def _write_raw_source_row(handle: Any, row: dict[str, Any], *, source_order: int) -> None:
    handle.write(
        json.dumps(
            _json_ready_raw_source_row(row, source_order=source_order),
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    handle.write("\n")


def _append_progress_event(*, jsonl_path: Path, latest_path: Path, payload: dict[str, Any]) -> None:
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    latest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def ingest_moex_baseline_window(
    *,
    client: MoexISSClient,
    coverage: list[DiscoveryRecord],
    table_path: Path,
    run_id: str,
    ingest_till_utc: str,
    refresh_window_days: int,
    stability_lag_minutes: int,
    refresh_overlap_minutes: int,
    progress_path: Path | None = None,
    progress_latest_path: Path | None = None,
    error_path: Path | None = None,
    error_latest_path: Path | None = None,
    append_batch_size: int = 2_000,
) -> dict[str, Any]:
    ingest_till = _parse_iso_utc(ingest_till_utc)
    stable_till = ingest_till - timedelta(minutes=stability_lag_minutes)
    if stable_till <= datetime(1970, 1, 1, tzinfo=UTC):
        raise ValueError("stability_lag_minutes produced an invalid stable ingest cutoff")
    if refresh_window_days <= 0:
        raise ValueError("refresh_window_days must be > 0")
    if refresh_overlap_minutes < 0:
        raise ValueError("refresh_overlap_minutes must be >= 0")
    if append_batch_size <= 0:
        raise ValueError("append_batch_size must be > 0")

    source_rows_path = _raw_source_rows_stage_path(table_path=table_path, run_id=run_id)
    source_rows_path.parent.mkdir(parents=True, exist_ok=True)
    source_rows_path.write_text("", encoding="utf-8")
    source_order = 0
    window_scopes: list[dict[str, Any]] = []
    ingest_marks = _utc_now_iso()
    progress_jsonl = progress_path or (table_path.parent.parent / "raw-ingest-progress.jsonl")
    progress_latest = progress_latest_path or (
        table_path.parent.parent / "raw-ingest-progress.latest.json"
    )
    error_jsonl = error_path or (table_path.parent.parent / "raw-ingest-errors.jsonl")
    error_latest = error_latest_path or (table_path.parent.parent / "raw-ingest-error.latest.json")

    watermark_min_ts_close_utc = (
        _to_iso_utc(min(_parse_iso_utc(item.coverage_begin_utc) for item in coverage))
        if coverage
        else None
    )
    watermarks = compute_raw_watermarks_spark_delta(
        table_path=table_path,
        keys={_watermark_key_for_discovery(item) for item in coverage},
        min_ts_close_utc=watermark_min_ts_close_utc,
    )

    for item in coverage:
        coverage_begin = _parse_iso_utc(item.coverage_begin_utc)
        coverage_end = _parse_iso_utc(item.coverage_end_utc)
        window_end = min(coverage_end, stable_till)
        window_start = max(coverage_begin, window_end - timedelta(days=refresh_window_days))
        watermark_key = _watermark_key_for_discovery(item)
        watermark_raw = watermarks.get(watermark_key)
        if watermark_raw:
            watermark_dt = _parse_iso_utc(watermark_raw)
            if _tail_watermark_covers_window(
                watermark_dt=watermark_dt,
                window_end=window_end,
                source_interval=item.source_interval,
            ):
                continue
            if refresh_overlap_minutes > 0:
                overlap_start = watermark_dt - timedelta(minutes=refresh_overlap_minutes)
                window_start = max(window_start, overlap_start)
        if window_start > window_end:
            continue

        item_source_rows = 0
        window_scopes.append(
            {
                "internal_id": item.internal_id,
                "timeframe": item.source_timeframe,
                "source_interval": item.source_interval,
                "moex_secid": item.moex_secid,
                "window_start_utc": _to_iso_utc(window_start),
                "window_end_utc": _to_iso_utc(window_end),
                "watermark_utc": watermark_raw or "",
            }
        )
        try:
            iter_candles = getattr(client, "iter_candles", None)
            if callable(iter_candles):
                candles = iter_candles(
                    engine=item.moex_engine,
                    market=item.moex_market,
                    board=item.moex_board,
                    secid=item.moex_secid,
                    interval=item.source_interval,
                    date_from=window_start.date(),
                    date_till=window_end.date(),
                )
            else:
                candles = client.fetch_candles(
                    engine=item.moex_engine,
                    market=item.moex_market,
                    board=item.moex_board,
                    secid=item.moex_secid,
                    interval=item.source_interval,
                    date_from=window_start.date(),
                    date_till=window_end.date(),
                )

            with source_rows_path.open("a", encoding="utf-8") as source_rows_handle:
                for candle in candles:
                    ts_open_dt = _parse_moex_datetime(candle.begin)
                    ts_close_dt = _parse_moex_datetime(candle.end)
                    if ts_close_dt < window_start or ts_close_dt > window_end:
                        continue
                    item_source_rows += 1
                    source_order += 1
                    _write_raw_source_row(
                        source_rows_handle,
                        {
                            "internal_id": item.internal_id,
                            "finam_symbol": item.finam_symbol,
                            "moex_engine": item.moex_engine,
                            "moex_market": item.moex_market,
                            "moex_board": item.moex_board,
                            "moex_secid": item.moex_secid,
                            "asset_group": item.asset_group,
                            "timeframe": item.source_timeframe,
                            "source_interval": item.source_interval,
                            "ts_open": _to_iso_utc(ts_open_dt),
                            "ts_close": _to_iso_utc(ts_close_dt),
                            "open": candle.open,
                            "high": candle.high,
                            "low": candle.low,
                            "close": candle.close,
                            "volume": candle.volume,
                            "open_interest": None,
                            "ingest_run_id": run_id,
                            "ingested_at_utc": ingest_marks,
                            "provenance_json": {
                                "source_provider": "moex_iss",
                                "source_interval": item.source_interval,
                                "source_timeframe": item.source_timeframe,
                                "requested_target_timeframes": item.requested_target_timeframes,
                                "run_id": run_id,
                                "window_start_utc": _to_iso_utc(window_start),
                                "window_end_utc": _to_iso_utc(window_end),
                                "stability_lag_minutes": stability_lag_minutes,
                                "refresh_overlap_minutes": refresh_overlap_minutes,
                                "discovery_url": item.discovery_url,
                            },
                        },
                        source_order=source_order,
                    )

            _safe_progress_print(
                "[moex-baseline-raw-source] "
                f"{run_id} {item.internal_id} {item.moex_secid} {item.source_timeframe} "
                f"src={item_source_rows} runtime_owner=spark_delta"
            )
        except Exception as exc:
            error_payload = {
                "run_id": run_id,
                "internal_id": item.internal_id,
                "moex_secid": item.moex_secid,
                "source_timeframe": item.source_timeframe,
                "source_interval": item.source_interval,
                "window_start_utc": _to_iso_utc(window_start),
                "window_end_utc": _to_iso_utc(window_end),
                "source_rows_before_error": item_source_rows,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "reported_at_utc": _utc_now_iso(),
            }
            _append_progress_event(
                jsonl_path=error_jsonl, latest_path=error_latest, payload=error_payload
            )
            raise

    if not window_scopes:
        progress_payload = {
            "run_id": run_id,
            "runtime_owner": "delta_rs",
            "source_rows": 0,
            "incremental_rows": 0,
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "deleted_rows": 0,
            "changed_windows": 0,
            "noop_reason": "watermarks_cover_requested_tail",
            "processed_at_utc": _utc_now_iso(),
        }
        _append_progress_event(
            jsonl_path=progress_jsonl,
            latest_path=progress_latest,
            payload=progress_payload,
        )
        return build_raw_ingest_run_report_v2(
            run_id=run_id,
            ingest_till_utc=ingest_till_utc,
            source_rows=0,
            incremental_rows=0,
            deduplicated_rows=0,
            stale_rows=0,
            watermark_by_key=_raw_report_watermarks(watermarks),
            raw_table_path=table_path.as_posix(),
            raw_ingest_progress_path=progress_jsonl.as_posix(),
            raw_ingest_error_path=error_jsonl.as_posix(),
            raw_ingest_error_latest_path=error_latest.as_posix(),
            changed_windows=[],
        )

    return run_moex_raw_ingest_spark_delta_job(
        table_path=table_path,
        source_rows=[],
        source_rows_path=source_rows_path,
        window_scopes=window_scopes,
        initial_watermarks=watermarks,
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
        refresh_overlap_minutes=refresh_overlap_minutes,
        progress_path=progress_jsonl,
        progress_latest_path=progress_latest,
        error_path=error_jsonl,
        error_latest_path=error_latest,
    )


def ingest_moex_bootstrap_window(
    *,
    client: MoexISSClient,
    coverage: list[DiscoveryRecord],
    table_path: Path,
    run_id: str,
    ingest_till_utc: str,
    bootstrap_window_days: int,
    stability_lag_minutes: int,
    refresh_overlap_minutes: int,
    progress_path: Path | None = None,
    progress_latest_path: Path | None = None,
    error_path: Path | None = None,
    error_latest_path: Path | None = None,
    append_batch_size: int = 2_000,
) -> dict[str, Any]:
    ingest_till = _parse_iso_utc(ingest_till_utc)
    stable_till = ingest_till - timedelta(minutes=stability_lag_minutes)
    if stable_till <= datetime(1970, 1, 1, tzinfo=UTC):
        raise ValueError("stability_lag_minutes produced an invalid stable ingest cutoff")
    if append_batch_size <= 0:
        raise ValueError("append_batch_size must be > 0")
    source_rows_path = _raw_source_rows_stage_path(table_path=table_path, run_id=run_id)
    source_rows_path.parent.mkdir(parents=True, exist_ok=True)
    source_rows_path.write_text("", encoding="utf-8")
    source_order = 0
    window_scopes: list[dict[str, Any]] = []
    source_rows_by_scope: dict[tuple[str, str, int, str, str, str], int] = {}
    watermarks = compute_raw_watermarks_spark_delta(
        table_path=table_path,
        keys={_watermark_key_for_discovery(item) for item in coverage},
    )

    ingest_marks = _utc_now_iso()
    progress_jsonl = progress_path or (table_path.parent.parent / "raw-ingest-progress.jsonl")
    progress_latest = progress_latest_path or (
        table_path.parent.parent / "raw-ingest-progress.latest.json"
    )
    error_jsonl = error_path or (table_path.parent.parent / "raw-ingest-errors.jsonl")
    error_latest = error_latest_path or (table_path.parent.parent / "raw-ingest-error.latest.json")

    for item in coverage:
        coverage_begin = _parse_iso_utc(item.coverage_begin_utc)
        coverage_end = _parse_iso_utc(item.coverage_end_utc)
        window_end = min(coverage_end, stable_till)
        window_start = max(coverage_begin, window_end - timedelta(days=bootstrap_window_days))
        watermark_key = _watermark_key_for_discovery(item)
        watermark_raw = watermarks.get(watermark_key)
        if watermark_raw:
            watermark_dt = _parse_iso_utc(watermark_raw)
            if watermark_dt >= window_end and refresh_overlap_minutes <= 0:
                continue
            if refresh_overlap_minutes > 0:
                overlap_start = watermark_dt - timedelta(minutes=refresh_overlap_minutes)
                window_start = max(window_start, overlap_start)
        if window_start > window_end:
            continue

        item_source_rows = 0
        scope_payload = {
            "internal_id": item.internal_id,
            "timeframe": item.source_timeframe,
            "source_interval": item.source_interval,
            "moex_secid": item.moex_secid,
            "window_start_utc": _to_iso_utc(window_start),
            "window_end_utc": _to_iso_utc(window_end),
            "watermark_utc": watermark_raw or "",
        }
        scope_key = (
            item.internal_id,
            item.source_timeframe,
            item.source_interval,
            item.moex_secid,
            str(scope_payload["window_start_utc"]),
            str(scope_payload["window_end_utc"]),
        )
        window_scopes.append(scope_payload)
        source_rows_by_scope.setdefault(scope_key, 0)
        try:
            iter_candles = getattr(client, "iter_candles", None)
            if callable(iter_candles):
                candles = iter_candles(
                    engine=item.moex_engine,
                    market=item.moex_market,
                    board=item.moex_board,
                    secid=item.moex_secid,
                    interval=item.source_interval,
                    date_from=window_start.date(),
                    date_till=window_end.date(),
                )
            else:
                candles = client.fetch_candles(
                    engine=item.moex_engine,
                    market=item.moex_market,
                    board=item.moex_board,
                    secid=item.moex_secid,
                    interval=item.source_interval,
                    date_from=window_start.date(),
                    date_till=window_end.date(),
                )

            with source_rows_path.open("a", encoding="utf-8") as source_rows_handle:
                for candle in candles:
                    ts_open_dt = _parse_moex_datetime(candle.begin)
                    ts_close_dt = _parse_moex_datetime(candle.end)
                    if ts_close_dt < window_start or ts_close_dt > window_end:
                        continue
                    item_source_rows += 1

                    row = {
                        "internal_id": item.internal_id,
                        "finam_symbol": item.finam_symbol,
                        "moex_engine": item.moex_engine,
                        "moex_market": item.moex_market,
                        "moex_board": item.moex_board,
                        "moex_secid": item.moex_secid,
                        "asset_group": item.asset_group,
                        "timeframe": item.source_timeframe,
                        "source_interval": item.source_interval,
                        "ts_open": _to_iso_utc(ts_open_dt),
                        "ts_close": _to_iso_utc(ts_close_dt),
                        "open": candle.open,
                        "high": candle.high,
                        "low": candle.low,
                        "close": candle.close,
                        "volume": candle.volume,
                        "open_interest": None,
                        "ingest_run_id": run_id,
                        "ingested_at_utc": ingest_marks,
                        "provenance_json": {
                            "source_provider": "moex_iss",
                            "source_interval": item.source_interval,
                            "source_timeframe": item.source_timeframe,
                            "requested_target_timeframes": item.requested_target_timeframes,
                            "run_id": run_id,
                            "window_start_utc": _to_iso_utc(window_start),
                            "window_end_utc": _to_iso_utc(window_end),
                            "stability_lag_minutes": stability_lag_minutes,
                            "refresh_overlap_minutes": refresh_overlap_minutes,
                            "discovery_url": item.discovery_url,
                        },
                    }
                    source_order += 1
                    _write_raw_source_row(
                        source_rows_handle,
                        row,
                        source_order=source_order,
                    )
            source_rows_by_scope[scope_key] = item_source_rows
            _safe_progress_print(
                "[moex-raw-ingest-source] "
                f"{run_id} {item.internal_id} {item.moex_secid} {item.source_timeframe} "
                f"src={item_source_rows} runtime_owner=spark_delta"
            )
        except Exception as exc:
            error_payload = {
                "run_id": run_id,
                "internal_id": item.internal_id,
                "moex_secid": item.moex_secid,
                "source_timeframe": item.source_timeframe,
                "source_interval": item.source_interval,
                "window_start_utc": _to_iso_utc(window_start),
                "window_end_utc": _to_iso_utc(window_end),
                "source_rows_before_error": item_source_rows,
                "runtime_owner": "spark_delta",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "reported_at_utc": _utc_now_iso(),
            }
            _append_progress_event(
                jsonl_path=error_jsonl, latest_path=error_latest, payload=error_payload
            )
            raise

    report = run_moex_raw_ingest_spark_delta_job(
        table_path=table_path,
        source_rows=[],
        source_rows_path=source_rows_path,
        window_scopes=window_scopes,
        initial_watermarks=watermarks,
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
        refresh_overlap_minutes=refresh_overlap_minutes,
        progress_path=progress_jsonl,
        progress_latest_path=progress_latest,
        error_path=error_jsonl,
        error_latest_path=error_latest,
        emit_progress_event=not window_scopes,
    )
    incremental_by_scope = {
        (
            str(item.get("internal_id", "")),
            str(item.get("source_timeframe", "")),
            int(item.get("source_interval", 0)),
            str(item.get("moex_secid", "")),
            str(item.get("window_start_utc", "")),
            str(item.get("window_end_utc", "")),
        ): int(item.get("incremental_rows", 0))
        for item in list(report.get("changed_windows", []))
        if isinstance(item, dict)
    }
    for scope in window_scopes:
        scope_key = (
            str(scope["internal_id"]),
            str(scope["timeframe"]),
            int(scope["source_interval"]),
            str(scope["moex_secid"]),
            str(scope["window_start_utc"]),
            str(scope["window_end_utc"]),
        )
        progress_payload = {
            "run_id": run_id,
            "internal_id": scope_key[0],
            "moex_secid": scope_key[3],
            "source_timeframe": scope_key[1],
            "source_interval": scope_key[2],
            "window_start_utc": scope_key[4],
            "window_end_utc": scope_key[5],
            "source_rows": source_rows_by_scope.get(scope_key, 0),
            "incremental_rows": incremental_by_scope.get(scope_key, 0),
            "deduplicated_rows": 0,
            "stale_rows": 0,
            "runtime_owner": "spark_delta",
            "hot_table_counts_owner": "spark_delta",
            "processed_at_utc": _utc_now_iso(),
        }
        _append_progress_event(
            jsonl_path=progress_jsonl, latest_path=progress_latest, payload=progress_payload
        )
    return report


def _write_coverage_artifacts(
    coverage: list[DiscoveryRecord], *, output_dir: Path
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "coverage-report.json"
    csv_path = output_dir / "coverage-report.csv"

    payload = [item.to_dict() for item in coverage]
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if payload:
        columns = list(payload[0].keys())
    else:
        columns = [
            "internal_id",
            "finam_symbol",
            "moex_engine",
            "moex_market",
            "moex_board",
            "moex_secid",
            "asset_group",
            "requested_target_timeframes",
            "source_interval",
            "source_timeframe",
            "coverage_begin_utc",
            "coverage_end_utc",
            "discovered_at_utc",
            "discovery_url",
        ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in payload:
            writer.writerow(row)
    return json_path, csv_path


def run_moex_foundation(
    *,
    mapping_registry_path: Path,
    universe_path: Path,
    output_dir: Path,
    run_id: str,
    timeframes: set[str],
    bootstrap_window_days: int,
    ingest_till_utc: str,
    stability_lag_minutes: int = 20,
    expand_contract_chain: bool = False,
    contract_discovery_step_days: int = DEFAULT_CONTRACT_DISCOVERY_STEP_DAYS,
    contract_discovery_lookback_days: int | None = None,
    refresh_overlap_minutes: int = DEFAULT_REFRESH_OVERLAP_MINUTES,
    client: MoexISSClient | None = None,
) -> FoundationRunReport:
    if not timeframes:
        raise ValueError("timeframes must be non-empty")
    unsupported = sorted(set(timeframes) - set(SOURCE_INTERVAL_BY_TARGET_TIMEFRAME))
    if unsupported:
        unsupported_text = ", ".join(unsupported)
        raise ValueError(f"unsupported timeframe(s): {unsupported_text}")
    if bootstrap_window_days <= 0:
        raise ValueError("bootstrap_window_days must be > 0")
    if stability_lag_minutes < 0:
        raise ValueError("stability_lag_minutes must be >= 0")
    if contract_discovery_step_days <= 0:
        raise ValueError("contract_discovery_step_days must be > 0")
    if contract_discovery_lookback_days is not None and contract_discovery_lookback_days <= 0:
        raise ValueError("contract_discovery_lookback_days must be > 0")
    if refresh_overlap_minutes < 0:
        raise ValueError("refresh_overlap_minutes must be >= 0")

    universe = load_universe(universe_path)
    mappings = load_mapping_registry(mapping_registry_path)
    validate_mapping_registry(mappings)
    validate_universe_mapping_alignment(universe, mappings)

    discovered_at_utc = _utc_now_iso()
    request_log_path = output_dir / "moex-request-log.jsonl"
    request_latest_path = output_dir / "moex-request.latest.json"
    if client is None:
        moex_client = MoexISSClient(
            request_event_hook=lambda payload: _append_progress_event(
                jsonl_path=request_log_path,
                latest_path=request_latest_path,
                payload=payload,
            )
        )
    else:
        moex_client = client
    effective_contract_discovery_lookback_days = (
        bootstrap_window_days
        if contract_discovery_lookback_days is None
        else contract_discovery_lookback_days
    )
    coverage = discover_coverage(
        client=moex_client,
        universe=universe,
        mappings=mappings,
        timeframes=timeframes,
        discovered_at_utc=discovered_at_utc,
        ingest_till_utc=ingest_till_utc,
        bootstrap_window_days=bootstrap_window_days,
        expand_contract_chain=expand_contract_chain,
        contract_discovery_step_days=contract_discovery_step_days,
        contract_discovery_lookback_days=effective_contract_discovery_lookback_days,
    )
    coverage_json, coverage_csv = _write_coverage_artifacts(coverage, output_dir=output_dir)

    raw_table_path = output_dir / "delta" / "raw_moex_history.delta"
    ingest_report = ingest_moex_bootstrap_window(
        client=moex_client,
        coverage=coverage,
        table_path=raw_table_path,
        run_id=run_id,
        ingest_till_utc=ingest_till_utc,
        bootstrap_window_days=bootstrap_window_days,
        stability_lag_minutes=stability_lag_minutes,
        refresh_overlap_minutes=refresh_overlap_minutes,
        progress_path=output_dir / "raw-ingest-progress.jsonl",
        progress_latest_path=output_dir / "raw-ingest-progress.latest.json",
        error_path=output_dir / "raw-ingest-errors.jsonl",
        error_latest_path=output_dir / "raw-ingest-error.latest.json",
    )
    raw_ingest_report_path = output_dir / "raw-ingest-report.json"
    raw_ingest_report_path.write_text(
        json.dumps(ingest_report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    return FoundationRunReport(
        run_id=run_id,
        route_signal="worker:capability-only",
        timeframe_set=sorted(timeframes),
        source_interval_set=sorted({row.source_interval for row in coverage}),
        source_timeframe_set=sorted({row.source_timeframe for row in coverage}),
        expand_contract_chain=expand_contract_chain,
        contract_discovery_step_days=contract_discovery_step_days,
        contract_discovery_lookback_days=effective_contract_discovery_lookback_days,
        refresh_overlap_minutes=refresh_overlap_minutes,
        universe_size=len([row for row in universe if row.is_active]),
        coverage_rows=len(coverage),
        coverage_report_path=coverage_json.as_posix(),
        coverage_table_path=coverage_csv.as_posix(),
        moex_request_log_path=request_log_path.as_posix(),
        moex_request_latest_path=request_latest_path.as_posix(),
        raw_ingest_report_path=raw_ingest_report_path.as_posix(),
        raw_ingest_progress_path=str(ingest_report["raw_ingest_progress_path"]),
        raw_ingest_error_path=str(ingest_report["raw_ingest_error_path"]),
        raw_ingest_error_latest_path=str(ingest_report["raw_ingest_error_latest_path"]),
        raw_table_path=raw_table_path.as_posix(),
        source_rows=int(ingest_report["source_rows"]),
        incremental_rows=int(ingest_report["incremental_rows"]),
        deduplicated_rows=int(ingest_report["deduplicated_rows"]),
        stale_rows=int(ingest_report["stale_rows"]),
        watermark_by_key=dict(ingest_report["watermark_by_key"]),
        real_bindings=[
            "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/<SECID>/candleborders.json",
            "https://iss.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/<SECID>/candles.json",
        ],
    )
