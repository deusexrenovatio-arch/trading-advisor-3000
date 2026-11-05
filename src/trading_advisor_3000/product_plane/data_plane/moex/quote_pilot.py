"""Bounded source acquisition for the explicitly invoked multi-asset pilot."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from trading_advisor_3000.product_plane.contracts.market_quotes import validate_quote_series
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexISSClient

REPO_ROOT = Path(__file__).resolve().parents[5]
PILOT_ROOT = REPO_ROOT / ".tmp" / "moex-multi-asset-pilot"
EVIDENCE_FILES = ("acquisition.json", "series.jsonl", "candles.jsonl", "discovery.json")


def validate_pilot_path(path: Path) -> Path:
    """Confine every pilot write/read to this candidate's disposable namespace."""
    if PILOT_ROOT.resolve() != PILOT_ROOT:
        raise ValueError("pilot namespace must not be a symlink or junction")
    candidate = path if path.is_absolute() else REPO_ROOT / path
    resolved = candidate.resolve()
    if not resolved.is_relative_to(PILOT_ROOT):
        raise ValueError("pilot path must stay inside candidate .tmp/moex-multi-asset-pilot")
    return resolved


def validate_run_id(run_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run_id):
        raise ValueError("run_id must be a simple nonempty identifier, at most 64 characters")
    return run_id


def _closed_scope(date_from: date, date_till: date) -> None:
    today_moscow = datetime.now(ZoneInfo("Europe/Moscow")).date()
    if date_from > date_till or (date_till - date_from).days > 90:
        raise ValueError("pilot requires an ordered date range of at most 91 days")
    if date_till >= today_moscow:
        raise ValueError("pilot only accepts completed source calendar days")


def evidence_hashes(acquisition_dir: Path) -> dict[str, str]:
    root = validate_pilot_path(acquisition_dir)
    return {
        name: hashlib.sha256(validate_pilot_path(root / name).read_bytes()).hexdigest()
        for name in EVIDENCE_FILES
    }


def load_acquisition(acquisition_dir: Path) -> dict[str, Any]:
    root = validate_pilot_path(acquisition_dir)
    metadata = json.loads(validate_pilot_path(root / "acquisition.json").read_text("utf-8"))
    validate_run_id(metadata["run_id"])
    if metadata.get("schema_version") != "moex_quote_pilot_acquisition.v1":
        raise ValueError("unsupported acquisition version")
    _closed_scope(
        date.fromisoformat(metadata["date_from"]),
        date.fromisoformat(metadata["date_till"]),
    )
    hashes = evidence_hashes(root)
    for name, expected in metadata["evidence_hashes"].items():
        if name not in EVIDENCE_FILES or hashes[name] != expected:
            raise ValueError(f"source evidence hash mismatch: {name}")
    if set(metadata["evidence_hashes"]) != set(EVIDENCE_FILES) - {"acquisition.json"}:
        raise ValueError("acquisition must pin every source evidence file")
    return metadata


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


def _descriptor(
    *,
    engine: str,
    market: str,
    board: str,
    secid: str,
    asset_class: str,
    metadata: dict[str, Any],
    metadata_asof: str,
    aliases: list[str] | None = None,
) -> dict[str, Any]:
    if (
        not isinstance(secid, str)
        or not re.fullmatch(r"[A-Z0-9_.-]{1,36}", secid)
        or metadata.get("SECID") != secid
        or metadata.get("BOARDID", board) != board
    ):
        raise ValueError("source metadata does not match the selected security/board")
    security_id = f"MOEX:{engine}:{market}:{secid}"
    is_index = asset_class == "index"
    record = {
        "schema_version": "market_quote_series.v1",
        "series_id": f"{security_id}:{board}:source_native",
        "security_id": security_id,
        "source": "moex_iss",
        "asset_class": asset_class,
        "engine": engine,
        "market": market,
        "board": board,
        "secid": secid,
        "underlying_security_id": "MOEX:stock:shares:SBER" if asset_class == "futures" else None,
        "source_timezone": "Europe/Moscow",
        "price_basis": "source_native",
        "price_unit": "index_points" if is_index else "source_native",
        "volume_unit": "not_applicable" if is_index else "source_native",
        "turnover_unit": "not_applicable" if is_index else "source_native",
        "tradable": not is_index,
        "metadata_asof": metadata_asof,
        "metadata_json": _json(metadata),
        "aliases_json": _json(aliases or []),
    }
    validate_quote_series(record)
    return record


def acquire_quote_pilot(
    *,
    acquisition_dir: Path,
    date_from: date,
    date_till: date,
    run_id: str,
    client: MoexISSClient | None = None,
) -> dict[str, Any]:
    """Fetch source-native daily data; no production universe or writer changes."""
    root = validate_pilot_path(acquisition_dir)
    validate_run_id(run_id)
    _closed_scope(date_from, date_till)
    if root.exists():
        raise ValueError("acquisition evidence is immutable; choose a new acquisition directory")
    source = client or MoexISSClient()
    descriptors = []
    for secid, market, board, kind, aliases in (
        ("SBER", "shares", "TQBR", "equity", []),
        ("IMOEX", "index", "SNDX", "index", ["TradingView:RUS:IRUS"]),
        ("IMOEX2", "index", "SNDX", "index", ["TradingView:RUS:IRUS2"]),
    ):
        metadata = source.fetch_security_snapshot(
            engine="stock",
            market=market,
            board=board,
            secid=secid,
        )
        if metadata.get("SECID") != secid:
            raise ValueError(f"metadata SECID mismatch: {secid}")
        descriptors.append(
            _descriptor(
                engine="stock",
                market=market,
                board=board,
                secid=secid,
                asset_class=kind,
                metadata=metadata,
                metadata_asof=_now(),
                aliases=aliases,
            )
        )

    # Explicit pilot family mapping, verified against official ASSETCODE metadata.
    # Sampling discovers observed expired contracts; it cannot prove chain completeness.
    contracts: dict[str, dict[str, Any]] = {}
    snapshots = sorted(
        {date_from + timedelta(days=n) for n in range(0, (date_till - date_from).days + 1, 14)}
        | {date_till}
    )
    history_evidence = []
    for snapshot_date in snapshots:
        records = source.fetch_history_board_securities(
            engine="futures",
            market="forts",
            board="RFUD",
            trade_date=snapshot_date,
        )
        matched = [row for row in records if row.assetcode == "SBRF"]
        history_evidence.append(
            {
                "date": snapshot_date.isoformat(),
                "rows": [row.payload for row in matched],
                "fetched_at": _now(),
            }
        )
        for row in matched:
            if row.numtrades > 0 or row.volume > 0:
                contracts[row.secid] = row.payload
    current_contracts = [
        row
        for row in source.fetch_futures_contract_securities()
        if row.get("ASSETCODE") == "SBRF" and row.get("BOARDID") == "RFUD"
    ]
    if not current_contracts:
        raise ValueError("official futures metadata did not confirm pilot ASSETCODE=SBRF")
    for row in current_contracts:
        secid = row.get("SECID")
        if not isinstance(secid, str) or not secid:
            raise ValueError("source futures metadata requires a nonempty SECID")
        contracts[secid] = row
    for secid, metadata in sorted(contracts.items()):
        descriptors.append(
            _descriptor(
                engine="futures",
                market="forts",
                board="RFUD",
                secid=secid,
                asset_class="futures",
                metadata=metadata,
                metadata_asof=_now(),
            )
        )

    raw_records = []
    for series in descriptors:
        fetched_at = _now()
        rows = source.iter_candle_source_rows(
            engine=series["engine"],
            market=series["market"],
            board=series["board"],
            secid=series["secid"],
            interval=24,
            date_from=date_from,
            date_till=date_till,
        )
        for row in rows:
            raw_records.append(
                {
                    "series_id": series["series_id"],
                    "timeframe": "1d",
                    "source_run_id": run_id,
                    "fetched_at": fetched_at,
                    "payload_json": _json(row),
                }
            )
    if not raw_records:
        raise ValueError("MOEX returned no source candles")
    nonempty = {row["series_id"] for row in raw_records}
    if any(row["series_id"] not in nonempty for row in descriptors[:3]):
        raise ValueError("pilot requires candles for SBER, IMOEX and IMOEX2")
    if not any(
        row["asset_class"] == "futures" and row["series_id"] in nonempty for row in descriptors
    ):
        raise ValueError("pilot requires a source-discovered futures series with candles")
    discovery = {
        "family_assetcode": "SBRF",
        "underlying_security_id": "MOEX:stock:shares:SBER",
        "policy": "sampled_history_plus_current_metadata",
        "history_snapshots": history_evidence,
        "current_contract_metadata": current_contracts,
        "observed_contracts": sorted(contracts),
        "complete_chain_proven": False,
        "tradingview_alias_equivalence_proven": False,
    }
    root.mkdir(parents=True, exist_ok=False)
    for name, records in (("series.jsonl", descriptors), ("candles.jsonl", raw_records)):
        with (root / name).open("x", encoding="utf-8", newline="\n") as handle:
            for record in records:
                handle.write(_json(record) + "\n")
    (root / "discovery.json").write_text(_json(discovery) + "\n", "utf-8")
    hashes = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in EVIDENCE_FILES
        if name != "acquisition.json"
    }
    metadata = {
        "schema_version": "moex_quote_pilot_acquisition.v1",
        "run_id": run_id,
        "date_from": date_from.isoformat(),
        "date_till": date_till.isoformat(),
        "fetched_at": _now(),
        "source": "moex_iss",
        "timeframe": "1d",
        "series_count": len(descriptors),
        "source_row_count": len(raw_records),
        "evidence_hashes": hashes,
    }
    (root / "acquisition.json").write_text(_json(metadata) + "\n", "utf-8")
    return load_acquisition(root)
