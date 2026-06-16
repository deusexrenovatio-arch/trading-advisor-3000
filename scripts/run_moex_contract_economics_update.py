from __future__ import annotations

# ruff: noqa: E402
import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    has_delta_log,
    replace_delta_table_rows,
    write_delta_table_rows,
)
from trading_advisor_3000.product_plane.data_plane.moex.economics import (
    moex_economics_store_contract,
)
from trading_advisor_3000.product_plane.data_plane.moex.iss_client import MoexISSClient
from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    resolve_external_root,
)
from trading_advisor_3000.spark_jobs.moex_contract_economics_job import (
    run_moex_contract_economics_spark_job,
)

DEFAULT_MOEX_TIMEOUT_SECONDS = 20.0
DEFAULT_MOEX_MAX_RETRIES = 2
DEFAULT_MOEX_RETRY_BACKOFF_SECONDS = 0.8
DEFAULT_MOEX_RETRY_JITTER_RATIO = 0.0


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, object] | list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _append_request_event(
    *, jsonl_path: Path, latest_path: Path, payload: dict[str, object]
) -> None:
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    event = dict(payload)
    event.setdefault("emitted_at_utc", _utc_now_iso())
    with jsonl_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    _write_json(latest_path, event)


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ-money-math")


def _hash_payload(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _raw_payload_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _delta_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _trade_date_replace_predicate(
    rows: list[dict[str, object]], *, fallback_trade_date: date
) -> str:
    trade_dates = sorted(
        {
            str(row.get("trade_date") or fallback_trade_date.isoformat()).strip()[:10]
            for row in rows
            if str(row.get("trade_date") or fallback_trade_date.isoformat()).strip()
        }
    )
    if not trade_dates:
        trade_dates = [fallback_trade_date.isoformat()]
    if len(trade_dates) == 1:
        return f"trade_date = {_delta_string_literal(trade_dates[0])}"
    joined = ", ".join(_delta_string_literal(item) for item in trade_dates)
    return f"trade_date IN ({joined})"


def _payload_value(payload: dict[str, object], *keys: str) -> object:
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    return None


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise SystemExit(f"{path.as_posix()}:{line_no} must be a JSON object")
        rows.append({str(key): value for key, value in payload.items()})
    return rows


def _date_window(*, trade_date: str, date_from: str = "", date_till: str = "") -> tuple[date, ...]:
    start = date.fromisoformat(date_from or trade_date)
    end = date.fromisoformat(date_till or date_from or trade_date)
    if end < start:
        raise SystemExit("--date-till must be greater than or equal to --date-from/--trade-date")
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return tuple(days)


def _payload_trade_date(payload: dict[str, object]) -> str:
    for key in ("trade_date", "tradedate", "TRADEDATE", "date", "DATE"):
        value = payload.get(key)
        if value is not None and value != "":
            return str(value)[:10]
    return ""


def _payloads_for_trade_date(
    payload_rows: Sequence[dict[str, object]],
    *,
    trade_date: date,
    source_label: str,
    allow_undated_single_day: bool,
) -> list[dict[str, object]]:
    wanted = trade_date.isoformat()
    matched = [payload for payload in payload_rows if _payload_trade_date(payload) == wanted]
    if matched:
        return matched
    if allow_undated_single_day:
        undated = [payload for payload in payload_rows if not _payload_trade_date(payload)]
        if undated:
            return undated
    raise SystemExit(
        f"{source_label} JSONL has no rows for {wanted}; historical ranges require "
        "date-stamped payload rows"
    )


def _table_columns(table_name: str) -> dict[str, str]:
    return dict(moex_economics_store_contract()[table_name]["columns"])


def _moex_client_kwargs(args: object) -> dict[str, object]:
    timeout_seconds = float(getattr(args, "moex_timeout_seconds"))
    max_retries = int(getattr(args, "moex_max_retries"))
    retry_backoff_seconds = float(getattr(args, "moex_retry_backoff_seconds"))
    retry_jitter_ratio = float(getattr(args, "moex_retry_jitter_ratio"))
    if timeout_seconds <= 0:
        raise SystemExit("--moex-timeout-seconds must be > 0")
    if max_retries < 0:
        raise SystemExit("--moex-max-retries must be >= 0")
    if retry_backoff_seconds < 0:
        raise SystemExit("--moex-retry-backoff-seconds must be >= 0")
    if retry_jitter_ratio < 0:
        raise SystemExit("--moex-retry-jitter-ratio must be >= 0")
    return {
        "timeout_seconds": timeout_seconds,
        "max_retries": max_retries,
        "retry_backoff_seconds": retry_backoff_seconds,
        "retry_jitter_ratio": retry_jitter_ratio,
    }


def _raw_contract_rows(
    *,
    payload_rows: list[dict[str, object]],
    trade_date: date,
    fetched_at_utc: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    source_url = "https://iss.moex.com/iss/engines/futures/markets/forts/securities.json"
    for payload in payload_rows:
        secid = str(payload.get("SECID") or payload.get("secid") or "").strip()
        if not secid:
            continue
        rows.append(
            {
                "source_id": "moex_iss_forts_securities",
                "source_url": source_url,
                "source_document_id": f"{secid}-{trade_date.isoformat()}",
                "source_document_hash": _hash_payload(payload),
                "fetched_at_utc": fetched_at_utc,
                "engine": "futures",
                "market": "forts",
                "board": str(payload.get("BOARDID") or "RFUD"),
                "moex_secid": secid,
                "trade_date": trade_date.isoformat(),
                "assetcode": str(payload.get("ASSETCODE") or payload.get("assetcode") or ""),
                "contract_shortname": str(payload.get("SHORTNAME") or ""),
                "last_trade_date": payload.get("MATDATE") or payload.get("LASTTRADEDATE"),
                "last_del_date": payload.get("MATDATE") or payload.get("LASTDELDATE"),
                "min_step": payload.get("MINSTEP"),
                "lot_volume": payload.get("LOTVOLUME"),
                "official_step_price": payload.get("STEPPRICE"),
                "official_initial_margin": payload.get("INITIALMARGIN"),
                "last_settle_price": payload.get("LASTSETTLEPRICE"),
                "raw_payload_json": _raw_payload_json(payload),
            }
        )
    return rows


def _raw_fx_rows(
    *,
    payload_rows: list[dict[str, object]],
    trade_date: date,
    fetched_at_utc: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    source_url = (
        "https://iss.moex.com/iss/statistics/engines/futures/markets/"
        "indicativerates/securities.json"
    )
    for payload in payload_rows:
        fx_pair = str(payload.get("secid") or payload.get("SECID") or "").strip()
        if not fx_pair:
            continue
        row_trade_date = _payload_trade_date(payload) or trade_date.isoformat()
        clearing_type = str(payload.get("clearing") or payload.get("clearing_type") or "mc").lower()
        rows.append(
            {
                "source_id": "moex_iss_indicative_rates",
                "source_url": source_url,
                "source_document_id": f"{fx_pair}-{row_trade_date}-{clearing_type}",
                "source_document_hash": _hash_payload(payload),
                "fetched_at_utc": fetched_at_utc,
                "trade_date": row_trade_date,
                "trade_time": str(payload.get("tradetime") or ""),
                "fx_pair": fx_pair.upper(),
                "clearing_type": clearing_type,
                "rate": payload.get("rate") or payload.get("RATE"),
                "raw_payload_json": _raw_payload_json(payload),
            }
        )
    return rows


def _risk_rows_from_jsonl(
    *,
    rows: list[dict[str, object]],
    table_name: str,
    trade_date: date,
    fetched_at_utc: str,
    source_id: str,
) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    columns = _table_columns(table_name)
    for payload in rows:
        assetcode = str(payload.get("assetcode") or payload.get("ASSETCODE") or "").strip()
        if not assetcode:
            continue
        row: dict[str, object] = {
            "source_id": payload.get("source_id") or source_id,
            "source_url": payload.get("source_url")
            or "https://www.nationalclearingcentre.com/rates/derivativesStaticParams",
            "source_document_id": payload.get("source_document_id")
            or f"{assetcode}-{trade_date.isoformat()}-{table_name}",
            "source_document_hash": payload.get("source_document_hash") or _hash_payload(payload),
            "fetched_at_utc": payload.get("fetched_at_utc") or fetched_at_utc,
            "trade_date": str(payload.get("trade_date") or trade_date.isoformat())[:10],
            "assetcode": assetcode.upper(),
            "raw_payload_json": _raw_payload_json(payload),
        }
        for column_name in columns:
            if column_name in row:
                continue
            if column_name in payload:
                row[column_name] = payload[column_name]
            elif column_name.upper() in payload:
                row[column_name] = payload[column_name.upper()]
            elif column_name == "radius_pct":
                row[column_name] = _payload_value(payload, "radius_pct", "radius", "RADIUS")
        normalized.append(row)
    return normalized


def _raw_rms_limits_rows(
    *,
    payload_rows: list[dict[str, object]],
    trade_date: date,
    fetched_at_utc: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    source_url = "https://iss.moex.com/iss/rms/engines/futures/objects/limits.json"
    for payload in payload_rows:
        assetcode = str(payload.get("assetcode") or payload.get("ASSETCODE") or "").strip()
        if not assetcode:
            continue
        row_trade_date = _payload_trade_date(payload) or trade_date.isoformat()
        rows.append(
            {
                "source_id": "moex_iss_rms_limits",
                "source_url": source_url,
                "source_document_id": f"{assetcode}-{row_trade_date}-limits",
                "source_document_hash": _hash_payload(payload),
                "fetched_at_utc": fetched_at_utc,
                "trade_date": row_trade_date,
                "assetcode": assetcode.upper(),
                "mr1": _payload_value(payload, "mr1", "MR1"),
                "mr2": _payload_value(payload, "mr2", "MR2"),
                "mr3": _payload_value(payload, "mr3", "MR3"),
                "lk1": _payload_value(payload, "lk1", "LK1"),
                "lk2": _payload_value(payload, "lk2", "LK2"),
                "title": str(payload.get("title") or payload.get("TITLE") or ""),
                "group_title": str(payload.get("group_title") or payload.get("GROUP_TITLE") or ""),
                "update_time": payload.get("updatetime") or payload.get("update_time"),
                "raw_payload_json": _raw_payload_json(payload),
            }
        )
    return rows


def _raw_rms_staticparams_rows(
    *,
    payload_rows: list[dict[str, object]],
    trade_date: date,
    fetched_at_utc: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    source_url = "https://iss.moex.com/iss/rms/engines/futures/objects/staticparams.json"
    for payload in payload_rows:
        assetcode = str(payload.get("assetcode") or payload.get("ASSETCODE") or "").strip()
        if not assetcode:
            continue
        row_trade_date = _payload_trade_date(payload) or trade_date.isoformat()
        rows.append(
            {
                "source_id": "moex_iss_rms_staticparams",
                "source_url": source_url,
                "source_document_id": f"{assetcode}-{row_trade_date}-staticparams",
                "source_document_hash": _hash_payload(payload),
                "fetched_at_utc": fetched_at_utc,
                "trade_date": row_trade_date,
                "assetcode": assetcode.upper(),
                "radius_pct": _payload_value(payload, "radius_pct", "radius", "RADIUS"),
                "update_time": payload.get("updatetime") or payload.get("update_time"),
                "raw_payload_json": _raw_payload_json(payload),
            }
        )
    return rows


def _write_raw_table_for_mode(
    *,
    table_path: Path,
    table_name: str,
    rows: list[dict[str, object]],
    trade_date: date,
    mode: str,
    allow_bootstrap_overwrite: bool,
) -> str:
    columns = _table_columns(table_name)
    if mode == "bootstrap":
        if has_delta_log(table_path) and not allow_bootstrap_overwrite:
            raise SystemExit(
                f"{table_name} already exists at {table_path.as_posix()}; "
                "use --mode update for a scoped regular refresh, or pass "
                "--allow-bootstrap-overwrite for an intentional one-time rebuild"
            )
        write_delta_table_rows(table_path=table_path, rows=rows, columns=columns)
        return "overwrite_bootstrap"

    predicate = _trade_date_replace_predicate(rows, fallback_trade_date=trade_date)
    replace_delta_table_rows(
        table_path=table_path,
        rows=rows,
        columns=columns,
        predicate=predicate,
    )
    return f"scoped_update:{predicate}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Bootstrap or update MOEX money-math side tables without reloading historical bars."
        )
    )
    parser.add_argument("--mode", choices=["bootstrap", "update"], default="bootstrap")
    parser.add_argument("--allow-bootstrap-overwrite", action="store_true")
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument("--date-from", default="")
    parser.add_argument("--date-till", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--raw-economics-root", default="")
    parser.add_argument("--canonical-economics-root", default="")
    parser.add_argument("--canonical-session-calendar-path", default="")
    parser.add_argument("--evidence-root", default="")
    parser.add_argument("--moex-timeout-seconds", type=float, default=DEFAULT_MOEX_TIMEOUT_SECONDS)
    parser.add_argument("--moex-max-retries", type=int, default=DEFAULT_MOEX_MAX_RETRIES)
    parser.add_argument(
        "--moex-retry-backoff-seconds",
        type=float,
        default=DEFAULT_MOEX_RETRY_BACKOFF_SECONDS,
    )
    parser.add_argument(
        "--moex-retry-jitter-ratio",
        type=float,
        default=DEFAULT_MOEX_RETRY_JITTER_RATIO,
    )
    parser.add_argument("--contracts-jsonl", default="")
    parser.add_argument("--fx-jsonl", default="")
    parser.add_argument("--rms-source", choices=["iss", "jsonl"], default="iss")
    parser.add_argument("--rms-limits-jsonl", default="")
    parser.add_argument("--rms-staticparams-jsonl", default="")
    args = parser.parse_args()

    trade_dates = _date_window(
        trade_date=str(args.trade_date),
        date_from=str(args.date_from).strip(),
        date_till=str(args.date_till).strip(),
    )
    trade_date = trade_dates[0]
    run_id = args.run_id.strip() or _default_run_id()
    raw_root = resolve_external_root(
        args.raw_economics_root,
        repo_root=ROOT,
        field_name="--raw-economics-root",
        default_subdir="raw/economics",
    )
    canonical_root = resolve_external_root(
        args.canonical_economics_root,
        repo_root=ROOT,
        field_name="--canonical-economics-root",
        default_subdir="canonical/economics",
    )
    evidence_root = resolve_external_root(
        args.evidence_root,
        repo_root=ROOT,
        field_name="--evidence-root",
        default_subdir=f"baseline-update/{run_id}/economics-refresh",
    )

    fetched_at_utc = _utc_now_iso()
    request_log_path = evidence_root / "moex-request-log.jsonl"
    request_latest_path = evidence_root / "moex-request.latest.json"
    client = MoexISSClient(
        **_moex_client_kwargs(args),
        request_event_hook=lambda payload: _append_request_event(
            jsonl_path=request_log_path,
            latest_path=request_latest_path,
            payload=payload,
        ),
    )
    contracts = []
    if args.contracts_jsonl.strip():
        contract_payloads = _read_jsonl(Path(args.contracts_jsonl))
        for current_date in trade_dates:
            contracts.extend(
                _raw_contract_rows(
                    payload_rows=_payloads_for_trade_date(
                        contract_payloads,
                        trade_date=current_date,
                        source_label="contracts",
                        allow_undated_single_day=len(trade_dates) == 1,
                    ),
                    trade_date=current_date,
                    fetched_at_utc=fetched_at_utc,
                )
            )
    else:
        if len(trade_dates) > 1:
            raise SystemExit(
                "MOEX ISS contract securities is a current-snapshot endpoint; "
                "multi-day or historical economics backfill requires --contracts-jsonl "
                "with one or more date-stamped contract snapshots"
            )
        contracts = _raw_contract_rows(
            payload_rows=client.fetch_futures_contract_securities(),
            trade_date=trade_date,
            fetched_at_utc=fetched_at_utc,
        )

    if args.fx_jsonl.strip():
        fx_payloads = []
        all_fx_payloads = _read_jsonl(Path(args.fx_jsonl))
        for current_date in trade_dates:
            fx_payloads.extend(
                _payloads_for_trade_date(
                    all_fx_payloads,
                    trade_date=current_date,
                    source_label="fx rates",
                    allow_undated_single_day=len(trade_dates) == 1,
                )
            )
    else:
        fx_payloads = client.fetch_futures_indicative_rates(
            date_from=trade_dates[0],
            date_till=trade_dates[-1],
        )
    fx = _raw_fx_rows(
        payload_rows=fx_payloads,
        trade_date=trade_date,
        fetched_at_utc=fetched_at_utc,
    )
    if args.rms_source == "iss":
        limits = []
        staticparams = []
        for current_date in trade_dates:
            limits.extend(
                _raw_rms_limits_rows(
                    payload_rows=client.fetch_futures_rms_limits(trade_date=current_date),
                    trade_date=current_date,
                    fetched_at_utc=fetched_at_utc,
                )
            )
            staticparams.extend(
                _raw_rms_staticparams_rows(
                    payload_rows=client.fetch_futures_rms_staticparams(trade_date=current_date),
                    trade_date=current_date,
                    fetched_at_utc=fetched_at_utc,
                )
            )
    else:
        if not args.rms_limits_jsonl or not args.rms_staticparams_jsonl:
            raise SystemExit(
                "--rms-limits-jsonl and --rms-staticparams-jsonl are required "
                "when --rms-source jsonl"
            )
        limits = []
        staticparams = []
        limit_payloads = _read_jsonl(Path(args.rms_limits_jsonl))
        static_payloads = _read_jsonl(Path(args.rms_staticparams_jsonl))
        for current_date in trade_dates:
            limits.extend(
                _risk_rows_from_jsonl(
                    rows=_payloads_for_trade_date(
                        limit_payloads,
                        trade_date=current_date,
                        source_label="rms limits",
                        allow_undated_single_day=len(trade_dates) == 1,
                    ),
                    table_name="raw_moex_rms_limits",
                    trade_date=current_date,
                    fetched_at_utc=fetched_at_utc,
                    source_id="ncc_derivatives_limits_import",
                )
            )
            staticparams.extend(
                _risk_rows_from_jsonl(
                    rows=_payloads_for_trade_date(
                        static_payloads,
                        trade_date=current_date,
                        source_label="rms staticparams",
                        allow_undated_single_day=len(trade_dates) == 1,
                    ),
                    table_name="raw_moex_rms_staticparams",
                    trade_date=current_date,
                    fetched_at_utc=fetched_at_utc,
                    source_id="ncc_derivatives_staticparams_import",
                )
            )

    raw_paths = {
        "raw_moex_contract_securities": raw_root / "raw_moex_contract_securities.delta",
        "raw_moex_indicative_fx_rates": raw_root / "raw_moex_indicative_fx_rates.delta",
        "raw_moex_rms_limits": raw_root / "raw_moex_rms_limits.delta",
        "raw_moex_rms_staticparams": raw_root / "raw_moex_rms_staticparams.delta",
    }
    raw_write_modes = {}
    raw_write_modes["raw_moex_contract_securities"] = _write_raw_table_for_mode(
        table_path=raw_paths["raw_moex_contract_securities"],
        table_name="raw_moex_contract_securities",
        rows=contracts,
        trade_date=trade_date,
        mode=args.mode,
        allow_bootstrap_overwrite=args.allow_bootstrap_overwrite,
    )
    raw_write_modes["raw_moex_indicative_fx_rates"] = _write_raw_table_for_mode(
        table_path=raw_paths["raw_moex_indicative_fx_rates"],
        table_name="raw_moex_indicative_fx_rates",
        rows=fx,
        trade_date=trade_date,
        mode=args.mode,
        allow_bootstrap_overwrite=args.allow_bootstrap_overwrite,
    )
    raw_write_modes["raw_moex_rms_limits"] = _write_raw_table_for_mode(
        table_path=raw_paths["raw_moex_rms_limits"],
        table_name="raw_moex_rms_limits",
        rows=limits,
        trade_date=trade_date,
        mode=args.mode,
        allow_bootstrap_overwrite=args.allow_bootstrap_overwrite,
    )
    raw_write_modes["raw_moex_rms_staticparams"] = _write_raw_table_for_mode(
        table_path=raw_paths["raw_moex_rms_staticparams"],
        table_name="raw_moex_rms_staticparams",
        rows=staticparams,
        trade_date=trade_date,
        mode=args.mode,
        allow_bootstrap_overwrite=args.allow_bootstrap_overwrite,
    )

    session_calendar_path = (
        Path(args.canonical_session_calendar_path).resolve()
        if args.canonical_session_calendar_path.strip()
        else None
    )

    report = run_moex_contract_economics_spark_job(
        raw_contract_specs_path=raw_paths["raw_moex_contract_securities"],
        raw_fx_rates_path=raw_paths["raw_moex_indicative_fx_rates"],
        raw_rms_limits_path=raw_paths["raw_moex_rms_limits"],
        raw_rms_staticparams_path=raw_paths["raw_moex_rms_staticparams"],
        output_dir=canonical_root,
        canonical_session_calendar_path=session_calendar_path,
        run_id=run_id,
        report_path=evidence_root / "contract-economics-report.json",
    )
    report["update_mode"] = args.mode
    report["date_window"] = {
        "date_from": trade_dates[0].isoformat(),
        "date_till": trade_dates[-1].isoformat(),
        "trade_dates": [item.isoformat() for item in trade_dates],
    }
    report["source_modes"] = {
        "contracts": "jsonl" if args.contracts_jsonl.strip() else "moex_iss_current_snapshot",
        "fx": "jsonl" if args.fx_jsonl.strip() else "moex_iss_date_range",
        "rms": args.rms_source,
    }
    report["moex_request_policy"] = _moex_client_kwargs(args)
    report["moex_request_log_path"] = request_log_path.as_posix()
    report["moex_request_latest_path"] = request_latest_path.as_posix()
    if len(trade_dates) > 1:
        report["source_limitations"] = [
            "historical multi-day economics requires date-stamped contract snapshots; "
            "MOEX ISS contract securities is not used for multi-day contract backfill"
        ]
    report["raw_write_modes"] = raw_write_modes
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
