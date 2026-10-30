"""Bounded ISS history support for the existing Dagster baseline owner.

Python projects small official metadata; Delta owns atomic keyed replacements.
Current specifications are crosschecks, never backdated official observations.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from deltalake import DeltaTable

from trading_advisor_3000.product_plane.data_plane.delta_runtime import (
    delta_equals_predicate,
    read_delta_table_rows,
    write_delta_table_rows,
)

from .contract_parameter_continuation import (
    MoexContractParameterEvidence,
    bind_contract_parameter_input,
    validate_contract_parameter_evidence,
)
from .economics import MoexContractParameterRegime

HISTORY_SOURCE = "moex_iss_history_forts_securities_unmodified"
PREPARED_SOURCE = "moex_history_specification_enriched_staging"
HISTORY_URL = (
    "https://iss.moex.com/iss/history/engines/futures/markets/forts/boards/RFUD/securities.json"
)
_KEYS = ("source_id", "board", "moex_secid", "trade_date")
_IGNORED = {
    "fetched_at_utc",
    "materialization_run_id",
    "materialized_at_utc",
    "source_delta_version",
    "parent_raw_snapshot_sha256",
}


def _json(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _positive(value, name):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"native history requires finite positive {name}")
    return float(value)


def project_open_interest(value):
    """Preserve accepted NULL/zero/native-int64 observations, without publication guesses."""
    if value is None or value == "":
        return None, "MISSING_IN_SOURCE", None
    text = str(value).strip()
    if isinstance(value, bool) or not re.fullmatch(r"[+]?[0-9]+", text):
        raise ValueError("OPENPOSITION must be a nonnegative native int64 count")
    count = int(text)
    if count > 2**63 - 1:
        raise ValueError("OPENPOSITION exceeds int64")
    return count, "OBSERVED_ZERO" if count == 0 else "OBSERVED", text


def _business(row):
    return {
        key: value.isoformat() if isinstance(value, (date, datetime)) else value
        for key, value in row.items()
        if key not in _IGNORED
    }


def write_changed_support_rows(table_path, rows, *, columns, pending_scope_path=None):
    """Native predicate write; compare only requested metadata keys before mutation."""
    if not table_path.is_absolute():
        raise ValueError("native support table path must be absolute")
    if not rows:
        table = DeltaTable(str(table_path))
        return {
            "table_path": table_path.resolve().as_posix(),
            "table_id": table.metadata().id,
            "delta_version": table.version(),
            "changed_rows": 0,
            "previous_delta_version": table.version(),
            "changed_keys": [],
        }
    grouped = {}
    for row in rows:
        key = tuple(str(row[name]) for name in _KEYS)
        if key in grouped:
            raise ValueError("duplicate native support source/contract/date key")
        grouped[key] = row
    exists = (table_path / "_delta_log").is_dir()
    table = DeltaTable(str(table_path)) if exists else None
    identity = table.metadata().id if table is not None else None
    if table is not None and table.metadata().partition_columns:
        raise ValueError("native economics metadata must remain unpartitioned")
    previous_reference = (
        {
            "table_path": table_path.resolve().as_posix(),
            "table_id": identity,
            "delta_version": table.version(),
        }
        if table is not None
        else None
    )
    changed, predicates, dirty_keys = [], [], []
    for key, row in grouped.items():
        scope = dict(zip(_KEYS, key, strict=True))
        old = (
            read_delta_table_rows(
                table_path, filters=[(name, "=", value) for name, value in scope.items()], limit=3
            )
            if exists
            else []
        )
        if len(old) == 1 and _business(old[0]) == _business(row):
            continue
        if len(old) > 1:
            raise ValueError("duplicate native support keys; dirty scope unproven")
        dirty_keys.extend(
            {key: str(item[key]) for key in ("internal_id", "moex_secid", "trade_date")}
            for item in [*old, row]
            if "internal_id" in item
        )
        changed.append(row)
        predicates.append("(" + delta_equals_predicate(scope) + ")")
    if changed:
        if pending_scope_path is not None:
            from trading_advisor_3000.product_plane.common.data_snapshots import atomic_json

            pending = (
                json.loads(pending_scope_path.read_text(encoding="utf-8"))
                if pending_scope_path.exists()
                else {}
            )
            previous = pending.get("support_changed_keys", [])
            expected = pending.get("support_output_reference") or pending.get(
                "support_input_reference"
            )
            if previous and expected != previous_reference:
                pending["support_scope_unproven"] = True
            if not previous:
                pending["support_input_reference"] = previous_reference
            pending.update(
                status="PENDING",
                reason="oi_write_pending_canonical",
                support_changed_keys=list(
                    {_json(key): key for key in [*previous, *dirty_keys]}.values()
                ),
            )
            atomic_json(pending_scope_path, pending)
        write_delta_table_rows(
            table_path=table_path,
            rows=changed,
            columns=columns,
            mode="overwrite" if exists else "append",
            compact=False,
            replace_predicate=" OR ".join(predicates) if exists else None,
        )
    current = DeltaTable(str(table_path))
    if pending_scope_path is not None and changed:
        from trading_advisor_3000.product_plane.common.data_snapshots import atomic_json

        pending = json.loads(pending_scope_path.read_text(encoding="utf-8"))
        if previous_reference and current.version() != previous_reference["delta_version"] + 1:
            pending["support_scope_unproven"] = True
        pending["support_output_reference"] = {
            "table_path": table_path.resolve().as_posix(),
            "table_id": current.metadata().id,
            "delta_version": current.version(),
        }
        atomic_json(pending_scope_path, pending)
    if identity is not None and current.metadata().id != identity:
        raise RuntimeError("native support write changed Delta table identity")
    return {
        "table_path": table_path.resolve().as_posix(),
        "table_id": current.metadata().id,
        "delta_version": current.version(),
        "changed_rows": len(changed),
        "previous_delta_version": table.version() if table is not None else None,
        "changed_keys": list({_json(key): key for key in dirty_keys}.values()),
    }


def _columns(table_path):
    table = DeltaTable(str(table_path))
    fields = json.loads(table.schema().to_json())["fields"]
    if table.metadata().partition_columns or any(
        type(field["type"]) is not str for field in fields
    ):
        raise ValueError("native support requires existing unpartitioned flat schema")
    return {field["name"]: field["type"] for field in fields}


def _fx_records(rows):
    result = {}
    for row in rows:
        pair = str(
            row.get("fx_pair")
            or row.get("currency_pair")
            or row.get("secid")
            or row.get("SECID")
            or ""
        ).upper()
        day = str(row.get("trade_date") or row.get("tradedate") or row.get("TRADEDATE") or "")[:10]
        kind = str(
            row.get("clearing_type") or row.get("clearing") or row.get("CLEARING") or ""
        ).upper()
        rate = row.get("rate") if "rate" in row else row.get("RATE")
        if pair == "USD/RUB" and kind in {"TC", "MC"}:
            key = day, kind
            value = _positive(rate, "USD/RUB rate")
            if key in result and result[key] != value:
                raise ValueError("conflicting native dated USD rate")
            result[key] = value
    return result


def _source_timestamp(value, name):
    text = str(value or "")
    if "T" not in text and " " not in text:
        raise ValueError(f"current crosscheck requires actual {name} timestamp")
    try:
        stamp = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"current crosscheck requires valid {name} timestamp") from exc
    market_tz = ZoneInfo("Europe/Moscow")
    return stamp.replace(tzinfo=market_tz) if stamp.tzinfo is None else stamp.astimezone(market_tz)


def _check_current(current_securities, rules, fx, *, dated_fx_rows):
    """Match the actual MC available at each specification's MOEX IMTIME."""
    by_asset = {rule.assetcode: rule for rule in rules}
    mc_dates = [day for day, kind in fx if kind == "MC"]
    if not mc_dates:
        raise ValueError("current snapshot requires an actual dated MC observation")
    observations = {}
    for row in dated_fx_rows:
        pair = str(
            row.get("fx_pair")
            or row.get("currency_pair")
            or row.get("secid")
            or row.get("SECID")
            or ""
        ).upper()
        kind = str(
            row.get("clearing_type") or row.get("clearing") or row.get("CLEARING") or ""
        ).upper()
        if pair != "USD/RUB" or kind != "MC":
            continue
        day = str(row.get("trade_date") or row.get("tradedate") or row.get("TRADEDATE") or "")[:10]
        clock = row.get("tradetime") or row.get("TRADETIME")
        stamp = _source_timestamp(f"{day}T{clock or ''}", "dated MC time")
        if day in observations and observations[day] != stamp:
            raise ValueError("conflicting native dated MC observation time")
        observations[day] = stamp
    if set(observations) != set(mc_dates):
        raise ValueError("current snapshot requires actual dated MC observation times")
    profiles, descriptions, matches = {}, {}, []
    for payload in current_securities:
        asset = str(payload.get("ASSETCODE") or "")
        if asset not in by_asset:
            continue
        rule = by_asset[asset]
        step, lot = (
            _positive(payload.get("MINSTEP"), "MINSTEP"),
            _positive(payload.get("LOTVOLUME"), "LOTVOLUME"),
        )
        if step != rule.min_step or lot != rule.lot_volume:
            raise ValueError(f"current native units changed: {asset}")
        mc = 1.0
        if rule.quote_currency == "USD":
            imtime = _source_timestamp(payload.get("IMTIME") or payload.get("imtime"), "IMTIME")
            available = [day for day, stamp in observations.items() if stamp <= imtime]
            if not available:
                raise ValueError(
                    f"current snapshot has no actual dated MC at or before IMTIME: {asset}"
                )
            mc_date = max(available, key=observations.__getitem__)
            mc = fx[mc_date, "MC"]
            matches.append(
                {
                    "secid": str(payload.get("SECID") or ""),
                    "specification_imtime": imtime.isoformat(),
                    "dated_mc_date": mc_date,
                    "dated_mc_time": observations[mc_date].isoformat(),
                    "dated_mc_rate": mc,
                }
            )
        expected = rule.tick_value_quote * mc
        error = abs(_positive(payload.get("STEPPRICE"), "STEPPRICE") / expected - 1.0)
        if error >= 0.001:
            raise ValueError(f"current datedMC crosscheck failed: {asset}")
        secid = str(payload.get("SECID") or "")
        if not secid or secid in descriptions:
            raise ValueError("current securities require unique contract identifiers")
        descriptions[secid] = payload
        profile = profiles.setdefault(asset, {"assetcode": asset, "rows": 0, "maximum": 0.0})
        profile["rows"] += 1
        profile["maximum"] = max(profile["maximum"], error)
    if set(profiles) != set(by_asset):
        raise ValueError("current snapshot does not cover all reviewed thirteen assets")
    mc_date = max((match["dated_mc_date"] for match in matches), default=max(mc_dates))
    return descriptions, {
        "status": "PASS",
        "method": "current_snapshot_dated_mc_crosscheck",
        "dated_mc_date": mc_date,
        "dated_mc_rate": fx[mc_date, "MC"],
        "profiles": list(profiles.values()),
        "contracts": matches,
        "not_daily_official": True,
    }


def _expired_contract_description(prepared_path, verified, *, secid, rule, trade_date, as_of_date):
    """Resolve static metadata from the admitted snapshot, never mutable latest."""
    import pyarrow.dataset as ds

    table = DeltaTable(str(prepared_path), version=verified["prepared_input_version"])
    if table.metadata().id != verified["prepared_input_table_id"]:
        raise ValueError("expired description input identity mismatch")
    columns = [
        "trade_date",
        "last_trade_date",
        "last_del_date",
        "min_step",
        "lot_volume",
        "source_document_id",
        "source_document_hash",
        "enrichment_provenance_json",
    ]
    predicate = (
        (ds.field("source_id") == PREPARED_SOURCE)
        & (ds.field("board") == "RFUD")
        & (ds.field("moex_secid") == secid)
        & (ds.field("assetcode") == rule.assetcode)
        & (ds.field("trade_date") <= trade_date)
    )
    rows = (
        table.to_pyarrow_dataset()
        .scanner(columns=columns, filter=predicate)
        .head(10001)
        .to_pylist()
    )
    if not rows or len(rows) > 10000:
        raise ValueError(f"expired contract lacks bounded admitted description: {secid}")
    latest = max(row["trade_date"] for row in rows)
    candidates = [row for row in rows if row["trade_date"] == latest]
    if len(candidates) != 1:
        raise ValueError(f"ambiguous admitted expired description: {secid}")
    row = candidates[0]
    expiry = date.fromisoformat(str(row["last_trade_date"]))
    delivery = row["last_del_date"]
    if not trade_date <= expiry < as_of_date or (
        delivery is not None and date.fromisoformat(str(delivery)) < expiry
    ):
        raise ValueError(f"expired description does not cover requested date: {secid}")
    if (
        _positive(row["min_step"], "archived MINSTEP") != rule.min_step
        or _positive(row["lot_volume"], "archived LOTVOLUME") != rule.lot_volume
    ):
        raise ValueError(f"expired description units differ from reviewed rule: {secid}")
    original_json = row["enrichment_provenance_json"]
    if not isinstance(original_json, str):
        raise ValueError(f"expired description lacks stored provenance: {secid}")
    digest = hashlib.sha256(original_json.encode("utf-8")).hexdigest()
    provenance = json.loads(original_json)
    if (
        row["source_document_hash"] != digest
        or row["source_document_id"] != f"prepared-specification:{digest}"
        or not isinstance(provenance, dict)
        or not re.fullmatch(r"[0-9a-f]{64}", str(provenance.get("description_hash", "")))
        or provenance.get("original_source_id") != HISTORY_SOURCE
        or provenance.get("original_source_document_id") != f"RFUD:{secid}:{latest}"
        or provenance.get("description_lot") != rule.lot_volume
        or provenance.get("rule_min_step") != rule.min_step
        or provenance.get("rule_lot_volume") != rule.lot_volume
    ):
        raise ValueError(f"expired description provenance mismatch: {secid}")
    description = {"LASTTRADEDATE": expiry.isoformat(), "LASTDELDATE": delivery}
    reference = {
        "table_path": prepared_path.resolve().as_posix(),
        "table_id": table.metadata().id,
        "delta_version": table.version(),
        "moex_secid": secid,
        "trade_date": str(latest),
        "source_document_id": row["source_document_id"],
        "source_document_hash": digest,
        "description_hash": provenance["description_hash"],
        "parameter_validation_evidence_sha256": provenance.get(
            "parameter_validation_evidence_sha256"
        ),
    }
    return description, reference


@dataclass(frozen=True)
class NativeHistoryRenewal:
    base_rules: tuple
    base_evidence: object
    observed_date_start: str
    observed_date_end: str
    admitted_date_end: str
    tables: dict
    current_crosscheck: dict
    source_hash: str
    proof_sha256: str
    _owner: object


_RENEWAL_OWNER = object()


def refresh_native_history_support(
    *,
    client,
    raw_contract_specs_path,
    prepared_path,
    daily_open_interest_path,
    trade_dates,
    base_rules,
    base_evidence,
    current_securities,
    dated_fx_rows,
    run_id,
    pending_scope_path=None,
):
    """Existing public history acquisition, bounded projections and owning native writes."""
    verified = validate_contract_parameter_evidence(base_rules, base_evidence)
    if verified is None:
        raise ValueError("native history renewal requires the existing admitted base bundle")
    bind_contract_parameter_input(prepared_path, verified)
    if (
        len(
            {
                path.resolve()
                for path in (raw_contract_specs_path, prepared_path, daily_open_interest_path)
            }
        )
        != 3
    ):
        raise ValueError("native support owning targets must be distinct")
    dates = tuple(sorted(set(trade_dates)))
    if not dates or any(type(day) is not date for day in dates) or len(dates) > 32:
        raise ValueError("native history renewal requires 1–32 concrete changed dates")
    if (dates[-1] - dates[0]).days + 1 != len(dates):
        raise ValueError("native history renewal requires a contiguous bounded date window")
    fx = _fx_records(dated_fx_rows)
    descriptions, crosscheck = _check_current(
        current_securities, base_rules, fx, dated_fx_rows=dated_fx_rows
    )
    rules = {rule.assetcode: rule for rule in base_rules}
    columns = {
        "raw": _columns(raw_contract_specs_path),
        "prepared": _columns(prepared_path),
        "oi": _columns(daily_open_interest_path),
    }
    for path, identity in (
        (raw_contract_specs_path, "ae9e49f9-9e8f-405d-9e07-0a2079eee691"),
        (prepared_path, verified["prepared_input_table_id"]),
        (daily_open_interest_path, "026176bf-9d3b-4192-adcf-559cd33f7c93"),
    ):
        if DeltaTable(str(path)).metadata().id != identity:
            raise ValueError("native history renewal owning input identity mismatch")
    rows = {"raw": [], "prepared": [], "oi": []}
    observed, admitted = [], []
    fetched_at = datetime.now(UTC).isoformat()
    as_of_date = datetime.fromisoformat(fetched_at).astimezone(ZoneInfo("Europe/Moscow")).date()
    for day in dates:
        day_rows = client.fetch_history_board_securities(
            engine="futures", market="forts", board="RFUD", trade_date=day
        )
        assets = set()
        for record in day_rows:
            payload = record.payload
            asset, secid = str(payload.get("ASSETCODE") or ""), str(payload.get("SECID") or "")
            if asset not in rules:
                continue
            if payload.get("TRADEDATE") != day.isoformat() or payload.get("BOARDID") != "RFUD":
                raise ValueError("official history key differs from requested date/board")
            archived_description = None
            description, rule = descriptions.get(secid), rules[asset]
            if description is None:
                description, archived_description = _expired_contract_description(
                    prepared_path,
                    verified,
                    secid=secid,
                    rule=rule,
                    trade_date=day,
                    as_of_date=as_of_date,
                )
            count, status, text = project_open_interest(payload.get("OPENPOSITION"))
            settle = _positive(payload.get("SETTLEPRICE"), "SETTLEPRICE")
            raw = dict(
                source_id=HISTORY_SOURCE,
                source_url=HISTORY_URL,
                source_document_id=f"RFUD:{secid}:{day.isoformat()}",
                source_document_hash=_hash(payload),
                fetched_at_utc=fetched_at,
                engine="futures",
                market="forts",
                board="RFUD",
                moex_secid=secid,
                trade_date=day.isoformat(),
                assetcode=asset,
                contract_shortname=payload.get("SHORTNAME"),
                last_trade_date=None,
                last_del_date=None,
                min_step=None,
                lot_volume=None,
                official_step_price=None,
                official_initial_margin=None,
                last_settle_price=settle,
                raw_payload_json=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                enrichment_provenance_json=None,
            )
            provenance = dict(
                original_history_hash=raw["source_document_hash"],
                original_source_id=HISTORY_SOURCE,
                original_source_document_id=raw["source_document_id"],
                description_hash=(
                    archived_description["description_hash"]
                    if archived_description
                    else _hash(description)
                ),
                description_lot=rule.lot_volume,
                rule_min_step=rule.min_step,
                rule_lot_volume=rule.lot_volume,
                parameter_source="reviewed_static_parameter_continuation",
                parameter_validation_evidence_sha256=verified["dated_official_tc_evidence_sha256"],
                current_snapshot_not_daily_official=True,
                current_snapshot_crosscheck_sha256=_hash(crosscheck),
            )
            if archived_description is not None:
                provenance["archived_description"] = archived_description
            prepared_hash = _hash(provenance)
            prepared = dict(
                raw,
                source_id=PREPARED_SOURCE,
                source_document_id=f"prepared-specification:{prepared_hash}",
                source_document_hash=prepared_hash,
                min_step=rule.min_step,
                lot_volume=rule.lot_volume,
                last_trade_date=description.get("LASTTRADEDATE"),
                last_del_date=description.get("LASTDELDATE"),
                enrichment_provenance_json=_json(provenance),
            )
            oi = {key: raw[key] for key in columns["oi"] if key in raw}
            oi.update(
                internal_id=f"FUT_{asset}",
                source_open_interest_text=text,
                open_interest=count,
                observation_status=status,
                observation_frequency="1d",
                available_at_utc=None,
                availability_status="HISTORICAL_PUBLICATION_TIME_UNVERIFIED",
                source_delta_path=str(raw_contract_specs_path),
                source_delta_version=0,
                parent_raw_snapshot_sha256="pending",
                materialization_run_id=run_id,
                materialized_at_utc=fetched_at,
            )
            rows["raw"].append(raw)
            rows["prepared"].append(prepared)
            rows["oi"].append(oi)
            assets.add(asset)
        if assets:
            if assets != set(rules):
                raise ValueError(
                    "official changed date does not cover all reviewed thirteen assets"
                )
            observed.append(day.isoformat())
            if (day.isoformat(), "TC") in fx:
                admitted.append(day.isoformat())
    if not observed or not admitted:
        raise ValueError("no actual history/datedTC date admitted; no writes")
    if set(observed) != set(admitted):
        raise ValueError("actual history date lacks exact datedTC; no support writes")
    rows["prepared"] = [row for row in rows["prepared"] if row["trade_date"] <= max(admitted)]
    for name in rows:
        if any(set(row) != set(columns[name]) for row in rows[name]):
            raise ValueError(f"native {name} support row differs from owning schema")
    source_hash = _hash(sorted(row["source_document_hash"] for row in rows["raw"]))
    raw_ref = write_changed_support_rows(
        raw_contract_specs_path, rows["raw"], columns=columns["raw"]
    )
    scopes_sha = _hash(sorted({(row["moex_secid"], row["internal_id"]) for row in rows["oi"]}))
    for row in rows["oi"]:
        row.update(
            source_delta_version=raw_ref["delta_version"], parent_raw_snapshot_sha256=scopes_sha
        )
    prepared_ref = write_changed_support_rows(
        prepared_path, rows["prepared"], columns=columns["prepared"]
    )
    oi_ref = write_changed_support_rows(
        daily_open_interest_path,
        rows["oi"],
        columns=columns["oi"],
        pending_scope_path=pending_scope_path,
    )
    tables = {"raw": raw_ref, "prepared": prepared_ref, "oi": oi_ref}
    proof = _hash(
        dict(
            tables=tables,
            crosscheck=crosscheck,
            source_hash=source_hash,
            observed_dates=[min(observed), max(observed)],
            admitted_date_end=max(admitted),
        )
    )
    return NativeHistoryRenewal(
        base_rules,
        base_evidence,
        min(observed),
        max(observed),
        max(admitted),
        tables,
        crosscheck,
        source_hash,
        proof,
        _RENEWAL_OWNER,
    )


def registered_history_refresh_kwargs(root: Path) -> dict:
    """Existing Evidence shape; accepted prepared roots fail closed after activation."""
    prepared_dir = root / "provenance/prepared/economics"
    prepared = prepared_dir / "raw_moex_contract_securities.delta"
    if not (prepared / "_delta_log").is_dir():
        return {}
    if DeltaTable(str(prepared)).metadata().id != "d550b9e4-e3d6-40dc-93f0-fc0e420e3b43":
        raise ValueError("registered native prepared input is not the accepted table")
    evidence = MoexContractParameterEvidence(
        "bounded-tail-20261010-v1",
        prepared_dir / "bounded-parameter-regimes.json",
        prepared_dir / "dated-official-tc-evidence.json",
    )
    rules = tuple(
        MoexContractParameterRegime(**row) for row in json.loads(evidence.regimes_path.read_text())
    )
    verified = validate_contract_parameter_evidence(rules, evidence)
    bind_contract_parameter_input(prepared, verified)
    oi_path = root / "raw/economics/raw_moex_daily_open_interest.delta"
    oi = DeltaTable(str(oi_path))
    if oi.metadata().id != "026176bf-9d3b-4192-adcf-559cd33f7c93":
        raise ValueError("registered native OI input is not the accepted table")
    return dict(
        economics_history_refresh=True,
        economics_source_paths={"raw_contract_specs_path": prepared},
        economics_parameter_evidence=dict(
            bundle_id=evidence.bundle_id,
            regimes_path=str(evidence.regimes_path),
            dated_official_tc_path=str(evidence.dated_official_tc_path),
        ),
        allow_calibrated_latest_margin=True,
        margin_calibration_as_of_date=date(2026, 8, 14),
        daily_open_interest_path=oi_path,
        daily_open_interest_version=oi.version(),
    )
