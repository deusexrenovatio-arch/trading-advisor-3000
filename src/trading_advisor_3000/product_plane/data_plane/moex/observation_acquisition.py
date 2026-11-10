"""Bounded original-byte intake for opt-in, unpublished native observations."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from trading_advisor_3000.product_plane.common.data_snapshots import atomic_json
from trading_advisor_3000.product_plane.common.immutable_files import (
    canonical_bytes as canonical_bytes,
)
from trading_advisor_3000.product_plane.common.immutable_files import child, pin, pinned_bytes
from trading_advisor_3000.product_plane.contracts.identity_v3 import validate_identity_contract

from .iss_client import MoexISSClient


class OriginalResponseCapture:
    def __init__(self, root: Path, *, max_requests: int):
        self.root = root.resolve()
        self.max_requests = max_requests
        self.records: list[dict] = []

    def __call__(self, metadata: dict, body: bytes) -> None:
        if len(self.records) >= self.max_requests or type(body) is not bytes:
            raise ValueError("source capture budget/type violation")
        body_path = self.root / "source" / "bodies" / (hashlib.sha256(body).hexdigest() + ".body")
        body_path.parent.mkdir(parents=True, exist_ok=True)
        if body_path.exists():
            if body_path.read_bytes() != body:
                raise ValueError("immutable original response changed")
        else:
            with body_path.open("xb") as handle:
                handle.write(body)
                handle.flush()
                import os

                os.fsync(handle.fileno())
        receipt = {**metadata, "body": pin(self.root, body_path), "body_bytes": len(body)}
        receipt_path = self.root / "source" / "http_receipts" / (uuid4().hex + ".json")
        atomic_json(receipt_path, receipt, immutable=True)
        self.records.append({"receipt": pin(self.root, receipt_path), "body": receipt["body"]})


def _rows(payload: dict, block: str) -> list[dict]:
    value = payload[block]
    columns = value["columns"]
    if len(columns) != len(set(columns)):
        raise ValueError("duplicate metadata columns")
    return [dict(zip(columns, row, strict=True)) for row in value["data"]]


def _fact(value: str | None, *, not_applicable: bool = False) -> dict:
    return {
        "state": "not_applicable"
        if not_applicable
        else "known"
        if value is not None
        else "unknown",
        "value": value,
    }


def verify_identity_inputs(root: Path, inputs: dict) -> None:
    original = json.loads(pinned_bytes(root, inputs["original_assignments"]))
    for supplied in inputs["instruments"]:
        matches = [
            row
            for row in original["instruments"]
            if row.get("instrument_id") == supplied["instrument_id"]
        ]
        if len(matches) != 1 or any(
            matches[0].get(old) != supplied[new]
            for old, new in [
                ("ticker", "source_symbol"),
                ("asset_id", "asset_id"),
                ("instrument_id", "instrument_id"),
                ("source_security_id", "legacy_source_security_id"),
                ("contract_id", "legacy_contract_id"),
            ]
        ):
            raise ValueError("explicit ID extraction differs from pinned original assignment")


def verified_assignments(identity_inputs: dict, metadata_payloads: dict[str, dict]) -> dict:
    """Revalidate individually supplied IDs; never generate identities or temporal facts."""
    assets, instruments, bindings = {}, [], []
    rows = identity_inputs["instruments"]
    if len({r["instrument_id"] for r in rows}) != len(rows) or len(
        {r["source_symbol"] for r in rows}
    ) != len(rows):
        raise ValueError("ambiguous explicit native assignments")
    descriptions = {}
    for item in rows:
        fields = _rows(metadata_payloads[item["source_symbol"]], "description")
        description = {r["name"]: r["value"] for r in fields}
        if len(description) != len(fields) or description["SECID"] != item["source_symbol"]:
            raise ValueError("ambiguous/wrong source description")
        descriptions[item["source_symbol"]] = description
    for item in rows:
        symbol, description = item["source_symbol"], descriptions[item["source_symbol"]]
        kind = {"common_share": "equity", "stock_index": "reference_index", "futures": "future"}[
            description["TYPE"]
        ]
        boards = [
            r
            for r in _rows(metadata_payloads[symbol], "boards")
            if (r["engine"], r["market"], r["boardid"])
            == (item["engine"], item["context"]["market"], item["context"]["board"])
        ]
        if (
            len(boards) != 1
            or item["context"]["provider"] != "moex_iss"
            or item["context"]["venue"] != "MOEX"
        ):
            raise ValueError("unconfirmed complete native source route")
        board = boards[0]
        if kind == "future":
            underlyings = [
                r
                for r in rows
                if r["asset_id"] == item["asset_id"]
                and descriptions[r["source_symbol"]]["TYPE"] == "common_share"
                and descriptions[r["source_symbol"]].get("EMITTER_ID")
                == description.get("EMITTER_ID")
            ]
            if (
                len(underlyings) != 1
                or description.get("ASSETCODE") != "SBRF"
                or description.get("EMITTER_ID") != "484"
                or description.get("CONTRACTNAME")
                != "Фьючерсный контракт на обыкновенные акции ПАО Сбербанк"
            ):
                raise ValueError("ordinary-Sber underlying is not source-confirmed")
            if not description.get("FRSTTRADE") or not description.get("LSTTRADE"):
                raise ValueError("native trial requires source lifetime dates")
            first, last = (
                date.fromisoformat(description["FRSTTRADE"]),
                date.fromisoformat(description["LSTTRADE"]),
            )
            if first > last:
                raise ValueError("reversed reported lifetime")
        else:
            asset = {"asset_id": item["asset_id"], "kind": kind}
            if assets.setdefault(item["asset_id"], asset) != asset:
                raise ValueError("asset kind conflict")
        instruments.append(
            {
                "instrument_id": item["instrument_id"],
                "asset_id": item["asset_id"],
                "kind": kind,
                "instrument_role": "derivative" if kind == "future" else "base",
                "venue": "MOEX",
                "market": board["market"],
                "board": board["boardid"],
                "tradable": False if kind == "reference_index" else board["is_traded"] == 1,
                "execution_eligible": False,
                "economics_state": "not_applicable" if kind == "reference_index" else "unknown",
                "size_variant": {
                    "state": "unknown" if kind == "future" else "not_applicable",
                    "value": None,
                },
                "specification": None,
                "expiry": None,
            }
        )
        units = {
            "price_currency": _fact(board.get("currencyid") or description.get("CURRENCYID")),
            "price_unit": _fact(description.get("UNIT") or board.get("unit")),
            "quantity_unit": _fact(None, not_applicable=kind == "reference_index"),
        }
        bindings.append(
            {
                **item,
                "kind": kind,
                "units": units,
                "reported_first_trade": description.get("FRSTTRADE"),
                "reported_last_trade": description.get("LSTTRADE"),
                "historical_mapping_verified": False,
            }
        )
    namespace = {
        "schema_version": 1,
        "namespace": identity_inputs["namespace"],
        "assets": list(assets.values()),
        "instruments": instruments,
    }
    validate_identity_contract("instrument_namespace.v1.json", namespace, namespace)
    for binding in bindings:
        validate_identity_contract("native_observation_units.v1.json", binding["units"], namespace)
    return {
        "schema_version": 1,
        "namespace": namespace,
        "bindings": bindings,
        "identity_inputs": identity_inputs,
    }


def acquire_observations(
    root: Path,
    *,
    identity_input_path: Path,
    date_from: date,
    date_till: date,
    max_requests: int = 100,
    max_rows: int = 20000,
    max_pages_per_slice: int = 16,
) -> dict:
    root = root.resolve()
    if (
        date_from > date_till
        or (date_till - date_from).days > 2
        or date_till >= datetime.now(UTC).date()
    ):
        raise ValueError("only a bounded closed source-day window is admitted")
    if any((root / name).exists() for name in ["published_current.json", "accepted_current.json"]):
        raise ValueError("trial root contains a publication pointer")
    identity_inputs = json.loads(identity_input_path.read_bytes())
    verify_identity_inputs(root, identity_inputs)
    capture = OriginalResponseCapture(root, max_requests=max_requests)
    client = MoexISSClient(
        timeout_seconds=12,
        max_retries=0,
        response_capture=capture,
        max_response_bytes=4 * 1024 * 1024,
    )

    def get(path, params):
        if len(capture.records) >= max_requests:
            raise ValueError("acquisition request budget exceeded")
        payload = client._get_json(
            path, params=params, event_context={"operation": "native_observation_acquisition"}
        )
        return payload, capture.records[-1]

    metadata, payloads = {}, {}
    for item in identity_inputs["instruments"]:
        symbol = item["source_symbol"]
        payloads[symbol], metadata[symbol] = get(
            f"/iss/securities/{symbol}.json", {"iss.meta": "on"}
        )
    assignments = verified_assignments(identity_inputs, payloads)
    pages, total_rows = [], 0
    for binding in assignments["bindings"]:
        symbol, engine, context = binding["source_symbol"], binding["engine"], binding["context"]
        for timeframe, interval in [("1m", 1), ("1d", 24)]:
            start = 0
            for _ in range(max_pages_per_slice):
                path = (
                    f"/iss/engines/{engine}/markets/{context['market']}"
                    f"/boards/{context['board']}/securities/{symbol}/candles.json"
                )
                payload, record = get(
                    path,
                    {
                        "iss.meta": "on",
                        "interval": str(interval),
                        "from": date_from.isoformat(),
                        "till": date_till.isoformat(),
                        "start": str(start),
                    },
                )
                block = payload["candles"]
                count = len(block["data"])
                if not isinstance(block["data"], list) or len(block["columns"]) != len(
                    set(block["columns"])
                ):
                    raise ValueError("invalid original candle page")
                pages.append(
                    {
                        "source_symbol": symbol,
                        "engine": engine,
                        "context": context,
                        "timeframe": timeframe,
                        "interval": interval,
                        "page_start": start,
                        "source_row_count": count,
                        "columns": block["columns"],
                        **record,
                    }
                )
                total_rows += count
                if total_rows > max_rows:
                    raise ValueError("source row budget exceeded")
                if count == 0:
                    if start == 0:
                        raise ValueError(
                            "unverified-empty source slice: " + symbol + " " + timeframe
                        )
                    break
                start += count
            else:
                raise ValueError("source page budget exceeded before terminal page")
    manifest = {
        "schema_version": 1,
        "input_mode": "native_observations.v1",
        "status": "acquired",
        "identity_inputs": pin(root, identity_input_path),
        "metadata": metadata,
        "pages": pages,
        "source_rows": total_rows,
        "window": {"date_from": date_from.isoformat(), "date_till": date_till.isoformat()},
        "limits": {
            "requests": max_requests,
            "rows": max_rows,
            "pages_per_slice": max_pages_per_slice,
            "response_bytes": 4 * 1024 * 1024,
        },
    }
    manifest_path, assignments_path = (
        root / "source" / "capture-manifest.json",
        root / "source" / "assignments.json",
    )
    atomic_json(manifest_path, manifest, immutable=True)
    atomic_json(assignments_path, assignments, immutable=True)
    return {
        "source_manifest": pin(root, manifest_path),
        "assignments": pin(root, assignments_path),
        "requests": len(capture.records),
        "source_rows": total_rows,
    }


def verify_source_bundle(root: Path, source_pin: dict, assignment_pin: dict) -> tuple[dict, dict]:
    manifest = json.loads(pinned_bytes(root, source_pin))
    assignments = json.loads(pinned_bytes(root, assignment_pin))
    if manifest["status"] != "acquired" or manifest["input_mode"] != "native_observations.v1":
        raise ValueError("source bundle is not an acquired observation input")
    identity_inputs = json.loads(pinned_bytes(root, manifest["identity_inputs"]))
    verify_identity_inputs(root, identity_inputs)
    limits = manifest["limits"]
    ceilings = {
        "requests": 100,
        "rows": 20000,
        "pages_per_slice": 16,
        "response_bytes": 4 * 1024 * 1024,
    }
    if set(limits) != set(ceilings) or any(
        type(limits[key]) is not int or not 0 < limits[key] <= maximum
        for key, maximum in ceilings.items()
    ):
        raise ValueError("unsupported source bundle budgets")
    if (
        len(manifest["metadata"]) + len(manifest["pages"]) > limits["requests"]
        or manifest["source_rows"] > limits["rows"]
    ):
        raise ValueError("source bundle exceeds its recorded budgets")

    def payload(record, expected_path, expected_query=None):
        receipt = json.loads(pinned_bytes(root, record["receipt"]))
        if child(root, record["body"]["path"]).stat().st_size > limits["response_bytes"]:
            raise ValueError("recorded source response exceeds the byte budget")
        raw = pinned_bytes(root, record["body"])
        request, final = urlsplit(receipt["request_url"]), urlsplit(receipt["final_url"])
        if (
            receipt["status"] != 200
            or receipt["body"] != record["body"]
            or receipt["body_bytes"] != len(raw)
            or request.scheme != "https"
            or request.hostname != "iss.moex.com"
            or final.scheme != "https"
            or final.hostname != "iss.moex.com"
            or request.path != expected_path
            or final.path != expected_path
        ):
            raise ValueError("source receipt/context/body association mismatch")
        if expected_query and any(
            parse_qs(url.query).get(k) != [str(v)]
            for url in [request, final]
            for k, v in expected_query.items()
        ):
            raise ValueError("source request selector mismatch")
        return json.loads(raw)

    metadata = {
        symbol: payload(record, f"/iss/securities/{symbol}.json")
        for symbol, record in manifest["metadata"].items()
    }
    if set(metadata) != {item["source_symbol"] for item in identity_inputs["instruments"]}:
        raise ValueError("source metadata/explicit assignments scope mismatch")
    if verified_assignments(identity_inputs, metadata) != assignments:
        raise ValueError("source-backed namespace/assignment changed")
    bindings = {b["source_symbol"]: b for b in assignments["bindings"]}
    offsets, terminals, total, page_counts = {}, set(), 0, {}
    for page in manifest["pages"]:
        binding = bindings[page["source_symbol"]]
        key = (page["source_symbol"], page["timeframe"])
        page_counts[key] = page_counts.get(key, 0) + 1
        if page_counts[key] > limits["pages_per_slice"]:
            raise ValueError("recorded source slice exceeds page budget")
        if (
            key in terminals
            or page["page_start"] != offsets.get(key, 0)
            or page["context"] != binding["context"]
            or page["engine"] != binding["engine"]
            or page["interval"] != {"1m": 1, "1d": 24}[page["timeframe"]]
        ):
            raise ValueError("source page order/context mismatch")
        context, symbol = page["context"], page["source_symbol"]
        path = (
            f"/iss/engines/{page['engine']}/markets/{context['market']}"
            f"/boards/{context['board']}/securities/{symbol}/candles.json"
        )
        block = payload(
            page,
            path,
            {
                "interval": page["interval"],
                "from": manifest["window"]["date_from"],
                "till": manifest["window"]["date_till"],
                "start": page["page_start"],
            },
        )["candles"]
        count = len(block["data"])
        if block["columns"] != page["columns"] or count != page["source_row_count"]:
            raise ValueError("original page structure changed")
        offsets[key] = page["page_start"] + count
        total += count
        if count == 0:
            if offsets[key] == 0:
                raise ValueError("unverified-empty source slice cannot be admitted")
            terminals.add(key)
    expected = {(b["source_symbol"], tf) for b in assignments["bindings"] for tf in ["1m", "1d"]}
    if terminals != expected or total != manifest["source_rows"]:
        raise ValueError("incomplete original source graph")
    return manifest, assignments
