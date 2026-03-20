from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib import request as urllib_request

from .registry import DataProviderRegistry, DataProviderSpec


PHASE9_PROVIDER_ROLES = {"historical_source", "live_feed"}
KNOWN_SESSION_STATES = {"open", "auction", "closed", "halted"}


def _require_non_empty(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty string")
    return value.strip()


def _parse_iso_utc(value: str, *, name: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise ValueError(f"{name} must be valid ISO-8601 timestamp") from exc


def _coerce_number(value: object, *, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric when present")
    return float(value)


@dataclass(frozen=True)
class Phase9ProviderContract:
    provider_id: str
    external_system: str
    role: str
    transport_kind: str
    required_env_names: tuple[str, ...]
    freshness_window_seconds: int | None
    session_timezone: str
    supports_incremental: bool
    supports_replay: bool
    description: str = ""

    def __post_init__(self) -> None:
        if self.role not in PHASE9_PROVIDER_ROLES:
            raise ValueError(f"unsupported phase9 provider role: {self.role}")
        if self.freshness_window_seconds is not None and self.freshness_window_seconds <= 0:
            raise ValueError("freshness_window_seconds must be positive when present")
        _require_non_empty("provider_id", self.provider_id)
        _require_non_empty("external_system", self.external_system)
        _require_non_empty("transport_kind", self.transport_kind)
        _require_non_empty("session_timezone", self.session_timezone)

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "external_system": self.external_system,
            "role": self.role,
            "transport_kind": self.transport_kind,
            "required_env_names": list(self.required_env_names),
            "freshness_window_seconds": self.freshness_window_seconds,
            "session_timezone": self.session_timezone,
            "supports_incremental": self.supports_incremental,
            "supports_replay": self.supports_replay,
            "description": self.description,
        }


@dataclass(frozen=True)
class Phase9PilotUniverse:
    universe_id: str
    instrument_ids: tuple[str, ...]
    contract_ids: tuple[str, ...]
    timeframes: tuple[str, ...]
    session_timezone: str
    roll_rule: str
    historical_provider_id: str
    live_provider_id: str

    def __post_init__(self) -> None:
        _require_non_empty("universe_id", self.universe_id)
        _require_non_empty("session_timezone", self.session_timezone)
        _require_non_empty("roll_rule", self.roll_rule)
        _require_non_empty("historical_provider_id", self.historical_provider_id)
        _require_non_empty("live_provider_id", self.live_provider_id)
        if not self.instrument_ids:
            raise ValueError("instrument_ids must be non-empty")
        if not self.contract_ids:
            raise ValueError("contract_ids must be non-empty")
        if not self.timeframes:
            raise ValueError("timeframes must be non-empty")

    def whitelist_contracts(self) -> set[str]:
        return set(self.contract_ids)

    def to_dict(self) -> dict[str, object]:
        return {
            "universe_id": self.universe_id,
            "instrument_ids": list(self.instrument_ids),
            "contract_ids": list(self.contract_ids),
            "timeframes": list(self.timeframes),
            "session_timezone": self.session_timezone,
            "roll_rule": self.roll_rule,
            "historical_provider_id": self.historical_provider_id,
            "live_provider_id": self.live_provider_id,
        }


@dataclass(frozen=True)
class Phase9LiveFeedObservation:
    contract_id: str
    event_ts: str
    session_state: str
    last_price: float | None = None

    def __post_init__(self) -> None:
        _require_non_empty("contract_id", self.contract_id)
        _parse_iso_utc(self.event_ts, name="event_ts")
        if self.session_state not in KNOWN_SESSION_STATES:
            raise ValueError(f"unsupported session_state: {self.session_state}")
        if self.last_price is not None and self.last_price <= 0:
            raise ValueError("last_price must be positive when present")

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "event_ts": self.event_ts,
            "session_state": self.session_state,
            "last_price": self.last_price,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "Phase9LiveFeedObservation":
        if not isinstance(payload, dict):
            raise ValueError("live observation must be object")
        return cls(
            contract_id=_require_non_empty("contract_id", str(payload.get("contract_id", ""))),
            event_ts=_require_non_empty("event_ts", str(payload.get("event_ts", ""))),
            session_state=_require_non_empty("session_state", str(payload.get("session_state", ""))),
            last_price=_coerce_number(payload.get("last_price"), name="last_price"),
        )


def phase9_data_provider_registry() -> DataProviderRegistry:
    return DataProviderRegistry(
        providers=[
            DataProviderSpec(
                provider_id="moex-history",
                provider_kind="market",
                asset_classes=("futures",),
                supports_incremental=True,
                supports_replay=True,
                latency_profile="batch-daily",
                description="Phase 9 historical provider freeze for MOEX pilot backfill",
            ),
            DataProviderSpec(
                provider_id="quik-live",
                provider_kind="market",
                asset_classes=("futures",),
                supports_incremental=True,
                supports_replay=False,
                latency_profile="intraday-live",
                description="Phase 9 live feed freeze for QUIK pilot battle runs",
            ),
        ]
    )


def default_phase9_provider_contracts() -> dict[str, Phase9ProviderContract]:
    return {
        "moex-history": Phase9ProviderContract(
            provider_id="moex-history",
            external_system="MOEX",
            role="historical_source",
            transport_kind="moex-iss-http",
            required_env_names=(),
            freshness_window_seconds=None,
            session_timezone="Europe/Moscow",
            supports_incremental=True,
            supports_replay=True,
            description="Frozen historical source for Phase 9 pilot bootstrap.",
        ),
        "quik-live": Phase9ProviderContract(
            provider_id="quik-live",
            external_system="QUIK",
            role="live_feed",
            transport_kind="quik-json-snapshot",
            required_env_names=(),
            freshness_window_seconds=90,
            session_timezone="Europe/Moscow",
            supports_incremental=True,
            supports_replay=False,
            description="Frozen primary live market feed for Phase 9 battle runs.",
        ),
    }


def get_phase9_provider_contract(provider_id: str) -> Phase9ProviderContract:
    contracts = default_phase9_provider_contracts()
    try:
        return contracts[provider_id]
    except KeyError as exc:
        raise ValueError(f"unsupported phase9 provider: {provider_id}") from exc


def default_phase9_pilot_universe() -> Phase9PilotUniverse:
    return Phase9PilotUniverse(
        universe_id="phase9-moex-futures-pilot",
        instrument_ids=("BR", "Si"),
        contract_ids=("BR-6.26", "Si-6.26"),
        timeframes=("15m",),
        session_timezone="Europe/Moscow",
        roll_rule="max_open_interest_then_latest_ts_close",
        historical_provider_id="moex-history",
        live_provider_id="quik-live",
    )


def build_phase9_dataset_version(
    *,
    provider_id: str,
    pilot_universe: Phase9PilotUniverse,
    watermark_by_key: dict[str, str],
) -> str:
    if not watermark_by_key:
        raise ValueError("watermark_by_key must be non-empty")
    normalized = sorted((str(key), str(value)) for key, value in watermark_by_key.items())
    latest_watermark = max(item[1] for item in normalized)
    latest_dt = _parse_iso_utc(latest_watermark, name="watermark")
    suffix = hashlib.sha1(
        json.dumps(
            {
                "provider_id": provider_id,
                "universe_id": pilot_universe.universe_id,
                "watermarks": normalized,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:8]
    return (
        f"{pilot_universe.universe_id}-{provider_id}-"
        f"{latest_dt.strftime('%Y%m%dT%H%M%SZ')}-{suffix}"
    )


def _parse_phase9_live_snapshot_payload(
    payload: object,
) -> tuple[str | None, list[Phase9LiveFeedObservation]]:
    provider_id: str | None = None
    rows_payload: object
    if isinstance(payload, dict):
        raw_provider_id = payload.get("provider_id")
        if isinstance(raw_provider_id, str) and raw_provider_id.strip():
            provider_id = raw_provider_id.strip()
        rows_payload = payload.get("rows", [])
    else:
        rows_payload = payload
    if not isinstance(rows_payload, list):
        raise ValueError("live snapshot rows must be a list")
    rows = [Phase9LiveFeedObservation.from_dict(item) for item in rows_payload]
    return provider_id, rows


def load_phase9_live_snapshot(
    path: Path,
) -> tuple[str | None, list[Phase9LiveFeedObservation]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _parse_phase9_live_snapshot_payload(payload)


def load_phase9_live_snapshot_from_url(
    url: str,
    *,
    timeout_seconds: float = 10.0,
) -> tuple[str | None, list[Phase9LiveFeedObservation]]:
    request = urllib_request.Request(
        url=url,
        method="GET",
        headers={"Accept": "application/json"},
    )
    with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
        payload = json.load(response)
    return _parse_phase9_live_snapshot_payload(payload)


def evaluate_phase9_live_smoke(
    *,
    provider_id: str,
    snapshot_rows: list[Phase9LiveFeedObservation],
    as_of_ts: str,
    pilot_universe: Phase9PilotUniverse | None = None,
    max_lag_seconds: int | None = None,
) -> dict[str, object]:
    provider = get_phase9_provider_contract(provider_id)
    if provider.role != "live_feed":
        raise ValueError(f"provider is not configured as live feed: {provider_id}")
    universe = pilot_universe or default_phase9_pilot_universe()
    if universe.live_provider_id != provider_id:
        raise ValueError("pilot universe live_provider_id does not match provider_id")

    threshold_seconds = max_lag_seconds or provider.freshness_window_seconds or 90
    as_of = _parse_iso_utc(as_of_ts, name="as_of_ts")
    latest_by_contract: dict[str, Phase9LiveFeedObservation] = {}
    for row in snapshot_rows:
        current = latest_by_contract.get(row.contract_id)
        if current is None:
            latest_by_contract[row.contract_id] = row
            continue
        if _parse_iso_utc(row.event_ts, name="event_ts") > _parse_iso_utc(current.event_ts, name="event_ts"):
            latest_by_contract[row.contract_id] = row

    expected_contract_ids = list(universe.contract_ids)
    missing_contract_ids: list[str] = []
    stale_contract_ids: list[str] = []
    invalid_session_contract_ids: list[str] = []
    rows_report: list[dict[str, object]] = []

    for contract_id in expected_contract_ids:
        observation = latest_by_contract.get(contract_id)
        if observation is None:
            missing_contract_ids.append(contract_id)
            continue
        event_dt = _parse_iso_utc(observation.event_ts, name="event_ts")
        lag_seconds = int((as_of - event_dt).total_seconds())
        is_stale = lag_seconds > threshold_seconds
        if is_stale:
            stale_contract_ids.append(contract_id)
        if observation.session_state != "open":
            invalid_session_contract_ids.append(contract_id)
        rows_report.append(
            {
                "contract_id": contract_id,
                "event_ts": observation.event_ts,
                "session_state": observation.session_state,
                "lag_seconds": lag_seconds,
                "is_stale": is_stale,
                "last_price": observation.last_price,
            }
        )

    status = "ok"
    if missing_contract_ids or stale_contract_ids or invalid_session_contract_ids:
        status = "degraded"

    return {
        "status": status,
        "provider": provider.to_dict(),
        "pilot_universe": universe.to_dict(),
        "as_of_ts": as_of_ts,
        "max_lag_seconds": threshold_seconds,
        "expected_contract_ids": expected_contract_ids,
        "missing_contract_ids": missing_contract_ids,
        "stale_contract_ids": stale_contract_ids,
        "invalid_session_contract_ids": invalid_session_contract_ids,
        "rows": rows_report,
    }
