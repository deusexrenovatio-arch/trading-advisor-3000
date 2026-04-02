from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from trading_advisor_3000.product_plane.contracts import DecisionCandidate, DecisionPublication, RuntimeSignal, SignalEvent
from trading_advisor_3000.product_plane.research.ids import candidate_id_from_candidate

from .store import _publication_dedup_key, _publication_event_type

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - optional dependency path
    psycopg = None
    dict_row = None


def _event_id(seed: str) -> str:
    return "SEVT-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12].upper()


def _parse_ts(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _format_ts(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    raise TypeError(f"unsupported timestamp type: {type(value)!r}")


class PostgresSignalStore:
    def __init__(self, *, dsn: str, schema_name: str = "signal") -> None:
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required to use PostgresSignalStore")
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must be non-empty string")
        self._dsn = dsn
        self._schema_name = schema_name

    def _connect(self):
        assert psycopg is not None
        assert dict_row is not None
        return psycopg.connect(self._dsn, autocommit=False, row_factory=dict_row)

    def _signals_table(self) -> str:
        return f"{self._schema_name}.active_signals"

    def _events_table(self) -> str:
        return f"{self._schema_name}.signal_events"

    def _publications_table(self) -> str:
        return f"{self._schema_name}.publications"

    @staticmethod
    def _signal_from_row(row: dict[str, object]) -> RuntimeSignal:
        return RuntimeSignal.from_dict(
            {
                "signal_id": row["signal_id"],
                "strategy_version_id": row["strategy_version_id"],
                "contract_id": row["contract_id"],
                "mode": row["mode"],
                "side": row["side"],
                "entry_price": row["entry_price"],
                "stop_price": row["stop_price"],
                "target_price": row["target_price"],
                "confidence": row["confidence"],
                "state": row["state"],
                "opened_at": _format_ts(row["opened_at"]),
                "updated_at": _format_ts(row["updated_at"]),
                "expires_at": _format_ts(row.get("expires_at")),
                "publication_message_id": row.get("publication_message_id"),
            }
        )

    @staticmethod
    def _event_from_row(row: dict[str, object]) -> SignalEvent:
        payload = row.get("payload_json")
        if isinstance(payload, str):
            payload_json = json.loads(payload)
        elif isinstance(payload, dict):
            payload_json = {str(key): value for key, value in payload.items()}
        else:
            payload_json = {}
        return SignalEvent.from_dict(
            {
                "event_id": row["event_id"],
                "signal_id": row["signal_id"],
                "event_ts": _format_ts(row["event_ts"]),
                "event_type": row["event_type"],
                "reason_code": row["reason_code"],
                "payload_json": payload_json,
            }
        )

    @staticmethod
    def _publication_from_row(row: dict[str, object]) -> DecisionPublication:
        return DecisionPublication.from_dict(
            {
                "publication_id": row["publication_id"],
                "signal_id": row["signal_id"],
                "channel": row["channel"],
                "message_id": row["message_id"],
                "publication_type": row["publication_type"],
                "status": row["status"],
                "published_at": _format_ts(row["published_at"]),
            }
        )

    def _fetch_signal(self, cur, signal_id: str) -> RuntimeSignal | None:
        cur.execute(
            f"""
            SELECT
                signal_id,
                strategy_version_id,
                contract_id,
                mode,
                side,
                entry_price,
                stop_price,
                target_price,
                confidence,
                state,
                opened_at,
                updated_at,
                expires_at,
                publication_message_id
            FROM {self._signals_table()}
            WHERE signal_id = %s
            """,
            (signal_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return self._signal_from_row(row)

    def _insert_event(
        self,
        cur,
        *,
        signal_id: str,
        event_ts: str,
        event_type: str,
        reason_code: str,
        payload_json: dict[str, object],
        dedup_key: str,
    ) -> None:
        cur.execute(
            f"""
            INSERT INTO {self._events_table()} (
                event_id,
                signal_id,
                event_ts,
                event_type,
                reason_code,
                payload_json,
                dedup_key
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (dedup_key) DO NOTHING
            """,
            (
                _event_id(dedup_key),
                signal_id,
                _parse_ts(event_ts),
                event_type,
                reason_code,
                json.dumps(payload_json, ensure_ascii=False, sort_keys=True),
                dedup_key,
            ),
        )

    def _insert_publication(self, cur, publication: DecisionPublication) -> None:
        cur.execute(
            f"""
            INSERT INTO {self._publications_table()} (
                publication_id,
                signal_id,
                channel,
                message_id,
                publication_type,
                status,
                published_at,
                dedup_key
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dedup_key) DO NOTHING
            """,
            (
                publication.publication_id,
                publication.signal_id,
                publication.channel,
                publication.message_id,
                publication.publication_type.value,
                publication.status.value,
                _parse_ts(publication.published_at),
                _publication_dedup_key(publication),
            ),
        )

    def _upsert_signal(self, cur, signal: RuntimeSignal) -> None:
        cur.execute(
            f"""
            INSERT INTO {self._signals_table()} (
                signal_id,
                strategy_version_id,
                contract_id,
                mode,
                side,
                entry_price,
                stop_price,
                target_price,
                confidence,
                state,
                opened_at,
                updated_at,
                expires_at,
                publication_message_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (signal_id) DO UPDATE SET
                strategy_version_id = EXCLUDED.strategy_version_id,
                contract_id = EXCLUDED.contract_id,
                mode = EXCLUDED.mode,
                side = EXCLUDED.side,
                entry_price = EXCLUDED.entry_price,
                stop_price = EXCLUDED.stop_price,
                target_price = EXCLUDED.target_price,
                confidence = EXCLUDED.confidence,
                state = EXCLUDED.state,
                opened_at = EXCLUDED.opened_at,
                updated_at = EXCLUDED.updated_at,
                expires_at = EXCLUDED.expires_at,
                publication_message_id = EXCLUDED.publication_message_id
            """,
            (
                signal.signal_id,
                signal.strategy_version_id,
                signal.contract_id,
                signal.mode.value,
                signal.side.value,
                signal.entry_price,
                signal.stop_price,
                signal.target_price,
                signal.confidence,
                signal.state,
                _parse_ts(signal.opened_at),
                _parse_ts(signal.updated_at),
                _parse_ts(signal.expires_at),
                signal.publication_message_id,
            ),
        )

    def upsert_candidate(self, candidate: DecisionCandidate, *, expires_at: str | None) -> tuple[RuntimeSignal, bool]:
        candidate_payload = {
            **candidate.to_dict(),
            "candidate_id": candidate_id_from_candidate(candidate),
        }
        with self._connect() as conn:
            with conn.cursor() as cur:
                existing = self._fetch_signal(cur, candidate.signal_id)
                if existing is None:
                    signal = RuntimeSignal(
                        signal_id=candidate.signal_id,
                        strategy_version_id=candidate.strategy_version_id,
                        contract_id=candidate.contract_id,
                        mode=candidate.mode,
                        side=candidate.side,
                        entry_price=candidate.entry_ref,
                        stop_price=candidate.stop_ref,
                        target_price=candidate.target_ref,
                        confidence=candidate.confidence,
                        state="candidate",
                        opened_at=candidate.ts_decision,
                        updated_at=candidate.ts_decision,
                        expires_at=expires_at,
                        publication_message_id=None,
                    )
                    self._upsert_signal(cur, signal)
                    self._insert_event(
                        cur,
                        signal_id=candidate.signal_id,
                        event_ts=candidate.ts_decision,
                        event_type="signal_opened",
                        reason_code="candidate_created",
                        payload_json=candidate_payload,
                        dedup_key=f"{candidate.signal_id}|opened|{candidate.ts_decision}",
                    )
                    return signal, True

                if (
                    existing.side == candidate.side
                    and abs(existing.entry_price - candidate.entry_ref) < 1e-9
                    and abs(existing.stop_price - candidate.stop_ref) < 1e-9
                    and abs(existing.target_price - candidate.target_ref) < 1e-9
                    and abs(existing.confidence - candidate.confidence) < 1e-9
                    and existing.updated_at == candidate.ts_decision
                ):
                    return existing, False

                updated = RuntimeSignal(
                    signal_id=existing.signal_id,
                    strategy_version_id=existing.strategy_version_id,
                    contract_id=existing.contract_id,
                    mode=existing.mode,
                    side=candidate.side,
                    entry_price=candidate.entry_ref,
                    stop_price=candidate.stop_ref,
                    target_price=candidate.target_ref,
                    confidence=candidate.confidence,
                    state=existing.state,
                    opened_at=existing.opened_at,
                    updated_at=candidate.ts_decision,
                    expires_at=expires_at if expires_at is not None else existing.expires_at,
                    publication_message_id=existing.publication_message_id,
                )
                self._upsert_signal(cur, updated)
                self._insert_event(
                    cur,
                    signal_id=candidate.signal_id,
                    event_ts=candidate.ts_decision,
                    event_type="signal_updated",
                    reason_code="candidate_upsert",
                    payload_json=candidate_payload,
                    dedup_key=(
                        f"{candidate.signal_id}|updated|{candidate.ts_decision}|"
                        f"{candidate.side.value}|{candidate.confidence:.8f}"
                    ),
                )
                return updated, True

    def mark_published(self, *, signal_id: str, publication: DecisionPublication) -> RuntimeSignal:
        with self._connect() as conn:
            with conn.cursor() as cur:
                existing = self._fetch_signal(cur, signal_id)
                if existing is None:
                    raise ValueError(f"signal not found: {signal_id}")
                updated = RuntimeSignal(
                    signal_id=existing.signal_id,
                    strategy_version_id=existing.strategy_version_id,
                    contract_id=existing.contract_id,
                    mode=existing.mode,
                    side=existing.side,
                    entry_price=existing.entry_price,
                    stop_price=existing.stop_price,
                    target_price=existing.target_price,
                    confidence=existing.confidence,
                    state="active" if existing.state in {"candidate", "active"} else existing.state,
                    opened_at=existing.opened_at,
                    updated_at=publication.published_at,
                    expires_at=existing.expires_at,
                    publication_message_id=publication.message_id,
                )
                self._upsert_signal(cur, updated)
                self._insert_event(
                    cur,
                    signal_id=signal_id,
                    event_ts=publication.published_at,
                    event_type=_publication_event_type(publication),
                    reason_code=publication.status.value,
                    payload_json=publication.to_dict(),
                    dedup_key=f"{signal_id}|publication|{_publication_dedup_key(publication)}",
                )
                self._insert_publication(cur, publication)
                return updated

    def close_signal(
        self,
        *,
        signal_id: str,
        closed_at: str,
        reason_code: str,
        publication: DecisionPublication | None = None,
    ) -> RuntimeSignal:
        with self._connect() as conn:
            with conn.cursor() as cur:
                existing = self._fetch_signal(cur, signal_id)
                if existing is None:
                    raise ValueError(f"signal not found: {signal_id}")
                if existing.state == "closed":
                    if publication is not None:
                        self._insert_publication(cur, publication)
                    return existing
                closed = RuntimeSignal(
                    signal_id=existing.signal_id,
                    strategy_version_id=existing.strategy_version_id,
                    contract_id=existing.contract_id,
                    mode=existing.mode,
                    side=existing.side,
                    entry_price=existing.entry_price,
                    stop_price=existing.stop_price,
                    target_price=existing.target_price,
                    confidence=existing.confidence,
                    state="closed",
                    opened_at=existing.opened_at,
                    updated_at=closed_at,
                    expires_at=closed_at,
                    publication_message_id=existing.publication_message_id,
                )
                self._upsert_signal(cur, closed)
                self._insert_event(
                    cur,
                    signal_id=signal_id,
                    event_ts=closed_at,
                    event_type="signal_closed",
                    reason_code=reason_code,
                    payload_json={"state": "closed"},
                    dedup_key=f"{signal_id}|closed|{closed_at}|{reason_code}",
                )
                if publication is not None:
                    self._insert_publication(cur, publication)
                return closed

    def cancel_signal(
        self,
        *,
        signal_id: str,
        canceled_at: str,
        reason_code: str,
        publication: DecisionPublication | None = None,
    ) -> RuntimeSignal:
        with self._connect() as conn:
            with conn.cursor() as cur:
                existing = self._fetch_signal(cur, signal_id)
                if existing is None:
                    raise ValueError(f"signal not found: {signal_id}")
                if existing.state == "canceled":
                    if publication is not None:
                        self._insert_publication(cur, publication)
                    return existing
                canceled = RuntimeSignal(
                    signal_id=existing.signal_id,
                    strategy_version_id=existing.strategy_version_id,
                    contract_id=existing.contract_id,
                    mode=existing.mode,
                    side=existing.side,
                    entry_price=existing.entry_price,
                    stop_price=existing.stop_price,
                    target_price=existing.target_price,
                    confidence=existing.confidence,
                    state="canceled",
                    opened_at=existing.opened_at,
                    updated_at=canceled_at,
                    expires_at=canceled_at,
                    publication_message_id=existing.publication_message_id,
                )
                self._upsert_signal(cur, canceled)
                self._insert_event(
                    cur,
                    signal_id=signal_id,
                    event_ts=canceled_at,
                    event_type="signal_canceled",
                    reason_code=reason_code,
                    payload_json={"state": "canceled"},
                    dedup_key=f"{signal_id}|canceled|{canceled_at}|{reason_code}",
                )
                if publication is not None:
                    self._insert_publication(cur, publication)
                return canceled

    def expire_signal(self, *, signal_id: str, expired_at: str) -> RuntimeSignal:
        with self._connect() as conn:
            with conn.cursor() as cur:
                existing = self._fetch_signal(cur, signal_id)
                if existing is None:
                    raise ValueError(f"signal not found: {signal_id}")
                if existing.state == "expired":
                    return existing
                expired = RuntimeSignal(
                    signal_id=existing.signal_id,
                    strategy_version_id=existing.strategy_version_id,
                    contract_id=existing.contract_id,
                    mode=existing.mode,
                    side=existing.side,
                    entry_price=existing.entry_price,
                    stop_price=existing.stop_price,
                    target_price=existing.target_price,
                    confidence=existing.confidence,
                    state="expired",
                    opened_at=existing.opened_at,
                    updated_at=expired_at,
                    expires_at=expired_at,
                    publication_message_id=existing.publication_message_id,
                )
                self._upsert_signal(cur, expired)
                self._insert_event(
                    cur,
                    signal_id=signal_id,
                    event_ts=expired_at,
                    event_type="signal_expired",
                    reason_code="validity_window_elapsed",
                    payload_json={"state": "expired"},
                    dedup_key=f"{signal_id}|expired|{expired_at}",
                )
                return expired

    def record_execution_fill(
        self,
        *,
        signal_id: str,
        event_ts: str,
        fill_id: str,
        role: str,
        contract_id: str,
        qty: int,
        price: float,
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._insert_event(
                    cur,
                    signal_id=signal_id,
                    event_ts=event_ts,
                    event_type="execution_fill",
                    reason_code=role,
                    payload_json={
                        "fill_id": fill_id,
                        "role": role,
                        "contract_id": contract_id,
                        "qty": qty,
                        "price": price,
                    },
                    dedup_key=f"{signal_id}|execution_fill|{fill_id}|{role}",
                )

    def record_context_slice(
        self,
        *,
        signal_id: str,
        event_ts: str,
        context_kind: str,
        provider_id: str,
        payload_json: dict[str, object],
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                self._insert_event(
                    cur,
                    signal_id=signal_id,
                    event_ts=event_ts,
                    event_type="signal_context",
                    reason_code=context_kind,
                    payload_json={
                        "context_kind": context_kind,
                        "provider_id": provider_id,
                        "payload": payload_json,
                    },
                    dedup_key=f"{signal_id}|signal_context|{context_kind}|{provider_id}|{event_ts}",
                )

    def list_active_signals(self) -> list[RuntimeSignal]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        signal_id,
                        strategy_version_id,
                        contract_id,
                        mode,
                        side,
                        entry_price,
                        stop_price,
                        target_price,
                        confidence,
                        state,
                        opened_at,
                        updated_at,
                        expires_at,
                        publication_message_id
                    FROM {self._signals_table()}
                    WHERE state IN ('candidate', 'active')
                    ORDER BY contract_id, updated_at, signal_id
                    """
                )
                return [self._signal_from_row(row) for row in cur.fetchall()]

    def get_signal(self, signal_id: str) -> RuntimeSignal | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                return self._fetch_signal(cur, signal_id)

    def list_signal_events(self) -> list[SignalEvent]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        event_id,
                        signal_id,
                        event_ts,
                        event_type,
                        reason_code,
                        payload_json
                    FROM {self._events_table()}
                    ORDER BY event_ts, event_id
                    """
                )
                return [self._event_from_row(row) for row in cur.fetchall()]

    def list_publications(self) -> list[DecisionPublication]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (signal_id)
                        publication_id,
                        signal_id,
                        channel,
                        message_id,
                        publication_type,
                        status,
                        published_at
                    FROM {self._publications_table()}
                    ORDER BY signal_id, published_at DESC, publication_id DESC
                    """
                )
                rows = [self._publication_from_row(row) for row in cur.fetchall()]
                return sorted(rows, key=lambda publication: (publication.signal_id, publication.published_at))

    def list_publication_events(self) -> list[DecisionPublication]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        publication_id,
                        signal_id,
                        channel,
                        message_id,
                        publication_type,
                        status,
                        published_at
                    FROM {self._publications_table()}
                    ORDER BY published_at, publication_id
                    """
                )
                return [self._publication_from_row(row) for row in cur.fetchall()]
