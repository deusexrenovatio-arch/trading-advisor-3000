from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from trading_advisor_3000.product_plane.contracts.trade_workspace import (
    ChangeTrade,
    CreateTrade,
    Entry,
    ExecutionSummary,
    Receipt,
    Snapshot,
)

from .policy import JournalError, apply_change


def canonical_hash(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def encode_cursor(value: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()


def decode_cursor(cursor: str | None, scope: dict, keys: set[str]) -> dict | None:
    if cursor is None:
        return None
    try:
        value = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        if set(value) != keys | {"scope"} or value["scope"] != canonical_hash(scope):
            raise ValueError("cursor scope")
        return value
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise JournalError("invalid_cursor", "Cursor does not match this query", 422) from exc


def validate_page(limit: int, cursor: str | None):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise JournalError("invalid_limit", "Page limit must be an integer in 1..100", 422)
    if cursor is not None and (not isinstance(cursor, str) or len(cursor) > 2048):
        raise JournalError("invalid_cursor", "Cursor exceeds bounds", 422)


class PostgresJournal:
    def __init__(self, dsn: str):
        if not dsn.strip():
            raise ValueError("Journal DSN is required")
        self._dsn = dsn

    def connect(self):
        return psycopg.connect(
            self._dsn,
            row_factory=dict_row,
            connect_timeout=5,
            options="-c statement_timeout=10000 -c lock_timeout=5000",
        )

    @staticmethod
    def _migration_sources():
        sources = {}
        for path in sorted((Path(__file__).parent / "migrations").glob("*.sql")):
            content = path.read_text(encoding="utf-8")
            sources[path.name] = (content, hashlib.sha256(content.encode("utf-8")).hexdigest())
        if not sources:
            raise JournalError("migration_required", "Journal migrations are unavailable", 503)
        return sources

    @staticmethod
    def _verify_installed_migrations(installed, sources):
        if set(installed) - set(sources):
            raise JournalError("migration_version", "Database has unknown migration", 503)
        for name, digest in installed.items():
            if digest != sources[name][1]:
                raise JournalError("migration_checksum", "Applied migration has changed", 503)

    def check_ready(self):
        """Read-only schema compatibility check, owned by Journal storage."""
        sources = self._migration_sources()
        with self.connect() as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            table = conn.execute(
                "SELECT to_regclass('journal.schema_migrations') AS name"
            ).fetchone()
            if table["name"] is None:
                raise JournalError("migration_required", "Apply Journal migrations explicitly", 503)
            installed = {
                row["version"]: row["sha256"]
                for row in conn.execute("SELECT version,sha256 FROM journal.schema_migrations")
            }
            self._verify_installed_migrations(installed, sources)
            if set(sources) - set(installed):
                raise JournalError("migration_required", "Apply Journal migrations explicitly", 503)

    def migrate(self):
        sources = self._migration_sources()
        with self.connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(823001012)")
            conn.execute("CREATE SCHEMA IF NOT EXISTS journal")
            conn.execute("""CREATE TABLE IF NOT EXISTS journal.schema_migrations (
                         version TEXT PRIMARY KEY, sha256 TEXT NOT NULL,
                         applied_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
            installed = {
                row["version"]: row["sha256"]
                for row in conn.execute("SELECT version,sha256 FROM journal.schema_migrations")
            }
            self._verify_installed_migrations(installed, sources)
            for name, (content, digest) in sources.items():
                if name in installed:
                    continue
                conn.execute(content)
                conn.execute(
                    "INSERT INTO journal.schema_migrations(version,sha256) VALUES (%s,%s)",
                    (name, digest),
                )

    @staticmethod
    def _receipt(conn, command_id: str, digest: str | None = None):
        row = conn.execute(
            "SELECT payload_hash,receipt FROM journal.command_receipts WHERE command_id=%s",
            (command_id,),
        ).fetchone()
        if row and digest is not None and row["payload_hash"] != digest:
            raise JournalError("command_conflict", "command_id was used with a different payload")
        return Receipt.model_validate(row["receipt"]) if row else None

    @staticmethod
    def _persist(conn, snapshot: Snapshot, entries: list[Entry], command_id: str, digest: str):
        conn.execute(
            "INSERT INTO journal.trade_revisions(trade_id,revision,snapshot) VALUES (%s,%s,%s)",
            (snapshot.trade_id, snapshot.revision, Jsonb(snapshot.model_dump(mode="json"))),
        )
        for entry in entries:
            fact = entry.execution
            conn.execute(
                """INSERT INTO journal.entries
                (entry_id,trade_id,revision,sequence,kind,event_at,recorded_at,contract_code,
                 quantity,price,fee,currency,supersedes_entry_id,payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    entry.entry_id,
                    entry.trade_id,
                    entry.revision,
                    entry.sequence,
                    entry.kind,
                    entry.event_at,
                    entry.recorded_at,
                    fact.contract_code if fact else None,
                    fact.contracts if fact else None,
                    fact.price.value if fact else None,
                    fact.fee.value if fact else None,
                    fact.currency.value if fact else None,
                    entry.supersedes_entry_id,
                    Jsonb(entry.model_dump(mode="json")),
                ),
            )
        contract = snapshot.positions[0].contract_code if snapshot.positions else None
        conn.execute(
            """UPDATE journal.trades SET current_revision=%s,status=%s,symbol=%s,
                     title=%s,direction=%s,contract_code=%s,open_contracts=%s,updated_at=%s
                     WHERE trade_id=%s""",
            (
                snapshot.revision,
                snapshot.status,
                snapshot.card.analysis.symbol,
                snapshot.card.title,
                snapshot.card.direction,
                contract,
                snapshot.execution_summary.total_contracts,
                snapshot.updated_at,
                snapshot.trade_id,
            ),
        )
        receipt = Receipt(
            command_id=command_id,
            trade_id=snapshot.trade_id,
            revision=snapshot.revision,
            entry_ids=[e.entry_id for e in entries],
            recorded_at=snapshot.updated_at,
        )
        conn.execute(
            """INSERT INTO journal.command_receipts
                     (command_id,payload_hash,trade_id,revision,receipt) VALUES (%s,%s,%s,%s,%s)""",
            (
                command_id,
                digest,
                snapshot.trade_id,
                snapshot.revision,
                Jsonb(receipt.model_dump(mode="json")),
            ),
        )
        return receipt

    def create_trade(self, command: CreateTrade) -> Receipt:
        digest = canonical_hash({"operation": "create", "payload": command.model_dump(mode="json")})
        with self.connect() as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (command.command_id,)
            )
            previous = self._receipt(conn, command.command_id, digest)
            if previous:
                return previous
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,1))", (command.trade_id,)
            )
            if conn.execute(
                "SELECT 1 FROM journal.trades WHERE trade_id=%s", (command.trade_id,)
            ).fetchone():
                raise JournalError("trade_exists", "Trade ID already exists")
            related = command.card.related_trade_id
            if related:
                row = conn.execute(
                    "SELECT status FROM journal.trades WHERE trade_id=%s FOR SHARE", (related,)
                ).fetchone()
                if not row or row["status"] != "closed":
                    raise JournalError(
                        "invalid_related_trade", "Linked episode must exist and be closed"
                    )
            now = datetime.now(timezone.utc)
            snapshot = Snapshot(
                trade_id=command.trade_id,
                revision=1,
                card=command.card,
                status=command.status,
                entry_ids=[],
                positions=[],
                execution_summary=ExecutionSummary(
                    status="not_recorded", total_contracts=None, reason="no_facts"
                ),
                created_at=now,
                updated_at=now,
                event_at=command.event_at,
                reason=command.reason,
                source=command.source,
            )
            conn.execute(
                """INSERT INTO journal.trades
                         (trade_id,current_revision,status,symbol,title,direction,created_at,updated_at)
                         VALUES (%s,1,%s,%s,%s,%s,%s,%s)""",
                (
                    snapshot.trade_id,
                    snapshot.status,
                    snapshot.card.analysis.symbol,
                    snapshot.card.title,
                    snapshot.card.direction,
                    now,
                    now,
                ),
            )
            return self._persist(conn, snapshot, [], command.command_id, digest)

    def apply_change(self, trade_id: str, command: ChangeTrade) -> Receipt:
        digest = canonical_hash(
            {
                "operation": "change",
                "trade_id": trade_id,
                "payload": command.model_dump(mode="json"),
            }
        )
        with self.connect() as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (command.command_id,)
            )
            previous = self._receipt(conn, command.command_id, digest)
            if previous:
                return previous
            header = conn.execute(
                "SELECT current_revision FROM journal.trades WHERE trade_id=%s FOR UPDATE",
                (trade_id,),
            ).fetchone()
            if not header:
                raise JournalError("trade_not_found", "Trade not found", 404)
            if header["current_revision"] != command.expected_revision:
                raise JournalError("revision_conflict", "expected_revision is stale")
            row = conn.execute(
                "SELECT snapshot FROM journal.trade_revisions WHERE trade_id=%s AND revision=%s",
                (trade_id, command.expected_revision),
            ).fetchone()
            old = Snapshot.model_validate(row["snapshot"])
            entries = [
                Entry.model_validate(row["payload"])
                for row in conn.execute(
                    "SELECT payload FROM journal.entries WHERE trade_id=%s "
                    "ORDER BY sequence LIMIT 5001",
                    (trade_id,),
                )
            ]
            new, added = apply_change(old, command, entries, datetime.now(timezone.utc))
            return self._persist(conn, new, added, command.command_id, digest)

    def get_trade(self, trade_id: str, revision: int | None = None) -> Snapshot:
        with self.connect() as conn:
            row = conn.execute(
                """SELECT r.snapshot FROM journal.trade_revisions r
                JOIN journal.trades t ON t.trade_id=r.trade_id
                WHERE r.trade_id=%s AND r.revision=COALESCE(%s,t.current_revision)""",
                (trade_id, revision),
            ).fetchone()
            if not row:
                raise JournalError("trade_not_found", "Trade revision not found", 404)
            return Snapshot.model_validate(row["snapshot"])

    def get_receipt(self, command_id: str) -> Receipt:
        with self.connect() as conn:
            receipt = self._receipt(conn, command_id)
            if receipt is None:
                raise JournalError("command_not_found", "Command not found", 404)
            return receipt

    def list_trades(
        self,
        *,
        scope: str = "current",
        query: str = "",
        limit: int = 25,
        cursor: str | None = None,
        status: str | None = None,
    ) -> dict:
        validate_page(limit, cursor)
        if (
            scope not in {"current", "archive", "all"}
            or not isinstance(query, str)
            or len(query) > 100
            or status
            not in {None, "draft", "planned", "active", "closed", "cancelled", "invalidated"}
        ):
            raise JournalError("invalid_filter", "Unsupported list filter", 422)
        filters = {"scope": scope, "query": query}
        if status is not None:
            filters["status"] = status
        after = decode_cursor(cursor, filters, {"at", "id"})
        clauses: list[str] = []
        params: list[object] = []
        if status is not None:
            clauses.append("status=%s")
            params.append(status)
        if scope != "all":
            clauses.append(
                "status "
                + ("NOT IN" if scope == "current" else "IN")
                + " ('closed','cancelled','invalidated')"
            )
        if query:
            literal = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append("(symbol ILIKE %s OR title ILIKE %s OR contract_code ILIKE %s)")
            params += [f"%{literal}%"] * 3
        if after:
            try:
                at = datetime.fromisoformat(after["at"])
                if at.tzinfo is None or not isinstance(after["id"], str):
                    raise ValueError("cursor")
            except (ValueError, TypeError) as exc:
                raise JournalError("invalid_cursor", "Invalid list cursor", 422) from exc
            clauses.append("(created_at,trade_id) > (%s,%s)")
            params += [at, after["id"]]
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM journal.trades" + where + " ORDER BY created_at,trade_id LIMIT %s",
                params + [limit + 1],
            ).fetchall()
        page = rows[:limit]
        next_cursor = None
        if len(rows) > limit:
            last = page[-1]
            next_cursor = encode_cursor(
                {
                    "scope": canonical_hash(filters),
                    "at": last["created_at"].isoformat(),
                    "id": last["trade_id"],
                }
            )
        return {"schema_version": 1, "items": page, "next_cursor": next_cursor}

    def get_timeline(
        self, trade_id: str, *, limit: int = 25, cursor: str | None = None, order: str = "asc"
    ) -> dict:
        validate_page(limit, cursor)
        if order not in {"asc", "desc"}:
            raise JournalError("invalid_order", "Unsupported timeline order", 422)
        filters = {"trade_id": trade_id}
        if order == "desc":
            filters["order"] = order
        after = decode_cursor(cursor, filters, {"revision", "position"})
        snapshot = self.get_trade(trade_id)
        revision, position = snapshot.revision, 0
        if order == "desc":
            position = revision * 10000 + 5001
        if after:
            revision, position = after["revision"], after["position"]
            if (
                type(revision) is not int
                or type(position) is not int
                or not 1 <= revision <= snapshot.revision
                or position < 0
                or position > revision * 10000 + 5000
            ):
                raise JournalError("invalid_cursor", "Invalid timeline cursor", 422)
        with self.connect() as conn:
            comparison = "<" if order == "desc" else ">"
            direction = "DESC" if order == "desc" else "ASC"
            rows = conn.execute(
                """SELECT ordinal,payload FROM (
                SELECT revision::bigint*10000 AS ordinal,
                  jsonb_build_object('kind','revision','revision',revision,
                    'status',snapshot->'status','reason',snapshot->'reason',
                    'event_at',snapshot->'event_at','recorded_at',snapshot->'updated_at') AS payload
                  FROM journal.trade_revisions WHERE trade_id=%s AND revision<=%s
                UNION ALL SELECT revision::bigint*10000+sequence AS ordinal,payload
                  FROM journal.entries WHERE trade_id=%s AND revision<=%s
                ) timeline WHERE ordinal"""
                + comparison
                + "%s ORDER BY ordinal "
                + direction
                + " LIMIT %s",
                (trade_id, revision, trade_id, revision, position, limit + 1),
            ).fetchall()
        next_cursor = None
        if len(rows) > limit:
            next_cursor = encode_cursor(
                {
                    "scope": canonical_hash(filters),
                    "revision": revision,
                    "position": rows[limit - 1]["ordinal"],
                }
            )
        return {
            "schema_version": 1,
            "trade_id": trade_id,
            "revision": revision,
            "items": [row["payload"] for row in rows[:limit]],
            "next_cursor": next_cursor,
        }


def main():
    parser = argparse.ArgumentParser(description="Apply versioned manual Journal migrations")
    parser.add_argument("command", choices=["migrate"])
    parser.parse_args()
    PostgresJournal(os.environ["TA3000_JOURNAL_DSN"]).migrate()
    print("Journal migrations applied")


if __name__ == "__main__":
    main()
