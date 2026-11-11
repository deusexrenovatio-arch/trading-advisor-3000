from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.conninfo import conninfo_to_dict

from trading_advisor_3000.product_plane.contracts.trade_workspace import ChangeTrade, CreateTrade
from trading_advisor_3000.product_plane.domain.journal.policy import JournalError
from trading_advisor_3000.product_plane.domain.journal.store import PostgresJournal
from trading_advisor_3000.product_plane.interfaces.trade_workspace import PREFIX, create_app

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/contracts"
SOURCE = {"source_type": "manual", "original_text": "test observation", "reference": None}
TIME = "2026-10-12T15:00:00Z"


@pytest.fixture(scope="module")
def store():
    dsn = os.environ.get("TA3000_JOURNAL_TEST_DSN")
    if not dsn:
        pytest.skip("isolated PostgreSQL test DSN required")
    if conninfo_to_dict(dsn).get("dbname") != "journal_test":
        pytest.fail("Journal integration tests require the isolated journal_test database")
    journal = PostgresJournal(dsn)
    journal.migrate()
    journal.migrate()
    return journal


def payload():
    result = json.loads((FIXTURES / "journal_create.v1.json").read_text(encoding="utf-8"))
    result["trade_id"] = "test-" + str(uuid4())
    result["command_id"] = "create-" + result["trade_id"]
    result["card"]["title"] = result["trade_id"]
    return result


def create(store):
    body = payload()
    store.create_trade(CreateTrade.model_validate(body))
    return body["trade_id"]


def command(revision, operations):
    return ChangeTrade.model_validate(
        {
            "schema_version": 1,
            "command_id": str(uuid4()),
            "expected_revision": revision,
            "event_at": TIME,
            "reason": "test change",
            "source": SOURCE,
            "operations": operations,
        }
    )


def note(text="observation", supersedes=None):
    return {
        "kind": "add_entry",
        "entry": {
            "kind": "note",
            "text": text,
            "event_at": TIME,
            "source": SOURCE,
            "execution": None,
            "supersedes_entry_id": supersedes,
        },
    }


def execution(
    action="increase",
    contracts=3,
    supersedes=None,
    event_at=TIME,
    contract_code="TEST-GOLD-DO-NOT-TRADE",
):
    unknown = {"value": None, "reason": "not_provided"}
    return {
        "kind": "add_entry",
        "entry": {
            "kind": "execution",
            "text": "synthetic execution fixture",
            "event_at": event_at,
            "source": SOURCE,
            "supersedes_entry_id": supersedes,
            "execution": {
                "venue": "MOEX",
                "contract_code": contract_code,
                "account": unknown,
                "action": action,
                "contracts": contracts,
                "price": unknown,
                "fee": unknown,
                "currency": unknown,
            },
        },
    }


def test_api_fixture_restart_and_unknown_silver(store):
    body = payload()
    with TestClient(create_app(store)) as client:
        reply = client.post(PREFIX + "/trades", json=body)
        assert reply.status_code == 201
        assert client.get("/ready").status_code == 200
        assert client.post(PREFIX + "/trades", json=body).json() == reply.json()
    with TestClient(create_app(PostgresJournal(store._dsn))) as client:
        saved = client.get(PREFIX + "/trades/" + body["trade_id"]).json()
        assert saved["card"]["entry"]["price"]["value"] == "4170"
        assert saved["execution_summary"]["total_contracts"] is None
        assert saved["card"]["technical_items"][0]["description"]
    silver = json.loads((FIXTURES / "journal_silver_create.v1.json").read_text(encoding="utf-8"))
    silver["trade_id"] = "silver-" + str(uuid4())
    silver["command_id"] = "create-" + silver["trade_id"]
    store.create_trade(CreateTrade.model_validate(silver))
    assert store.get_trade(silver["trade_id"]).card.entry.price.reason == "not_provided"


def test_same_command_normalized_replay_and_conflict(store):
    body = payload()
    original = store.create_trade(CreateTrade.model_validate(body))
    body["card"]["entry"]["price"]["value"] = "04170.00"
    assert store.create_trade(CreateTrade.model_validate(body)) == original
    body["reason"] = "different payload"
    with pytest.raises(JournalError, match="different payload"):
        store.create_trade(CreateTrade.model_validate(body))
    assert store.get_receipt(original.command_id) == original


def test_notes_supersedes_and_immutable_revisions(store):
    trade_id = create(store)
    first = store.apply_change(trade_id, command(1, [note()]))
    second_command = command(2, [note("corrected", first.entry_ids[0])])
    receipt = store.apply_change(trade_id, second_command)
    assert store.apply_change(trade_id, second_command) == receipt
    assert store.get_trade(trade_id, 1).entry_ids == []
    assert len(store.get_trade(trade_id, 2).entry_ids) == 1
    assert len(store.get_trade(trade_id).entry_ids) == 2
    with pytest.raises(JournalError):
        store.apply_change(trade_id, command(3, [note("again", first.entry_ids[0])]))
    for table in ["trade_revisions", "entries", "command_receipts"]:
        with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
            with store.connect() as conn:
                conn.execute(f"DELETE FROM journal.{table} WHERE trade_id=%s", (trade_id,))


def test_atomic_rollback_after_receipt_and_snapshot_insert(store, monkeypatch):
    trade_id = create(store)
    request = command(1, [note()])
    persist = store._persist

    def broken(*args):
        persist(*args)
        raise JournalError("injected_failure", "rollback proof")

    with monkeypatch.context() as context:
        context.setattr(store, "_persist", broken)
        with pytest.raises(JournalError, match="rollback"):
            store.apply_change(trade_id, request)
    assert store.get_trade(trade_id).revision == 1
    assert store.get_trade(trade_id).entry_ids == []
    with pytest.raises(JournalError) as missing:
        store.get_receipt(request.command_id)
    assert missing.value.status == 404
    assert store.apply_change(trade_id, request).revision == 2


def test_parallel_edits_and_parallel_command_retry(store):
    trade_id = create(store)
    changes = [command(1, [note("A")]), command(1, [note("B")])]

    def commit(change):
        try:
            return store.apply_change(trade_id, change).revision
        except JournalError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(commit, changes))
    assert sorted(map(str, results)) == ["2", "revision_conflict"]
    request = command(2, [note("retry")])
    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = list(pool.map(lambda _: store.apply_change(trade_id, request), range(2)))
    assert receipts[0] == receipts[1]
    assert len(store.get_trade(trade_id).entry_ids) == 2


def test_facts_partial_close_and_new_episode(store):
    trade_id = create(store)
    opened = store.apply_change(
        trade_id, command(1, [execution(), {"kind": "set_status", "status": "active"}])
    )
    assert store.get_trade(trade_id).execution_summary.total_contracts == 3
    store.apply_change(
        trade_id, command(2, [execution("reduce", 1, event_at="2026-10-12T16:00:00Z")])
    )
    assert store.get_trade(trade_id).execution_summary.total_contracts == 2
    with pytest.raises(JournalError):
        store.apply_change(trade_id, command(3, [execution("reduce", 3)]))
    store.apply_change(
        trade_id,
        command(
            3,
            [
                execution("reduce", 2, event_at="2026-10-12T17:00:00Z"),
                {"kind": "set_status", "status": "closed"},
            ],
        ),
    )
    with pytest.raises(JournalError):
        store.apply_change(
            trade_id, command(4, [execution(), {"kind": "correct_status", "status": "active"}])
        )
    linked = payload()
    linked["card"].update(related_trade_id=trade_id, relation="roll")
    assert store.create_trade(CreateTrade.model_validate(linked)).revision == 1
    assert opened.revision == 2


@pytest.mark.parametrize(
    "operations",
    [
        [{"kind": "set_status", "status": "active"}],
        [{"kind": "set_status", "status": "closed"}],
        [execution()],
        [note(supersedes="missing-entry")],
    ],
)
def test_invalid_batch_does_not_mutate(store, operations):
    trade_id = create(store)
    with pytest.raises(JournalError):
        store.apply_change(trade_id, command(1, operations))
    assert store.get_trade(trade_id).revision == 1


def test_contract_conflict_late_fact_and_correction(store):
    trade_id = create(store)
    opened = store.apply_change(
        trade_id, command(1, [execution(), {"kind": "set_status", "status": "active"}])
    )
    for invalid in [
        execution(contract_code="SECOND-TEST-CONTRACT"),
        execution("reduce", 1, event_at="2026-10-11T10:00:00Z"),
    ]:
        with pytest.raises(JournalError):
            store.apply_change(trade_id, command(2, [invalid]))
    store.apply_change(
        trade_id, command(2, [execution(contracts=4, supersedes=opened.entry_ids[0])])
    )
    assert store.get_trade(trade_id).execution_summary.total_contracts == 4
    assert store.get_trade(trade_id, 2).execution_summary.total_contracts == 3


def test_100_cards_search_pagination_and_pinned_timeline(store):
    prefix = "page-" + str(uuid4())
    for number in range(100):
        body = payload()
        body["card"]["title"] = f"{prefix}-{number}"
        store.create_trade(CreateTrade.model_validate(body))
    ids, cursor = [], None
    while True:
        page = store.list_trades(query=prefix, limit=13, cursor=cursor)
        assert len(page["items"]) <= 13
        ids += [item["trade_id"] for item in page["items"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert len(set(ids)) == 100
    trade_id = ids[0]
    store.apply_change(trade_id, command(1, [note()]))
    first = store.get_timeline(trade_id, limit=1)
    store.apply_change(trade_id, command(2, [note("later")]))
    cursor, items = first["next_cursor"], first["items"]
    while cursor:
        page = store.get_timeline(trade_id, limit=1, cursor=cursor)
        assert page["revision"] == 2
        items += page["items"]
        cursor = page["next_cursor"]
    assert len(items) == 3
    with pytest.raises(JournalError):
        store.list_trades(
            query="other", cursor=store.list_trades(query=prefix, limit=1)["next_cursor"]
        )


def test_api_validation_conflicts_and_storage_failure(store):
    with TestClient(create_app(store)) as client:
        body = payload()
        assert client.post(PREFIX + "/trades", json=body).status_code == 201
        body["reason"] = "changed"
        assert client.post(PREFIX + "/trades", json=body).status_code == 409
        body["card"]["entry"]["price"]["value"] = 4170.0
        invalid = client.post(PREFIX + "/trades", json=body)
        assert invalid.status_code == 422
        assert invalid.json()["code"] == "invalid_input"
        for cursor in ["?", "bnVsbA==", "W10=", "e30="]:
            assert client.get(PREFIX + "/trades", params={"cursor": cursor}).status_code == 422
        assert client.get(PREFIX + "/trades", params={"limit": 101}).status_code == 422
        assert client.get(PREFIX + "/trades/missing").status_code == 404
    with TestClient(
        create_app(PostgresJournal("host=127.0.0.1 port=1 dbname=journal_test"))
    ) as client:
        reply = client.get(PREFIX + "/trades")
        assert reply.status_code == 503
        assert "port=1" not in reply.text


def test_correction_of_old_increase_after_close_is_not_new_entry(store):
    trade_id = create(store)
    opened = store.apply_change(
        trade_id, command(1, [execution(), {"kind": "set_status", "status": "active"}])
    )
    store.apply_change(
        trade_id,
        command(
            2,
            [
                execution("reduce", 3, event_at="2026-10-12T17:00:00Z"),
                {"kind": "set_status", "status": "closed"},
            ],
        ),
    )
    store.apply_change(
        trade_id,
        command(
            3,
            [
                execution(contracts=4, supersedes=opened.entry_ids[0]),
                {"kind": "correct_status", "status": "active"},
            ],
        ),
    )
    assert store.get_trade(trade_id).execution_summary.total_contracts == 1
    assert store.get_trade(trade_id, 3).status == "closed"


@pytest.mark.parametrize("limit", [0, -1, 101, True])
def test_store_bounded_reads_enforced_without_http_layer(store, limit):
    trade_id = create(store)
    for reader in [
        lambda: store.list_trades(limit=limit),
        lambda: store.get_timeline(trade_id, limit=limit),
    ]:
        with pytest.raises(JournalError) as invalid:
            reader()
        assert invalid.value.status == 422


def test_analysis_metadata_edit_does_not_change_execution_identity(store):
    trade_id = create(store)
    store.apply_change(
        trade_id, command(1, [execution(), {"kind": "set_status", "status": "active"}])
    )
    card = store.get_trade(trade_id).card.model_dump(mode="json")
    card["analysis"]["timeframe"] = {"value": "D1", "reason": None}
    store.apply_change(trade_id, command(2, [{"kind": "replace_card", "card": card}]))
    assert store.get_trade(trade_id).card.analysis.timeframe.value == "D1"
    assert store.get_trade(trade_id, 2).card.analysis.timeframe.value is None
    card["analysis"]["symbol"] = "XAGUSD"
    with pytest.raises(JournalError, match="identity"):
        store.apply_change(trade_id, command(3, [{"kind": "replace_card", "card": card}]))


def test_applied_migration_checksum_survives_windows_checkout(store, tmp_path, monkeypatch):
    import trading_advisor_3000.product_plane.domain.journal.store as store_module

    migration = Path(store_module.__file__).parent / "migrations/001_journal.sql"
    copied = tmp_path / "migrations/001_journal.sql"
    copied.parent.mkdir()
    copied.write_bytes(migration.read_text(encoding="utf-8").replace("\n", "\r\n").encode())
    monkeypatch.setattr(store_module, "__file__", str(tmp_path / "store.py"))
    store.migrate()
    store.check_ready()


def test_http_ready_uses_public_journal_operation():
    class ReadyJournal:
        checked = False

        def check_ready(self):
            self.checked = True

    journal = ReadyJournal()
    with TestClient(create_app(journal)) as client:
        assert client.get("/ready").json() == {"service": "manual_journal", "status": "ready"}
    assert journal.checked


@pytest.mark.parametrize(
    "case,expected_code",
    [
        ("missing_table", "migration_required"),
        ("missing_version", "migration_required"),
        ("wrong_version", "migration_version"),
        ("wrong_checksum", "migration_checksum"),
    ],
)
def test_readiness_rejects_incompatible_database_without_repair(store, case, expected_code):
    # Only the explicitly guarded journal_test database is changed, and restored in finally.
    with store.connect() as conn:
        original = conn.execute(
            "SELECT * FROM journal.schema_migrations ORDER BY version"
        ).fetchall()
        if case == "missing_table":
            conn.execute(
                "ALTER TABLE journal.schema_migrations RENAME TO readiness_probe_migrations"
            )
        elif case == "missing_version":
            conn.execute("DELETE FROM journal.schema_migrations")
        elif case == "wrong_version":
            conn.execute("UPDATE journal.schema_migrations SET version='999_unknown.sql'")
        else:
            conn.execute("UPDATE journal.schema_migrations SET sha256='incorrect'")
        damaged = (
            None
            if case == "missing_table"
            else conn.execute("SELECT * FROM journal.schema_migrations ORDER BY version").fetchall()
        )
    try:
        with TestClient(create_app(store)) as client:
            response = client.get("/ready")
            assert response.status_code == 503
            assert response.json()["code"] == expected_code
        with store.connect() as conn:
            if case == "missing_table":
                assert (
                    conn.execute("SELECT to_regclass('journal.schema_migrations')").fetchone()[
                        "to_regclass"
                    ]
                    is None
                )
            else:
                rows = conn.execute(
                    "SELECT * FROM journal.schema_migrations ORDER BY version"
                ).fetchall()
                assert rows == damaged
    finally:
        with store.connect() as conn:
            if case == "missing_table":
                conn.execute(
                    "ALTER TABLE journal.readiness_probe_migrations RENAME TO schema_migrations"
                )
            else:
                conn.execute("DELETE FROM journal.schema_migrations")
                for row in original:
                    conn.execute(
                        "INSERT INTO journal.schema_migrations(version,sha256,applied_at) "
                        "VALUES (%s,%s,%s)",
                        (row["version"], row["sha256"], row["applied_at"]),
                    )


def test_readiness_rejects_missing_new_migration(store, tmp_path, monkeypatch):
    import trading_advisor_3000.product_plane.domain.journal.store as store_module

    source = Path(store_module.__file__).parent / "migrations/001_journal.sql"
    target = tmp_path / "migrations"
    target.mkdir()
    (target / source.name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    (target / "002_future.sql").write_text("SELECT 1;", encoding="utf-8")
    monkeypatch.setattr(store_module, "__file__", str(tmp_path / "store.py"))
    with TestClient(create_app(store)) as client:
        response = client.get("/ready")
        assert response.status_code == 503
        assert response.json()["code"] == "migration_required"


def test_status_filter_and_desc_timeline_are_bounded_and_pinned(store):
    body = payload()
    needle = body["card"]["title"]
    trade_id = body["trade_id"]
    store.create_trade(CreateTrade.model_validate(body))
    store.apply_change(trade_id, command(1, [note("first")]))
    store.apply_change(trade_id, command(2, [note("second")]))
    with TestClient(create_app(store)) as client:
        first = client.get(
            PREFIX + f"/trades/{trade_id}/timeline", params={"order": "desc", "limit": 1}
        ).json()
        assert first["items"][0]["text"] == "second"
        store.apply_change(trade_id, command(3, [note("newer")]))
        second = client.get(
            PREFIX + f"/trades/{trade_id}/timeline",
            params={"order": "desc", "limit": 1, "cursor": first["next_cursor"]},
        ).json()
        assert second["revision"] == 3
        assert second["items"][0]["kind"] == "revision"
        assert (
            client.get(
                PREFIX + f"/trades/{trade_id}/timeline", params={"cursor": first["next_cursor"]}
            ).status_code
            == 422
        )
        assert (
            client.get(PREFIX + "/trades", params={"query": needle, "status": "active"}).json()[
                "items"
            ]
            == []
        )
        assert (
            len(
                client.get(PREFIX + "/trades", params={"query": needle, "status": "draft"}).json()[
                    "items"
                ]
            )
            == 1
        )
    store.apply_change(
        trade_id, command(4, [execution(), {"kind": "set_status", "status": "active"}])
    )
    assert len(store.list_trades(query=needle, status="active")["items"]) == 1
    with pytest.raises(JournalError):
        store.list_trades(status="invalid-status")
    with pytest.raises(JournalError):
        store.get_timeline(trade_id, order="invalid-order")


def test_manual_root_opens_public_api_docs():
    with TestClient(create_app()) as client:
        response = client.get("/", follow_redirects=False)
        assert response.status_code == 307 and response.headers["location"] == "/docs"
        assert client.get("/docs").status_code == 200
        assert PREFIX + "/trades" in client.get("/openapi.json").json()["paths"]
