# Manual Journal: first local API slice

Change surface: product-plane. Solution Intent: staged. Date: 2026-11-11.
Contract: [Journal v1](docs/architecture/product-plane/trade-workspace/journal-v1.md).
This slice provides manual HTTP entry through FastAPI `/docs`, backed by PostgreSQL.
React forms follow separately. Journal starts independently of RuntimeStack,
market data, Spark, Intake and models. Saving an author plan creates no execution.

## Local database and launch

Python >=3.11; install this checkout's `runtime-api` and `dev-test` extras.
Run from the repository root. The example uses a new, separate PostgreSQL16
container/volume and loopback ports; the password is for this local proof only.

```powershell
docker run --pull=never --name ta3000-journal-pr-20261111 `
  -e POSTGRES_USER=journal -e POSTGRES_PASSWORD=journal_local_test -e POSTGRES_DB=journal_local `
  -p 127.0.0.1:55446:5432 -v ta3000-journal-pr-20261111:/var/lib/postgresql/data -d postgres:16
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
$env:TA3000_JOURNAL_DSN = 'host=127.0.0.1 port=55446 dbname=journal_local user=journal password=journal_local_test'
python -m trading_advisor_3000.product_plane.domain.journal.store migrate
python -m uvicorn trading_advisor_3000.product_plane.interfaces.trade_workspace:create_app --factory --host 127.0.0.1 --port 8106
```

Create the container only once; use `docker start ta3000-journal-pr-20261111`
for subsequent launches. The named volume preserves records. Keep it for restart
and rollback; removing it destroys this local Journal. Existing local Journal
and production databases are not migration targets for this first delivery.

Open [API forms](http://127.0.0.1:8106/docs). `/health` proves HTTP liveness;
`/ready` checks the installed migration set and checksums read-only. Missing or
incompatible migrations produce503; neither startup nor readiness repairs DDL.
Migration001 is preserved from the Stage1 source. Application rollback retains
the database; do not rewrite applied SQL or attempt a destructive down-migration.

## Manual roundtrip

`journal_create.v1.json` and `journal_silver_create.v1.json` under
`tests/product-plane/fixtures/contracts/` are explicit test author records.
POST a CreateTrade to `/api/trade-workspace/v1/trades`; keep its command_id and
payload for retries. Read the returned receipt and GET `/trades/{trade_id}`.
POST `journal_change.v1.json` to `/trades/{trade_id}/changes`, supplying a new
command_id and the current expected_revision. A successful change appends a
revision; GET with `?revision=1` still returns the original snapshot.

The same command/payload returns its receipt. Changed payload under that key
or a stale expected_revision returns409. After an uncertain response, read
`/commands/{command_id}` or retry the original payload/key; never invent a new
key to repeat the same accepted action. Resolve a409 explicitly after reading
the current card. Notes and executions are distinct, and unknown values carry
reasons. Journal does not infer fills, quantity, price or contract from plans.

## Tests, backup and restore

Use separate test and restore databases inside this new container:

```powershell
docker exec ta3000-journal-pr-20261111 createdb -U journal journal_test
docker exec ta3000-journal-pr-20261111 createdb -U journal journal_restore
$env:TA3000_JOURNAL_TEST_DSN = 'host=127.0.0.1 port=55446 dbname=journal_test user=journal password=journal_local_test'
python -m pytest tests/product-plane/contracts/test_trade_workspace_contracts.py tests/product-plane/integration/test_trade_workspace_journal.py -q
docker exec ta3000-journal-pr-20261111 pg_dump -U journal -d journal_local -Fc -f /tmp/journal.dump
docker exec ta3000-journal-pr-20261111 pg_restore -U journal -d journal_restore /tmp/journal.dump
```

Restore into an empty separate database, then read the same snapshots and
receipts with a new app instance configured for that database. Compare full
canonical payloads, not only counts. Restart the API/database and repeat pinned
readback. This local proof establishes the bounded manual slice; React,
Intake/model quality, current market compatibility and deployment remain separate.
