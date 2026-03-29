# Current stack conformance matrix

Legend:

- **Implemented** — evidenced by dependency surface plus runnable entrypoint / black-box proof.
- **Partial** — real code exists, but default runtime path or executable proof is missing.
- **Scaffold / proxy-only** — contract, manifest, stub, sample artifact, or design placeholder exists, but not real runtime proof.
- **Not evidenced** — declared in spec, but not proven in the reviewed dependency/runtime surfaces.
- **Action** — `implement`, `de-scope-via-ADR`, or `keep`.

| Surface | Declared in spec | Current observed state | Severity | Recommended action | Minimum proof required for closure |
| --- | --- | --- | --- | --- | --- |
| Python | yes | Implemented | Low | keep | package install + runtime tests |
| PostgreSQL / psycopg | yes | Partial: deps, migrations, Postgres store exist; default runtime still allows in-memory path | High | implement | default durable runtime path, restart proof, staging boot |
| Delta Lake | yes | Scaffold / proxy-only: manifest + JSONL sample artifacts | Critical | implement | physical Delta tables with `_delta_log`, readable by Delta runtime |
| Apache Spark | yes | Scaffold / proxy-only: SQL plan builders only | Critical | implement | runnable Spark local job / `pyspark` execution in CI |
| Dagster | yes | Scaffold / proxy-only: asset specs only | High | implement | `Definitions` load + asset materialization proof |
| .NET 8 sidecar | yes | Not implemented in repo | Critical | implement | `.sln/.csproj`, build/test/publish, HTTP smoke against compiled binary |
| StockSharp bridge | yes | Partial: transport contract + Python HTTP adapter + staging stub | Critical | implement | compiled sidecar implementing wire contract |
| FastAPI | yes | Not evidenced | Medium | implement or de-scope-via-ADR | ASGI app boot + HTTP smoke |
| aiogram | yes | Not evidenced; current custom in-memory publication engine | Medium | implement or de-scope-via-ADR | real Telegram adapter smoke with mocked API |
| vectorbt | yes | Not evidenced; custom backtest engine exists | Medium | implement or de-scope-via-ADR | import/use in research path or approved replacement ADR |
| Alembic | yes | Not evidenced; custom migration runner exists | Low/Medium | de-scope-via-ADR or implement | approved migration policy + executable migration proof |
| OpenTelemetry | yes | Not evidenced; current observability is file-bridge based | Medium | implement or de-scope-via-ADR | span/metric export smoke |
| Polars | yes | Not evidenced in reviewed surfaces | Low | de-scope-via-ADR or implement later | dependency + explicit usage |
| DuckDB | yes | Not evidenced in reviewed app runtime surfaces | Low | de-scope-via-ADR or implement later | dependency + explicit usage |
| Prometheus / Grafana / Loki | yes | Implemented as local file-bridge observability profile | Low | keep | compose smoke + dashboard/data-source proof |
| Docker Compose | yes | Implemented for observability/staging profiles | Low | keep | compose up/down smoke |
| GitHub Actions | implied | Implemented | Low | keep | workflow success |

## Required classification rule

Every stack item in the target architecture must be assigned one of exactly four states in the machine-readable registry:

- `implemented`
- `partial`
- `scaffold`
- `removed_by_adr`

Anything else is process debt.

## Architectural priority order

### Close first

1. Delta Lake
2. Spark
3. Dagster
4. Default durable runtime path
5. Real .NET sidecar

### Decide next

1. FastAPI
2. aiogram
3. vectorbt
4. OpenTelemetry
5. Alembic

### Optional after architecture is honest

1. Polars
2. DuckDB

## Release policy

No future “full module DoD”, “live-ready”, or “production-ready” language may be used while any **architecture-critical** surface remains `partial` or `scaffold`.
