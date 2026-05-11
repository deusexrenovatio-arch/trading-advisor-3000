# Dagster Staging

This profile runs the MOEX baseline updater in Dagster.

Active schedule:
- `moex_baseline_daily_update_schedule`

Active daily job:
- `moex_baseline_update_job`

Runtime instances are registered in:
- `deployment/runtime-instances/moex-runtime-instances.v1.yaml`

The active product staging data root is mounted into the containers at the legacy
path and a Docker-readable alias:
- `/ta3000-data/moex-historical`
- `/ta3000-data/staging/product/moex-historical`

On-demand test staging roots are mounted under:
- `/ta3000-data/staging/test/moex-verification`

The default host path is:
- `D:/TA3000-data/trading-advisor-3000-nightly`

The default host path for disposable test staging is:
- `D:/TA3000-data/trading-advisor-3000-verification`

## Start

```bash
docker compose -p dagster-staging -f deployment/docker/dagster-staging/docker-compose.dagster-staging.yml up -d --build
```

## Validate

```bash
docker exec ta3000-dagster-webserver python -c "import trading_advisor_3000.dagster_defs as d; print([s.name for s in d.moex_historical_definitions.schedules])"
```

Expected schedule:
- `moex_baseline_daily_update_schedule`

## Stop

```bash
docker compose -p dagster-staging -f deployment/docker/dagster-staging/docker-compose.dagster-staging.yml down
```
