# Dagster Staging

This profile runs the product-plane nightly data refresh in Dagster.

Active schedule:
- `moex_baseline_daily_update_schedule` (`0 2 * * *`, host-local 02:00)

Active daily baseline job:
- `moex_baseline_update_job`

Active handoff sensor:
- `research_data_prep_after_moex_sensor`

Active downstream refresh job:
- `research_data_prep_job`

The downstream refresh uses the calendar-expiry continuous-front policy by default:
- `front_calendar_expiry_t2_session_0900_2350_v1`

The baseline data root is mounted into the containers at:
- `/ta3000-data/moex-historical`

The default host path is:
- `D:/TA3000-data/trading-advisor-3000-nightly`

## Start

```bash
docker compose -p dagster-staging -f deployment/docker/dagster-staging/docker-compose.dagster-staging.yml up -d --build
```

## Validate

```bash
docker exec ta3000-dagster-webserver python -c "import trading_advisor_3000.dagster_defs as d; r=d.product_plane_definitions.get_repository_def(); print([s.name for s in r.schedule_defs]); print([s.name for s in r.sensor_defs])"
```

Expected schedule:
- `moex_baseline_daily_update_schedule`

Expected sensor:
- `research_data_prep_after_moex_sensor`

## Stop

```bash
docker compose -p dagster-staging -f deployment/docker/dagster-staging/docker-compose.dagster-staging.yml down
```
