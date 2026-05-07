from __future__ import annotations

from pathlib import Path

import yaml

from trading_advisor_3000.dagster_defs import (
    MOEX_BASELINE_DAILY_SCHEDULE_NAME,
    MOEX_BASELINE_UPDATE_JOB_NAME,
    MOEX_HISTORICAL_CUTOVER_JOB_NAME,
    PRODUCT_PLANE_NIGHTLY_JOB_NAMES,
    RESEARCH_BACKTEST_JOB_NAME,
    RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME,
    RESEARCH_DATA_PREP_JOB_NAME,
    RESEARCH_PROJECTION_JOB_NAME,
    STRATEGY_REGISTRY_REFRESH_JOB_NAME,
    assert_product_plane_definitions_executable,
    build_product_plane_definitions,
)


def test_product_plane_definitions_wire_nightly_baseline_to_research_refresh() -> None:
    definitions = build_product_plane_definitions()
    assert_product_plane_definitions_executable(definitions)

    repository = definitions.get_repository_def()
    job_names = {job.name for job in repository.get_all_jobs()}
    assert set(PRODUCT_PLANE_NIGHTLY_JOB_NAMES).issubset(job_names)
    assert MOEX_BASELINE_UPDATE_JOB_NAME in job_names
    assert MOEX_HISTORICAL_CUTOVER_JOB_NAME in job_names
    assert RESEARCH_DATA_PREP_JOB_NAME in job_names
    assert STRATEGY_REGISTRY_REFRESH_JOB_NAME in job_names
    assert RESEARCH_BACKTEST_JOB_NAME in job_names
    assert RESEARCH_PROJECTION_JOB_NAME in job_names

    schedule_names = {schedule.name for schedule in repository.schedule_defs}
    sensor_names = {sensor.name for sensor in repository.sensor_defs}
    assert MOEX_BASELINE_DAILY_SCHEDULE_NAME in schedule_names
    assert RESEARCH_DATA_PREP_AFTER_MOEX_SENSOR_NAME in sensor_names


def test_dagster_staging_runtime_loads_product_plane_definitions() -> None:
    compose_path = Path("deployment/docker/dagster-staging/docker-compose.dagster-staging.yml")
    payload = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    services = payload["services"]

    for service_name in ("dagster-webserver", "dagster-daemon"):
        command = services[service_name]["command"]
        attr_index = command.index("-a")
        assert command[attr_index + 1] == "product_plane_definitions"
