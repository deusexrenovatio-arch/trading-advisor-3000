from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.spark_jobs.canonical_bars_job import (
    _spark_config_overrides_from_env,
    _spark_runtime_dirs,
)


def test_spark_runtime_dirs_follow_env_override(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / "spark-runtime"
    monkeypatch.setenv("TA3000_SPARK_RUNTIME_ROOT", runtime_root.as_posix())

    runtime_dirs = _spark_runtime_dirs()

    assert runtime_dirs == {
        "runtime_root": runtime_root.as_posix(),
        "ivy": (runtime_root / ".ivy2").as_posix(),
        "local_dir": (runtime_root / "local").as_posix(),
    }
    assert (runtime_root / ".ivy2").is_dir()
    assert (runtime_root / "local").is_dir()


def test_spark_runtime_dirs_are_empty_without_env(monkeypatch) -> None:
    monkeypatch.delenv("TA3000_SPARK_RUNTIME_ROOT", raising=False)

    assert _spark_runtime_dirs() == {}


def test_spark_config_overrides_follow_env(monkeypatch) -> None:
    monkeypatch.setenv("TA3000_SPARK_DRIVER_MEMORY", "8g")
    monkeypatch.setenv("TA3000_SPARK_EXECUTOR_MEMORY", "8g")
    monkeypatch.setenv("TA3000_SPARK_DRIVER_MAX_RESULT_SIZE", "2g")
    monkeypatch.setenv("TA3000_SPARK_SQL_SHUFFLE_PARTITIONS", "16")

    assert _spark_config_overrides_from_env() == {
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.driver.maxResultSize": "2g",
        "spark.sql.shuffle.partitions": "16",
    }


def test_spark_config_overrides_ignore_empty_env(monkeypatch) -> None:
    monkeypatch.setenv("TA3000_SPARK_DRIVER_MEMORY", " ")
    monkeypatch.delenv("TA3000_SPARK_EXECUTOR_MEMORY", raising=False)
    monkeypatch.delenv("TA3000_SPARK_DRIVER_MAX_RESULT_SIZE", raising=False)
    monkeypatch.delenv("TA3000_SPARK_SQL_SHUFFLE_PARTITIONS", raising=False)

    assert _spark_config_overrides_from_env() == {}
