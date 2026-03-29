from __future__ import annotations

from pathlib import Path

from trading_advisor_3000.spark_jobs.canonical_bars_job import _spark_runtime_dirs


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
