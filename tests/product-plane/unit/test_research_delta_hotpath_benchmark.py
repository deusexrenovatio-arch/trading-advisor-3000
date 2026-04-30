from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_benchmark_module():
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "research_delta_hotpath_benchmark.py"
    spec = importlib.util.spec_from_file_location("research_delta_hotpath_benchmark", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_benchmark_default_output_dir_is_noncanonical_benchmark_zone() -> None:
    module = _load_benchmark_module()

    output_dir = module._default_output_dir(
        dataset_version="dataset-v1",
        strategy_label="trend-mtf-pullback-v1",
        run_stamp="20260429T000000Z",
    )

    assert "_benchmarks" in output_dir.parts
    assert output_dir.parent == module.BENCHMARK_OUTPUT_ROOT
    assert output_dir.name == "dataset_v1_trend_mtf_pullback_v1_optuna_owned_20260429T000000Z"


def test_benchmark_rejects_plain_research_runs_output_dir() -> None:
    module = _load_benchmark_module()
    canonical_run_like_dir = module.DEFAULT_RESEARCH_RUNS_ROOT / "trend_mtf_pullback_full_20260429"

    with pytest.raises(ValueError, match="canonical research/runs"):
        module._validate_benchmark_output_dir(canonical_run_like_dir)


def test_benchmark_allows_explicit_benchmark_output_dir() -> None:
    module = _load_benchmark_module()
    benchmark_dir = module.BENCHMARK_OUTPUT_ROOT / "trend_mtf_pullback_speed_20260429"

    module._validate_benchmark_output_dir(benchmark_dir)
