import importlib.util
from pathlib import Path


def _load_benchmark_module():
    repo_root = Path(__file__).resolve().parents[3]
    script_path = repo_root / "scripts" / "run_moex_cf_catch_up_update_benchmark.py"
    spec = importlib.util.spec_from_file_location(
        "run_moex_cf_catch_up_update_benchmark", script_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_moex_cf_catch_up_benchmark_groups_all_windows_by_default() -> None:
    module = _load_benchmark_module()
    windows = [{"window_hash_sha256": str(index)} for index in range(5)]

    batches = module._batch_windows(windows, batch_size=0)

    assert batches == [windows]


def test_moex_cf_catch_up_benchmark_respects_explicit_batch_size() -> None:
    module = _load_benchmark_module()
    windows = [{"window_hash_sha256": str(index)} for index in range(5)]

    batches = module._batch_windows(windows, batch_size=2)

    assert batches == [windows[:2], windows[2:4], windows[4:]]
