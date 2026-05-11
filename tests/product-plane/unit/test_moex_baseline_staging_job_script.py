from __future__ import annotations

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_moex_baseline_update_staging_job.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location(
        "run_moex_baseline_update_staging_job", SCRIPT_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load script module: {SCRIPT_PATH.as_posix()}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_seed_fixture(root: Path) -> None:
    raw_log = (
        root / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta" / "_delta_log"
    )
    canonical_log = (
        root / "canonical" / "moex" / "baseline-4y-current" / "canonical_bars.delta" / "_delta_log"
    )
    raw_log.mkdir(parents=True)
    canonical_log.mkdir(parents=True)
    (raw_log / "00000000000000000000.json").write_text("{}", encoding="utf-8")
    (canonical_log / "00000000000000000000.json").write_text("{}", encoding="utf-8")


def test_staging_job_seed_copies_baseline_raw_and_canonical_trees(tmp_path: Path) -> None:
    module = _load_script_module()
    source_root = tmp_path / "source-root"
    target_root = tmp_path / "target-root"
    _write_seed_fixture(source_root)

    report = module._seed_baseline_root(
        source_root=source_root,
        target_root=target_root,
        overwrite=False,
    )

    assert report["source_root"] == source_root.as_posix()
    assert (
        target_root
        / "raw"
        / "moex"
        / "baseline-4y-current"
        / "raw_moex_history.delta"
        / "_delta_log"
    ).exists()
    assert (
        target_root
        / "canonical"
        / "moex"
        / "baseline-4y-current"
        / "canonical_bars.delta"
        / "_delta_log"
    ).exists()


def test_staging_job_seed_rejects_non_empty_target_without_overwrite(tmp_path: Path) -> None:
    module = _load_script_module()
    source_root = tmp_path / "source-root"
    target_root = tmp_path / "target-root"
    _write_seed_fixture(source_root)
    _write_seed_fixture(target_root)

    try:
        module._seed_baseline_root(
            source_root=source_root,
            target_root=target_root,
            overwrite=False,
        )
    except FileExistsError as exc:
        assert "--overwrite-seed" in str(exc)
    else:
        raise AssertionError("expected non-empty seed target to be rejected")


def test_staging_job_seed_allows_precreated_empty_target(tmp_path: Path) -> None:
    module = _load_script_module()
    source_root = tmp_path / "source-root"
    target_root = tmp_path / "target-root"
    _write_seed_fixture(source_root)
    (target_root / "raw" / "moex" / "baseline-4y-current" / "raw_moex_history.delta").mkdir(
        parents=True
    )
    (target_root / "canonical" / "moex" / "baseline-4y-current").mkdir(parents=True)

    module._seed_baseline_root(
        source_root=source_root,
        target_root=target_root,
        overwrite=False,
    )

    assert (
        target_root
        / "raw"
        / "moex"
        / "baseline-4y-current"
        / "raw_moex_history.delta"
        / "_delta_log"
    ).exists()
