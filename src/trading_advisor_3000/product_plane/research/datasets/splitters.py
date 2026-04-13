from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetWindow:
    window_id: str
    train_start: int
    train_stop: int
    test_start: int
    test_stop: int

    def to_dict(self) -> dict[str, int | str]:
        return {
            "window_id": self.window_id,
            "train_start": self.train_start,
            "train_stop": self.train_stop,
            "test_start": self.test_start,
            "test_stop": self.test_stop,
        }


@dataclass(frozen=True)
class HoldoutSplitConfig:
    holdout_ratio: float = 0.2

    def __post_init__(self) -> None:
        if not 0.0 < self.holdout_ratio < 1.0:
            raise ValueError("holdout_ratio must be between 0 and 1")


@dataclass(frozen=True)
class WalkForwardSplitConfig:
    train_size: int
    test_size: int
    step_size: int | None = None

    def __post_init__(self) -> None:
        if self.train_size <= 0:
            raise ValueError("train_size must be positive")
        if self.test_size <= 0:
            raise ValueError("test_size must be positive")
        if self.step_size is not None and self.step_size <= 0:
            raise ValueError("step_size must be positive when set")


def build_holdout_window(total_rows: int, config: HoldoutSplitConfig | None = None) -> DatasetWindow:
    config = config or HoldoutSplitConfig()
    if total_rows < 2:
        raise ValueError("holdout split requires at least 2 rows")
    test_rows = max(1, int(total_rows * config.holdout_ratio))
    train_rows = total_rows - test_rows
    if train_rows <= 0:
        raise ValueError("holdout split must leave at least one training row")
    return DatasetWindow(
        window_id="holdout-01",
        train_start=0,
        train_stop=train_rows,
        test_start=train_rows,
        test_stop=total_rows,
    )


def build_walk_forward_windows(
    total_rows: int,
    config: WalkForwardSplitConfig,
) -> tuple[DatasetWindow, ...]:
    if total_rows < (config.train_size + config.test_size):
        raise ValueError("walk-forward split requires enough rows for the first train/test window")
    windows: list[DatasetWindow] = []
    cursor = 0
    step = config.step_size or config.test_size
    while cursor + config.train_size + config.test_size <= total_rows:
        train_stop = cursor + config.train_size
        test_stop = train_stop + config.test_size
        windows.append(
            DatasetWindow(
                window_id=f"wf-{len(windows) + 1:02d}",
                train_start=cursor,
                train_stop=train_stop,
                test_start=train_stop,
                test_stop=test_stop,
            )
        )
        cursor += step
    return tuple(windows)


def split_config_to_dict(config: HoldoutSplitConfig | WalkForwardSplitConfig | None) -> dict[str, Any]:
    if config is None:
        return {}
    if isinstance(config, HoldoutSplitConfig):
        return {"holdout_ratio": config.holdout_ratio}
    return {
        "train_size": config.train_size,
        "test_size": config.test_size,
        "step_size": config.step_size,
    }
