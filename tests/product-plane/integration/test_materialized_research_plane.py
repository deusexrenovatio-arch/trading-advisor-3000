from __future__ import annotations

import importlib.util
from pathlib import Path


def test_legacy_materialized_research_plane_entrypoint_is_removed(tmp_path: Path) -> None:
    del tmp_path
    import trading_advisor_3000.product_plane.research as research

    assert not hasattr(research, "run_research_from_bars")
    assert importlib.util.find_spec("trading_advisor_3000.product_plane.research.pipeline") is None
