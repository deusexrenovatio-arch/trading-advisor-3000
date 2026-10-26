"""Producer integration coverage; delivered only with staging hook activation."""

import pyarrow as pa
import pytest
from deltalake import write_deltalake
from test_staging_source_scope import protected as protected

from trading_advisor_3000.product_plane.common.staging_execution import StagingExecutionError


def test_baseline_counts_default_support_inputs_before_extraction(protected, monkeypatch):
    import inspect

    from trading_advisor_3000.product_plane.common import staging_execution
    from trading_advisor_3000.product_plane.common.staging_source_scope import parameter_value
    from trading_advisor_3000.product_plane.data_plane.moex import baseline_update

    plan, data, save = protected
    canonical = data / "canonical"
    canonical.mkdir()
    write_deltalake(str(canonical / "canonical_roll_map.delta"), pa.table({"test": [1]}))
    args = {
        "mapping_registry_path": data / "mapping.yaml",
        "universe_path": data / "universe.yaml",
        "raw_table_path": data / "raw.delta",
        "canonical_bars_path": canonical / "bars.delta",
        "canonical_provenance_path": canonical / "provenance.delta",
        "evidence_dir": data / "runs",
        "run_id": "test",
        "timeframes": ["1d"],
        "ingest_till_utc": "2030-01-02T00:00:00Z",
        "refresh_window_days": 1,
        "contract_discovery_lookback_days": 7,
        "max_changed_window_days": 1,
    }
    bound = inspect.signature(baseline_update.run_moex_baseline_update).bind(**args)
    bound.apply_defaults()
    expected = dict(bound.arguments)
    expected.pop("run_id")
    expected.pop("ingest_till_utc")
    plan["producer_parameters"]["run_moex_baseline_update"] = parameter_value(expected)
    save()
    # Root-run authentication is independently tested; isolate this earlier IO boundary.
    monkeypatch.setattr(staging_execution, "require_staging_compute", lambda **kwargs: None)
    monkeypatch.setattr(
        baseline_update,
        "runtime_provenance",
        lambda: pytest.fail("preflight reached snapshot/extraction"),
    )
    with pytest.raises(StagingExecutionError, match="source bytes"):
        baseline_update.run_moex_baseline_update(**args)

