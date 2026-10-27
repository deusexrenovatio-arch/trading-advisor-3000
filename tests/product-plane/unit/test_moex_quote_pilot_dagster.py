from __future__ import annotations

from datetime import date

import pytest
from dagster import DagsterInstance

from trading_advisor_3000.dagster_defs import moex_quote_pilot_assets as assets
from trading_advisor_3000.product_plane.data_plane.moex import quote_pilot


@pytest.fixture
def namespace(tmp_path, monkeypatch):
    root = tmp_path / "pilot"
    monkeypatch.setattr(quote_pilot, "PILOT_ROOT", root)
    return root


def test_replay_job_passes_verified_acquisition_to_materializer(namespace, monkeypatch):
    calls = []
    acquisition = namespace / "acquisitions" / "source"
    output = namespace / "data"
    metadata = {
        "run_id": "source",
        "date_from": "2026-09-01",
        "date_till": "2026-09-02",
        "source_row_count": 2,
    }
    report = {
        "status": "PASS",
        "run_id": "replay",
        "report_path": "accepted.json",
        "row_counts": {"quote_bars": 2},
    }

    def load(path):
        calls.append(("verify", path))
        return metadata

    def materialize(**kwargs):
        calls.append(("materialize", kwargs))
        return report

    monkeypatch.setattr(assets, "load_acquisition", load)
    monkeypatch.setattr(assets, "run_moex_quote_pilot_spark_job", materialize)
    config = assets.build_moex_quote_pilot_run_config(
        acquisition_dir=acquisition,
        output_root=output,
        run_id="replay",
    )
    with DagsterInstance.ephemeral() as instance:
        result = assets.moex_quote_pilot_job.execute_in_process(
            run_config=config,
            instance=instance,
        )
    assert result.success
    assert result.output_for_node(assets.MATERIALIZE_OP_NAME) == report
    assert calls == [
        ("verify", acquisition),
        (
            "materialize",
            {
                "acquisition_dir": acquisition,
                "output_root": output,
                "run_id": "replay",
                "spark_master": "local[2]",
            },
        ),
    ]


def test_fetch_job_acquires_explicit_dates_before_materializing(namespace, monkeypatch):
    calls = []

    def fetch(**kwargs):
        calls.append(kwargs)
        return {
            "run_id": kwargs["run_id"],
            "date_from": "2026-09-01",
            "date_till": "2026-09-02",
            "source_row_count": 3,
        }

    def materialize(**kwargs):
        assert len(calls) == 1
        assert kwargs["acquisition_dir"] == namespace / "new-source"
        return {"status": "PASS", "report_path": "accepted.json", "row_counts": {"quote_bars": 3}}

    monkeypatch.setattr(assets, "acquire_quote_pilot", fetch)
    monkeypatch.setattr(assets, "run_moex_quote_pilot_spark_job", materialize)
    config = assets.build_moex_quote_pilot_run_config(
        acquisition_dir=namespace / "new-source",
        output_root=namespace / "data",
        run_id="fetched",
        fetch=True,
        date_from=date(2026, 9, 1),
        date_till=date(2026, 9, 2),
    )
    with DagsterInstance.ephemeral() as instance:
        result = assets.moex_quote_pilot_job.execute_in_process(
            run_config=config,
            instance=instance,
        )
    assert result.success
    assert calls == [
        {
            "acquisition_dir": namespace / "new-source",
            "run_id": "fetched",
            "date_from": date(2026, 9, 1),
            "date_till": date(2026, 9, 2),
        }
    ]


def test_config_rejects_outside_root_overlap_and_ambiguous_dates(namespace):
    def build(**kwargs):
        return assets.build_moex_quote_pilot_run_config(
            acquisition_dir=namespace / "source",
            output_root=namespace / "data",
            run_id="safe",
            **kwargs,
        )

    with pytest.raises(ValueError, match="inside candidate"):
        assets.build_moex_quote_pilot_run_config(
            acquisition_dir=namespace / "source",
            output_root=namespace.parent / "current",
            run_id="safe",
        )
    with pytest.raises(ValueError, match="overlap"):
        assets.build_moex_quote_pilot_run_config(
            acquisition_dir=namespace / "data" / "source",
            output_root=namespace / "data",
            run_id="safe",
        )
    with pytest.raises(ValueError, match="requires"):
        build(fetch=True)
    with pytest.raises(ValueError, match="replay reads dates"):
        build(date_from=date(2026, 9, 1), date_till=date(2026, 9, 2))


def test_cli_returns_failure_status(namespace, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(
        assets.moex_quote_pilot_job,
        "execute_in_process",
        lambda **kwargs: SimpleNamespace(success=False),
    )
    assert (
        assets.main(
            [
                "--acquisition-dir",
                str(namespace / "source"),
                "--output-root",
                str(namespace / "data"),
                "--run-id",
                "failure",
            ]
        )
        == 1
    )
