from __future__ import annotations

from trading_advisor_3000.app.data_plane import (
    default_phase9_quik_connector_config,
    render_phase9_quik_lua_script,
)


def test_phase9_quik_connector_defaults_to_spbfut_and_pilot_contracts() -> None:
    config = default_phase9_quik_connector_config(
        export_path="D:/trading advisor 3000/artifacts/phase9/quik_live_snapshot.json"
    )

    assert config.provider_id == "quik-live"
    assert config.poll_interval_ms == 1000
    assert [item.contract_id for item in config.bindings] == ["BR-6.26", "Si-6.26"]
    assert [item.sec_code for item in config.bindings] == ["BRM6", "SiM6"]
    assert all(item.class_code == "SPBFUT" for item in config.bindings)


def test_phase9_quik_connector_lua_script_contains_export_path_and_datasource_calls() -> None:
    config = default_phase9_quik_connector_config(
        export_path="D:/trading advisor 3000/artifacts/phase9/quik_live_snapshot.json",
        poll_interval_ms=1500,
    )
    script = render_phase9_quik_lua_script(config)

    assert 'provider_id = "quik-live"' in script
    assert 'export_path = "D:/trading advisor 3000/artifacts/phase9/quik_live_snapshot.json"' in script
    assert 'CreateDataSource(binding.class_code, binding.sec_code, binding.interval)' in script
    assert '"LAST"' in script
    assert 'contract_id = "BR-6.26"' in script
    assert 'sec_code = "BRM6"' in script
    assert 'contract_id = "Si-6.26"' in script
    assert 'sec_code = "SiM6"' in script
    assert "sleep(CONFIG.poll_interval_ms)" in script
