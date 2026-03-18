from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PROFILE_DIR = ROOT / "deployment" / "docker" / "staging-gateway"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_staging_gateway_profile_files_exist() -> None:
    expected = {
        PROFILE_DIR / "docker-compose.staging-gateway.yml",
        PROFILE_DIR / "README.md",
        PROFILE_DIR / ".env.staging-gateway.example",
        PROFILE_DIR / "prometheus" / "prometheus.yml",
        PROFILE_DIR / "gateway" / "sidecar_gateway_stub.py",
    }
    for path in expected:
        assert path.exists(), f"missing staging gateway artifact: {path}"


def test_staging_gateway_compose_wires_runtime_sidecar_and_prometheus() -> None:
    compose = _read(PROFILE_DIR / "docker-compose.staging-gateway.yml")
    assert "runtime-profile:" in compose
    assert "stocksharp-sidecar-gateway:" in compose
    assert "prometheus:" in compose
    assert "trading_advisor_3000.app.runtime.ops.profile_server" in compose
    assert "TA3000_SIDECAR_TRANSPORT: ${TA3000_SIDECAR_TRANSPORT:-http}" in compose
    assert "TA3000_SIDECAR_BASE_URL: ${TA3000_SIDECAR_BASE_URL:-http://stocksharp-sidecar-gateway:18081}" in compose
    assert "./gateway/sidecar_gateway_stub.py:/opt/gateway/sidecar_gateway_stub.py:ro" in compose
    assert "TA3000_GATEWAY_KILL_SWITCH" in compose


def test_staging_gateway_prometheus_scrapes_runtime_and_gateway() -> None:
    prometheus = _read(PROFILE_DIR / "prometheus" / "prometheus.yml")
    assert 'job_name: "ta3000-runtime-operational-profile"' in prometheus
    assert 'targets: ["runtime-profile:8088"]' in prometheus
    assert 'job_name: "ta3000-sidecar-gateway"' in prometheus
    assert 'targets: ["stocksharp-sidecar-gateway:18081"]' in prometheus


def test_staging_gateway_env_template_has_fail_closed_defaults_and_rotation_fields() -> None:
    env_template = _read(PROFILE_DIR / ".env.staging-gateway.example")
    assert "TA3000_ENABLE_LIVE_EXECUTION=" in env_template
    assert "TA3000_ENABLE_STOCKSHARP_BRIDGE=" in env_template
    assert "TA3000_ENABLE_QUIK_CONNECTOR=" in env_template
    assert "TA3000_ENABLE_FINAM_TRANSPORT=" in env_template
    assert "TA3000_STOCKSHARP_API_KEY_ROTATED_AT=" in env_template
    assert "TA3000_FINAM_API_TOKEN_ROTATED_AT=" in env_template
    assert "TA3000_GATEWAY_KILL_SWITCH=0" in env_template
