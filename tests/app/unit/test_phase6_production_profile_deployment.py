from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PROFILE_DIR = ROOT / "deployment" / "docker" / "production-like"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_phase6_production_like_profile_files_exist() -> None:
    expected = {
        PROFILE_DIR / "docker-compose.production-like.yml",
        PROFILE_DIR / "README.md",
        PROFILE_DIR / ".env.production-like.example",
        PROFILE_DIR / "prometheus" / "prometheus.yml",
    }
    for path in expected:
        assert path.exists(), f"missing phase6 production-like artifact: {path}"


def test_phase6_compose_wires_runtime_sidecar_postgres_and_prometheus() -> None:
    compose = _read(PROFILE_DIR / "docker-compose.production-like.yml")

    assert "runtime-profile:" in compose
    assert "stocksharp-sidecar-stub:" in compose
    assert "postgres:" in compose
    assert "prometheus:" in compose
    assert "trading_advisor_3000.app.runtime.ops.profile_server" in compose
    assert "TA3000_STOCKSHARP_API_KEY" in compose
    assert "TA3000_FINAM_API_TOKEN" in compose
    assert "TA3000_ENFORCE_LIVE_SECRETS" in compose
    assert "TA3000_ENABLE_LIVE_EXECUTION: ${TA3000_ENABLE_LIVE_EXECUTION:-}" in compose
    assert "TA3000_ENABLE_STOCKSHARP_BRIDGE: ${TA3000_ENABLE_STOCKSHARP_BRIDGE:-}" in compose
    assert "TA3000_ENABLE_QUIK_CONNECTOR: ${TA3000_ENABLE_QUIK_CONNECTOR:-}" in compose
    assert "TA3000_ENABLE_FINAM_TRANSPORT: ${TA3000_ENABLE_FINAM_TRANSPORT:-}" in compose
    assert "TA3000_ENABLE_LIVE_EXECUTION: ${TA3000_ENABLE_LIVE_EXECUTION:-1}" not in compose
    assert "TA3000_ENABLE_STOCKSHARP_BRIDGE: ${TA3000_ENABLE_STOCKSHARP_BRIDGE:-1}" not in compose
    assert "TA3000_ENABLE_QUIK_CONNECTOR: ${TA3000_ENABLE_QUIK_CONNECTOR:-1}" not in compose
    assert "TA3000_ENABLE_FINAM_TRANSPORT: ${TA3000_ENABLE_FINAM_TRANSPORT:-1}" not in compose
    assert "http://127.0.0.1:8088/ready" in compose
    assert "./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" in compose


def test_phase6_env_template_contains_required_placeholders() -> None:
    env_template = _read(PROFILE_DIR / ".env.production-like.example")

    assert "TA3000_STOCKSHARP_API_KEY=CHANGE_ME_STOCKSHARP_KEY" in env_template
    assert "TA3000_FINAM_API_TOKEN=CHANGE_ME_FINAM_TOKEN" in env_template
    assert "TA3000_POSTGRES_PASSWORD=CHANGE_ME_DB_PASSWORD" in env_template


def test_phase6_prometheus_scrapes_runtime_operational_profile() -> None:
    prometheus = _read(PROFILE_DIR / "prometheus" / "prometheus.yml")

    assert 'job_name: "ta3000-runtime-operational-profile"' in prometheus
    assert "metrics_path: /metrics" in prometheus
    assert 'targets: ["runtime-profile:8088"]' in prometheus
