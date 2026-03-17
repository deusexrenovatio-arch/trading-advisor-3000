from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OBSERVABILITY_DIR = ROOT / "deployment" / "docker" / "observability"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_phase5_observability_files_exist() -> None:
    expected = {
        OBSERVABILITY_DIR / "docker-compose.observability.yml",
        OBSERVABILITY_DIR / "README.md",
        OBSERVABILITY_DIR / "prometheus" / "prometheus.yml",
        OBSERVABILITY_DIR / "loki" / "loki-config.yml",
        OBSERVABILITY_DIR / "grafana" / "provisioning" / "datasources" / "datasources.yml",
        OBSERVABILITY_DIR / "grafana" / "provisioning" / "dashboards" / "dashboards.yml",
        OBSERVABILITY_DIR / "grafana" / "dashboards" / "ta3000-phase5-overview.json",
    }
    for path in expected:
        assert path.exists(), f"missing observability artifact: {path}"


def test_phase5_observability_compose_wires_required_services() -> None:
    compose = _read(OBSERVABILITY_DIR / "docker-compose.observability.yml")
    assert "prometheus:" in compose
    assert "loki:" in compose
    assert "grafana:" in compose
    assert "./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" in compose
    assert "./loki/loki-config.yml:/etc/loki/local-config.yaml:ro" in compose
    assert "./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources:ro" in compose
    assert "./grafana/dashboards:/var/lib/grafana/dashboards:ro" in compose


def test_phase5_prometheus_and_loki_configs_have_expected_smoke_targets() -> None:
    prometheus = _read(OBSERVABILITY_DIR / "prometheus" / "prometheus.yml")
    assert 'job_name: "trading-advisor-runtime"' in prometheus
    assert 'targets: ["host.docker.internal:9464"]' in prometheus

    loki = _read(OBSERVABILITY_DIR / "loki" / "loki-config.yml")
    assert "http_listen_port: 3100" in loki
    assert "store: tsdb" in loki
    assert "object_store: filesystem" in loki


def test_phase5_dashboard_is_valid_json_and_contains_key_panels() -> None:
    dashboard_path = OBSERVABILITY_DIR / "grafana" / "dashboards" / "ta3000-phase5-overview.json"
    dashboard = json.loads(_read(dashboard_path))
    assert dashboard["title"] == "TA3000 Phase5 Overview"
    panels = dashboard.get("panels", [])
    assert isinstance(panels, list) and panels
    panel_types = {str(panel.get("type")) for panel in panels}
    assert {"timeseries", "bargauge", "logs"} <= panel_types
