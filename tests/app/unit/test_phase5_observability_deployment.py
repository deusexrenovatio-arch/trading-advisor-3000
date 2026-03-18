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
        OBSERVABILITY_DIR / "metrics_file_server.py",
        OBSERVABILITY_DIR / "prometheus" / "prometheus.yml",
        OBSERVABILITY_DIR / "loki" / "loki-config.yml",
        OBSERVABILITY_DIR / "promtail" / "promtail-config.yml",
        OBSERVABILITY_DIR / "runtime-artifacts" / "observability.prometheus.metrics.txt",
        OBSERVABILITY_DIR / "runtime-artifacts" / "observability.loki.events.jsonl",
        OBSERVABILITY_DIR / "grafana" / "provisioning" / "datasources" / "datasources.yml",
        OBSERVABILITY_DIR / "grafana" / "provisioning" / "dashboards" / "dashboards.yml",
        OBSERVABILITY_DIR / "grafana" / "dashboards" / "ta3000-phase5-overview.json",
    }
    for path in expected:
        assert path.exists(), f"missing observability artifact: {path}"


def test_phase5_observability_compose_wires_required_services() -> None:
    compose = _read(OBSERVABILITY_DIR / "docker-compose.observability.yml")
    assert "metrics-file-exporter:" in compose
    assert "prometheus:" in compose
    assert "loki:" in compose
    assert "promtail:" in compose
    assert "grafana:" in compose
    assert "./metrics_file_server.py:/opt/bridge/metrics_file_server.py:ro" in compose
    assert "./runtime-artifacts:/runtime:ro" in compose
    assert "./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" in compose
    assert "./loki/loki-config.yml:/etc/loki/local-config.yaml:ro" in compose
    assert "./promtail/promtail-config.yml:/etc/promtail/promtail-config.yml:ro" in compose
    assert "./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources:ro" in compose
    assert "./grafana/dashboards:/var/lib/grafana/dashboards:ro" in compose


def test_phase5_prometheus_and_loki_configs_have_expected_smoke_targets() -> None:
    prometheus = _read(OBSERVABILITY_DIR / "prometheus" / "prometheus.yml")
    assert 'job_name: "trading-advisor-runtime"' in prometheus
    assert 'targets: ["metrics-file-exporter:9464"]' in prometheus

    loki = _read(OBSERVABILITY_DIR / "loki" / "loki-config.yml")
    assert "http_listen_port: 3100" in loki
    assert "store: tsdb" in loki
    assert "object_store: filesystem" in loki

    promtail = _read(OBSERVABILITY_DIR / "promtail" / "promtail-config.yml")
    assert "job_name: trading-advisor-runtime" in promtail
    assert "job: trading-advisor-runtime" in promtail
    assert "__path__: /runtime/observability.loki.events.jsonl" in promtail
    assert "url: http://loki:3100/loki/api/v1/push" in promtail


def test_phase5_dashboard_is_valid_json_and_contains_key_panels() -> None:
    dashboard_path = OBSERVABILITY_DIR / "grafana" / "dashboards" / "ta3000-phase5-overview.json"
    dashboard = json.loads(_read(dashboard_path))
    assert dashboard["title"] == "TA3000 Phase5 Overview"
    panels = dashboard.get("panels", [])
    assert isinstance(panels, list) and panels
    panel_types = {str(panel.get("type")) for panel in panels}
    assert {"timeseries", "bargauge", "logs"} <= panel_types


def test_phase5_runtime_artifacts_are_parseable() -> None:
    metrics = _read(OBSERVABILITY_DIR / "runtime-artifacts" / "observability.prometheus.metrics.txt")
    assert "ta3000_strategy_signals_total" in metrics
    assert "ta3000_latency_status_total" in metrics

    loki_lines = _read(OBSERVABILITY_DIR / "runtime-artifacts" / "observability.loki.events.jsonl").splitlines()
    parsed = [json.loads(line) for line in loki_lines if line.strip()]
    assert parsed
    assert {str(item.get("stream")) for item in parsed} >= {"latency", "strategy_dashboard"}
