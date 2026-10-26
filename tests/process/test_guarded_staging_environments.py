from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deployment" / "docker" / "dagster-staging"


def test_staging_profiles_share_the_native_route_and_isolation():
    common = yaml.safe_load((COMPOSE / "docker-compose.dagster-staging-common.yml").read_text())
    for service in common["services"].values():
        assert "product_plane_definitions" in service["command"]
        assert service["environment"]["TA3000_STAGING_PLAN"] == "/staging-control/plan.json"
        volumes = service["volumes"]
        assert any("/workspace:ro" in item for item in volumes)
        assert any("/staging-control:ro" in item for item in volumes)
        assert any(item.endswith(":/staging-quota:ro") for item in volumes)
        assert any(item.endswith("/.ta3000-staging.json:ro") for item in volumes)
        assert not any("docker.sock" in item or "TA3000-production" in item for item in volumes)
        assert service["cpus"] and service["mem_limit"]
    branch = yaml.safe_load((COMPOSE / "docker-compose.dagster-branch.yml").read_text())
    acceptance = yaml.safe_load((COMPOSE / "docker-compose.dagster-acceptance.yml").read_text())
    assert "3011" in branch["services"]["webserver"]["ports"][0]
    assert "3012" in acceptance["services"]["webserver"]["ports"][0]
    assert "daemon" not in branch["services"]
    assert "profiles" not in acceptance["services"]["daemon"]
    assert branch["name"] != acceptance["name"]
