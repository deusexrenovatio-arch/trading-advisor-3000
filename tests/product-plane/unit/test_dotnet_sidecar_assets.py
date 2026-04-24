from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SIDECAR_DIR = ROOT / "deployment" / "stocksharp-sidecar"
SIDECAR_DOC = ROOT / "docs" / "architecture" / "product-plane" / "dotnet-sidecar-closure.md"
SIDECAR_RUNBOOK = ROOT / "docs" / "runbooks" / "app" / "dotnet-sidecar-runbook.md"
SIDECAR_WIRE_API_DOC = ROOT / "docs" / "architecture" / "product-plane" / "sidecar-wire-api-v1.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_dotnet_sidecar_dotnet_sidecar_solution_and_projects_exist() -> None:
    expected = {
        SIDECAR_DIR / "TradingAdvisor3000.StockSharpSidecar.sln",
        SIDECAR_DIR / "src" / "TradingAdvisor3000.StockSharpSidecar" / "TradingAdvisor3000.StockSharpSidecar.csproj",
        SIDECAR_DIR / "tests" / "TradingAdvisor3000.StockSharpSidecar.Tests" / "TradingAdvisor3000.StockSharpSidecar.Tests.csproj",
        SIDECAR_DIR / "src" / "TradingAdvisor3000.StockSharpSidecar" / "Program.cs",
    }
    for path in expected:
        assert path.exists(), f"missing dotnet-sidecar artifact: {path}"


def test_dotnet_sidecar_sidecar_scripts_cover_build_test_publish_and_compiled_smoke() -> None:
    prove_script = _read(SIDECAR_DIR / "scripts" / "prove.ps1")
    assert "build.ps1" in prove_script
    assert "test.ps1" in prove_script
    assert "publish.ps1" in prove_script
    assert "smoke_stocksharp_sidecar_binary.py" in prove_script
    assert "/v1/admin/kill-switch" in _read(ROOT / "scripts" / "smoke_stocksharp_sidecar_binary.py")
    assert "/metrics" in _read(ROOT / "scripts" / "smoke_stocksharp_sidecar_binary.py")
    assert "kill_switch_active" in _read(ROOT / "scripts" / "smoke_stocksharp_sidecar_binary.py")

    build_script = _read(SIDECAR_DIR / "scripts" / "build.ps1")
    test_script = _read(SIDECAR_DIR / "scripts" / "test.ps1")
    publish_script = _read(SIDECAR_DIR / "scripts" / "publish.ps1")

    assert "$DotnetExecutable build" in build_script
    assert "$DotnetExecutable test" in test_script
    assert "$DotnetExecutable publish" in publish_script
    assert "$LASTEXITCODE -ne 0" in build_script
    assert "$LASTEXITCODE -ne 0" in test_script
    assert "$LASTEXITCODE -ne 0" in publish_script
    assert "$LASTEXITCODE -ne 0" in prove_script


def test_dotnet_sidecar_readme_documents_compiled_binary_proof_path() -> None:
    readme = _read(SIDECAR_DIR / "README.md")
    assert ".NET 8" in readme
    assert "python scripts/smoke_stocksharp_sidecar_binary.py" in readme
    assert "artifacts/dotnet-sidecar/stocksharp-sidecar/python-smoke.json" in readme
    assert "powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/prove.ps1" in readme
    assert "python scripts/run_f1d_sidecar_immutable_evidence.py" in readme


def test_dotnet_sidecar_docs_use_execution_policy_bypass_commands() -> None:
    sidecar_doc = _read(SIDECAR_DOC)
    runbook = _read(SIDECAR_RUNBOOK)

    command = "powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/prove.ps1"
    assert command in sidecar_doc
    assert command in runbook
    assert "powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/build.ps1" in sidecar_doc
    assert "powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/build.ps1" in runbook


def test_dotnet_sidecar_wire_contract_documents_kill_switch_and_metrics_behavior() -> None:
    contract_doc = _read(SIDECAR_WIRE_API_DOC)
    assert "POST /v1/admin/kill-switch" in contract_doc
    assert "kill_switch_active" in contract_doc
    assert "GET /metrics" in contract_doc
    assert "ta3000_sidecar_gateway_kill_switch" in contract_doc
