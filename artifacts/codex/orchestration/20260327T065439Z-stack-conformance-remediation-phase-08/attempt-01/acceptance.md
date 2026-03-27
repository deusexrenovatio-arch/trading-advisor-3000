# Acceptance Result

- Verdict: BLOCKED
- Summary: Governed route and compiled-binary proof are real: I reran build/test/publish/prove, the phase asset test, transport tests, stack-conformance validation, and docs link validation successfully. The phase is still blocked because the operator docs prescribe direct .ps1 commands that fail in the current governed environment (`ExecutionPolicy=Restricted`), and the advertised sidecar operator surface is not fully closed at contract/test level (`/v1/admin/kill-switch` is implemented and advertised but missing from the canonical wire-contract doc, while `/metrics` and kill-switch are not exercised by the phase proof).
- Route Signal: acceptance:governed-phase-route
- Used Skills: phase-acceptance-governor, architecture-review, testing-suite, docs-sync

## Blockers
- B1: Operator proving instructions are not executable as documented
  why: README, phase doc, and runbook instruct direct `./deployment/stocksharp-sidecar/scripts/*.ps1` execution, but this repo environment is `ExecutionPolicy=Restricted` and direct invocation fails. The working evidence path required `powershell -NoProfile -ExecutionPolicy Bypass -File ...` and `TA3000_DOTNET_BIN`.
  remediation: Update the operator-facing proving instructions and phase evidence commands to the actual supported invocation, or make direct script execution work without bypass in the governed environment; then rerun the proving sequence and doc validation.
- B2: Advertised sidecar endpoints are not fully closed in contract and automated proof
  why: The real sidecar exposes `/v1/admin/kill-switch` and phase docs advertise it, but the canonical wire-contract document still omits that endpoint. The phase-specific automated proof also does not exercise `/v1/admin/kill-switch` or `/metrics` on the compiled sidecar, so the public operator surface is not fully closed by contract plus executed tests.
  remediation: Either narrow the slice to the existing contract surface, or update the canonical wire-contract doc and add executed automated coverage/proving for `/v1/admin/kill-switch`, readiness/submit behavior under kill-switch, and `/metrics` on the real published sidecar.
- P-EVIDENCE_GAP-1: Required evidence is missing
  why: Direct `.ps1` instructions in the updated docs do not reproduce the proven path in the current environment.
  remediation: Produce the missing evidence and rerun acceptance.
- P-EVIDENCE_GAP-2: Required evidence is missing
  why: No phase-specific executed proof covers the compiled sidecar `/metrics` endpoint.
  remediation: Produce the missing evidence and rerun acceptance.
- P-EVIDENCE_GAP-3: Required evidence is missing
  why: `/v1/admin/kill-switch` is implemented and advertised, but not recorded in the canonical sidecar wire-contract doc or phase-specific automated proof.
  remediation: Produce the missing evidence and rerun acceptance.
- P-PROHIBITED_FINDING-1: Prohibited acceptance finding present
  why: Undocumented public endpoint in the accepted sidecar surface.
  remediation: Resolve the prohibited condition and rerun acceptance.
- P-PROHIBITED_FINDING-2: Prohibited acceptance finding present
  why: Operator guidance that cannot be executed as written in the governed environment.
  remediation: Resolve the prohibited condition and rerun acceptance.

## Evidence Gaps
- Direct `.ps1` instructions in the updated docs do not reproduce the proven path in the current environment.
- No phase-specific executed proof covers the compiled sidecar `/metrics` endpoint.
- `/v1/admin/kill-switch` is implemented and advertised, but not recorded in the canonical sidecar wire-contract doc or phase-specific automated proof.

## Prohibited Findings
- Undocumented public endpoint in the accepted sidecar surface.
- Operator guidance that cannot be executed as written in the governed environment.

## Policy Blockers
- P-EVIDENCE_GAP-1: Required evidence is missing
  why: Direct `.ps1` instructions in the updated docs do not reproduce the proven path in the current environment.
  remediation: Produce the missing evidence and rerun acceptance.
- P-EVIDENCE_GAP-2: Required evidence is missing
  why: No phase-specific executed proof covers the compiled sidecar `/metrics` endpoint.
  remediation: Produce the missing evidence and rerun acceptance.
- P-EVIDENCE_GAP-3: Required evidence is missing
  why: `/v1/admin/kill-switch` is implemented and advertised, but not recorded in the canonical sidecar wire-contract doc or phase-specific automated proof.
  remediation: Produce the missing evidence and rerun acceptance.
- P-PROHIBITED_FINDING-1: Prohibited acceptance finding present
  why: Undocumented public endpoint in the accepted sidecar surface.
  remediation: Resolve the prohibited condition and rerun acceptance.
- P-PROHIBITED_FINDING-2: Prohibited acceptance finding present
  why: Operator guidance that cannot be executed as written in the governed environment.
  remediation: Resolve the prohibited condition and rerun acceptance.

## Rerun Checks
- powershell -NoProfile -ExecutionPolicy Bypass -File deployment/stocksharp-sidecar/scripts/prove.ps1 -Port 18101
- python -m pytest tests/app/unit/test_phase8_dotnet_sidecar_assets.py -q
- python -m pytest tests/app/unit/test_real_execution_http_transport.py -q
- python scripts/validate_stack_conformance.py
- python scripts/validate_docs_links.py --roots AGENTS.md docs
