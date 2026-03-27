# Module Phase Brief

Updated: 2026-03-25 15:26 UTC

## Parent

- Parent Brief: docs/codex/modules/stack-conformance-remediation.parent.md
- Execution Contract: docs/codex/contracts/stack-conformance-remediation.execution-contract.md

## Phase

- Name: E1 - Real .NET Sidecar Closure
- Status: completed

## Objective

- Land a real in-repo .NET sidecar project that implements the current wire contract and can be built, tested, published, and smoked from Python.

## In Scope

- `.sln` or `.csproj` project surfaces
- sidecar build/test/publish scripts
- staging proof path for the compiled binary
- Python integration smoke against the compiled sidecar

## Out Of Scope

- broker-production rollout beyond the agreed contract
- release-wide re-acceptance
- unrelated governance validator work

## Change Surfaces

- CTX-ORCHESTRATION
- ARCH-DOCS
- GOV-RUNTIME

## Constraints

- README text plus Python transport is not sidecar closure.
- The proof must involve a compiled sidecar process.
- Keep the implementation minimal but real against the existing wire contract.

## Acceptance Gate

- `dotnet build`, `dotnet test`, and `dotnet publish` succeed for the in-repo sidecar project.
- The compiled sidecar boots and answers the required health and transport endpoints.
- Python integration smoke succeeds against the compiled sidecar.

## Disprover

- Leave only stub/docs surfaces without a compiled sidecar and confirm the phase fails.

## Done Evidence

- Real in-repo sidecar project exists.
- Build/test/publish proof exists.
- Python transport smoke talks to the compiled sidecar.

## Rollback Note

- Revert the sidecar slice if it cannot build or smoke deterministically in the agreed environment.
