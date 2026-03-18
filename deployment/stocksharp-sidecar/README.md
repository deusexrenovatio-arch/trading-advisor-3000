# StockSharp Sidecar Status

This directory is not a real StockSharp/.NET sidecar implementation.

## What exists today
- Python live bridge logic under `src/trading_advisor_3000/app/execution/adapters/`
- HTTP wire contract in `docs/architecture/app/sidecar-wire-api-v1.md`
- staging gateway stub/profile under `deployment/docker/staging-gateway/`
- rollout and incident runbooks for staging-first transport proving

## What does not exist here
- no `.sln` / `.csproj` real sidecar project,
- no real QUIK/Finam broker process in the repo,
- no claim of live production readiness.

## Reading rule
Treat the current sidecar surface as a transport contract and staging stub baseline only.
