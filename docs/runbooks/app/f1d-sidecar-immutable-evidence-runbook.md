# F1-D Sidecar Immutable Evidence Runbook

## Purpose
Execute the governed `F1-D` chain that upgrades sidecar proof from ad-hoc local runs to immutable, commit-linked evidence.

## Preconditions
1. `.NET 8 SDK` is installed (`dotnet --info` succeeds).
2. Python environment can import `trading_advisor_3000` from `src/`.
3. No process is occupying the smoke port (`18091` by default).
4. Optional: set `TA3000_DOTNET_BIN=<absolute-path-to-dotnet.exe>` when `dotnet` is not on `PATH`.

## Canonical Command
From repository root:

```powershell
python scripts/run_f1d_sidecar_immutable_evidence.py --output-root artifacts/f1/phase04/sidecar-immutable
```

## What The Command Proves
1. Reproducible chain from clean checkout: build -> test -> publish -> compiled-binary smoke.
2. Immutable hash manifest for proof artifacts and compiled publish outputs.
3. Commit-linked manifest with command-level exit codes and replay metadata.
4. Negative disprovers fail closed for:
   - broken binary path,
   - kill-switch readiness contract break,
   - artifact hash mismatch.

## Evidence Artifacts
Each run creates an immutable directory:
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/environment.json`
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/build.json`
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/test.json`
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/publish.json`
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/smoke.json`
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/hashes.json`
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/hashes.sha256`
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/negative-tests.json`
- `artifacts/f1/phase04/sidecar-immutable/<run-id>/manifest.json`

## Failure Triage
1. `dotnet --info` fails:
   - install `.NET 8 SDK` or point `TA3000_DOTNET_BIN` to an SDK-backed binary.
2. Build/test/publish fails:
   - inspect step logs under `.../<run-id>/logs/`.
3. Smoke fails:
   - inspect `smoke.stderr.log` plus `smoke.json` details.
4. Hash or disprover failure:
   - treat as evidence-chain integrity failure; do not reuse that run as phase proof.
