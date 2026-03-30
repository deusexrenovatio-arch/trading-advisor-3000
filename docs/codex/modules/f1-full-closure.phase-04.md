# Module Phase Brief

Updated: 2026-03-30 10:08 UTC

## Parent

- Parent Brief: docs/codex/modules/f1-full-closure.parent.md
- Execution Contract: docs/codex/contracts/f1-full-closure.execution-contract.md

## Phase

- Name: F1-D - Sidecar Immutable Evidence Hardening
- Status: planned

## Objective

- Keep `E1` accepted while removing its remaining evidence debt by making build, test, publish, and compiled-binary smoke proof reproducible and immutable.

## In Scope

- canonical sidecar proof artifacts for build, test, publish, smoke, environment, and hashes
- CI or self-hosted replay for the .NET sidecar proof chain
- negative tests for broken binary paths, kill-switch readiness failure, and artifact-hash mismatch

## Out Of Scope

- replacing the sidecar architecture itself
- real broker connector implementation beyond proof-hardening needs
- final release-decision packaging

## Change Surfaces

- ARCH-DOCS
- GOV-RUNTIME
- PROCESS-STATE
- CTX-ORCHESTRATION

## Constraints

- One-off local proof is insufficient; the phase must produce immutable, commit-linked artifacts.
- Artifact hashing is mandatory.
- The proof chain must remain reproducible from a clean checkout.

## Acceptance Gate

- Build, test, publish, and smoke are replayable with recorded exit codes and artifact hashes.
- The evidence pack links artifacts to commit SHA and environment manifest.
- Negative tests prove that stale or fake sidecar artifacts are rejected.

## Disprover

- Point the smoke path at a non-compiled stub or mismatch the recorded artifact hash and confirm the phase fails.

## Done Evidence

- Canonical immutable sidecar artifacts exist for build, test, publish, smoke, environment, and hashes.
- The proof chain is replayed in governed automation, not only locally.
- `E1` no longer depends on ad-hoc evidence.

## Release Gate Impact

- Surface Transition: `E1` evidence debt `ad-hoc -> immutable governed proof`
- Minimum Proof Class: staging-real
- Accepted State Label: real_contour_closed

## Release Surface Ownership

- Owned Surfaces: sidecar_immutable_evidence
- Delivered Proof Class: staging-real
- Required Real Bindings: compiled sidecar artifact set, governed replay environment, and immutable artifact hashes
- Target Downgrade Is Forbidden: yes

## What This Phase Does Not Prove

- Release readiness: this phase does not prove `ALLOW_RELEASE_READINESS`.
- Live contour closure: this phase does not prove a real broker connector or production rollout.

## Rollback Note

- Revert any hardened evidence claim if it cannot be reproduced from clean checkout with the recorded artifacts.
