# Module Phase Brief

Updated: 2026-03-25 15:26 UTC

## Parent

- Parent Brief: docs/codex/modules/stack-conformance-remediation.parent.md
- Execution Contract: docs/codex/contracts/stack-conformance-remediation.execution-contract.md

## Phase

- Name: D2 - Spark Execution Closure
- Status: completed

## Objective

- Turn the current Spark-plan scaffolding into an executed Spark runtime path that produces the agreed outputs in a reproducible local or CI profile.

## In Scope

- runnable local Spark entrypoint
- smallest supported dataset/profile for execution proof
- output contract checks against the agreed Delta artifacts
- targeted docs/runbooks for the Spark proof path

## Out Of Scope

- Dagster orchestration closure
- durable runtime default work
- sidecar and release phases

## Change Surfaces

- CTX-DATA
- GOV-RUNTIME
- ARCH-DOCS

## Constraints

- SQL plan strings alone do not count as Spark proof.
- The execution path must remain reproducible in the intended acceptance environment.
- Keep the phase focused on executed work, not on broad platform redesign.

## Acceptance Gate

- A Spark job executes in the agreed proof profile.
- Output Delta tables match the contract.
- Acceptance tests rely on execution results, not plan-string inspection.

## Disprover

- Leave SQL builders intact but break real Spark execution and confirm the phase fails.

## Done Evidence

- Local or CI-compatible Spark runtime path exists.
- Targeted tests or smokes exercise real Spark execution.
- Produced outputs can be tied to the accepted Delta contract.

## Rollback Note

- Revert the Spark runtime slice if the chosen proof path cannot be reproduced deterministically.
