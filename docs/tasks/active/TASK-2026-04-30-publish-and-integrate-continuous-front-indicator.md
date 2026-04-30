# Task Note
Updated: 2026-04-30 17:37 UTC

## Goal
- Deliver: Publish and integrate continuous-front indicator stack

## Task Request Contract
- Objective: publish the incoming continuous-front indicator hybrid changes as reviewable PRs on top of the existing continuous-front stack and prepare them for ordered integration.
- Change Surface: mixed; product-plane research runtime/config/docs plus shell process-state evidence.
- In Scope: continuous-front indicator rule catalog, sidecar Delta runtime, materialization/Dagster wiring, focused tests, architecture/runbook docs, task outcomes, PR publication, and merge readiness checks.
- Out of Scope: direct push to `main`, live intraday promotion, production `research/gold/current` refresh, and broad formula coverage claims beyond the checked validation surface.
- Constraints: keep the existing stack order (#73 -> #74 -> #75), publish new layers above #75, use `py -3.11` for local checks, and preserve PR-only integration policy.
- Done Evidence: clean branches, passing focused tests, passing loop/pr gates or documented post-closeout fallback, pushed branches, draft/ready PR URLs, and explicit integration order.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- The new runtime layer is split into `codex/continuous-front-04-indicator-runtime`; docs/process evidence is being collected in this branch above it.

## Solution Intent
- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: focused unit/integration tests for continuous-front indicator sidecars, Dagster data-prep sourcing, bootstrap continuous-front materialization, continuous-front contracts, and local loop/pr gates; this is PR-stack readiness, not production-current promotion.
- Shortcut Waiver: none

## First-Time-Right Report
1. Confirmed coverage: objective and acceptance path are explicit.
2. Missing or risky scenarios: unknown integrations and policy drifts.
3. Resource/time risks and chosen controls: phased patches and deterministic checks.
4. Highest-priority fixes or follow-ups: stabilize contract and validation first.

## Repetition Control
- Max Same-Path Attempts: 2
- Stop Trigger: same failure repeats after two focused edits.
- Reset Action: pause edits, capture failing check, and reframe the approach.
- New Search Space: validator contract, routing logic, and docs alignment.
- Next Probe: run the smallest failing command before next patch.

## Task Outcome
- Outcome Status: in_progress
- Decision Quality: pending
- Final Contexts: pending
- Route Match: pending
- Primary Rework Cause: none
- Incident Signature: none
- Improvement Action: pending
- Improvement Artifact: pending

## Blockers
- No blocker.

## Next Step
- Run focused tests, publish PR4/PR5, then integrate the ordered stack when checks allow.

## Validation
- `py -3.11 -m pytest tests/product-plane/continuous_front_indicators tests/product-plane/unit/test_research_indicator_layer.py tests/product-plane/unit/test_research_derived_indicator_layer.py -q`
- `py -3.11 scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `py -3.11 scripts/run_pr_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
