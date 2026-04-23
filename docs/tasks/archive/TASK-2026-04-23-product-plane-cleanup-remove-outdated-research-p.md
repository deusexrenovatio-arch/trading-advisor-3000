# Task Note
Updated: 2026-04-23 13:29 UTC

## Goal
- Deliver: Product-plane cleanup: remove outdated research paths, fallback shims, proof/debug leftovers, and rename legacy test/function names to current product names
- Change Surface: product-plane

## Task Request Contract
- Objective: make the research branch merge-ready by removing obsolete research entrypoints, retiring stale naming, and aligning docs/tests with the current operator route.
- In Scope: `src/trading_advisor_3000/product_plane/research/*`, `src/trading_advisor_3000/dagster_defs/research_assets.py`, product-plane research docs/runbooks/checklists, and research tests tied to operator routing.
- Out of Scope: shell governance surfaces, MOEX route contracts outside research routing, and unrelated runtime/API contours.
- Constraints: keep shell surfaces domain-free, preserve the supported `run_campaign` route, do not reintroduce compatibility bridges, and prefer deletions over hiding stale paths behind new aliases.
- Done Evidence: remove obsolete research job modules and update route/docs/tests so only current product paths remain referenced.
- Priority Rule: quality and safety over speed when tradeoffs appear.

## Current Delta
- Session started and baseline scope captured.
- Cleanup is narrowed to product-plane research routing, naming, and operator-facing documentation.

## Solution Intent
- Solution Class: staged
- Critical Contour: multi-contour
- Contours In Scope: data-integration-closure, runtime-publication-closure
- Forbidden Shortcuts: none
- Closure Evidence: integration test coverage for the canonical dataset handoff plus publication contour verification on the supported research route
- Contour Evidence Markers: integration test, canonical dataset, downstream research, runtime-ready surface, runtime output, durable store, publication contour, end-to-end publication
- Staged Marker: not full target closure
- Shortcut Waiver: none

Chosen path: remove obsolete research routing surfaces and stale naming directly instead of preserving them behind compatibility wrappers or hidden internal entrypoints.
Why it is not a shortcut: the cleanup keeps the supported `run_campaign` route, leaves scheduled Dagster freshness explicit, and aligns tests/docs with the current product-plane execution contract instead of masking old paths.
Future shape preserved: campaign execution stays operator-facing, scheduled freshness stays Dagster-owned, and retired helper routes remain absent from the supported branch surface.

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
- Outcome Status: completed
- Decision Quality: correct_after_replan
- Final Contexts: CTX-OPS, CTX-RESEARCH
- Route Match: matched
- Primary Rework Cause: workflow_gap
- Incident Signature: none
- Improvement Action: workflow
- Improvement Artifact: docs/runbooks/app/research-campaign-route.md

## Blockers
- No blocker.

## Next Step
- Prepare PR for merge into main through the normal protected-branch route.

## Validation
- `python scripts/validate_task_request_contract.py`
- `python scripts/validate_session_handoff.py`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
