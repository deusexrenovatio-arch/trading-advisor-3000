# Module Parent Brief

Updated: 2026-03-24 15:00 UTC

## Source

- Package Zip: D:/trading advisor 3000/.tmp/spec-intake-lab/docs/codex/packages/inbox/PLAN_tech-package.zip
- Execution Contract: docs/codex/contracts/plan-tech.execution-contract.md

## Module Objective

- Add lightweight anti-shortcut governance for critical contours without introducing heavyweight bureaucracy or new mandatory gate lanes.

## Why This Is Module Path

- The change spans contracts, runtime validators, gate wiring, routing behavior, tests, and pilot acceptance docs.
- The plan explicitly requires ordered rollout and pilot-only expansion, which should be preserved as separate reviewable phases.

## Phase Order

1. Phase 01 - Policy and Critical Contour Foundations
2. Phase 02 - Validator and Gate Enforcement
3. Phase 03 - Routing and Pilot Passports
4. Phase 04 - Observation Counters and Expansion Criteria

## Global Constraints

- Keep the anti-shortcut discipline limited to critical contours.
- Preserve loop/pr/nightly and Phase 8 proving as the main harness.
- Do not add a new mandatory lane or approval ceremony.
- Keep shell control-plane files free of product business logic.

## Global Done Evidence

- Policy model, config, validators, tests, and pilot passports exist in reviewable phases.
- The loop gate remains green after each phase.
- Pilot contours can distinguish `target`, `staged`, and forbidden shortcut claims through deterministic evidence.

## Open Risks

- Validator rules may produce false positives until the pilot contours are exercised on real tasks.
- Task-note burden can grow if the critical contour trigger rules are too broad.
- Acceptance passports may drift from actual downstream evidence requirements if they are not kept narrow.

## Next Phase To Execute

- docs/codex/modules/plan-tech.phase-01.md
