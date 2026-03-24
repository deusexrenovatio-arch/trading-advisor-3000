# Anti-Shortcut Governance

## Purpose
- Keep critical contours from closing through the locally easiest path when that path hides architectural debt or fake closure.
- Extend the existing harness with lightweight, machine-checkable rules instead of adding heavyweight approval process.

## Scope
- Apply the extra discipline only to tasks that hit a configured critical contour.
- Keep low-risk and docs-only work on the existing lightweight path.

## Solution Classes
- `target`: the intended closure shape is implemented and proven by the contour's required evidence.
- `staged`: the path intentionally preserves the target shape but stops at a bounded intermediate state and names the missing link explicitly.
- `fallback`: a consciously weaker stop-gap is used, must be declared as fallback, and must not pretend to be target closure.

## Forbidden Shortcut Patterns
- `synthetic-upstream-boundary`: a real upstream output is replaced with a fixture, stub, or synthetic stand-in while being presented as true closure.
- `scaffold-only-closure`: wiring or scaffolding is presented as full closure without the contour's required evidence.
- `undeclared-runtime-substitution`: a different runtime, transport, or technology is used without saying so explicitly.
- `hidden-fallback-path`: fallback behavior exists but is not declared as fallback.
- `sample-evidence-instead-of-contour-evidence`: smoke checks, sample artifacts, manifests, or screenshots are presented instead of the contour's required evidence.

## Critical Contour Task Extension
For configured critical contours, extend the active task note with:
- `Solution Class`
- `Critical Contour`
- `Forbidden Shortcuts`
- `Closure Evidence`
- `Shortcut Waiver`

Also add one short design checkpoint that states:
- chosen path,
- why it is not a shortcut,
- what future target shape is preserved.

## Guardrails
- No new mandatory lane.
- No manual approval board or waiver registry.
- No repo-wide rollout before the pilot contours prove low enough noise.

## Pilot Only
Phase 01 starts with only:
- `data-integration-closure`
- `runtime-publication-closure`

The machine-readable pilot config lives in [configs/critical_contours.yaml](configs/critical_contours.yaml).
