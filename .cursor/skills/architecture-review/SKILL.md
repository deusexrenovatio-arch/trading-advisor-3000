---
name: architecture-review
description: Review architecture boundaries, dependency direction, and decision trade-offs with dual-surface and clean-architecture checks.
classification: KEEP_CORE
wave: WAVE_1
status: ACTIVE
owner_surface: CTX-ARCHITECTURE
scope: dual-surface architecture boundary review, trade-off framing, and adapter boundary control
routing_triggers:
  - "architecture"
  - "boundaries"
  - "dependencies"
  - "module review"
  - "adr"
  - "trade-off"
---

# Architecture Review

## Purpose
Catch architecture regressions before merge: wrong dependency direction, boundary leaks across contexts, weak decision rationale, and shell/product-plane contamination.

## Fit For This Repository
- Repository is dual-surface: `shell` and `product-plane` with explicit change-surface policy.
- This skill is a review lens, not a mandatory full refactor recipe.
- Use clean-architecture and ports/adapters ideas only where they reduce risk for changed paths.

## Core Review Lens

### 1) Surface Isolation (Dual-Surface First)
- Shell paths must stay governance/process oriented.
- Product runtime logic must stay in product-plane paths.
- Any patch that mixes both surfaces must justify why `mixed` is required.

### 2) Dependency Direction
- Business/domain intent should not depend on delivery tech details.
- In product-plane, flag imports where inner logic depends directly on HTTP/UI/broker transport/persistence adapters.
- Prefer dependency inversion through explicit interfaces/contracts at boundaries.

### 3) Ports And Adapters Pragmatism
- For external boundaries (broker, DB, APIs, messaging), verify there is a replaceable boundary interface.
- Confirm that adapter replacement is feasible without rewriting core decision logic.
- Ask: can core behavior be tested with in-memory or fake adapters?

### 4) Bounded Context Hygiene
- Review whether changed modules keep a coherent language and responsibility.
- Flag cross-context data reach-through (direct reads of foreign internals) when contracts/events should be used.
- Prefer explicit context mapping over hidden coupling.

### 5) Testability As Architecture Signal
- Architectural claims must map to executable tests at the correct boundary level.
- If logic requires full external stack for basic behavior checks, likely a boundary leak.

### 6) Trade-Off And Reversibility Lens
- For non-trivial architecture choices, require explicit trade-off framing:
  - what is gained;
  - what is sacrificed;
  - what future change becomes harder.
- Prefer reversible decisions when uncertainty is high.
- Flag irreversible coupling introduced without migration strategy.

### 7) Decision Record Hygiene (Mini ADR)
- If a change shifts boundaries, ownership, or dependency direction, capture a short decision note:
  - context;
  - decision;
  - alternatives considered;
  - consequences.
- Keep ADR depth proportional to risk; avoid heavy templates for trivial local edits.

### 8) Quality Attribute Fit
- Review expected impact on:
  - scalability;
  - reliability;
  - maintainability;
  - observability.
- Flag architecture claims that are not backed by changed-path evidence.

## Workflow
1. Confirm requested outcome and declare change surface: `shell`, `product-plane`, or `mixed`.
2. Map changed files to architecture zones/contexts before judging design quality.
3. Run boundary checks:
   - dependency direction
   - context leaks
   - boundary contract usage
4. For non-trivial decisions, compare at least two viable options and state trade-offs explicitly.
5. Classify findings by severity:
   - `P0`: release-blocking architectural risk
   - `P1`: significant coupling/regression risk
   - `P2`: maintainability debt, non-blocking
6. For each P0/P1, propose a minimal remediation path:
   - boundary extraction
   - interface inversion
   - event/contract handoff
7. Record assumptions, trade-offs, reversibility notes, and residual risk.

## Anti-Overload Rules
- Do not force full clean/hexagonal rewrite for a local patch.
- Do not duplicate generic code-quality review; keep this skill architecture-focused.
- Do not import external skill boilerplate verbatim when repository constraints are narrower.
- Prefer incremental boundary hardening over framework-level redesign.
- Avoid ADR overproduction: small changes can use concise decision notes.

## Validation
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD --snapshot-mode changed-files --profile none`
- `python scripts/validate_skills.py --strict`
- `python scripts/sync_skills_catalog.py --check`

## Boundaries

This skill should NOT:
- replace product decisions with architecture dogma detached from delivery needs.
- mix shell governance constraints into product internals when no shell surface changed.
- accept synthetic/scaffold evidence as closure for high-risk architectural claims.
- absorb standalone code-review workflow; code-review remains a separate skill.
