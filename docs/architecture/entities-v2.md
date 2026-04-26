# Entities v2

## Purpose
This file defines control-plane entities for the AI delivery shell.
It excludes trading-domain entities by design.

### TaskSession
- Represents one active delivery session bound to worktree and branch.
- Key fields: `session_id`, `started_at`, `expires_at`, `branch`.

### TaskNote
- Canonical narrative for one task lifecycle.
- Key fields: goal, contract, first-time-right report, repetition control, outcome.

### TaskOutcome
- Durable closeout record written to `memory/task_outcomes.yaml`.
- Key fields: `task_id`, `outcome_status`, `decision_quality`, `route_match`, `closed_at`.

### PlanItem
- Canonical planning unit in `plans/items`.
- Key fields: `id`, `lane`, `status`, `execution_mode`, `acceptance`, `checks`.

### MemoryDecision
- Architecture or governance decision captured in decision ledger.
- Key fields: `id`, `date`, `context`, `decision`, `impact`.

### MemoryPattern
- Repeatable practice captured for future sessions.
- Key fields: `id`, `date`, `pattern`, `when_to_use`.

### ContextCard
- Ownership and guardrails for bounded routing contexts.
- Key fields: owned paths, guarded paths, risk level, minimum checks.

### ChangeSurface
- Classification of diff into executable validation surfaces.
- Key fields: `primary_surface`, `surfaces`, `docs_only`, command profile expansion.

### GateResult
- Machine-readable result of loop/PR/nightly gates.
- Key fields: gate name, surface, commands, pass/fail.

### SkillAsset
- Runtime repo-local skill descriptor under `.codex/skills`.
- Key fields: name, description, governance status, product-plane owner surface, catalog synchronization state.

### ArchitectureArtifact
- Architecture source documents and generated map outputs.
- Key fields: layer source docs, entity docs, generated map, ADR links.

### GovernanceReport
- Aggregated process telemetry and dashboard outputs.
- Key fields: baseline metrics, KPI rollup, improvement actions.
