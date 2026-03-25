# App Architecture Docs

This section stores product-plane architecture artifacts on top of the existing AI shell.

Read these first:
- [STATUS.md](docs/architecture/app/STATUS.md) - current implemented reality and no-go zones.
- [CONTRACT_SURFACES.md](docs/architecture/app/CONTRACT_SURFACES.md) - current versioned boundary inventory.

Phase naming rule:
- `S0-S8` refers to shell delivery phases.
- `P0-P7` refers to product capability phases from the product spec.
- [phase8-ci-pilot-operational-proving.md](docs/architecture/app/phase8-ci-pilot-operational-proving.md) is a shell-controlled evidence overlay, not proof of full product closure.

## Phase 0 Package
- `docs/architecture/app/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
- `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`
- `docs/architecture/app/product-plane-spec-v2/02_Repository_Structure.md`
- `docs/architecture/app/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`

## Phase Artifacts
- `docs/architecture/app/phase0-plan.md` - shell alignment and repo landing.
- `docs/architecture/app/phase1-contracts-and-scaffolding.md` - contracts freeze and scaffolding.
- `docs/architecture/app/phase2a-data-plane-mvp.md` - data plane MVP implementation and acceptance.
- `docs/architecture/app/phase2b-research-plane-mvp.md` - research plane MVP implementation and acceptance.
- `docs/architecture/app/phase2c-runtime-mvp.md` - runtime MVP implementation and acceptance.
- `docs/architecture/app/phase2d-execution-mvp.md` - execution MVP implementation and acceptance.
- `docs/architecture/app/phase3-shadow-forward-system-integration.md` - shadow-forward and integrated system replay.
- `docs/architecture/app/phase4-live-execution-integration.md` - controlled live execution integration and reconciliation hardening.
- `docs/architecture/app/phase5-review-analytics-observability.md` - review dashboards, latency analytics, and observability plumbing.
- `docs/architecture/app/phase6-operational-hardening.md` - retry/idempotency hardening, secrets policy, recovery and production-like ops profile.
- `docs/architecture/app/phase7-scale-up-readiness.md` - extension seams for providers/adapters and expansion performance backlog.
- `docs/architecture/app/phase8-ci-pilot-operational-proving.md` - CI lane parity, operational proving entrypoint, and fail-closed evidence flow.
- `docs/architecture/app/mcp-and-real-execution-rollout.md` - MCP Wave 1-3 rollout + staging-first real HTTP execution transport closure.
- `docs/architecture/app/sidecar-wire-api-v1.md` - wire-level HTTP/JSON sidecar contract for staging-first real transport.
- `docs/architecture/app/phase0-3-acceptance-verdict-2026-03-17.md` - architecture acceptance disposition (MVP vs full target closure).

## Phase 9 Update
- `docs/architecture/app/current-repo-baseline-2026-03-18.md` - baseline snapshot and explicit external integration starting point.
- `docs/architecture/app/phase9-gap-analysis.md` - delta from current scaffold to battle runs and optional canary.
- `docs/architecture/app/phase9-battle-runs-and-real-signal-rollout.md` - main Phase 9 rollout doc with integration matrix and 9A/9B separation.
- `docs/architecture/app/phase9-architecture-and-stop-rules.md` - end-to-end external-system flow, stop rules, and kill-switch boundaries.
- `docs/architecture/app/phase9-moex-breakout-v1-spec.md` - first landed production strategy spec and replay/evidence contract for the pilot universe.
- `docs/architecture/app/phase9-battle-run-env-contract.md` - explicit WS-C env contract for Telegram, PostgreSQL, and observability-backed battle runs.
- `docs/architecture/app/phase9-sidecar-delivery-contract.md` - explicit WS-D freeze for delivery mode, readiness probes, and dry-run/canary sequencing.
- `docs/architecture/app/phase9-phase9a-battle-run-integration.md` - integrated WS-E flow that assembles data, strategy, runtime, and evidence into one Phase 9A run.
- `docs/architecture/app/phase9-module-dod.md` - module-level closure expectations for each integration surface.
- `docs/architecture/app/phase9-workstreams-and-patch-sets.md` - workstream order and patch-set discipline.
- `docs/architecture/app/phase9-mcp-and-secrets-update.md` - MCP and secrets policy update for battle runs.

## Related Checklists
- `docs/checklists/app/phase0-acceptance-checklist.md`
- `docs/checklists/app/phase1-acceptance-checklist.md`
- `docs/checklists/app/phase2a-acceptance-checklist.md`
- `docs/checklists/app/phase2b-acceptance-checklist.md`
- `docs/checklists/app/phase2c-acceptance-checklist.md`
- `docs/checklists/app/phase2d-acceptance-checklist.md`
- `docs/checklists/app/phase3-acceptance-checklist.md`
- `docs/checklists/app/phase4-acceptance-checklist.md`
- `docs/checklists/app/phase5-acceptance-checklist.md`
- `docs/checklists/app/phase6-acceptance-checklist.md`
- `docs/checklists/app/phase7-acceptance-checklist.md`
- `docs/checklists/app/phase8-acceptance-checklist.md`
- `docs/checklists/app/phase9-acceptance-checklist.md`
- `docs/checklists/app/real-broker-canary-checklist.md`

## Phase 9 Templates
- `docs/templates/app/REAL_STRATEGY_SPEC.md`
- `docs/templates/app/PHASE9_EVIDENCE_PACKAGE.md`

## Boundary Rule
Product-plane changes must not break shell contracts and must not move trading business logic into shell-sensitive paths.
