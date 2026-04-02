# App Architecture Docs

This directory is the product-plane architecture index inside the dual-surface repository.

Read these first:
- [STATUS.md](docs/architecture/app/STATUS.md) - current implemented reality and no-go zones.
- [CONTRACT_SURFACES.md](docs/architecture/app/CONTRACT_SURFACES.md) - current versioned boundary inventory.
- [contract-change-policy.md](docs/architecture/app/contract-change-policy.md) - compatibility and versioning policy for release-blocking envelopes.

Repository naming note:
- Use `product-plane` as the canonical term in docs and PR text.
- The filesystem path stays `docs/architecture/app/` for compatibility with existing links, validators, and historical artifacts.

Phase naming rule:
- `S0-S8` refers to shell delivery phases.
- `P0-P7` refers to product capability phases from the product spec.
- [phase8-ci-pilot-operational-proving.md](docs/architecture/app/phase8-ci-pilot-operational-proving.md) is a shell-controlled evidence overlay, not proof of full product closure.

## Phase 0 package
- `docs/architecture/app/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
- `docs/architecture/app/product-plane-spec-v2/00_AI_Shell_Alignment.md`
- `docs/architecture/app/product-plane-spec-v2/02_Repository_Structure.md`
- `docs/architecture/app/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`

## Phase artifacts
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
- `docs/architecture/app/phase8-dotnet-sidecar-closure.md` - in-repo .NET sidecar implementation and proving scope.
- `docs/architecture/app/f1e-real-broker-process-closure.md` - governed staging-real broker contour closure (with fail-closed disprover and recovery replay).
- `docs/architecture/app/mcp-and-real-execution-rollout.md` - MCP Wave 1-3 rollout + staging-first real HTTP execution transport closure.
- `docs/architecture/app/sidecar-wire-api-v1.md` - wire-level HTTP/JSON sidecar contract for staging-first real transport.
- `docs/architecture/app/phase0-3-acceptance-verdict-2026-03-17.md` - architecture acceptance disposition (MVP vs full target closure).
- `docs/architecture/app/moex-spark-deltalake-integration-tz.md` - scoped technical assignment for MOEX historical ingestion + Spark jobs + Delta Lake quality/resampling contour.

## Related checklists
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
- `docs/checklists/app/data-integration-closure-passport.md`
- `docs/checklists/app/runtime-publication-closure-passport.md`

## Boundary rule
Product-plane changes must not break shell contracts and must not move trading business logic into shell-sensitive paths.
