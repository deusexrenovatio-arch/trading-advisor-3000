# Product-Plane Architecture Docs

This directory is the product-plane architecture index inside the dual-surface repository.

Read these first:
- [trading-advisor-3000.md](docs/architecture/trading-advisor-3000.md) - canonical whole-repository orientation map.
- [STATUS.md](docs/architecture/product-plane/STATUS.md) - current implemented reality and no-go zones.
- [CONTRACT_SURFACES.md](docs/architecture/product-plane/CONTRACT_SURFACES.md) - current versioned boundary inventory.
- [contract-change-policy.md](docs/architecture/product-plane/contract-change-policy.md) - compatibility and versioning policy for release-blocking envelopes.
- [approved-universe-v1.md](docs/architecture/product-plane/approved-universe-v1.md) - governed universe and promotion contract for medium-term multi-asset evaluation.
- [moex-historical-route-decision.md](docs/architecture/product-plane/moex-historical-route-decision.md) - authoritative job ownership, reusable vs retired entrypoints, and one fixed historical data route.
- [moex-baseline-storage-runbook.md](docs/runbooks/app/moex-baseline-storage-runbook.md) - authoritative MOEX data-root layout for raw, canonical, and derived storage.
- [moex-historical-route-architecture.md](docs/architecture/product-plane/moex-historical-route-architecture.md) - target-shape route architecture; active operator route truth stays in [moex-historical-route-decision.md](docs/architecture/product-plane/moex-historical-route-decision.md).

Repository naming note:
- Use `product-plane` as the canonical term in docs and PR text.
- Canonical docs root is `docs/architecture/product-plane/`.
- Legacy `docs/architecture/app/` compatibility redirects are retired; minimal historical anchors may remain for immutable planning references.

Phase naming rule:
- `S0-S8` refers to shell delivery phases.
- `P0-P7` refers to product capability phases from the product spec.
- [phase8-ci-pilot-operational-proving.md](docs/architecture/product-plane/phase8-ci-pilot-operational-proving.md) is a shell-controlled evidence overlay, not proof of full product closure.

## Phase 0 package
- `docs/architecture/product-plane/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
- `docs/architecture/product-plane/product-plane-spec-v2/00_AI_Shell_Alignment.md`
- `docs/architecture/product-plane/product-plane-spec-v2/02_Repository_Structure.md`
- `docs/architecture/product-plane/product-plane-spec-v2/06_Phases_and_Acceptance_Gates.md`

## Phase artifacts
- `docs/architecture/product-plane/phase0-plan.md` - shell alignment and repo landing.
- `docs/architecture/product-plane/phase1-contracts-and-scaffolding.md` - contracts freeze and scaffolding.
- `docs/architecture/product-plane/phase2a-data-plane-mvp.md` - data plane MVP implementation and acceptance.
- `docs/architecture/product-plane/phase2b-research-plane-mvp.md` - research plane MVP implementation and acceptance.
- `docs/architecture/product-plane/phase2c-runtime-mvp.md` - runtime MVP implementation and acceptance.
- `docs/architecture/product-plane/phase2d-execution-mvp.md` - execution MVP implementation and acceptance.
- `docs/architecture/product-plane/phase3-shadow-forward-system-integration.md` - shadow-forward and integrated system replay.
- `docs/architecture/product-plane/phase4-live-execution-integration.md` - controlled live execution integration and reconciliation hardening.
- `docs/architecture/product-plane/phase5-review-analytics-observability.md` - review dashboards, latency analytics, and observability plumbing.
- `docs/architecture/product-plane/phase6-operational-hardening.md` - retry/idempotency hardening, secrets policy, recovery and production-like ops profile.
- `docs/architecture/product-plane/phase7-scale-up-readiness.md` - extension seams for providers/adapters and expansion performance backlog.
- `docs/architecture/product-plane/phase8-ci-pilot-operational-proving.md` - CI lane parity, operational proving entrypoint, and fail-closed evidence flow.
- `docs/architecture/product-plane/phase8-dotnet-sidecar-closure.md` - in-repo .NET sidecar implementation and proving scope.
- `docs/architecture/product-plane/f1e-real-broker-process-closure.md` - governed staging-real broker contour closure (with fail-closed disprover and recovery replay).
- `docs/architecture/product-plane/mcp-and-real-execution-rollout.md` - MCP Wave 1-3 rollout with base-profile `mempalace` + staging-first real HTTP execution transport closure.
- `docs/architecture/product-plane/sidecar-wire-api-v1.md` - wire-level HTTP/JSON sidecar contract for staging-first real transport.
- `docs/architecture/product-plane/phase0-3-acceptance-verdict-2026-03-17.md` - architecture acceptance disposition (MVP vs full target closure).

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

