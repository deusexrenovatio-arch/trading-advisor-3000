# Product-Plane Architecture Docs

This directory is the product-plane architecture index inside the dual-surface repository.

Read these first:
- [trading-advisor-3000.md](docs/architecture/trading-advisor-3000.md) - canonical whole-repository orientation map.
- [STATUS.md](docs/architecture/product-plane/STATUS.md) - current implemented reality and no-go zones.
- [CONTRACT_SURFACES.md](docs/architecture/product-plane/CONTRACT_SURFACES.md) - current versioned boundary inventory.
- [native-runtime-ownership.md](docs/architecture/product-plane/native-runtime-ownership.md) - architecture rule for Spark, Delta Lake, Dagster, pandas-ta-classic, vectorbt, Optuna, DuckDB, and Python ownership.
- [research-plane-platform.md](docs/architecture/product-plane/research-plane-platform.md) - stable map of the current research-plane primary path.
- [continuous-signal-layer-technical-specification.md](docs/architecture/product-plane/continuous-signal-layer-technical-specification.md) - implementation-ready target spec for continuous adjusted signal bars, raw active-contract execution alignment, indicator/derived retargeting, and vectorbt loader changes.
- [contract-change-policy.md](docs/architecture/product-plane/contract-change-policy.md) - compatibility and versioning policy for release-blocking envelopes.
- [approved-universe-v1.md](docs/architecture/product-plane/approved-universe-v1.md) - governed universe and promotion contract for medium-term multi-asset evaluation.
- [moex-historical-route-decision.md](docs/architecture/product-plane/moex-historical-route-decision.md) - authoritative job ownership, reusable vs retired entrypoints, and one fixed historical data route.
- [moex-baseline-storage-runbook.md](docs/runbooks/app/moex-baseline-storage-runbook.md) - authoritative MOEX data-root layout for raw, canonical, and derived storage.
- [moex-historical-route-architecture.md](docs/architecture/product-plane/moex-historical-route-architecture.md) - target-shape route architecture; active operator route truth stays in [moex-historical-route-decision.md](docs/architecture/product-plane/moex-historical-route-decision.md).

Repository naming note:
- Use `product-plane` as the canonical term in docs and PR text.
- Canonical docs root is `docs/architecture/product-plane/`.
- Legacy `docs/architecture/app/` compatibility redirects are retired; minimal historical anchors may remain for immutable planning references.

Active naming rule:
- Active file names, test suites, scripts, and selectors use capability names rather than phase labels.
- Historical phase shorthand may appear only inside immutable archive/spec context when needed for provenance.
- `python scripts/validate_product_surface_naming.py` enforces the active-name side of this rule for changed product-facing surfaces.
- [shell-delivery-operational-proving.md](docs/architecture/product-plane/shell-delivery-operational-proving.md) is a shell-controlled evidence overlay, not proof of full product closure.

## Bootstrap Package
- `docs/architecture/product-plane/product-plane-spec-v2/TECHNICAL_REQUIREMENTS.md`
- `docs/architecture/product-plane/product-plane-spec-v2/00_AI_Shell_Alignment.md`
- `docs/architecture/product-plane/product-plane-spec-v2/02_Repository_Structure.md`
- `docs/architecture/product-plane/product-plane-spec-v2/06_Capability_Slices_and_Acceptance_Gates.md`

## Phase artifacts
- `docs/architecture/product-plane/product-plane-bootstrap-plan.md` - shell alignment and repo landing.
- `docs/architecture/product-plane/contracts-and-scaffolding.md` - contracts freeze and scaffolding.
- `docs/architecture/product-plane/historical-data-plane.md` - data plane MVP implementation and acceptance.
- `docs/architecture/product-plane/research-plane.md` - research plane MVP implementation and acceptance.
- `docs/architecture/product-plane/research-plane-platform.md` - current stable research-plane architecture and primary-path semantics.
- `docs/architecture/product-plane/runtime-lifecycle.md` - runtime MVP implementation and acceptance.
- `docs/architecture/product-plane/execution-flow.md` - execution MVP implementation and acceptance.
- `docs/architecture/product-plane/shadow-replay-integration.md` - shadow-forward and integrated system replay.
- `docs/architecture/product-plane/controlled-live-execution.md` - controlled live execution integration and reconciliation hardening.
- `docs/architecture/product-plane/review-observability.md` - review dashboards, latency analytics, and observability plumbing.
- `docs/architecture/product-plane/operational-hardening.md` - retry/idempotency hardening, secrets policy, recovery and production-like ops profile.
- `docs/architecture/product-plane/scale-up-readiness.md` - extension seams for providers/adapters and expansion performance backlog.
- `docs/architecture/product-plane/shell-delivery-operational-proving.md` - CI lane parity, operational proving entrypoint, and fail-closed evidence flow.
- `docs/architecture/product-plane/dotnet-sidecar-closure.md` - in-repo .NET sidecar implementation and proving scope.
- `docs/architecture/product-plane/f1e-real-broker-process-closure.md` - governed staging-real broker contour closure (with fail-closed disprover and recovery replay).
- `docs/architecture/product-plane/mcp-and-real-execution-rollout.md` - MCP Wave 1-3 rollout with base-profile `mempalace` + staging-first real HTTP execution transport closure.
- `docs/architecture/product-plane/sidecar-wire-api-v1.md` - wire-level HTTP/JSON sidecar contract for staging-first real transport.
- `docs/architecture/product-plane/bootstrap-through-shadow-acceptance-verdict-2026-03-17.md` - architecture acceptance disposition (MVP vs full target closure).

## Related checklists
- `docs/checklists/app/product-plane-bootstrap-acceptance-checklist.md`
- `docs/checklists/app/contracts-and-scaffolding-acceptance-checklist.md`
- `docs/checklists/app/historical-data-plane-acceptance-checklist.md`
- `docs/checklists/app/research-plane-acceptance-checklist.md`
- `docs/checklists/app/runtime-lifecycle-acceptance-checklist.md`
- `docs/checklists/app/execution-flow-acceptance-checklist.md`
- `docs/checklists/app/shadow-replay-acceptance-checklist.md`
- `docs/checklists/app/controlled-live-execution-acceptance-checklist.md`
- `docs/checklists/app/review-observability-acceptance-checklist.md`
- `docs/checklists/app/operational-hardening-acceptance-checklist.md`
- `docs/checklists/app/scale-up-readiness-acceptance-checklist.md`
- `docs/checklists/app/shell-delivery-operational-proving-acceptance-checklist.md`
- `docs/checklists/app/data-integration-closure-passport.md`
- `docs/checklists/app/runtime-publication-closure-passport.md`

## Boundary rule
Product-plane changes must not break shell contracts and must not move trading business logic into shell-sensitive paths.

Product-plane data, research, compute, optimization, and orchestration changes
must also follow [native-runtime-ownership.md](docs/architecture/product-plane/native-runtime-ownership.md):
native library primitives own their strong zones, while Python coordinates,
adapts contracts, validates, and records evidence unless an explicit fallback is
documented.

