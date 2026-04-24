# App Runbooks

Product runtime and operational runbooks live in this directory.

- `docs/runbooks/app/bootstrap.md` - canonical local bootstrap and migration path.
- `docs/runbooks/app/research-campaign-route.md` - supported Product Plane front door for research campaigns.
- `docs/runbooks/app/research-plane-operations.md` - operational reading guide for campaign runs, artifacts, and failure interpretation.
- `docs/runbooks/app/shadow-replay-runbook.md` - integrated shadow-forward replay and outcome validation.
- `docs/runbooks/app/live-execution-incident-runbook.md` - controlled live execution sync and incident response.
- `docs/runbooks/app/observability-runbook.md` - review metrics validation and observability smoke/triage.
- `docs/runbooks/app/operational-hardening-runbook.md` - operational resilience, secrets policy, and DR recovery sequence.
- `docs/runbooks/app/scale-up-readiness-runbook.md` - expansion seam validation for providers, context, and adapters.
- `docs/runbooks/app/shell-delivery-operational-proving-runbook.md` - consolidated lane proving, fail-closed evidence, and triage workflow.
- `docs/runbooks/app/mcp-wave-rollout-runbook.md` - Wave 1-3 MCP rollout with base-profile `mempalace`, preflight checks, and incident recovery.
- `docs/runbooks/app/real-execution-transport-runbook.md` - staging-first real HTTP transport rollout and incident handling.
- `docs/runbooks/app/moex-raw-ingest-runbook.md` - MOEX raw-ingest step (mapping, coverage discovery, bootstrap ingest, idempotent rerun proof).
- `docs/runbooks/app/moex-canonical-refresh-runbook.md` - MOEX canonical-refresh step (deterministic resampling, fail-closed QC, contract/runtime compatibility).
- `docs/runbooks/app/moex-dagster-route-runbook.md` - MOEX Dagster-route proof (real staging evidence collection, binding report generation, and governed rerun path).
- `docs/runbooks/app/moex-baseline-storage-runbook.md` - authoritative MOEX data-root layout, daily baseline update flow, and retention policy for raw/canonical/derived storage.
- `docs/runbooks/app/moex-reconciliation-runbook.md` - MOEX reconciliation contour (Finam archive ingest, overlap drift metrics, threshold-driven alert/escalation evidence).
- `docs/runbooks/app/moex-operations-readiness-runbook.md` - MOEX operations readiness (scheduler observability, recovery replay, monitoring evidence, release decision bundle).
