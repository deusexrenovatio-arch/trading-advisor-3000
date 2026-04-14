# App Runbooks

Product runtime and operational runbooks live in this directory.

- `docs/runbooks/app/bootstrap.md` - canonical local bootstrap and migration path.
- `docs/runbooks/app/phase3-system-replay-runbook.md` - integrated shadow-forward replay and outcome validation.
- `docs/runbooks/app/phase4-live-execution-incident-runbook.md` - controlled live execution sync and incident response.
- `docs/runbooks/app/phase5-observability-runbook.md` - review metrics validation and observability smoke/triage.
- `docs/runbooks/app/phase6-operational-hardening-runbook.md` - operational resilience, secrets policy, and DR recovery sequence.
- `docs/runbooks/app/phase7-scale-up-readiness-runbook.md` - expansion seam validation for providers, context, and adapters.
- `docs/runbooks/app/phase8-operational-proving-runbook.md` - consolidated lane proving, fail-closed evidence, and triage workflow.
- `docs/runbooks/app/mcp-wave-rollout-runbook.md` - Wave 1-3 MCP rollout with base-profile `mempalace`, preflight checks, and incident recovery.
- `docs/runbooks/app/real-execution-transport-runbook.md` - staging-first real HTTP transport rollout and incident handling.
- `docs/runbooks/app/moex-phase01-foundation-runbook.md` - MOEX foundation contour (mapping, coverage discovery, bootstrap ingest, idempotent rerun proof).
- `docs/runbooks/app/moex-phase02-canonical-runbook.md` - MOEX canonical contour (deterministic resampling, fail-closed QC, contract/runtime compatibility).
- `docs/runbooks/app/moex-phase03-dagster-cutover-runbook.md` - MOEX Dagster cutover contour (real staging evidence collection, binding report generation, and governed rerun path).
- `docs/runbooks/app/moex-baseline-storage-runbook.md` - authoritative MOEX data-root layout, promotion flow, and retention policy for raw/canonical/derived storage.
- `docs/runbooks/app/moex-phase03-reconciliation-runbook.md` - MOEX reconciliation contour (Finam archive ingest, overlap drift metrics, threshold-driven alert/escalation evidence).
- `docs/runbooks/app/moex-phase04-production-hardening-runbook.md` - MOEX production hardening contour (scheduler observability, recovery replay, monitoring evidence, release decision bundle).
