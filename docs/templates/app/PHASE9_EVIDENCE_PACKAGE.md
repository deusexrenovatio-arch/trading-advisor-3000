# PHASE9_EVIDENCE_PACKAGE

## 1. Baseline
- git ref:
- date range:
- pilot universe:
- Phase 9 surface: `9A | 9B`

## 2. External systems used

| System | Role in this run | Version / route | Required secrets category | Health / freshness probe | Evidence artifact |
| --- | --- | --- | --- | --- | --- |
| `MOEX` | historical source |  |  |  |  |
| `QUIK` | live feed or execution hop |  |  |  |  |
| `Telegram` | publication |  |  |  |  |
| `PostgreSQL` | runtime state |  |  |  |  |
| `Prometheus / Loki / Grafana` | observability |  |  |  |  |
| `HTTP sidecar gateway` | optional 9B boundary |  |  |  |  |
| `StockSharp` | optional 9B sidecar |  |  |  |  |
| `Finam` | optional 9B broker route |  |  |  |  |
| `MCP` | support-only inspection, if used |  |  |  |  |

## 3. Baseline commands
- `python -m pytest tests/app -q`
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/run_pr_gate.py --from-git --git-ref HEAD`
- `python scripts/run_phase8_operational_proving.py --from-git --git-ref HEAD --output artifacts/phase8-operational-proving.json`

## 4. Data evidence
- `MOEX` bootstrap log:
- dataset version:
- freshness report:
- roll/session note:

## 5. Strategy evidence
- strategy spec:
- backtest report:
- replay report:

## 6. Runtime and Telegram evidence
- `Telegram` shadow channel identifier:
- publication samples:
- lifecycle audit sample:
- `PostgreSQL` restart / replay note:

## 7. Observability evidence
- metrics snapshot:
- log snapshot:
- dashboard export or screenshot reference:

## 8. Phase 9B optional execution evidence
- `HTTP gateway` readiness snapshot:
- `StockSharp` version and build hash:
- canary report:
- reconciliation report:
- incident note:

## 9. Verdict
- Phase 9A: `accepted | rejected`
- Phase 9B: `accepted | rejected | deferred`

## 10. Operator notes
Free-form decision, limitations, and next-cycle constraints.
