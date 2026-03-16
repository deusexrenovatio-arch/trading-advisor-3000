# CTX-EXTERNAL-SOURCES

## Scope
Generic external-source contracts and ingestion governance stubs.

## Owned Paths
- `docs/agent-contexts/CTX-EXTERNAL-SOURCES.md`
- relevant ingestion/runbook docs in `docs/runbooks/`

## Guarded Paths
- `src/trading_advisor_3000/`

## Input/Output Contract
- Input: source metadata, contract shape, and reliability constraints.
- Output: process-level source governance without domain-specific adapters.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
