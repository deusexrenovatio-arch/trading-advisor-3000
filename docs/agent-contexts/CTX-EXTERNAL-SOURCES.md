# CTX-EXTERNAL-SOURCES

## Scope
Generic external-source contracts and ingestion governance stubs.

## Inside This Context
- Source intake policy, external source reliability expectations, lineage shape, and source-governance runbook notes.
- This context describes how a source should enter the system before concrete ingestion code becomes `CTX-DATA`.
- Typical questions: what metadata is required, what lineage must be captured, what reliability/quality constraints apply?
- Not inside: concrete adapter implementation unless the change is still policy-only.

## Owned Paths
- `docs/agent-contexts/CTX-EXTERNAL-SOURCES.md`
- relevant ingestion/runbook docs in `docs/runbooks/`

## Guarded Paths
- `src/trading_advisor_3000/`

## Input/Output Contract
- Input: source metadata, contract shape, and reliability constraints.
- Output: process-level source governance without domain-specific adapters.

## Navigation Facets
- source-contracts
- lineage
- integration-policy

## Search Seeds
- `docs/agent-contexts/CTX-EXTERNAL-SOURCES.md`
- `docs/runbooks/`
- `src/trading_advisor_3000/product_plane/data_plane/`

## Navigation Notes
- Use this for source governance and reliability policy before adding concrete adapters.
- Once implementation touches actual ingestion code, route the code through `CTX-DATA`.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python scripts/validate_docs_links.py --roots AGENTS.md docs`
