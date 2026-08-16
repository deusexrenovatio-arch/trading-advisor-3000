# Domain Docs

TA3000 uses a multi-context domain-doc layout.

Start from `CONTEXT-MAP.md` at the repo root, then read only the context sources relevant to the task. The map points to existing TA3000 hot docs and architecture docs; it is intentionally thin and should not duplicate them.

## Before Exploring

1. Read `AGENTS.md`.
2. Read `CONTEXT-MAP.md`.
3. Select the relevant context from the map.
4. Read only the listed sources needed for the task.
5. For non-trivial code work, run `python scripts/context_router.py --from-git --format text` before broad repo reading.

## ADRs

If `docs/adr/` or context-local ADR folders appear later, read ADRs that touch the area being changed.

If an output contradicts an ADR or hot doc, surface the conflict explicitly instead of silently overriding it.

## Vocabulary

Use the vocabulary from the selected context's source docs. Do not collapse TA3000 shell, product-plane, data/research, and runtime terms into one generic glossary.
