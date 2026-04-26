# Agent Context Map

## Purpose
Keep hot context deterministic by routing work to bounded ownership cards and
small search seeds.

Context routing is not only a label. It is the first step before broad reading:
the agent should run the router, open the primary context card, follow its
search seeds through Serena when code is involved, and open secondary context
cards only for files that actually matched them.

## Prompt-To-Search Funnel
1. Classify the request surface and run or consume context router output.
2. Open the primary context card from `navigation_order`.
3. Read `Inside This Context` to understand what lives there and what does not.
4. Use `Search Seeds` as Serena entrypoints for code discovery.
5. Load secondary context cards only for their matched files or guarded paths.
6. Record a context footprint when the task crosses more than one context.

## Context Catalog
- `CTX-OPS` -> `docs/agent-contexts/CTX-OPS.md`
- `CTX-CONTRACTS` -> `docs/agent-contexts/CTX-CONTRACTS.md`
- `CTX-ARCHITECTURE` -> `docs/agent-contexts/CTX-ARCHITECTURE.md`
- `CTX-DATA` -> `docs/agent-contexts/CTX-DATA.md`
- `CTX-RESEARCH` -> `docs/agent-contexts/CTX-RESEARCH.md`
- `CTX-ORCHESTRATION` -> `docs/agent-contexts/CTX-ORCHESTRATION.md`
- `CTX-API-UI` -> `docs/agent-contexts/CTX-API-UI.md`
- `CTX-DOMAIN` -> `docs/agent-contexts/CTX-DOMAIN.md`
- `CTX-EXTERNAL-SOURCES` -> `docs/agent-contexts/CTX-EXTERNAL-SOURCES.md`
- `CTX-SKILLS` -> `docs/agent-contexts/CTX-SKILLS.md`

## Rename Freeze
- `CTX-STRATEGY` is migrated as `CTX-DOMAIN`.
- `CTX-NEWS` is migrated as `CTX-EXTERNAL-SOURCES`.

## Routing Script
- `python scripts/context_router.py --from-git --format text`
- `python scripts/validate_agent_contexts.py`

## Navigation Output
- `primary_context` is the default first card to read.
- `navigation_order` is the preferred order for loading context cards.
- `facets` name the smaller sub-areas inside a context.
- `search_seeds` are the first paths or symbols to inspect before broad search.
- `unmapped_files` must be classified before implementation.

## Context Card Contract
Every context card should include:
- `Scope`: the ownership boundary in one paragraph.
- `Inside This Context`: the living inventory of concepts, files, runtime behavior, and typical questions.
- `Owned Paths`: paths that make this context an owner.
- `Guarded Paths`: nearby paths that require another context or review lens.
- `Navigation Facets`: sub-areas for small mental routing.
- `Search Seeds`: first Serena/direct-inspection entrypoints.
- `Navigation Notes`: caveats that prevent over-reading or wrong ownership.

## Rules
1. Prefer one primary context per patch.
2. If multiple contexts are touched, split patch sequence.
3. Keep cold paths (`plans/`, `memory/`, artifacts, archives, package dump, local tool config) out of default hot retrieval.
4. For non-trivial code, use the primary context's search seeds as Serena entrypoints before opening whole files.
5. Treat `CTX-DOMAIN` as a residual context and confirm that no narrower product-plane context applies.
