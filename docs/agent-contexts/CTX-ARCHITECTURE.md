# CTX-ARCHITECTURE

## Scope
Architecture-as-docs package, ADRs, and boundary validation tests.

## Inside This Context
- Repository map, shell/product-plane boundaries, module ownership, layer rules, ADRs, and architecture tests.
- This context orients cross-module work and checks whether a path matches the intended system shape.
- Typical questions: which layer owns this behavior, does this dependency cross a boundary, which architecture doc is canonical?
- Not inside: exact implementation proof; use Serena or direct code inspection after orientation.

## Owned Paths
- `docs/architecture/`
- `tests/architecture/`

## Guarded Paths
- `src/trading_advisor_3000/`

## Navigation Facets
- repository-map
- module-boundaries
- layer-policy
- architecture-tests

## Search Seeds
- `docs/architecture/trading-advisor-3000.md`
- `docs/architecture/repository-surfaces.md`
- `docs/architecture/product-plane/STATUS.md`
- `tests/architecture/`

## Navigation Notes
- Use this as a companion context for critical contours and boundary-sensitive changes.
- Use Graphify only for orientation, then use Serena or direct source inspection for implementation proof.

## Minimum Checks
- `python scripts/run_loop_gate.py --from-git --git-ref HEAD`
- `python -m pytest tests/architecture -q`
