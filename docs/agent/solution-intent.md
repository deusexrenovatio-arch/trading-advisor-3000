## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: inherited RAW validation at6205a62f/base134ffce1: 12 distinct integration cases including native Spark/Delta raw-owner regressions, +59 unit and actual PR gate PASS; RAW runtime/tests unchanged; not full target closure of canonical dataset/downstream research or runtime-ready surface.
- Shortcut Waiver: none

Change surface: mixed. Product-plane raw UTC close-year writer behavior plus
required shell delivery metadata only. No domain logic enters shell surfaces.
Chosen path: actual raw owning writers and contract policy, keeping the other
eight heavy families unchanged until their close carriers/producers are ready.
Future shape preserved: all nine families require physical proof before promotion;
raw publication/root swap remains deferred. Design and residual limits:
`docs/runbooks/app/raw-close-year-writer-delivery.md`.
