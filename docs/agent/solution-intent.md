## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: native Spark/Delta economics regressions, focused unit tests, loop gate and PR gate; not full target closure of canonical dataset/downstream research or runtime-ready surface.
- Shortcut Waiver: none

Change surface: mixed. Product-plane historical economics behavior plus required
shell delivery metadata only. No trading logic enters shell files. Design and
residual limits: `docs/runbooks/app/historical-economics-delivery.md`.
