## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only, old-projection reuse, stale L2 retry, stale higher-TF dependency reuse, whole frozen runtime copy
- Closure Evidence: native Spark/Delta source-projection-to-L2 regression, focused native Arrow/unit tests, loop gate and PR gate; not corrected real snapshot or full composed primary-promotion closure.
- Shortcut Waiver: none

Change surface: mixed. Product-plane MTF source dependency/invalidation and persisted L2 recovery metadata behavior
plus required shell delivery metadata only. No trading logic enters shell files.
Design, staged execution and residual limits:
`docs/runbooks/app/mtf-source-projection-delivery.md`.
