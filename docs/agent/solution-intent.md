## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only, old-projection reuse, stale L2 retry, stale higher-TF dependency reuse, whole frozen runtime copy
- Closure Evidence: 47 focused unit cases, four real native Spark/Delta MTF source-to-L2 and partial-publication recovery cases, loop gate and PR gate; corrected real snapshot and full composed primary-promotion closure remain pending.
- Shortcut Waiver: none

Change surface: mixed. Product-plane MTF source dependency/invalidation and
persisted L2 recovery metadata plus required shell delivery metadata only.
No trading logic enters shell files. Design, staged execution and residual limits:
`docs/runbooks/app/mtf-source-projection-delivery.md`.
