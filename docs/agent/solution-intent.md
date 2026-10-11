## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: focused pure timing regressions, unchanged legacy report assertions, loop gate and PR gate; not full data-integration or runtime-ready closure.
- Shortcut Waiver: none

Change surface: mixed. Product-plane shared timing helper and compatibility
exports plus required shell delivery metadata only. No trading logic enters
shell files. Contract, validation and residual limits:
`docs/runbooks/app/stage-timing-delivery.md`.
