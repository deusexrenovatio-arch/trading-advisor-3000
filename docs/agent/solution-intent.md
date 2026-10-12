## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: fresh R2 source write and metadata load passed, then the native derived phase was OOM-killed. Its CF refresh plan retained every source family until writing. One public-flow weakref regression demonstrates three resident families before the patch and at most one afterward, with all six fixture partitions and replacement scopes preserved. Actual full-size completion remains pending.
- Shortcut Waiver: none

Change surface: mixed. Product-plane CF derived materialization and required
shell delivery metadata. CF planning stores only series/timeframe/profile
instructions. Planning and the lazy writer release completed source families;
writing reloads the same full-history family and MTF dependencies. Numerical
builders, hashes, timestamps, output columns, global replacement predicates,
native batch writer and storage carrier policy remain unchanged.

Self-review: retaining all CF payloads in caches/refresh_plan is forbidden.
The existing native-contract extension path is preserved. The regression mocks
numerical/Spark/Delta execution and checks real public-flow object lifetimes,
MTF family completeness and scoped replacements. No synthetic RSS threshold,
numerical parity, full-size OOM closure or production publication is claimed.
