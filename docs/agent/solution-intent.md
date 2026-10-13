## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: regression of the automatic Data-only asset selection and per-timeframe canonical replacement bounds; native CF -> research recovery on the existing published Data parent. Local tests alone do not establish runtime publication.
- Shortcut Waiver: none

Change surface: mixed. Product-plane nightly routing plus required intent metadata.
Native Runtime Choice: Dagster owns asset selection and execution; Spark/Delta own
CF replacement and research materialization. Python projects canonical scopes.

The automatic research route must select only Data-prep and publication assets,
without requiring a strategy campaign. CF overlap must cover the canonical
publish deletion scope: at least one complete target timeframe before the raw
changed-window start. The configured overlap remains a lower bound.

Self-review: campaign assets remain available to explicit campaign runs. The
nightly route cannot launch them without campaign configuration. Strict provenance,
economics, snapshot ownership and parent/code guards remain required. Regressions
cover asset selection and 4h/daily/week scope; actual recovery is reported from
the Dagster run and published manifests. Host memory failures remain separate.
