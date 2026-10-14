## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: none
- Closure Evidence: native Spark regression for separated explicit windows and unchanged provenance-only fallback; existing Delta publisher overlap/sidecar regression; physical restoration and pinned Data/research publication recorded separately.
- Shortcut Waiver: none

Change surface: mixed. Product-plane replacement-boundary correction plus required delivery metadata. No domain logic moves into shell.

Native Runtime Choice: Spark owns scoped key selection and Delta mutations; Dagster owns replay and downstream continuation. Python coordinates metadata only.

An existing explicit publish-scope file defines the authoritative replacement windows, including an empty declared scope. Contract min/max provenance inference remains the compatibility path when no explicit file exists. Disjoint explicit windows must preserve the intervening history.

Self-review: replacement scope no longer unions explicit windows with a broad contract min/max envelope. Tables, business keys, UUIDs and partition layout stay unchanged. The native separated-window regression catches intermediate-bar deletion; provenance-only fallback and the existing delete-only sidecar behavior remain covered. Historical sparse replay requires complete source context and a separate physical publication receipt.
