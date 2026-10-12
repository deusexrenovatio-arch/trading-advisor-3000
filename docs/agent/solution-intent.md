## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: fresh CF/L0/L1/R2 completed without OOM; acceptance stopped at the sidecar writer's empty append missing its physical year carrier. Saved actual metadata identifies two annual frame tables and a flat input table. One regression verifies both annual callers keep the integer year before DELETE, the input caller retains flat business schema without compaction, UTC year follows authoritative provenance close, and business timestamps/hash payloads are preserved. Actual corrected acceptance remains pending.
- Shortcut Waiver: none

Change surface: mixed. One product-plane CF sidecar Spark owner and required
shell delivery metadata. Hidden storage-close metadata carries the existing
research_bar_views.bar_end_ts through its input/base/derived frames, outside
business hashes and shared Arrow contracts. Physical year uses the common UTC
helper; legacy business ts_close and numerical values are preserved. Only this
owner's two annual frame tables use yearly layout and 512 MiB native compaction;
cf_indicator_input_frame retains the accepted flat layout/business schema.
Existing layout/carrier/type guards run before empty append or scoped DELETE;
existing INT/BIGINT types, Delta identity, history and pinned files are retained.

Self-review: a missing physical carrier or unsupported layout may not mutate
the existing target. The regression exercises casts and write ordering with
Spark/Delta boundaries mocked; no numerical/native parity replay or actual
table scan is added. Business-close inference, global contracts and other
writers are unchanged. The initial three-annual fixture assumption is retained
as superseded evidence; the final single case uses actual flat input plus two
annual frames. Actual acceptance/publication remains a separate gate.
