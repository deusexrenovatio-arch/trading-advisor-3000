## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, tests/product-plane/fixtures/, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: the persistent baseline canonical publisher failed when its private stale-key Delta basename collided with a governed snapshot input. One native Delta/materialization-scope regression drives the actual merge caller with mock Spark only, verifies private scratch reads and preserved pins, and retains rejection of foreign paths with governed names. Actual baseline retry remains a separate operator step after delivery; no core calculation is repeated here.
- Shortcut Waiver: none

Change surface: mixed. Product-plane canonical-publish private scratch naming
and required shell delivery metadata. The four existing stale-key paths share
a helper using __stale_keys_<table>.delta under the same private run directory.
The actual reader remains read_spark_delta with unchanged snapshot pinning and
governed-name collision rejection; no unscoped read or global guard waiver.

Self-review: private intermediates cannot reuse governed canonical basenames.
The one regression uses real Delta metadata and the native snapshot scope,
with only the Spark read/merge boundary mocked. The helper is consumed by all
four existing callsites. Canonical key/delete/upsert algorithms, values, table
IDs, provenance, published pins, pending changed windows and recovery reports
remain unchanged. No data jobs, storage migrations, numerical parity runs or
production changes are performed in this patch; persistent retry is pending.
