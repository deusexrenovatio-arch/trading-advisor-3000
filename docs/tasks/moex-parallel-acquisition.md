# MOEX parallel raw acquisition

- Change Surface: product-plane
- Semantic Risk: data acquisition behavior and recovery contract
- Scope: explicitly authorized raw candles in isolated rebuild staging only.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: synthetic closure, Python Delta row scans, implicit promotion
- Closure Evidence: connector parity and recovery tests; native Spark/Delta write/read
  probe; real-run coverage receipts, Delta log, counts and unique-key verification
  still required before claiming acquisition complete.
- Shortcut Waiver: none

Chosen path: bounded HTTP concurrency, immutable per-contract/timeframe source
parts, then one native Spark/Delta materialization. Existing MOEX pagination,
retry and UTC parsing remain authoritative. Python owns connector IO and
checkpoint files; Dagster owns the run; Spark owns row reconciliation and Delta.
Canonical, research, publication and scheduler remain outside this authorization.

## Recovery contract

- Each completed part carries its exact scope, SHA256, bytes and row count.
- Resume verifies parts before reuse; corruption fails closed, never overwrites.
- A failed partial download preserves other completed parts.
- An exclusive download lock prevents simultaneous writers. After a killed
  process, verify its container has stopped before manually clearing its stale lock.
- Each Delta attempt receives a new output directory; source parts survive failed
  writes. Never retry the former truncate-on-start sequential entrypoint.
- Cutover preserves the old source and independent compressed checkpoints. Adopt
  only scopes proven complete by sequential request-log transition; the last
  active scope is downloaded again, while its original bytes remain preserved.
- HTTP concurrency is bounded to 16 workers; initial operation uses 12 with a
  shared 16 requests/second limit. Overload responses reduce the requested rate.
- The parallel connector reuses one HTTP connection per active scope, including
  native request retries, through the existing httpx dependency. Sequential
  callers retain the original urllib transport. A real local HTTP server proves
  connection reuse and the retry contract.
- Docker temporary storage must permit native library execution (`/tmp:rw,exec`).
  Dagster whole-stage retries are zero. Production data roots are not mounted.

## Verification and self-review

Seven focused tests cover sequential output parity, cutoff filtering, partial
failure/resume, corruption, overlapping scopes, concurrent workers and duplicate
writers, and source preservation across failed Delta attempts. Native Linux proof checks actual Delta commit/readback, UTC timestamps
and unique keys; synthetic proof does not establish real-data completeness.

The old behavior of redownloading completed scopes or truncating saved parts is
forbidden by recovery tests. Remaining risks: MOEX throttling and latency; an
interrupted active scope must restart; same-disk backups do not protect against
physical disk failure. The two-hour target requires a measured sustained rate
and is not a completion guarantee.
