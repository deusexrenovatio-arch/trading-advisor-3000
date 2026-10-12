# MOEX specification enrichment

Change surface: `product-plane`.

## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: fixture path, sample artifact, synthetic upstream, scaffold-only
- Closure Evidence: native Spark integration test; full frozen raw join and Delta readback; independent price-grid and turnover audits; unchanged source hashes.
- Shortcut Waiver: none

Use the existing Spark/Delta economics job with explicitly prepared contract inputs.
Preserve source history, attach dated parameter evidence and contract descriptions,
and use official margin snapshots only on matching contract/date keys. This keeps
the native pipeline and its fail-closed validations; no Python data-plane rewrite.

The new frozen FX input includes cross pairs such as USD/JPY. The economics loader
must admit only actual */RUB rates. History turnover fields can appear either in
the legacy nested payload or directly in the unmodified ISS history payload.

Independent full-scope evidence is in staging run
`a7f622c5-c33d-46b3-bea9-507944613889`: 511 contracts and 30,780,287 price bars.
Twenty archived SILV/PLD descriptions contain obsolete lots; the 2016 MOEX notice,
2018 specification and observed turnover agree on SILV=10 and PLD=1.

Remaining boundary: price history begins in 2018, while frozen daily settlement
and FX inputs begin in 2020. Parameter enrichment alone does not close that gap.
No publication, scheduler change or full research-readiness claim is included.

## Self-review

- Changed contract: canonical FX inputs represent actual RUB quotes; both raw
  history payload shapes retain turnover evidence; 11 additional parameter
  regimes stop on 16.06.2022 without overlapping the existing regimes. RGBI
  starts on its official launch date, 28.02.2022. Missing margin is explicitly
  `unavailable` even when a calibration factor exists.
- Forbidden old behavior: treating USD/JPY as USD/RUB, silently dropping
  top-level history turnover, or extrapolating the audited parameters to 2018.
- Regression proof: `test_economics_fx_excludes_cross_pairs_and_keeps_rub_identity`
  and the flat-payload case failed before the fix, then passed; the early-regime
  boundary test failed before extension. Native reconstruction covers 2020 and
  the existing 2024 case. The missing-formula margin-quality test also failed
  before repair and covers exact-rank, nearest-rank and official precedence.
- Residual risk: current ISS descriptions are not historical daily snapshots;
  their lot exceptions require preserved source evidence. Exact-date official
  observations leave three expired-contract rows without STEPPRICE; 13 launch-day
  rows across RGBI, SPYF, NASD and WHEAT have missing MR1. Missing pre-2020 inputs
  and per-bar as-of coverage remain downstream work.

The native job caches its economics frame for reuse across validation, writes
and reporting. The first full run demonstrated repeated evaluation of the same
large plan; caching changes neither formulas nor the fail-closed checks.
