## Solution Intent

- Solution Class: staged
- Critical Contour: data-integration-closure
- Forbidden Shortcuts: none
- Closure Evidence: failing-then-passing bootstrap and baseline regressions for terminal minute/hour/day/week candles, unchanged stability cutoff, native Spark/Delta repair and pinned downstream readback recorded separately from code acceptance.
- Shortcut Waiver: none

Change surface: mixed. Product-plane source boundary fix plus this required delivery metadata; domain logic remains in the product plane.

Native Runtime Choice: MOEX client owns source acquisition; Spark DataFrames and the existing Delta raw writer own reconciliation; Dagster owns Data/research continuation. Arrow owns bounded inspection. Python coordinates metadata and evidence only.

The ISS candleborders end identifies the last candle opening. The acquisition envelope includes that candle's possible close, capped by the existing stability cutoff. Exact source closes remain unchanged. Known terminal candles cannot be skipped using low-liquidity watermark grace. Local-tail cutoff semantics remain unchanged.

Self-review: old behavior forbidding a closed terminal candle solely because its close follows the last opening is removed. No table schema, business key, table UUID or yearly layout changes. Regression coverage includes both acquisition paths and the stable cutoff. Historical repair/publication and old-root retirement require separate physical evidence.

Retirement compatibility: active runtime defaults, Compose binds, local readers and bootstrap now select the accepted primary-20261011 root. Historical fixtures and benchmark inputs retain their provenance. Economics preservation uses a separately labelled source archive inside primary; it does not silently replace the accepted economics model.
