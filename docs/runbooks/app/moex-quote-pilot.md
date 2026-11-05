# MOEX multi-asset quote pilot

Change surface: product-plane. Verification ingestion only.
Decision: [multi-asset market data pilot](docs/architecture/product-plane/market-data-multi-asset-pilot.md).

The explicit job fetches SBER/TQBR, observed SBRF/RFUD dated contracts, and
IMOEX/IMOEX2/SNDX daily native candles. It is not registered in the production
definitions or nightly schedule. Use the existing standalone proof image with
only the candidate checkout mounted at `/workspace`; do not mount product data.

All acquisition/output/report paths must stay under the candidate's
`.tmp/moex-multi-asset-pilot/`. Symlink escapes, current-day data, more than
91 source calendar days, reused acquisition directories and reused report IDs
are rejected. Futures discovery samples official history every 14 days and the
last requested day, plus current ASSETCODE=SBRF metadata; full chain coverage
is not claimed. Current specification snapshots are not historical economics.

Inside the isolated container, fetch and materialize:

```text
python -m trading_advisor_3000.dagster_defs.moex_quote_pilot_assets --fetch --date-from 2026-09-01 --date-till 2026-10-26 --acquisition-dir /workspace/.tmp/moex-multi-asset-pilot/acquisitions/source-1 --output-root /workspace/.tmp/moex-multi-asset-pilot/data --run-id source-1
```

Replay immutable source evidence through the same ephemeral Dagster job:

```text
python -m trading_advisor_3000.dagster_defs.moex_quote_pilot_assets --acquisition-dir /workspace/.tmp/moex-multi-asset-pilot/acquisitions/source-1 --output-root /workspace/.tmp/moex-multi-asset-pilot/data --run-id replay-1
```

The output contains `raw_quote_candles.delta`, `quote_series.delta` and
`quote_bars.delta`. Delta MERGE preserves unrequested keys. Raw observations
are immutable within a source_run_id; a correction requires a new acquisition.
The selected acquisition rematerializes its keys without a newest-source rule.
There is no multi-table transaction or current promotion. Only a successful
immutable `reports/<run_id>.json` admits reads, pinning all three Delta versions
and source evidence hashes. Failed/partial writes do not admit a report. Retry
with a fresh report ID after resolving the failure; a writer lock prevents
overlapping writers to the same output. After a hard process crash, inspect
the failed process before manually clearing its isolated `.writer.lock`.
Retain pinned versions and source files; no pilot VACUUM runs automatically.

Use the bounded public consumer; it applies series, source_run_id, timeframe,
source-date predicates and row limit before rows reach Python:

```python
from datetime import date
from pathlib import Path
from trading_advisor_3000.product_plane.data_plane.moex.quote_pilot_reader import read_quote_pilot_bars

bars = read_quote_pilot_bars(
    report_path=Path('/workspace/.tmp/moex-multi-asset-pilot/data/reports/source-1.json'),
    series_id='MOEX:stock:shares:SBER:TQBR:source_native',
    date_from=date(2026, 10, 26), date_till=date(2026, 10, 26), limit=10,
)
```

Results retain source begin/end and UTC equivalents; source end is inclusive.
Prices/volume/turnover remain source-native and are not execution economics.
Typed indices have null volume/turnover/OI; original source aggregates remain
in raw payloads. All source candles currently lack OI; null is not fabricated
zero. IRUS/IRUS2 aliases are display metadata; TradingView feed equivalence,
calendar completeness, corporate-action adjustments, continuous fronts,
indicators and backtest admission are not established by this ingestion pilot.

Required proof: actual source acquisition; Dagster/Spark/Delta execution; three
physical `_delta_log` directories and readable schemas/counts; independent
source-to-consumer readback; same-source replay without duplicates; correction
and unrequested-series preservation; old accepted snapshot preserved; malformed
input and path escapes rejected. Test or architecture PASS is not deployment.
