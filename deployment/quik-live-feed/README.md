# Phase 9 QUIK Live Feed Connector

This bundle provides a local-market-data connector for Phase 9 live-feed extraction:

`QUIK -> Lua export script -> JSON snapshot file -> run_phase9_real_data_smoke.py`

## What it is

- a generated `QUIK Lua` script that polls the frozen pilot universe
- a JSON config file with `contract_id -> class_code/sec_code` bindings
- a snapshot file path that the app can read directly

## Build the connector bundle

```bash
python scripts/build_phase9_quik_connector.py
```

Default outputs:
- `deployment/quik-live-feed/phase9_quik_live_export.lua`
- `deployment/quik-live-feed/phase9_quik_live_export.config.json`
- `artifacts/phase9/quik_live_snapshot.json`

## Install in QUIK

1. Open `deployment/quik-live-feed/phase9_quik_live_export.lua`.
2. Load the script into the local `QUIK` terminal.
3. Make sure the export directory exists and `QUIK` can write to it.
4. Start the script and leave it running while the terminal is connected.

## Validate from the repo

```bash
python scripts/run_phase9_real_data_smoke.py --provider quik-live --snapshot-path artifacts/phase9/quik_live_snapshot.json --as-of-ts <UTC timestamp>
```

## Assumptions

- default `class_code` is `SPBFUT`
- default pilot contracts are `BR-6.26` and `Si-6.26`
- internal `contract_id` values are mapped to `QUIK` `sec_code` values through the Phase 9 contract map

## Notes

- this connector is for live market data only
- broker execution remains a separate `StockSharp -> QUIK -> Finam` path
- if your local `QUIK` installation uses a different class code, rebuild with:

```bash
python scripts/build_phase9_quik_connector.py --class-code <YOUR_CLASS_CODE>
```
