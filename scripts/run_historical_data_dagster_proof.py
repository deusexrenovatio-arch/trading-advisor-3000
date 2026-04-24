from __future__ import annotations

import argparse
import json
from pathlib import Path

from trading_advisor_3000.dagster_defs import materialize_historical_data_proof_assets


DEFAULT_FIXTURE = Path("tests/product-plane/fixtures/data_plane/raw_backfill_sample.jsonl")
DEFAULT_OUTPUT_DIR = Path(".tmp/historical-data-dagster-proof")


def _parse_contracts(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Execute the historical-data Dagster materialization proof. "
            "This is a proof-only contour and not the canonical MOEX historical refresh route."
        )
    )
    parser.add_argument("--source", default=DEFAULT_FIXTURE.as_posix(), help="Path to JSONL source fixture")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR.as_posix(), help="Directory for Delta outputs")
    parser.add_argument(
        "--contracts",
        default="BR-6.26,Si-6.26",
        help="Comma-separated contract allowlist for the proof profile",
    )
    parser.add_argument("--output-json", default="", help="Optional JSON report output path")
    args = parser.parse_args()

    report = materialize_historical_data_proof_assets(
        source_path=Path(args.source),
        output_dir=Path(args.output_dir),
        whitelist_contracts=_parse_contracts(args.contracts),
    )

    report_json = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report_json + "\n", encoding="utf-8")
    print(report_json)


if __name__ == "__main__":
    main()
