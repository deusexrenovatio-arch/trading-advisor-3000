from __future__ import annotations

import argparse
from pathlib import Path

from trading_advisor_3000.product_plane.research.campaigns import run_campaign

from ._common import print_summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the canonical Dagster-first research campaign route.")
    parser.add_argument("--config", required=True, help="Path to campaign YAML config.")
    return parser


def run_campaign_job(*, config: Path) -> dict[str, object]:
    return run_campaign(config_path=config)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = run_campaign_job(config=Path(args.config))
    print_summary(payload)
    return 0 if payload["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
