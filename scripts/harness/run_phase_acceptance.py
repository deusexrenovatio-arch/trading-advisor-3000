from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute acceptance stage and produce verdict artifacts.")
    parser.add_argument("--run-id", required=False, help="Harness run identifier.")
    return parser


def main() -> None:
    parser = build_parser()
    parser.parse_args()
    raise SystemExit("run_phase_acceptance is scaffolding-only in WP-01")


if __name__ == "__main__":
    main()
