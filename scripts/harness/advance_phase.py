from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Advance harness run state to the next phase when gates pass.")
    parser.add_argument("--run-id", required=False, help="Harness run identifier.")
    return parser


def main() -> None:
    parser = build_parser()
    parser.parse_args()
    raise SystemExit("advance_phase is scaffolding-only in WP-01")


if __name__ == "__main__":
    main()
