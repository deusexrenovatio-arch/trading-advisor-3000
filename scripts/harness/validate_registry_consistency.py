from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate consistency between canonical registry and generated outputs.")
    parser.add_argument("--run-id", required=False, help="Harness run identifier.")
    return parser


def main() -> None:
    parser = build_parser()
    parser.parse_args()
    raise SystemExit("validate_registry_consistency is scaffolding-only in WP-01")


if __name__ == "__main__":
    main()
