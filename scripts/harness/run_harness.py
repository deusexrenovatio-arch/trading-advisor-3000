from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Phase-driven local harness orchestrator.")
    parser.add_argument(
        "mode",
        choices=(
            "intake",
            "plan",
            "run-current-phase",
            "run-to-completion",
            "render-docs",
            "validate",
        ),
        help="Harness operation mode.",
    )
    parser.add_argument("--run-id", required=False, help="Harness run identifier.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(f"run_harness mode '{args.mode}' is scaffolding-only in WP-01")


if __name__ == "__main__":
    main()
