from __future__ import annotations

import argparse
from pathlib import Path

from trading_advisor_3000.product_plane.research.datasets import ContinuousFrontPolicy
from trading_advisor_3000.spark_jobs import DEFAULT_SPARK_MASTER, run_continuous_front_spark_job

from ._common import print_summary


def _csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the continuous_front_refresh Spark contour.")
    parser.add_argument("--canonical-output-dir", required=True, help="Directory containing canonical Delta tables.")
    parser.add_argument("--output-dir", required=True, help="Directory for continuous_front Delta outputs.")
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--run-id", default="continuous_front_refresh")
    parser.add_argument("--instruments", default="", help="Optional comma-separated instrument ids.")
    parser.add_argument("--timeframes", default="", help="Optional comma-separated timeframes.")
    parser.add_argument("--start-ts", default="", help="Optional inclusive UTC start timestamp.")
    parser.add_argument("--end-ts", default="", help="Optional inclusive UTC end timestamp.")
    parser.add_argument("--spark-master", default=DEFAULT_SPARK_MASTER)
    parser.add_argument("--roll-policy-mode", default="liquidity_oi_v1")
    parser.add_argument("--confirmation-bars", type=int, default=1)
    parser.add_argument("--candidate-share-min", type=float, default=0.0)
    parser.add_argument("--advantage-ratio-min", type=float, default=1.0)
    return parser


def run_continuous_front_refresh_job(
    *,
    canonical_output_dir: Path,
    output_dir: Path,
    dataset_version: str,
    run_id: str = "continuous_front_refresh",
    instruments: tuple[str, ...] = (),
    timeframes: tuple[str, ...] = (),
    start_ts: str = "",
    end_ts: str = "",
    spark_master: str = DEFAULT_SPARK_MASTER,
    policy: ContinuousFrontPolicy | None = None,
) -> dict[str, object]:
    return run_continuous_front_spark_job(
        canonical_bars_path=canonical_output_dir / "canonical_bars.delta",
        canonical_session_calendar_path=canonical_output_dir / "canonical_session_calendar.delta",
        canonical_roll_map_path=canonical_output_dir / "canonical_roll_map.delta",
        output_dir=output_dir,
        dataset_version=dataset_version,
        policy=policy,
        run_id=run_id,
        instrument_ids=instruments,
        timeframes=timeframes,
        start_ts=start_ts or None,
        end_ts=end_ts or None,
        spark_master=spark_master,
    )


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    policy = ContinuousFrontPolicy.from_config(
        {
            "roll_policy_mode": args.roll_policy_mode,
            "confirmation_bars": args.confirmation_bars,
            "candidate_share_min": args.candidate_share_min,
            "advantage_ratio_min": args.advantage_ratio_min,
        }
    )
    payload = run_continuous_front_refresh_job(
        canonical_output_dir=Path(args.canonical_output_dir),
        output_dir=Path(args.output_dir),
        dataset_version=args.dataset_version,
        run_id=args.run_id,
        instruments=_csv(args.instruments),
        timeframes=_csv(args.timeframes),
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        spark_master=args.spark_master,
        policy=policy,
    )
    print_summary(payload)
    return 0 if payload["status"] in {"PASS", "SKIP"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
