"""One-time, explicitly authorized canonical migration of the accepted v6 baseline."""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trading_advisor_3000.product_plane.common.data_snapshots import atomic_json, runtime_provenance
from trading_advisor_3000.product_plane.data_plane.moex.economics_cutover import (
    apply_cutover,
    plan_cutover,
    rollback_cutover,
    verify_cutover,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    release = commands.add_parser("release-record")
    release.add_argument("--output", type=Path, required=True)
    release.add_argument("--release-sha", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--data-root", type=Path, required=True)
    plan.add_argument("--acceptance", type=Path, required=True)
    plan.add_argument("--release-sha", required=True)
    plan.add_argument("--release-record", type=Path, required=True)
    for name in ("apply", "verify", "rollback", "status"):
        command = commands.add_parser(name)
        command.add_argument("--plan", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "release-record":
        provenance = runtime_provenance()
        if (
            not re.fullmatch(r"[0-9a-f]{40}", args.release_sha)
            or args.release_sha != provenance["git_commit"]
        ):
            raise ValueError("cutover release SHA differs from deployed code")
        clean = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            check=True,
        )
        if clean.stdout.strip():
            raise ValueError("freeze a clean release before planning cutover")
        result = {**provenance, "clean_release": True}
        atomic_json(args.output, result, immutable=True)
    elif args.command == "plan":
        provenance = runtime_provenance()
        record = json.loads(args.release_record.read_text())
        if (
            not record.get("clean_release")
            or record.get("git_commit") != args.release_sha
            or any(
                provenance[key] != record.get(key) for key in ("source_sha256", "contracts_sha256")
            )
        ):
            raise ValueError("runtime source differs from frozen clean host release")
        result = plan_cutover(args.data_root, args.acceptance, code_version=args.release_sha)
    elif args.command == "status":
        result = json.loads((args.plan.parent / "status.json").read_text())
    else:
        from trading_advisor_3000.spark_jobs.canonical_bars_job import _create_spark_session

        spark = _create_spark_session("economics-production-cutover", "local[2]")
        try:
            operation = {
                "apply": apply_cutover,
                "verify": verify_cutover,
                "rollback": rollback_cutover,
            }[args.command]
            result = operation(args.plan, spark)
        finally:
            spark.stop()
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
