from __future__ import annotations

# ruff: noqa: E402
import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from trading_advisor_3000.product_plane.data_plane.moex.storage_roots import (
    RAW_BASELINE_TABLE_RELATIVE_PATH,
    RAW_LAYOUT_MIGRATION_STORAGE_DIRNAME,
    configured_moex_historical_data_root,
    resolve_external_file_path,
    resolve_external_root,
)
from trading_advisor_3000.spark_jobs.moex_raw_layout_migration_job import (
    RAW_LAYOUT_MIGRATION_REPORT_FILENAME,
    promote_moex_raw_layout_migration,
    run_moex_raw_layout_migration_spark_job,
)


def _default_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def _default_current_table_path() -> Path:
    return configured_moex_historical_data_root(repo_root=ROOT) / RAW_BASELINE_TABLE_RELATIVE_PATH


def _default_stage_paths(*, output_root: Path, run_id: str) -> tuple[Path, Path]:
    run_dir = output_root / run_id
    return (
        run_dir / "raw_moex_history.layout-staged.delta",
        run_dir / RAW_LAYOUT_MIGRATION_REPORT_FILENAME,
    )


def _default_backup_path(current_table_path: Path, run_id: str) -> Path:
    return current_table_path.with_name(f"raw_moex_history.pre-layout-{run_id}.delta")


def _print(payload: dict[str, object]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Stage and explicitly promote the MOEX raw Delta physical layout migration. "
            "Spark owns the table rewrite; promote is a separate root swap guarded by "
            "the PASS report."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    stage = subparsers.add_parser(
        "stage", help="Rewrite current raw Delta into staged partitioned layout"
    )
    stage.add_argument("--source-table-path", default="")
    stage.add_argument("--output-root", default="")
    stage.add_argument("--staged-table-path", default="")
    stage.add_argument("--report-path", default="")
    stage.add_argument("--run-id", default="")
    stage.add_argument("--spark-master", default="")
    stage.add_argument("--overwrite-staged", action="store_true")

    promote = subparsers.add_parser(
        "promote", help="Promote a passed staged layout into the stable raw path"
    )
    promote.add_argument("--current-table-path", default="")
    promote.add_argument("--staged-table-path", required=True)
    promote.add_argument("--backup-table-path", default="")
    promote.add_argument("--report-path", required=True)
    promote.add_argument("--run-id", default="")

    args = parser.parse_args()

    if args.command == "stage":
        run_id = args.run_id.strip() or _default_run_id()
        source_table_path = (
            resolve_external_file_path(
                args.source_table_path,
                repo_root=ROOT,
                field_name="--source-table-path",
            )
            if str(args.source_table_path).strip()
            else _default_current_table_path()
        )
        output_root = resolve_external_root(
            args.output_root,
            repo_root=ROOT,
            field_name="--output-root",
            default_subdir=RAW_LAYOUT_MIGRATION_STORAGE_DIRNAME,
        )
        default_staged_table_path, default_report_path = _default_stage_paths(
            output_root=output_root,
            run_id=run_id,
        )
        staged_table_path = (
            resolve_external_file_path(
                args.staged_table_path,
                repo_root=ROOT,
                field_name="--staged-table-path",
            )
            if str(args.staged_table_path).strip()
            else default_staged_table_path
        )
        report_path = (
            resolve_external_file_path(
                args.report_path,
                repo_root=ROOT,
                field_name="--report-path",
            )
            if str(args.report_path).strip()
            else default_report_path
        )
        report = run_moex_raw_layout_migration_spark_job(
            source_table_path=source_table_path,
            staged_table_path=staged_table_path,
            report_path=report_path,
            run_id=run_id,
            spark_master=args.spark_master.strip() or "local[*]",
            overwrite_staged=bool(args.overwrite_staged),
        )
        _print(report)
        if report.get("status") != "PASS":
            raise SystemExit("raw layout migration staging blocked by validation checks")
        return

    run_id = args.run_id.strip() or _default_run_id()
    current_table_path = (
        resolve_external_file_path(
            args.current_table_path,
            repo_root=ROOT,
            field_name="--current-table-path",
        )
        if str(args.current_table_path).strip()
        else _default_current_table_path()
    )
    staged_table_path = resolve_external_file_path(
        args.staged_table_path,
        repo_root=ROOT,
        field_name="--staged-table-path",
    )
    backup_table_path = (
        resolve_external_file_path(
            args.backup_table_path,
            repo_root=ROOT,
            field_name="--backup-table-path",
        )
        if str(args.backup_table_path).strip()
        else _default_backup_path(current_table_path, run_id)
    )
    report_path = resolve_external_file_path(
        args.report_path,
        repo_root=ROOT,
        field_name="--report-path",
    )
    result = promote_moex_raw_layout_migration(
        current_table_path=current_table_path,
        staged_table_path=staged_table_path,
        backup_table_path=backup_table_path,
        report_path=report_path,
    )
    _print(result)


if __name__ == "__main__":
    main()
