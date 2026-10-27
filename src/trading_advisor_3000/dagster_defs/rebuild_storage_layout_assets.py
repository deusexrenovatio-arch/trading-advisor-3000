from __future__ import annotations

from pathlib import Path

from dagster import Field as DagsterField
from dagster import job, op

from trading_advisor_3000.spark_jobs.rebuild_storage_layout_job import (
    run_rebuild_storage_layout_spark_job,
)

REBUILD_STORAGE_LAYOUT_OP_NAME = "rebuild_storage_layout"
REBUILD_STORAGE_LAYOUT_JOB_NAME = "rebuild_storage_layout_job"
REBUILD_STORAGE_LAYOUT_OP_CONFIG_SCHEMA = {
    "layout_manifest_path": DagsterField(str),
    "source_root": DagsterField(str),
    "target_root": DagsterField(str),
    "published_current_root": DagsterField(str),
    "report_path": DagsterField(str),
    "run_id": DagsterField(str),
    "spark_master": DagsterField(str, is_required=False),
}


def build_rebuild_storage_layout_run_config(
    *,
    layout_manifest_path: Path,
    source_root: Path,
    target_root: Path,
    published_current_root: Path,
    report_path: Path,
    run_id: str,
    spark_master: str | None = None,
) -> dict[str, object]:
    config = {
        "layout_manifest_path": Path(layout_manifest_path).resolve().as_posix(),
        "source_root": Path(source_root).resolve().as_posix(),
        "target_root": Path(target_root).resolve().as_posix(),
        "published_current_root": Path(published_current_root).resolve().as_posix(),
        "report_path": Path(report_path).resolve().as_posix(),
        "run_id": run_id.strip(),
    }
    if not config["run_id"]:
        raise ValueError("rebuild storage layout run_id must be non-empty")
    if spark_master:
        config["spark_master"] = spark_master.strip()
    return {"ops": {REBUILD_STORAGE_LAYOUT_OP_NAME: {"config": config}}}


@op(
    name=REBUILD_STORAGE_LAYOUT_OP_NAME,
    config_schema=REBUILD_STORAGE_LAYOUT_OP_CONFIG_SCHEMA,
)
def rebuild_storage_layout(context) -> dict[str, object]:
    config = dict(context.op_config)
    report = run_rebuild_storage_layout_spark_job(
        layout_manifest_path=Path(str(config["layout_manifest_path"])),
        source_root=Path(str(config["source_root"])),
        target_root=Path(str(config["target_root"])),
        published_current_root=Path(str(config["published_current_root"])),
        report_path=Path(str(config["report_path"])),
        run_id=str(config["run_id"]),
        spark_master=str(config.get("spark_master") or "local[*]"),
    )
    context.add_output_metadata(
        {
            "status": str(report.get("status") or ""),
            "table_count": int(report.get("table_count") or 0),
            "row_count": int(report.get("row_count") or 0),
            "parquet_file_count": int(report.get("parquet_file_count") or 0),
            "report_path": str(report.get("report_path") or ""),
        }
    )
    return report


@job(name=REBUILD_STORAGE_LAYOUT_JOB_NAME)
def rebuild_storage_layout_job():
    rebuild_storage_layout()


__all__ = [
    "REBUILD_STORAGE_LAYOUT_JOB_NAME",
    "REBUILD_STORAGE_LAYOUT_OP_NAME",
    "build_rebuild_storage_layout_run_config",
    "rebuild_storage_layout",
    "rebuild_storage_layout_job",
]
