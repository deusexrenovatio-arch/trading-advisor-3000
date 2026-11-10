"""Explicit verification-only quote job; excluded from production definitions."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from dagster import DagsterInstance, job, op
from dagster import Field as DagsterField

from trading_advisor_3000.product_plane.data_plane.moex.quote_pilot import (
    acquire_quote_pilot,
    load_acquisition,
    validate_pilot_path,
    validate_run_id,
)
from trading_advisor_3000.spark_jobs.moex_quote_pilot_job import run_moex_quote_pilot_spark_job

ACQUIRE_OP_NAME = "moex_quote_pilot_acquire"
MATERIALIZE_OP_NAME = "moex_quote_pilot_materialize"
QUOTE_PILOT_JOB_NAME = "moex_quote_pilot_job"


def build_moex_quote_pilot_run_config(
    *,
    acquisition_dir: Path,
    output_root: Path,
    run_id: str,
    fetch: bool = False,
    date_from: date | None = None,
    date_till: date | None = None,
    spark_master: str = "local[2]",
) -> dict[str, object]:
    acquisition = validate_pilot_path(acquisition_dir)
    output = validate_pilot_path(output_root)
    validate_run_id(run_id)
    if output == acquisition or output in acquisition.parents or acquisition in output.parents:
        raise ValueError("acquisition and materialization roots must not overlap")
    if fetch and (date_from is None or date_till is None):
        raise ValueError("fetch requires date_from and date_till")
    if not fetch and (date_from is not None or date_till is not None):
        raise ValueError("replay reads dates from immutable acquisition evidence")
    if not spark_master.strip():
        raise ValueError("spark_master must be nonempty")
    source_config: dict[str, object] = {
        "acquisition_dir": acquisition.as_posix(),
        "run_id": run_id,
        "fetch": fetch,
    }
    if date_from is not None and date_till is not None:
        source_config.update(date_from=date_from.isoformat(), date_till=date_till.isoformat())
    return {
        "ops": {
            ACQUIRE_OP_NAME: {"config": source_config},
            MATERIALIZE_OP_NAME: {
                "config": {
                    "output_root": output.as_posix(),
                    "run_id": run_id,
                    "spark_master": spark_master,
                }
            },
        }
    }


@op(
    name=ACQUIRE_OP_NAME,
    config_schema={
        "acquisition_dir": DagsterField(str),
        "run_id": DagsterField(str),
        "fetch": DagsterField(bool, default_value=False),
        "date_from": DagsterField(str, is_required=False),
        "date_till": DagsterField(str, is_required=False),
    },
)
def moex_quote_pilot_acquire(context) -> str:
    config = context.op_config
    acquisition = validate_pilot_path(Path(config["acquisition_dir"]))
    run_id = validate_run_id(config["run_id"])
    if config["fetch"]:
        if "date_from" not in config or "date_till" not in config:
            raise ValueError("fetch requires date_from and date_till")
        metadata = acquire_quote_pilot(
            acquisition_dir=acquisition,
            run_id=run_id,
            date_from=date.fromisoformat(config["date_from"]),
            date_till=date.fromisoformat(config["date_till"]),
        )
    else:
        if "date_from" in config or "date_till" in config:
            raise ValueError("replay reads dates from immutable acquisition evidence")
        metadata = load_acquisition(acquisition)
    context.add_output_metadata(
        {
            "source_run_id": metadata["run_id"],
            "acquisition_dir": acquisition.as_posix(),
            "date_from": metadata["date_from"],
            "date_till": metadata["date_till"],
            "source_row_count": metadata["source_row_count"],
        }
    )
    return acquisition.as_posix()


@op(
    name=MATERIALIZE_OP_NAME,
    config_schema={
        "output_root": DagsterField(str),
        "run_id": DagsterField(str),
        "spark_master": DagsterField(str, default_value="local[2]"),
    },
)
def moex_quote_pilot_materialize(context, acquisition_dir: str) -> dict:
    config = context.op_config
    report = run_moex_quote_pilot_spark_job(
        acquisition_dir=Path(acquisition_dir),
        output_root=Path(config["output_root"]),
        run_id=config["run_id"],
        spark_master=config["spark_master"],
    )
    context.add_output_metadata(
        {
            "status": report["status"],
            "report_path": report["report_path"],
            "quote_bar_count": report["row_counts"]["quote_bars"],
        }
    )
    return report


@job(name=QUOTE_PILOT_JOB_NAME)
def moex_quote_pilot_job():
    moex_quote_pilot_materialize(moex_quote_pilot_acquire())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquisition-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--date-from", type=date.fromisoformat)
    parser.add_argument("--date-till", type=date.fromisoformat)
    parser.add_argument("--spark-master", default="local[2]")
    args = parser.parse_args(argv)
    try:
        config = build_moex_quote_pilot_run_config(
            acquisition_dir=args.acquisition_dir,
            output_root=args.output_root,
            run_id=args.run_id,
            fetch=args.fetch,
            date_from=args.date_from,
            date_till=args.date_till,
            spark_master=args.spark_master,
        )
        with DagsterInstance.ephemeral() as instance:
            result = moex_quote_pilot_job.execute_in_process(
                run_config=config,
                instance=instance,
                raise_on_error=False,
            )
        if not result.success:
            return 1
        report = result.output_for_node(MATERIALIZE_OP_NAME)
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "run_id": report["run_id"],
                    "report_path": report["report_path"],
                    "row_counts": report["row_counts"],
                },
                ensure_ascii=False,
            )
        )
        return 0
    except (OSError, ValueError) as exc:
        parser.exit(2, f"quote pilot: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
