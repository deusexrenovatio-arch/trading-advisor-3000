"""Check this CI run's native staging artifact in the existing PR gate."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path

JOBS = ("moex_baseline_update_job", "moex_cf_catch_up_job", "research_data_prep_job")
STAGES = (
    "moex_baseline_update",
    "continuous_front_bars",
    "research_datasets",
    "research_indicator_frames",
    "research_derived_indicator_frames",
    "continuous_front_indicator_acceptance_report",
    "research_data_snapshot",
)


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def requires_acceptance(paths):
    return any(
        p.startswith(("src/", "configs/", "deployment/"))
        and not p.endswith(".md")
        or p in {"pyproject.toml", "uv.lock"}
        or p.startswith("scripts/")
        and any(x in p for x in ("moex", "research", "staging"))
        for p in paths
    )


def validate_result(value, *, base, head, tree, ci_run=None, ci_attempt=None):
    if (
        value.get("version") != 1
        or value.get("stage") != "acceptance"
        or value.get("status") != "PASS"
    ):
        raise ValueError("Full native acceptance is missing, failed or short-only")
    expected = {"base_sha": base, "head_sha": head, "tree_sha": tree}
    if any(value.get(k) != v for k, v in expected.items()):
        raise ValueError("Stale base/head/tested tree")
    if ci_run is not None and str(value.get("ci_run_id")) != str(ci_run):
        raise ValueError("Artifact belongs to another CI workflow run")
    if ci_attempt is not None and str(value.get("ci_attempt")) != str(ci_attempt):
        raise ValueError("Artifact belongs to another CI attempt")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", value.get("image_digest", "")) or not value.get(
        "input_pins"
    ):
        raise ValueError("Missing actual image/input pins")
    upstream = None
    ids = set()
    for name in JOBS:
        run = value.get("runs", {}).get(name, {})
        if run.get("status") != "SUCCESS" or not run.get("run_id") or run["run_id"] in ids:
            raise ValueError("Missing successful distinct native jobs")
        if upstream and run.get("upstream_run_id") != upstream:
            raise ValueError("Broken upstream lineage")
        ids.add(run["run_id"])
        upstream = run["run_id"]
    if (
        not set(STAGES).issubset(value.get("executed_stages", []))
        or value.get("changed_windows", 0) < 1
    ):
        raise ValueError("Real nonempty route execution incomplete")
    consumer = value.get("consumer", {})
    if not value.get("qc") or any(v != "PASS" for v in value["qc"].values()):
        raise ValueError("Native QC/publication incomplete")
    if (
        consumer.get("entrypoint") != "research.io.load_backtest_frames"
        or consumer.get("cache_hit") is not False
        or consumer.get("rows", 0) < 1
        or consumer.get("snapshot_id") != value.get("snapshot_id")
    ):
        raise ValueError("Fresh normal consumer proof missing")
    return value


def check_staging_acceptance(changed_files, *, base_ref=None, head_ref=None, receipt=None):
    try:
        return _check(changed_files, base_ref, head_ref, receipt)
    except (ValueError, OSError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
        print(f"staging acceptance: BLOCKED ({exc})")
        return 2


def _check(changed_files, base_ref, head_ref, receipt):
    if base_ref and head_ref:
        changed_files = git(
            Path.cwd(), "diff", "--name-only", f"{base_ref}..{head_ref}"
        ).splitlines()
    if not requires_acceptance(changed_files):
        return 0
    if not base_ref or not head_ref or not receipt:
        raise ValueError("Native staging result required for this build")
    tree = git(Path.cwd(), "merge-tree", "--write-tree", base_ref, head_ref).splitlines()[0]
    validate_result(
        json.loads(Path(receipt).read_text()),
        base=git(Path.cwd(), "rev-parse", base_ref),
        head=git(Path.cwd(), "rev-parse", head_ref),
        tree=tree,
        ci_run=os.environ.get("GITHUB_RUN_ID"),
        ci_attempt=os.environ.get("GITHUB_RUN_ATTEMPT"),
    )
    print("staging acceptance: PASS")
    return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-ref", required=True)
    p.add_argument("--head-ref", required=True)
    p.add_argument("--receipt")
    p.add_argument("--emit-github-output")
    a = p.parse_args()
    if a.emit_github_output:
        paths = git(Path.cwd(), "diff", "--name-only", f"{a.base_ref}..{a.head_ref}").splitlines()
        with Path(a.emit_github_output).open("a", encoding="utf-8") as out:
            out.write(f"required={str(requires_acceptance(paths)).lower()}\n")
    else:
        raise SystemExit(
            check_staging_acceptance(
                [], base_ref=a.base_ref, head_ref=a.head_ref, receipt=a.receipt
            )
        )
