"""Governance-shell admission: contour-selected evidence, bound to this build."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

from compute_change_surface import compute_surface


def git(root, *args):
    return subprocess.check_output(["git", *args], cwd=root, text=True).strip()


def evidence_level(paths, mapping="configs/change_surface_mapping.yaml"):
    return compute_surface(paths, mapping_path=Path(mapping))["evidence_level"]


def validate_result(value, *, base, head, tree, ci_run=None, ci_attempt=None):
    if (
        not isinstance(value, dict)
        or value.get("version") != 1
        or value.get("evidence_level") != "native"
        or value.get("stage") != "acceptance"
        or value.get("status") != "PASS"
    ):
        raise ValueError("Required native evidence is missing, failed or short-only")
    if any(
        value.get(k) != v for k, v in {"base_sha": base, "head_sha": head, "tree_sha": tree}.items()
    ):
        raise ValueError("Stale base/head/tested tree")
    if ci_run is not None and str(value.get("ci_run_id")) != str(ci_run):
        raise ValueError("Artifact belongs to another CI workflow run")
    if ci_attempt is not None and str(value.get("ci_attempt")) != str(ci_attempt):
        raise ValueError("Artifact belongs to another CI attempt")
    return value


def check_pr_evidence(
    changed_files,
    *,
    base_ref=None,
    head_ref=None,
    receipt=None,
    mapping="configs/change_surface_mapping.yaml",
):
    try:
        if base_ref and head_ref:
            changed_files = git(
                Path.cwd(), "diff", "--name-only", f"{base_ref}..{head_ref}"
            ).splitlines()
        level = evidence_level(changed_files, mapping)
        print(f"PR evidence: level={level}")
        if level == "code":
            return 0  # Ordinary code/PR checks still run in run_pr_gate.
        if not base_ref or not head_ref or not receipt:
            raise ValueError("Native producer result required for this build")
        tree = git(Path.cwd(), "merge-tree", "--write-tree", base_ref, head_ref).splitlines()[0]
        validate_result(
            json.loads(Path(receipt).read_text()),
            base=git(Path.cwd(), "rev-parse", base_ref),
            head=git(Path.cwd(), "rev-parse", head_ref),
            tree=tree,
            ci_run=os.environ.get("GITHUB_RUN_ID"),
            ci_attempt=os.environ.get("GITHUB_RUN_ATTEMPT"),
        )
        print("PR evidence: PASS")
        return 0
    except (ValueError, OSError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
        print(f"PR evidence: BLOCKED ({exc})")
        return 2


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-ref", required=True)
    p.add_argument("--head-ref", required=True)
    p.add_argument("--receipt")
    p.add_argument("--mapping", default="configs/change_surface_mapping.yaml")
    p.add_argument("--emit-github-output")
    a = p.parse_args()
    if a.emit_github_output:
        paths = git(Path.cwd(), "diff", "--name-only", f"{a.base_ref}..{a.head_ref}").splitlines()
        level = evidence_level(paths, a.mapping)
        with Path(a.emit_github_output).open("a", encoding="utf-8") as out:
            out.write(f"evidence_level={level}\n")
    else:
        raise SystemExit(
            check_pr_evidence(
                [], base_ref=a.base_ref, head_ref=a.head_ref, receipt=a.receipt, mapping=a.mapping
            )
        )
