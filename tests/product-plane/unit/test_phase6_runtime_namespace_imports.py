from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_runtime_namespace_import_does_not_require_pyarrow() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env = dict(os.environ)
    python_path_entries = [str(repo_root / "src")]
    if env.get("PYTHONPATH"):
        python_path_entries.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(python_path_entries)

    snippet = r"""
import builtins
import importlib

original_import = builtins.__import__

def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "pyarrow" or name.startswith("pyarrow."):
        raise ModuleNotFoundError("pyarrow is blocked in runtime contour import test")
    return original_import(name, globals, locals, fromlist, level)

builtins.__import__ = guarded_import
importlib.import_module("trading_advisor_3000.product_plane.runtime")
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        check=False,
        capture_output=True,
        text=True,
        cwd=repo_root,
        env=env,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
