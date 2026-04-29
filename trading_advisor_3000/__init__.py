from __future__ import annotations

from pathlib import Path
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

_SRC_PACKAGE_DIR = Path(__file__).resolve().parent.parent / "src" / __name__
if _SRC_PACKAGE_DIR.exists():
    _src_path = str(_SRC_PACKAGE_DIR)
    if _src_path in __path__:
        __path__.remove(_src_path)
    __path__.insert(0, _src_path)
