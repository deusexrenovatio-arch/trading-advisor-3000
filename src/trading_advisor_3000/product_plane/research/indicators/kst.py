"""Native KST with validation before ROC values enter rolling means."""

import numpy as np
import pandas as pd
import pandas_ta_classic as ta


def compute_kst(frame):
    # KST contains finite ROC/rolling windows, with no recursive state helpers.
    # Bind the native provider statically for the numerical contract compiler.
    kst_function = ta.kst
    close = pd.to_numeric(frame["close"], errors="coerce")
    native = pd.to_numeric(frame.get("native_close", close), errors="coerce")
    # Bound cancellation error using this row's operands, not a global maximum
    # (which would introduce future dependence and change with a tail slice).
    scale = pd.concat([close.abs(), native.abs(), (native - close).abs()], axis=1).max(axis=1)
    invalid = ~np.isfinite(close) | close.abs().le(8 * np.finfo(float).eps * scale)
    if not invalid.any():
        return kst_function(close)

    native_roc = kst_function.__globals__["roc"]

    def checked_roc(close, length=None, **kwargs):
        result = native_roc(close, length=length, **kwargs)
        if result is None:
            return None
        bad_base = invalid.reindex(close.index).shift(int(length or 10), fill_value=False)
        return result.mask(bad_base | ~np.isfinite(result))

    # Same pattern as NativeCheckpoint: private function namespace only.
    # Native ROC/KST arithmetic and module globals remain unchanged.
    guarded = type(kst_function)(
        kst_function.__code__,
        {**kst_function.__globals__, "roc": checked_roc},
        kst_function.__name__,
        kst_function.__defaults__,
        kst_function.__closure__,
    )
    guarded.__kwdefaults__ = kst_function.__kwdefaults__
    result = guarded(close)
    if result is not None:
        result.attrs["invalid_price_inputs"] = {
            "reason": "invalid_roc_price_base",
            "indices": [
                int(i) if isinstance(i, (int, np.integer)) else str(i) for i in close.index[invalid]
            ],
        }
    return result
