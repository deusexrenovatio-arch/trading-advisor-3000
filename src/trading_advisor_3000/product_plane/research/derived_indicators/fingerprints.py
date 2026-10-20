"""Versioned canonical source fingerprints; Arrow/NumPy buffers, stdlib SHA256."""

import hashlib
import struct

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

_MAGIC = b"TA3000-COLUMNAR-SHA256\x00v1\x00"


def _tag(dtype):
    if pa.types.is_dictionary(dtype):
        return _tag(dtype.value_type)
    if pa.types.is_null(dtype):
        return "null"
    if pa.types.is_boolean(dtype):
        return "bool"
    if pa.types.is_integer(dtype):
        return ("i" if pa.types.is_signed_integer(dtype) else "u") + str(dtype.bit_width)
    if pa.types.is_float32(dtype) or pa.types.is_float64(dtype):
        return "f" + str(dtype.bit_width)
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "utf8"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return "binary"
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return f"list<{_tag(dtype.value_type)}>"
    raise TypeError(f"unsupported canonical fingerprint type: {dtype}")


def _frame(digest, data):
    digest.update(struct.pack("<Q", len(data)))
    digest.update(data)


def _array(digest, array):
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    if pa.types.is_dictionary(array.type):
        array = array.dictionary_decode()
    tag = _tag(array.type)
    missing = pc.is_null(array)
    if tag in {"f32", "f64"}:
        missing = pc.or_(missing, pc.fill_null(pc.is_nan(array), False))
    mask = missing.to_numpy(zero_copy_only=False)
    digest.update(struct.pack("<Q", len(array)))
    digest.update(mask.astype(np.uint8).tobytes())
    if tag == "null":
        return
    if tag.startswith("list<"):
        lengths = pc.fill_null(pc.list_value_length(array), 0).to_numpy(zero_copy_only=False)
        digest.update(lengths.astype("<u8").tobytes())
        _array(digest, pc.list_flatten(array))
    elif tag in {"utf8", "binary"}:
        array = pc.cast(pc.fill_null(array, "" if tag == "utf8" else b""), pa.large_binary())
        offsets = np.frombuffer(
            array.buffers()[1], dtype="<i8", count=len(array) + 1, offset=array.offset * 8
        )
        digest.update(np.diff(offsets).astype("<u8").tobytes())
        if offsets[-1] > offsets[0]:
            digest.update(memoryview(array.buffers()[2])[offsets[0] : offsets[-1]])
    else:
        values = (
            pc.fill_null(array, False if tag == "bool" else 0).to_numpy(zero_copy_only=False).copy()
        )
        values[mask] = 0
        if tag in {"f32", "f64"}:
            values[values == 0] = 0  # canonical positive zero
        dtype = "u1" if tag == "bool" else "<" + tag[0] + str(array.type.bit_width // 8)
        digest.update(values.astype(dtype).tobytes())


def columnar_fingerprint(table: pa.Table, *, domain: str) -> str:
    """Type/presence-aware raw canonical identity, deliberately distinct from DTO JSON."""
    digest = hashlib.sha256(_MAGIC)
    _frame(digest, domain.encode("utf-8"))
    digest.update(struct.pack("<QI", len(table), len(table.column_names)))
    for name in sorted(table.column_names, key=lambda name: name.encode("utf-8")):
        _frame(digest, name.encode("utf-8"))
        _frame(digest, _tag(table[name].type).encode("ascii"))
        _array(digest, table[name])
    return "columnar-v1:" + digest.hexdigest()
