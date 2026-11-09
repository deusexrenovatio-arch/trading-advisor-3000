"""Disposable, byte-bounded IPC cache; no disk, mutable Arrow buffers or truth."""

from collections import OrderedDict
from threading import RLock


class SnapshotReadCache:
    def __init__(self, *, max_bytes: int):
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("positive cache byte budget required")
        self.max_bytes = max_bytes
        self._entries: OrderedDict[tuple[str, str], bytes] = OrderedDict()
        self._bytes = 0
        self._lock = RLock()

    @property
    def size_bytes(self) -> int:
        with self._lock:
            return self._bytes

    def get(self, namespace: str, key: str) -> bytes | None:
        with self._lock:
            entry = self._entries.get((namespace, key))
            if entry is not None:
                self._entries.move_to_end((namespace, key))
            return entry

    def put(self, namespace: str, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError("cache accepts immutable IPC bytes only")
        slot = (namespace, key)
        cost = len(value) + len(namespace.encode()) + len(key.encode())
        with self._lock:
            previous = self._entries.pop(slot, None)
            if previous is not None:
                self._bytes -= len(previous) + len(namespace.encode()) + len(key.encode())
            if cost > self.max_bytes:
                return
            while self._entries and self._bytes + cost > self.max_bytes:
                (old_namespace, old_key), old = self._entries.popitem(last=False)
                self._bytes -= len(old) + len(old_namespace.encode()) + len(old_key.encode())
            self._entries[slot] = value
            self._bytes += cost
