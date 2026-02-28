from __future__ import annotations

import secrets
import threading

UINT64_MAX = (1 << 64) - 1
UINT32_MAX = (1 << 32) - 1

# Module-level (static) state
_used_u64: set[int] = set()
_used_u32: set[int] = set()
_lock_u64 = threading.Lock()
_lock_u32 = threading.Lock()


# ---------- uint64 ----------

def new_uint64_id(max_tries: int = 1000) -> int:
    """Generate a fresh uint64 and record it as used (thread-safe)."""
    for _ in range(max_tries):
        x = secrets.randbits(64)
        with _lock_u64:
            if x not in _used_u64:
                _used_u64.add(x)
                return x
    raise RuntimeError("Could not generate a unique uint64 id (too many collisions?)")


def is_uint64_id_used(x: int) -> bool:
    """Check whether x was used before (thread-safe)."""
    if not (0 <= x <= UINT64_MAX):
        raise ValueError("x must be a uint64 (0..2^64-1)")
    with _lock_u64:
        return x in _used_u64


def mark_uint64_id_used(x: int) -> bool:
    """Record x as used; return True if newly recorded, False if it was already used."""
    if not (0 <= x <= UINT64_MAX):
        raise ValueError("x must be a uint64 (0..2^64-1)")
    with _lock_u64:
        if x in _used_u64:
            return False
        _used_u64.add(x)
        return True


def reset_uint64_id_store() -> None:
    """Clear all remembered uint64 IDs (useful for tests)."""
    with _lock_u64:
        _used_u64.clear()


# ---------- uint32 ----------

def new_uint32_id(max_tries: int = 1000) -> int:
    """Generate a fresh uint32 and record it as used (thread-safe)."""
    for _ in range(max_tries):
        x = secrets.randbits(32)
        with _lock_u32:
            if x not in _used_u32:
                _used_u32.add(x)
                return x
    raise RuntimeError("Could not generate a unique uint32 id (too many collisions?)")


def is_uint32_id_used(x: int) -> bool:
    """Check whether x was used before (thread-safe)."""
    if not (0 <= x <= UINT32_MAX):
        raise ValueError("x must be a uint32 (0..2^32-1)")
    with _lock_u32:
        return x in _used_u32


def mark_uint32_id_used(x: int) -> bool:
    """Record x as used; return True if newly recorded, False if it was already used."""
    if not (0 <= x <= UINT32_MAX):
        raise ValueError("x must be a uint32 (0..2^32-1)")
    with _lock_u32:
        if x in _used_u32:
            return False
        _used_u32.add(x)
        return True


def reset_uint32_id_store() -> None:
    """Clear all remembered uint32 IDs (useful for tests)."""
    with _lock_u32:
        _used_u32.clear()
