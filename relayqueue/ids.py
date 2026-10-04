"""Time-sortable, prefixed identifiers (``evt_…``, ``dlv_…``, ``ep_…``)."""

from __future__ import annotations

import secrets
import time

_ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz"  # Crockford base32, lowercase


def _b32(n: int, length: int) -> str:
    out = []
    for _ in range(length):
        out.append(_ALPHABET[n & 31])
        n >>= 5
    return "".join(reversed(out))


def new_id(prefix: str) -> str:
    """26 chars: 48-bit millisecond timestamp + 80 random bits (ULID layout)."""
    ms = int(time.time() * 1000)
    rand = int.from_bytes(secrets.token_bytes(10), "big")
    return f"{prefix}_{_b32(ms, 10)}{_b32(rand, 16)}"
