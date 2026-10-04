"""Retry classification and exponential backoff."""

from __future__ import annotations

import random
from email.utils import parsedate_to_datetime
import time

# Status codes that indicate a temporary condition on the receiver side.
RETRYABLE_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}


def is_success(status: int) -> bool:
    return 200 <= status < 300


def is_retryable_status(status: int) -> bool:
    return status in RETRYABLE_STATUS or status >= 500


def backoff_delay(attempt: int, base: float, cap: float, rng: random.Random | None = None) -> float:
    """Exponential backoff with "equal jitter".

    attempt is 1-based (the attempt that just failed). The delay grows as
    ``base * 2**(attempt-1)`` capped at ``cap``; half of it is fixed and half is
    random, which spreads retries out so a recovering receiver is not hit by a
    synchronized thundering herd.
    """
    rng = rng or random
    exp = min(cap, base * (2 ** max(0, attempt - 1)))
    return exp / 2 + rng.uniform(0, exp / 2)


def parse_retry_after(value: str | None, cap: float) -> float | None:
    if not value:
        return None
    value = value.strip()
    try:
        secs = float(value)
    except ValueError:
        try:
            secs = parsedate_to_datetime(value).timestamp() - time.time()
        except (TypeError, ValueError):
            return None
    return max(0.0, min(cap, secs))
