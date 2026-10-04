"""Webhook payload signing and verification.

The scheme is compatible with the `Standard Webhooks <https://www.standardwebhooks.com>`_
specification:

* ``webhook-id``        – the stable event id (identical on every retry / replay)
* ``webhook-timestamp`` – unix seconds when this attempt was signed
* ``webhook-signature`` – space separated list of ``v1,<base64 HMAC-SHA256>``

The signed content is ``f"{id}.{timestamp}.{body}"``. During secret rotation
the payload is signed with both the new and the previous secret, so receivers
can roll their configuration without dropping deliveries.

Receivers can copy :func:`verify` (it only depends on the standard library).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import time
from typing import Iterable, Mapping

SECRET_PREFIX = "whsec_"
DEFAULT_TOLERANCE_SECONDS = 300


class SignatureError(Exception):
    pass


def generate_secret() -> str:
    return SECRET_PREFIX + base64.b64encode(secrets.token_bytes(32)).decode()


def _key(secret: str) -> bytes:
    if secret.startswith(SECRET_PREFIX):
        return base64.b64decode(secret[len(SECRET_PREFIX):])
    return secret.encode()


def compute_signature(secret: str, msg_id: str, timestamp: int, body: str | bytes) -> str:
    if isinstance(body, bytes):
        body = body.decode()
    content = f"{msg_id}.{timestamp}.{body}".encode()
    digest = hmac.new(_key(secret), content, hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode()


def sign_headers(
    secrets_: Iterable[str], msg_id: str, body: str | bytes, timestamp: int | None = None
) -> dict[str, str]:
    ts = int(time.time()) if timestamp is None else int(timestamp)
    sigs = " ".join(compute_signature(s, msg_id, ts, body) for s in secrets_ if s)
    return {
        "webhook-id": msg_id,
        "webhook-timestamp": str(ts),
        "webhook-signature": sigs,
    }


def verify(
    secret: str,
    headers: Mapping[str, str],
    body: str | bytes,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
    now: float | None = None,
) -> str:
    """Verify a webhook. Returns the event id, raises :class:`SignatureError`."""
    lower = {k.lower(): v for k, v in headers.items()}
    try:
        msg_id = lower["webhook-id"]
        ts = int(lower["webhook-timestamp"])
        sig_header = lower["webhook-signature"]
    except (KeyError, ValueError) as exc:
        raise SignatureError("missing or malformed signature headers") from exc

    now = time.time() if now is None else now
    if abs(now - ts) > tolerance:
        raise SignatureError("timestamp outside tolerance (possible replay attack)")

    expected = compute_signature(secret, msg_id, ts, body).split(",", 1)[1]
    for candidate in sig_header.split():
        version, _, sig = candidate.partition(",")
        if version == "v1" and hmac.compare_digest(sig, expected):
            return msg_id
    raise SignatureError("no matching signature")
