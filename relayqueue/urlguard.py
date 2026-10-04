"""Destination URL validation and SSRF protection.

Two layers:

1. :func:`validate_url` runs when an endpoint is registered or updated. It
   rejects non-http(s) schemes, embedded credentials, and any hostname that
   resolves to a private, loopback, link-local, multicast, reserved or
   otherwise non-public address (e.g. ``169.254.169.254`` cloud metadata).

2. :class:`GuardedBackend` is plugged into the worker's HTTP client. It
   re-resolves the hostname *at connect time*, checks every address, and then
   connects to the vetted IP itself. That closes the DNS-rebinding hole where a
   hostname resolves to a public IP during registration and to ``127.0.0.1``
   at delivery time. TLS still verifies against the original hostname (SNI).
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from typing import Iterable
from urllib.parse import urlsplit

import httpcore
import httpx

MAX_URL_LENGTH = 2048
BLOCKED_HOSTNAMES = {"localhost", "localhost.localdomain", "metadata.google.internal"}


class UnsafeURL(ValueError):
    pass


class BlockedDestination(httpcore.ConnectError):
    """Raised at connect time when a host resolves to a forbidden address."""


def is_public_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped:
            return is_public_ip(ip.ipv4_mapped)
        if ip.sixtofour:
            return is_public_ip(ip.sixtofour)
        if ip.teredo:
            return False
    return ip.is_global and not ip.is_multicast and not ip.is_reserved


def _check_addresses(host: str, addresses: Iterable[str]) -> list[str]:
    addrs = list(dict.fromkeys(addresses))
    if not addrs:
        raise UnsafeURL(f"{host!r} did not resolve to any address")
    for a in addrs:
        ip = ipaddress.ip_address(a.split("%", 1)[0])
        if not is_public_ip(ip):
            raise UnsafeURL(f"{host!r} resolves to non-public address {ip}")
    return addrs


def validate_url(url: str, allow_private: bool = False, resolve: bool = True) -> str:
    """Return the normalised URL or raise :class:`UnsafeURL`."""
    if len(url) > MAX_URL_LENGTH:
        raise UnsafeURL("URL is too long")
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except ValueError as exc:
        raise UnsafeURL(f"malformed URL: {exc}") from exc
    if parts.scheme not in ("http", "https"):
        raise UnsafeURL("only http:// and https:// URLs are allowed")
    if not parts.hostname:
        raise UnsafeURL("URL must include a host")
    if parts.username or parts.password:
        raise UnsafeURL("credentials in the URL are not allowed; use the signing secret")
    if parts.fragment:
        raise UnsafeURL("URL fragments are not allowed")
    if allow_private:
        return url.strip()

    host = parts.hostname.rstrip(".").lower()
    if host in BLOCKED_HOSTNAMES or host.endswith(".localhost") or host.endswith(".internal"):
        raise UnsafeURL(f"{host!r} is an internal hostname")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        _check_addresses(host, [str(ip)])
    elif resolve:
        try:
            infos = socket.getaddrinfo(host, port or (443 if parts.scheme == "https" else 80),
                                       type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise UnsafeURL(f"could not resolve {host!r}") from exc
        _check_addresses(host, [i[4][0] for i in infos])
    return url.strip()


class GuardedBackend(httpcore.AsyncNetworkBackend):
    """Network backend that only ever connects to vetted public addresses."""

    def __init__(self, allow_private: bool = False):
        self._inner = httpcore.AnyIOBackend()
        self.allow_private = allow_private

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        if self.allow_private:
            return await self._inner.connect_tcp(host, port, timeout, local_address, socket_options)
        loop = asyncio.get_running_loop()
        try:
            infos = await asyncio.wait_for(
                loop.getaddrinfo(host, port, type=socket.SOCK_STREAM), timeout
            )
        except (socket.gaierror, asyncio.TimeoutError) as exc:
            raise httpcore.ConnectError(f"could not resolve {host!r}: {exc}") from exc
        try:
            addrs = _check_addresses(host, [i[4][0] for i in infos])
        except UnsafeURL as exc:
            raise BlockedDestination(f"blocked destination: {exc}") from exc
        return await self._inner.connect_tcp(addrs[0], port, timeout, local_address, socket_options)

    async def connect_unix_socket(self, *args, **kwargs):  # pragma: no cover
        raise BlockedDestination("unix sockets are not allowed")

    async def sleep(self, seconds: float) -> None:
        await self._inner.sleep(seconds)


def guarded_transport(limits: httpx.Limits, allow_private: bool) -> httpx.AsyncHTTPTransport:
    transport = httpx.AsyncHTTPTransport(limits=limits, retries=0)
    original = transport._pool
    transport._pool = httpcore.AsyncConnectionPool(
        ssl_context=original._ssl_context,
        max_connections=limits.max_connections,
        max_keepalive_connections=limits.max_keepalive_connections,
        keepalive_expiry=limits.keepalive_expiry,
        network_backend=GuardedBackend(allow_private),
    )
    return transport


def is_blocked_error(exc: BaseException) -> bool:
    while exc is not None:
        if isinstance(exc, BlockedDestination):
            return True
        exc = exc.__cause__ or exc.__context__
    return False
