"""Runtime configuration, read from environment variables (prefix ``RELAYQUEUE_``)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(f"RELAYQUEUE_{name}", default)


@dataclass(frozen=True)
class Settings:
    db_path: str = field(default_factory=lambda: _env("DB_PATH", "relayqueue.db"))

    # Security
    admin_token: str | None = field(default_factory=lambda: os.environ.get("RELAYQUEUE_ADMIN_TOKEN") or None)
    allow_private_networks: bool = field(
        default_factory=lambda: _env("ALLOW_PRIVATE_NETWORKS", "false").lower() in ("1", "true", "yes"))
    rotation_grace_seconds: float = field(default_factory=lambda: float(_env("ROTATION_GRACE", "86400")))

    # Delivery behaviour
    request_timeout: float = field(default_factory=lambda: float(_env("REQUEST_TIMEOUT", "10")))
    max_attempts: int = field(default_factory=lambda: int(_env("MAX_ATTEMPTS", "8")))
    backoff_base: float = field(default_factory=lambda: float(_env("BACKOFF_BASE", "2")))
    backoff_max: float = field(default_factory=lambda: float(_env("BACKOFF_MAX", "600")))

    # Worker / lease behaviour
    lease_seconds: float = field(default_factory=lambda: float(_env("LEASE_SECONDS", "30")))
    poll_interval: float = field(default_factory=lambda: float(_env("POLL_INTERVAL", "0.25")))
    worker_concurrency: int = field(default_factory=lambda: int(_env("WORKER_CONCURRENCY", "16")))
    heartbeat_interval: float = field(default_factory=lambda: float(_env("HEARTBEAT_INTERVAL", "5")))

    def validate(self) -> "Settings":
        if self.lease_seconds <= self.request_timeout:
            raise ValueError(
                "RELAYQUEUE_LEASE_SECONDS must be greater than RELAYQUEUE_REQUEST_TIMEOUT, "
                "otherwise a healthy in-flight request could have its claim taken over."
            )
        if self.max_attempts < 1:
            raise ValueError("RELAYQUEUE_MAX_ATTEMPTS must be >= 1")
        return self


def get_settings() -> Settings:
    return Settings().validate()
