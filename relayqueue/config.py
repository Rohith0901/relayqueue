"""Runtime configuration, read from environment variables (prefix ``RELAYQUEUE_``)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(f"RELAYQUEUE_{name}", default)


@dataclass(frozen=True)
class Settings:
    db_path: str = field(default_factory=lambda: _env("DB_PATH", "relayqueue.db"))
    request_timeout: float = field(default_factory=lambda: float(_env("REQUEST_TIMEOUT", "10")))
    poll_interval: float = field(default_factory=lambda: float(_env("POLL_INTERVAL", "0.25")))
    worker_concurrency: int = field(default_factory=lambda: int(_env("WORKER_CONCURRENCY", "16")))


def get_settings() -> Settings:
    return Settings()
