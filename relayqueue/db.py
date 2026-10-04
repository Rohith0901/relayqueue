"""SQLite persistence layer.

SQLite runs in WAL mode so the API process and many worker processes can share
one database file safely. Every write that must be atomic goes through
:func:`transaction`, which takes the write lock up-front (``BEGIN IMMEDIATE``)
so that two workers can never claim the same delivery.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS endpoints (
    id           TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    description  TEXT,
    created_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    body        TEXT NOT NULL,          -- exact bytes sent on every attempt
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    id                TEXT PRIMARY KEY,
    event_id          TEXT NOT NULL REFERENCES events(id),
    endpoint_id       TEXT NOT NULL REFERENCES endpoints(id),
    status            TEXT NOT NULL,          -- pending | in_flight | succeeded | failed
    attempts          INTEGER NOT NULL DEFAULT 0,
    last_status_code  INTEGER,
    last_error        TEXT,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL,
    delivered_at      REAL,
    UNIQUE (event_id, endpoint_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status, created_at);

CREATE TABLE IF NOT EXISTS attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id     TEXT NOT NULL REFERENCES deliveries(id),
    attempt_number  INTEGER NOT NULL,
    worker_id       TEXT,
    started_at      REAL NOT NULL,
    duration_ms     REAL,
    status_code     INTEGER,
    outcome         TEXT NOT NULL,   -- success | failed
    error           TEXT,
    response_body   TEXT
);
CREATE INDEX IF NOT EXISTS idx_attempts_delivery ON attempts(delivery_id);
"""


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")  # durable across process crashes in WAL mode
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path: str) -> None:
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a block inside ``BEGIN IMMEDIATE`` (exclusive writer lock)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
