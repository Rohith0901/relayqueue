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
    id               TEXT PRIMARY KEY,
    type             TEXT NOT NULL,
    body             TEXT NOT NULL,      -- exact bytes sent on every attempt
    idempotency_key  TEXT UNIQUE,        -- client supplied, deduplicates submissions
    request_hash     TEXT,               -- detects key reuse with a different payload
    created_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    id                TEXT PRIMARY KEY,
    event_id          TEXT NOT NULL REFERENCES events(id),
    endpoint_id       TEXT NOT NULL REFERENCES endpoints(id),
    -- pending -> in_flight -> succeeded
    --                     +-> retrying -> in_flight ...
    --                     +-> failed      (permanent error or attempts exhausted)
    status            TEXT NOT NULL,
    attempts          INTEGER NOT NULL DEFAULT 0,
    max_attempts      INTEGER NOT NULL,
    next_attempt_at   REAL NOT NULL,
    lease_owner       TEXT,               -- worker currently holding the claim
    lease_token       TEXT,               -- fencing token: stale workers cannot write
    lease_expires_at  REAL,               -- claim expires -> another worker may take it
    last_status_code  INTEGER,
    last_error        TEXT,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL,
    delivered_at      REAL,
    UNIQUE (event_id, endpoint_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_due   ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_lease ON deliveries(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_event ON deliveries(event_id);

CREATE TABLE IF NOT EXISTS attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id     TEXT NOT NULL REFERENCES deliveries(id),
    attempt_number  INTEGER NOT NULL,
    worker_id       TEXT,
    started_at      REAL NOT NULL,
    duration_ms     REAL,
    status_code     INTEGER,
    outcome         TEXT NOT NULL,   -- success | retry | failed | abandoned | stale
    error           TEXT,
    response_body   TEXT
);
CREATE INDEX IF NOT EXISTS idx_attempts_delivery ON attempts(delivery_id);
CREATE INDEX IF NOT EXISTS idx_attempts_started  ON attempts(started_at);

-- One row each time an expired claim (crashed / stalled worker) is taken over.
CREATE TABLE IF NOT EXISTS lease_recoveries (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id       TEXT NOT NULL,
    previous_owner    TEXT,
    lease_expired_at  REAL NOT NULL,
    reclaimed_at      REAL NOT NULL,
    reclaimed_by      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id     TEXT PRIMARY KEY,
    pid           INTEGER,
    hostname      TEXT,
    started_at    REAL NOT NULL,
    last_seen_at  REAL NOT NULL,
    processed     INTEGER NOT NULL DEFAULT 0,
    stopped_at    REAL
);
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
