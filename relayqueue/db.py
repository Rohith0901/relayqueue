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
CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    id            TEXT PRIMARY KEY,
    account_id    TEXT NOT NULL REFERENCES accounts(id),
    name          TEXT NOT NULL,
    prefix        TEXT NOT NULL,          -- first characters, safe to display
    key_hash      TEXT NOT NULL UNIQUE,   -- sha256 of the full key; key itself is never stored
    created_at    REAL NOT NULL,
    last_used_at  REAL,
    revoked_at    REAL
);

CREATE TABLE IF NOT EXISTS endpoints (
    id                         TEXT PRIMARY KEY,
    account_id                 TEXT NOT NULL REFERENCES accounts(id),
    url                        TEXT NOT NULL,
    description                TEXT,
    event_types                TEXT NOT NULL DEFAULT '["*"]',
    secret                     TEXT NOT NULL,
    previous_secret            TEXT,
    previous_secret_expires_at REAL,
    active                     INTEGER NOT NULL DEFAULT 1,
    created_at                 REAL NOT NULL,
    updated_at                 REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_endpoints_account ON endpoints(account_id);

CREATE TABLE IF NOT EXISTS events (
    id               TEXT PRIMARY KEY,
    account_id       TEXT NOT NULL REFERENCES accounts(id),
    type             TEXT NOT NULL,
    body             TEXT NOT NULL,      -- exact bytes sent on every attempt
    idempotency_key  TEXT,               -- client supplied, deduplicates submissions
    request_hash     TEXT,               -- detects key reuse with a different payload
    created_at       REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_idem ON events(account_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_events_account ON events(account_id, created_at);

CREATE TABLE IF NOT EXISTS deliveries (
    id                 TEXT PRIMARY KEY,
    account_id         TEXT NOT NULL REFERENCES accounts(id),
    event_id           TEXT NOT NULL REFERENCES events(id),
    endpoint_id        TEXT NOT NULL REFERENCES endpoints(id),
    -- pending -> in_flight -> succeeded
    --                     +-> retrying -> in_flight ...
    --                     +-> failed      (permanent error or attempts exhausted)
    -- succeeded / failed --(manual replay)--> pending
    status             TEXT NOT NULL,
    attempts           INTEGER NOT NULL DEFAULT 0,  -- attempts used from current budget
    max_attempts       INTEGER NOT NULL,
    next_attempt_at    REAL NOT NULL,
    lease_owner        TEXT,               -- worker currently holding the claim
    lease_token        TEXT,               -- fencing token: stale workers cannot write
    lease_expires_at   REAL,               -- claim expires -> another worker may take it
    last_status_code   INTEGER,
    last_error         TEXT,
    replay_count       INTEGER NOT NULL DEFAULT 0,
    current_replay_id  TEXT,
    created_at         REAL NOT NULL,
    updated_at         REAL NOT NULL,
    delivered_at       REAL,
    UNIQUE (event_id, endpoint_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_due     ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_lease   ON deliveries(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_event   ON deliveries(event_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_account ON deliveries(account_id, status, updated_at);

CREATE TABLE IF NOT EXISTS attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id     TEXT NOT NULL REFERENCES deliveries(id),
    attempt_number  INTEGER NOT NULL,
    replay_id       TEXT,            -- which manual replay (if any) this attempt belongs to
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

-- Audit trail: who replayed what, when, why, and from which state.
CREATE TABLE IF NOT EXISTS replays (
    id               TEXT PRIMARY KEY,
    account_id       TEXT NOT NULL REFERENCES accounts(id),
    delivery_id      TEXT NOT NULL REFERENCES deliveries(id),
    api_key_id       TEXT,
    actor            TEXT NOT NULL,
    reason           TEXT,
    previous_status  TEXT NOT NULL,
    previous_error   TEXT,
    previous_attempts INTEGER NOT NULL,
    created_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_replays_delivery ON replays(delivery_id);
CREATE INDEX IF NOT EXISTS idx_replays_account  ON replays(account_id, created_at);

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
