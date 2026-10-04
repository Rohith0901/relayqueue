"""Stage 2: timeouts, retries, permanent failures, crash recovery, idempotency."""

from __future__ import annotations

import dataclasses
import random
import threading
import time

import pytest
from fastapi.testclient import TestClient

from relayqueue import store
from relayqueue.api import create_app
from relayqueue.retry import backoff_delay, parse_retry_after
from relayqueue.worker import Worker, classify
from tests.conftest import run_worker_until


def _job(attempt=1, max_attempts=4):
    return store.Job("dlv_x", "evt_x", "t", "ep_x", "http://x", "{}", attempt, max_attempts, "tok")


def _status(conn, dlv_id):
    return store.get_delivery(conn, dlv_id)["status"]


def _setup(settings, url):
    client = TestClient(create_app(settings))
    client.post("/v1/endpoints", json={"url": url})
    evt = client.post("/v1/events", json={"type": "order.created", "data": {"n": 1}}).json()
    return client, evt["deliveries"][0]["id"]


# ----------------------------------------------------------------- classification

@pytest.mark.parametrize("status,err,expected", [
    (200, None, "success"),
    (204, None, "success"),
    (500, None, "retry"),
    (503, None, "retry"),
    (429, None, "retry"),
    (408, None, "retry"),
    (None, "timeout after 1s", "retry"),
    (None, "ConnectError: refused", "retry"),
    (400, None, "failed"),
    (401, None, "failed"),
    (404, None, "failed"),
    (410, None, "failed"),
    (301, None, "failed"),
])
def test_classify(settings, status, err, expected):
    outcome, reason, retry_at = classify(_job(), settings, status, err)
    assert outcome == expected
    if expected == "failed":
        assert reason.startswith("permanent failure")
    if expected == "retry":
        assert retry_at > time.time()


def test_classify_exhausted(settings):
    outcome, reason, _ = classify(_job(attempt=4, max_attempts=4), settings, 503, None)
    assert outcome == "failed"
    assert "attempts exhausted (4/4)" in reason


def test_retry_after_header_is_respected(settings):
    s = dataclasses.replace(settings, backoff_max=60)
    now = 1000.0
    _, _, retry_at = classify(_job(), s, 429, None, retry_after="30", now=now)
    assert retry_at >= now + 30


def test_backoff_grows_and_is_capped():
    rng = random.Random(1)
    delays = [backoff_delay(n, 1.0, 60.0, rng) for n in range(1, 12)]
    for n, d in enumerate(delays, start=1):
        exp = min(60.0, 2 ** (n - 1))
        assert exp / 2 <= d <= exp
    assert parse_retry_after("5", 60) == 5
    assert parse_retry_after("bogus", 60) is None


# ----------------------------------------------------------------- retries end to end

def test_temporary_failures_are_retried_until_success(settings, conn, scripted):
    srv = scripted(500, 503, 200)
    _, dlv = _setup(settings, srv.url)
    run_worker_until(Worker(settings), lambda: _status(conn, dlv) == "succeeded")
    d = store.get_delivery(conn, dlv)
    assert d["status"] == "succeeded"
    assert [a["outcome"] for a in d["attempt_log"]] == ["retry", "retry", "success"]
    assert [a["status_code"] for a in d["attempt_log"]] == [500, 503, 200]
    assert len(srv.calls) == 3


def test_permanent_failure_is_not_retried(settings, conn, scripted):
    srv = scripted(400)
    client, dlv = _setup(settings, srv.url)
    run_worker_until(Worker(settings), lambda: _status(conn, dlv) == "failed")
    time.sleep(0.3)
    assert len(srv.calls) == 1
    failed = client.get("/v1/failed-deliveries").json()
    assert [f["id"] for f in failed] == [dlv]
    assert failed[0]["last_error"] == "permanent failure: HTTP 400"
    assert failed[0]["endpoint_url"] == srv.url


def test_attempts_are_capped(settings, conn, scripted):
    srv = scripted(503)
    _, dlv = _setup(settings, srv.url)
    run_worker_until(Worker(settings), lambda: _status(conn, dlv) == "failed")
    d = store.get_delivery(conn, dlv)
    assert d["attempts"] == settings.max_attempts == len(srv.calls)
    assert d["last_error"].startswith("attempts exhausted (4/4)")


def test_timeout_counts_as_temporary(settings, conn, scripted):
    srv = scripted(("sleep", 1.5), 200)  # first call exceeds the 1s timeout
    _, dlv = _setup(settings, srv.url)
    run_worker_until(Worker(settings), lambda: _status(conn, dlv) == "succeeded")
    log = store.get_delivery(conn, dlv)["attempt_log"]
    assert log[0]["outcome"] == "retry" and "timeout" in log[0]["error"]
    assert log[1]["outcome"] == "success"


# ----------------------------------------------------------------- crash recovery

def test_expired_claim_is_recovered_by_another_worker(settings, conn, scripted):
    srv = scripted(200)
    _, dlv = _setup(settings, srv.url)

    # "Crashed" worker: claims the job with a short lease and never reports back.
    [job] = store.claim(conn, "crashed-worker", 10, lease_seconds=0.2)
    assert _status(conn, dlv) == "in_flight"
    assert store.claim(conn, "other", 10, lease_seconds=5) == []  # lease still valid
    time.sleep(0.3)

    run_worker_until(Worker(settings, worker_id="rescuer"), lambda: _status(conn, dlv) == "succeeded")
    d = store.get_delivery(conn, dlv)
    assert [(a["outcome"], a["worker_id"]) for a in d["attempt_log"]] == [
        ("abandoned", "crashed-worker"), ("success", "rescuer")]
    rec = conn.execute("SELECT * FROM lease_recoveries").fetchone()
    assert rec["previous_owner"] == "crashed-worker" and rec["reclaimed_by"] == "rescuer"


def test_stale_worker_cannot_overwrite_newer_state(settings, conn, scripted):
    srv = scripted(200)
    _, dlv = _setup(settings, srv.url)
    [job_a] = store.claim(conn, "A", 1, lease_seconds=0.1)
    time.sleep(0.15)
    [job_b] = store.claim(conn, "B", 1, lease_seconds=5)
    res_ok = store.Result("success", 200, None, "ok", time.time(), 5.0)
    res_fail = store.Result("failed", 400, "permanent failure: HTTP 400", "", time.time(), 5.0)
    assert store.record_result(conn, job_b, "B", res_ok) == "success"
    assert store.record_result(conn, job_a, "A", res_fail) == "stale"
    assert _status(conn, dlv) == "succeeded"


def test_poison_delivery_exhausts_attempts_via_expired_claims(settings, conn, scripted):
    srv = scripted(200)
    s = dataclasses.replace(settings, max_attempts=2)
    _, dlv = _setup(s, srv.url)
    for _ in range(2):
        assert len(store.claim(conn, "doomed", 1, lease_seconds=0.05)) == 1
        time.sleep(0.08)
    assert store.claim(conn, "next", 1, lease_seconds=5) == []
    d = store.get_delivery(conn, dlv)
    assert d["status"] == "failed" and "last claim expired" in d["last_error"]


def test_multiple_claimers_never_share_a_delivery(settings, conn, scripted):
    srv = scripted(200)
    client = TestClient(create_app(settings))
    client.post("/v1/endpoints", json={"url": srv.url})
    for i in range(200):
        client.post("/v1/events", json={"type": "t", "data": {"i": i}})

    claimed: list[str] = []
    lock = threading.Lock()

    def grab(name):
        from relayqueue.db import connect
        c = connect(settings.db_path)
        while True:
            jobs = store.claim(c, name, 7, lease_seconds=60)
            if not jobs:
                break
            with lock:
                claimed.extend(j.delivery_id for j in jobs)

    threads = [threading.Thread(target=grab, args=(f"w{i}",)) for i in range(6)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(claimed) == 200 == len(set(claimed))


# ----------------------------------------------------------------- idempotent submission

def test_idempotency_key_deduplicates_submissions(settings, conn, scripted):
    srv = scripted(200)
    client = TestClient(create_app(settings))
    client.post("/v1/endpoints", json={"url": srv.url})
    body = {"type": "payment.completed", "data": {"amount": 100}}
    r1 = client.post("/v1/events", json=body, headers={"Idempotency-Key": "pay-1"})
    r2 = client.post("/v1/events", json=body, headers={"Idempotency-Key": "pay-1"})
    assert r1.status_code == 202 and r2.status_code == 200
    assert r2.headers["Idempotent-Replayed"] == "true"
    assert r1.json()["id"] == r2.json()["id"]

    r3 = client.post("/v1/events", json={**body, "data": {"amount": 999}},
                     headers={"Idempotency-Key": "pay-1"})
    assert r3.status_code == 409
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 1


def test_concurrent_duplicate_submissions_create_one_event(settings, conn, scripted):
    srv = scripted(200)
    client = TestClient(create_app(settings))
    client.post("/v1/endpoints", json={"url": srv.url})
    ids, lock = [], threading.Lock()

    def submit():
        c = TestClient(create_app(settings))
        r = c.post("/v1/events", json={"type": "t", "data": {"x": 1}}, headers={"Idempotency-Key": "k"})
        with lock:
            ids.append((r.status_code, r.json()["id"]))

    threads = [threading.Thread(target=submit) for _ in range(12)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len({i for _, i in ids}) == 1
    assert sorted(code for code, _ in ids) == [200] * 11 + [202]
    assert conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 1
