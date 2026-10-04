"""Stage 4: prove reliability against the demo receiver.

The first group runs a worker in-process against each receiver behaviour.
The second group runs real OS processes (API, several workers, receiver) and
kills them with SIGKILL.
"""

from __future__ import annotations

import dataclasses
import json
import signal
import threading
import time

import pytest

from demo.harness import Cluster, wait_until
from relayqueue.worker import Worker
from tests.conftest import delivery, run_worker_until, status_of


def _publish(client, receiver, hook, n=1):
    client.post("/v1/endpoints", json={"url": f"{receiver.base_url}/hooks/{hook}"})
    ep = client.get("/v1/endpoints").json()[-1]
    secret = client.get(f"/v1/endpoints/{ep['id']}/secret").json()["secret"]
    receiver.state.db.execute("INSERT OR REPLACE INTO secrets VALUES (?, ?)", ("/hooks/" + hook.split("?")[0], secret))
    return [client.post("/v1/events", json={"type": "order.created", "data": {"i": i}}).json()["deliveries"][0]["id"]
            for i in range(n)]


def _hook_stats(receiver, hook):
    return receiver.state.stats()["paths"]["/hooks/" + hook]


# ------------------------------------------------------------------ receiver behaviours

def test_receiver_always_succeeds(settings, conn, client, receiver):
    ids = _publish(client, receiver, "ok", n=5)
    run_worker_until(Worker(settings), lambda: all(status_of(conn, i) == "succeeded" for i in ids))
    st = _hook_stats(receiver, "ok")
    assert st["requests"] == st["unique_events"] == 5 and st["duplicates"] == 0
    assert st["bad_signatures"] == 0
    assert all(r["signature"] == "valid" for r in receiver.state.events())


def test_receiver_fails_twice_then_succeeds(settings, conn, client, receiver):
    [dlv] = _publish(client, receiver, "fail-twice")
    run_worker_until(Worker(settings), lambda: status_of(conn, dlv) == "succeeded")
    log = delivery(conn, dlv)["attempt_log"]
    assert [(a["outcome"], a["status_code"]) for a in log] == [("retry", 500), ("retry", 500), ("success", 200)]
    assert _hook_stats(receiver, "fail-twice")["unique_events"] == 1


def test_receiver_responds_slowly_within_timeout(settings, conn, client, receiver):
    [dlv] = _publish(client, receiver, "slow?delay=0.6")
    run_worker_until(Worker(settings), lambda: status_of(conn, dlv) == "succeeded")
    [a] = delivery(conn, dlv)["attempt_log"]
    assert a["outcome"] == "success" and a["duration_ms"] >= 600


def test_receiver_slower_than_timeout_is_retried_and_deduplicated(settings, conn, client, receiver):
    # First request takes 1.5s (> 1s timeout): we give up, but the receiver still processed it.
    [dlv] = _publish(client, receiver, "slow?delay=1.5&times=1")
    run_worker_until(Worker(settings), lambda: status_of(conn, dlv) == "succeeded")
    log = delivery(conn, dlv)["attempt_log"]
    assert log[0]["outcome"] == "retry" and "timeout" in log[0]["error"]
    assert log[-1]["outcome"] == "success"
    time.sleep(0.7)  # let the slow handler finish
    st = _hook_stats(receiver, "slow")
    assert st["unique_events"] == 1 and st["duplicates"] == 1   # same webhook-id seen twice


def test_receiver_processes_but_drops_response(settings, conn, client, receiver):
    [dlv] = _publish(client, receiver, "drop-response")
    run_worker_until(Worker(settings), lambda: status_of(conn, dlv) == "succeeded")
    log = delivery(conn, dlv)["attempt_log"]
    assert log[0]["outcome"] == "retry" and log[0]["status_code"] is None
    assert log[1]["outcome"] == "success"
    st = _hook_stats(receiver, "drop-response")
    # The event was processed exactly once even though it was delivered twice.
    assert st == {**st, "requests": 2, "dropped": 1, "unique_events": 1, "duplicates": 1}


def test_receiver_rejects_forged_signature(settings, conn, client, receiver):
    [dlv] = _publish(client, receiver, "ok")
    receiver.state.db.execute("UPDATE secrets SET secret = 'whsec_' || 'AAAA'")
    run_worker_until(Worker(dataclasses.replace(settings, max_attempts=1)), lambda: status_of(conn, dlv) == "failed")
    assert _hook_stats(receiver, "ok")["bad_signatures"] == 1
    assert delivery(conn, dlv)["last_error"] == "permanent failure: HTTP 401"


# ------------------------------------------------------------------ real processes

@pytest.fixture
def cluster(tmp_path):
    c = Cluster(str(tmp_path / "cluster"))
    c.start_api()
    c.start_receiver()
    c.create_account("reliability")
    yield c
    c.close()


def _db(cluster):
    import sqlite3
    conn = sqlite3.connect(cluster.db_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_worker_killed_mid_delivery_is_recovered(cluster):
    """SIGKILL a worker while it holds claims on slow deliveries; a new worker finishes them."""
    cluster.add_endpoint("slow?delay=1.5")
    for i in range(20):
        assert cluster.publish("order.created", {"i": i}).status_code == 202
    cluster.start_worker("victim", concurrency=32)
    db = _db(cluster)
    wait_until(lambda: db.execute("SELECT COUNT(*) FROM deliveries WHERE status='in_flight'").fetchone()[0] == 20,
               10, what="claims")
    time.sleep(0.3)                      # requests are now in flight at the receiver
    cluster.kill_worker("victim")        # SIGKILL: no cleanup, no result recorded
    killed_at = time.time()
    assert db.execute("SELECT COUNT(*) FROM deliveries WHERE status='succeeded'").fetchone()[0] == 0

    cluster.start_worker("rescuer")
    wait_until(cluster.all_terminal, 30, what="recovery")
    recovered_in = time.time() - killed_at

    s = cluster.stats()
    assert s["deliveries"]["succeeded"] == 20 and s["deliveries"]["failed"] == 0
    rec = db.execute("SELECT * FROM lease_recoveries").fetchall()
    assert len(rec) == 20 and {r["previous_owner"] for r in rec} == {"victim"}
    assert {r["reclaimed_by"] for r in rec} == {"rescuer"}
    # bounded by lease (4s) + slow receiver (1.5s) + slack
    assert recovered_in < 4 + 1.5 + 4
    time.sleep(1.6)
    st = cluster.receiver_stats()["paths"]["/hooks/slow"]
    assert st["unique_events"] == 20                     # nothing lost
    assert st["duplicates"] == 20                        # every in-flight request was redelivered once…
    assert st["requests"] == 40                          # …and absorbed by the idempotent receiver


def test_graceful_shutdown_finishes_in_flight_work(cluster):
    cluster.add_endpoint("slow?delay=1")
    for i in range(10):
        cluster.publish("order.created", {"i": i})
    cluster.start_worker("w1")
    db = _db(cluster)
    wait_until(lambda: db.execute("SELECT COUNT(*) FROM deliveries WHERE status='in_flight'").fetchone()[0] == 10,
               10, what="claims")
    cluster.kill_worker("w1", signal.SIGTERM)   # drains before exiting
    assert db.execute("SELECT COUNT(*) FROM deliveries WHERE status='succeeded'").fetchone()[0] == 10
    assert db.execute("SELECT COUNT(*) FROM lease_recoveries").fetchone()[0] == 0


def test_duplicate_submissions_over_http(cluster):
    cluster.add_endpoint("ok")
    results, lock = [], threading.Lock()

    def submit(key):
        import httpx
        with httpx.Client(trust_env=False) as h:
            r = h.post(cluster.api_url + "/v1/events", json={"type": "payment.completed", "data": {"k": key}},
                       headers={**cluster.headers, "idempotency-key": key})
        with lock:
            results.append((key, r.status_code, r.json()["id"]))

    # 10 logical events, each submitted 5 times concurrently (a producer retrying after timeouts)
    threads = [threading.Thread(target=submit, args=(f"pay-{i % 10}",)) for i in range(50)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    by_key: dict[str, set] = {}
    for key, code, eid in results:
        assert code in (200, 202)
        by_key.setdefault(key, set()).add(eid)
    assert len(by_key) == 10 and all(len(v) == 1 for v in by_key.values())
    assert sum(code == 202 for _, code, _ in results) == 10

    cluster.start_worker("w1")
    wait_until(cluster.all_terminal, 20, what="delivery")
    st = cluster.receiver_stats()["paths"]["/hooks/ok"]
    assert st["requests"] == st["unique_events"] == 10


def test_multiple_workers_share_load_without_double_delivery(cluster):
    cluster.add_endpoint("ok")
    cluster.add_endpoint("fail-twice")
    n = 150
    for i in range(n):
        cluster.publish("order.created", {"i": i})
    for w in ("w1", "w2", "w3"):
        cluster.start_worker(w, concurrency=16)
    wait_until(cluster.all_terminal, 60, what="all deliveries")

    s = cluster.stats()
    assert s["deliveries"]["succeeded"] == 2 * n and s["deliveries"]["failed"] == 0
    db = _db(cluster)
    # exactly one successful attempt per delivery, and never two workers on one delivery
    assert db.execute("SELECT COUNT(*) FROM attempts WHERE outcome='success'").fetchone()[0] == 2 * n
    assert db.execute("SELECT COUNT(*) FROM attempts WHERE outcome IN ('stale','abandoned')").fetchone()[0] == 0
    per_worker = dict(db.execute(
        "SELECT worker_id, COUNT(*) FROM attempts GROUP BY worker_id").fetchall())
    assert set(per_worker) == {"w1", "w2", "w3"}, per_worker

    rs = cluster.receiver_stats()["paths"]
    assert rs["/hooks/ok"]["requests"] == rs["/hooks/ok"]["unique_events"] == n
    assert rs["/hooks/ok"]["duplicates"] == 0
    assert rs["/hooks/fail-twice"]["unique_events"] == n
    assert rs["/hooks/fail-twice"]["requests"] == 3 * n


def test_receiver_outage_and_worker_restart_lose_nothing(cluster):
    cluster.add_endpoint("ok")
    cluster.start_worker("w1")
    cluster.kill_receiver()                       # hard outage: connection refused
    for i in range(40):
        cluster.publish("order.created", {"i": i})
    time.sleep(1.5)                               # deliveries fail and back off
    cluster.kill_worker("w1")                     # …and the worker crashes too
    cluster.start_receiver()
    cluster.start_worker("w2")
    wait_until(cluster.all_terminal, 40, what="drain")
    s = cluster.stats()
    assert s["deliveries"]["succeeded"] == 40
    assert s["attempts"].get("retry", 0) > 0
    assert cluster.receiver_stats()["paths"]["/hooks/ok"]["unique_events"] == 40
