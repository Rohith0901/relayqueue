"""Stage 1: a single event is stored, delivered, and the result is recorded."""

import dataclasses
import json

from fastapi.testclient import TestClient

from relayqueue.api import create_app
from relayqueue.worker import Worker
from tests.conftest import delivery, run_worker_until, status_of


def test_one_delivery_end_to_end(settings, conn, client, receiver):
    ep = client.post("/v1/endpoints", json={"url": receiver.base_url + "/hooks/ok"}).json()
    assert ep["id"].startswith("ep_")

    resp = client.post("/v1/events", json={"type": "order.created", "data": {"order_id": 42}})
    assert resp.status_code == 202
    evt = resp.json()
    assert evt["id"].startswith("evt_")
    assert [d["status"] for d in evt["deliveries"]] == ["pending"]

    dlv_id = evt["deliveries"][0]["id"]
    worker = Worker(settings, worker_id="w-test")
    run_worker_until(worker, lambda: status_of(conn, dlv_id) == "succeeded")

    d = client.get(f"/v1/deliveries/{dlv_id}").json()
    assert d["status"] == "succeeded"
    assert d["last_status_code"] == 200
    [attempt] = d["attempt_log"]
    assert attempt["status_code"] == 200
    assert attempt["duration_ms"] > 0
    assert attempt["worker_id"] == "w-test"

    [got] = receiver.state.events()
    assert got["event_id"] == evt["id"] and got["outcome"] == "processed"
    body = json.loads(got["body"])
    assert body["data"] == {"order_id": 42}
    assert body["type"] == "order.created"


def test_failed_response_is_recorded(settings, conn, make_client):
    settings = dataclasses.replace(settings, max_attempts=1)
    c = TestClient(create_app(settings))
    c.headers.update(make_client().headers)
    c.post("/v1/endpoints", json={"url": "http://127.0.0.1:9/unreachable"})
    evt = c.post("/v1/events", json={"type": "x", "data": {}}).json()
    dlv_id = evt["deliveries"][0]["id"]
    run_worker_until(Worker(settings), lambda: status_of(conn, dlv_id) == "failed")
    assert "ConnectError" in delivery(conn, dlv_id)["attempt_log"][0]["error"]
