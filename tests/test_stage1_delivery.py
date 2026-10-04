"""Stage 1: a single event is stored, delivered, and the result is recorded."""

from fastapi.testclient import TestClient

from relayqueue import store
from relayqueue.api import create_app
from relayqueue.worker import Worker
from tests.conftest import run_worker_until


def test_one_delivery_end_to_end(settings, conn, receiver):
    client = TestClient(create_app(settings))

    ep = client.post("/v1/endpoints", json={"url": receiver.base_url + "/orders"}).json()
    assert ep["id"].startswith("ep_")

    resp = client.post("/v1/events", json={"type": "order.created", "data": {"order_id": 42}})
    assert resp.status_code == 202
    evt = resp.json()
    assert evt["id"].startswith("evt_")
    assert [d["status"] for d in evt["deliveries"]] == ["pending"]

    dlv_id = evt["deliveries"][0]["id"]
    worker = Worker(settings, worker_id="w-test")
    run_worker_until(worker, lambda: store.get_delivery(conn, dlv_id)["status"] == "succeeded")

    d = client.get(f"/v1/deliveries/{dlv_id}").json()
    assert d["status"] == "succeeded"
    assert d["last_status_code"] == 200
    [attempt] = d["attempt_log"]
    assert attempt["status_code"] == 200
    assert attempt["duration_ms"] > 0
    assert attempt["worker_id"] == "w-test"

    [got] = receiver.module.RECEIVED
    assert got["event_id"] == evt["id"]
    assert got["payload"]["data"] == {"order_id": 42}
    assert got["payload"]["type"] == "order.created"


def test_failed_response_is_recorded(settings, conn):
    client = TestClient(create_app(settings))
    client.post("/v1/endpoints", json={"url": "http://127.0.0.1:9/unreachable"})
    evt = client.post("/v1/events", json={"type": "x", "data": {}}).json()
    dlv_id = evt["deliveries"][0]["id"]
    run_worker_until(Worker(settings), lambda: store.get_delivery(conn, dlv_id)["status"] == "failed")
    d = store.get_delivery(conn, dlv_id)
    assert d["attempt_log"][0]["error"].startswith("ConnectError")
