"""Stage 3: API keys & ownership, history, audited replay, signing, SSRF protection."""

from __future__ import annotations

import dataclasses
import socket
import time

import pytest
from fastapi.testclient import TestClient

from relayqueue import signing, store
from relayqueue.api import create_app
from relayqueue.urlguard import UnsafeURL, validate_url
from relayqueue.worker import Worker
from tests.conftest import delivery, run_worker_until, status_of


# ----------------------------------------------------------------- API keys & ownership

def test_requests_without_a_valid_key_are_rejected(settings):
    c = TestClient(create_app(settings))
    assert c.get("/v1/endpoints").status_code == 401
    assert c.get("/v1/endpoints", headers={"authorization": "Bearer rq_nope"}).status_code == 401
    assert c.post("/v1/admin/accounts", json={"name": "x"}).status_code == 401
    assert c.post("/v1/admin/accounts", json={"name": "x"},
                  headers={"x-admin-token": "wrong"}).status_code == 401


def test_admin_api_disabled_without_token(settings):
    c = TestClient(create_app(dataclasses.replace(settings, admin_token=None)))
    assert c.post("/v1/admin/accounts", json={"name": "x"},
                  headers={"x-admin-token": "admin-secret"}).status_code == 403


def test_api_keys_can_be_created_and_revoked(client, settings):
    assert client.get("/v1/account").json()["account_name"] == "acme"
    new = client.post("/v1/api-keys", json={"name": "ci"}).json()
    c2 = TestClient(create_app(settings), headers={"x-api-key": new["key"]})
    assert c2.get("/v1/account").status_code == 200
    keys = client.get("/v1/api-keys").json()
    assert {k["name"] for k in keys} == {"default", "ci"}
    assert all("key" not in k and "key_hash" not in k for k in keys)
    assert client.delete(f"/v1/api-keys/{new['id']}").status_code == 204
    assert c2.get("/v1/account").status_code == 401


def test_keys_are_stored_hashed(client, conn):
    key = client.headers["authorization"].split()[1]
    row = conn.execute("SELECT * FROM api_keys").fetchone()
    assert key not in tuple(str(v) for v in row)


def test_accounts_cannot_see_each_others_resources(make_client, scripted):
    a, b = make_client("alpha"), make_client("beta")
    srv = scripted(200)
    ep = a.post("/v1/endpoints", json={"url": srv.url}).json()
    evt = a.post("/v1/events", json={"type": "t", "data": {}}).json()
    dlv = evt["deliveries"][0]["id"]

    for path in (f"/v1/endpoints/{ep['id']}", f"/v1/endpoints/{ep['id']}/secret",
                 f"/v1/events/{evt['id']}", f"/v1/deliveries/{dlv}"):
        assert b.get(path).status_code == 404, path
    assert b.patch(f"/v1/endpoints/{ep['id']}", json={"active": False}).status_code == 404
    assert b.post(f"/v1/endpoints/{ep['id']}/rotate-secret").status_code == 404
    assert b.post(f"/v1/deliveries/{dlv}/replay", json={}).status_code == 404
    assert b.get("/v1/endpoints").json() == []
    assert b.get("/v1/deliveries").json() == []
    assert b.get("/v1/events").json() == []

    # B's events never fan out to A's endpoints
    evt_b = b.post("/v1/events", json={"type": "t", "data": {}}).json()
    assert evt_b["deliveries"] == []


def test_idempotency_keys_are_scoped_per_account(make_client, scripted):
    a, b = make_client("alpha"), make_client("beta")
    body = {"type": "t", "data": {"x": 1}}
    ra = a.post("/v1/events", json=body, headers={"Idempotency-Key": "same"})
    rb = b.post("/v1/events", json=body, headers={"Idempotency-Key": "same"})
    assert ra.status_code == rb.status_code == 202
    assert ra.json()["id"] != rb.json()["id"]


def test_event_type_filters(client, scripted):
    orders, payments = scripted(200), scripted(200)
    client.post("/v1/endpoints", json={"url": orders.url, "event_types": ["order.*"]})
    client.post("/v1/endpoints", json={"url": payments.url, "event_types": ["payment.completed"]})
    e1 = client.post("/v1/events", json={"type": "order.created", "data": {}}).json()
    e2 = client.post("/v1/events", json={"type": "payment.completed", "data": {}}).json()
    e3 = client.post("/v1/events", json={"type": "user.updated", "data": {}}).json()
    assert [d["endpoint_url"] for d in e1["deliveries"]] == [orders.url]
    assert [d["endpoint_url"] for d in e2["deliveries"]] == [payments.url]
    assert e3["deliveries"] == []


# ----------------------------------------------------------------- signing

def test_payloads_are_signed_and_verifiable(settings, conn, client, scripted):
    srv = scripted(200)
    ep = client.post("/v1/endpoints", json={"url": srv.url}).json()
    evt = client.post("/v1/events", json={"type": "order.created", "data": {"id": 7}}).json()
    run_worker_until(Worker(settings), lambda: status_of(conn, evt["deliveries"][0]["id"]) == "succeeded")

    [call] = srv.calls
    assert signing.verify(ep["secret"], call["headers"], call["body"]) == evt["id"]
    with pytest.raises(signing.SignatureError):
        signing.verify(ep["secret"], call["headers"], call["body"].replace("7", "8"))
    with pytest.raises(signing.SignatureError):
        signing.verify(signing.generate_secret(), call["headers"], call["body"])
    with pytest.raises(signing.SignatureError, match="tolerance"):
        signing.verify(ep["secret"], call["headers"], call["body"], now=time.time() + 3600)


def test_secret_rotation_signs_with_both_secrets(settings, conn, client, scripted):
    srv = scripted(200)
    ep = client.post("/v1/endpoints", json={"url": srv.url}).json()
    rotated = client.post(f"/v1/endpoints/{ep['id']}/rotate-secret").json()
    assert rotated["previous_secret"] == ep["secret"] and rotated["secret"] != ep["secret"]
    evt = client.post("/v1/events", json={"type": "t", "data": {}}).json()
    run_worker_until(Worker(settings), lambda: status_of(conn, evt["deliveries"][0]["id"]) == "succeeded")
    [call] = srv.calls
    assert len(call["headers"]["webhook-signature"].split()) == 2
    signing.verify(ep["secret"], call["headers"], call["body"])        # old still accepted
    signing.verify(rotated["secret"], call["headers"], call["body"])   # new accepted


def test_event_id_is_stable_across_retries(settings, conn, client, scripted):
    srv = scripted(503, 503, 200)
    client.post("/v1/endpoints", json={"url": srv.url})
    evt = client.post("/v1/events", json={"type": "t", "data": {}}).json()
    run_worker_until(Worker(settings), lambda: status_of(conn, evt["deliveries"][0]["id"]) == "succeeded")
    ids = {c["headers"]["webhook-id"] for c in srv.calls}
    bodies = {c["body"] for c in srv.calls}
    assert ids == {evt["id"]} and len(bodies) == 1
    assert [c["headers"]["x-relayqueue-attempt"] for c in srv.calls] == ["1", "2", "3"]


# ----------------------------------------------------------------- history & replay

def test_manual_replay_with_audit_trail(settings, conn, client, scripted):
    srv = scripted(400, 200)  # permanent failure, then the receiver is fixed
    client.post("/v1/endpoints", json={"url": srv.url})
    evt = client.post("/v1/events", json={"type": "invoice.paid", "data": {}}).json()
    dlv = evt["deliveries"][0]["id"]
    run_worker_until(Worker(settings), lambda: status_of(conn, dlv) == "failed")

    r = client.post(f"/v1/deliveries/{dlv}/replay", json={"reason": "receiver bug fixed in v2.3"})
    assert r.status_code == 202
    rp = r.json()
    assert rp["previous_status"] == "failed" and rp["actor"].startswith("api_key:default")
    # a scheduled delivery cannot be replayed again
    assert client.post(f"/v1/deliveries/{dlv}/replay", json={}).status_code == 409

    run_worker_until(Worker(settings), lambda: status_of(conn, dlv) == "succeeded")
    d = client.get(f"/v1/deliveries/{dlv}").json()
    assert d["replay_count"] == 1
    assert [(a["outcome"], a["replay_id"]) for a in d["attempt_log"]] == [
        ("failed", None), ("success", rp["id"])]
    [audit] = d["replays"]
    assert audit["reason"] == "receiver bug fixed in v2.3"
    assert audit["previous_error"] == "permanent failure: HTTP 400"
    assert client.get("/v1/replays").json()[0]["id"] == rp["id"]
    assert {c["headers"]["webhook-id"] for c in srv.calls} == {evt["id"]}


def test_bulk_replay_of_failed_deliveries(settings, conn, client, scripted):
    srv = scripted(410)
    client.post("/v1/endpoints", json={"url": srv.url})
    ids = [client.post("/v1/events", json={"type": "t", "data": {"i": i}}).json()["deliveries"][0]["id"]
           for i in range(5)]
    run_worker_until(Worker(settings), lambda: all(status_of(conn, i) == "failed" for i in ids))
    assert len(client.get("/v1/failed-deliveries").json()) == 5
    srv.script = [200]
    r = client.post("/v1/deliveries/replay", json={"status": "failed", "reason": "endpoint restored"}).json()
    assert r["replayed"] == 5
    run_worker_until(Worker(settings), lambda: all(status_of(conn, i) == "succeeded" for i in ids))
    assert client.get("/v1/failed-deliveries").json() == []


def test_paused_endpoint_holds_deliveries(settings, conn, client, scripted):
    srv = scripted(200)
    ep = client.post("/v1/endpoints", json={"url": srv.url}).json()
    client.patch(f"/v1/endpoints/{ep['id']}", json={"active": False})
    client.patch(f"/v1/endpoints/{ep['id']}", json={"active": True})
    evt = client.post("/v1/events", json={"type": "t", "data": {}}).json()
    dlv = evt["deliveries"][0]["id"]
    client.patch(f"/v1/endpoints/{ep['id']}", json={"active": False})
    run_worker_until(Worker(settings), lambda: False, timeout=0.4)
    assert status_of(conn, dlv) == "pending" and srv.calls == []
    client.patch(f"/v1/endpoints/{ep['id']}", json={"active": True})
    run_worker_until(Worker(settings), lambda: status_of(conn, dlv) == "succeeded")


# ----------------------------------------------------------------- SSRF protection

@pytest.mark.parametrize("url", [
    "ftp://example.com/x",
    "file:///etc/passwd",
    "http://localhost/hook",
    "http://api.localhost/hook",
    "http://127.0.0.1/hook",
    "http://127.1/hook",
    "http://0.0.0.0/hook",
    "http://10.0.0.5/hook",
    "http://172.16.3.4/hook",
    "http://192.168.1.1/hook",
    "http://169.254.169.254/latest/meta-data",
    "http://100.64.0.1/hook",
    "http://[::1]/hook",
    "http://[fd00::1]/hook",
    "http://[::ffff:127.0.0.1]/hook",
    "http://user:pass@example.com/hook",
    "http://metadata.google.internal/",
    "http:///nohost",
])
def test_unsafe_urls_are_rejected(url):
    with pytest.raises(UnsafeURL):
        validate_url(url, allow_private=False)


def test_public_ip_is_accepted():
    assert validate_url("https://93.184.215.14/hook") == "https://93.184.215.14/hook"


def test_hostname_resolving_to_private_address_is_rejected(monkeypatch):
    def fake(host, *a, **k):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 443))]
    monkeypatch.setattr(socket, "getaddrinfo", fake)
    with pytest.raises(UnsafeURL, match="non-public"):
        validate_url("https://innocent.example.com/hook")


def test_api_rejects_private_destinations(settings, make_client):
    strict = dataclasses.replace(settings, allow_private_networks=False)
    c = TestClient(create_app(strict))
    c.headers.update(make_client().headers)
    r = c.post("/v1/endpoints", json={"url": "http://169.254.169.254/latest/meta-data"})
    assert r.status_code == 422 and "non-public" in r.json()["detail"]


def test_worker_blocks_private_address_at_connect_time(settings, conn, client, scripted):
    """Simulates DNS rebinding: the stored URL now points at a private address."""
    srv = scripted(200)
    client.post("/v1/endpoints", json={"url": srv.url})  # allowed by permissive test settings
    evt = client.post("/v1/events", json={"type": "t", "data": {}}).json()
    dlv = evt["deliveries"][0]["id"]
    strict = dataclasses.replace(settings, allow_private_networks=False)
    run_worker_until(Worker(strict), lambda: status_of(conn, dlv) == "failed")
    d = delivery(conn, dlv)
    assert "blocked destination" in d["last_error"] and d["last_error"].startswith("permanent")
    assert srv.calls == []


def test_dashboard_is_served(settings):
    r = TestClient(create_app(settings)).get("/")
    assert r.status_code == 200 and "RelayQueue" in r.text
