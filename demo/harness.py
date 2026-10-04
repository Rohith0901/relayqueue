"""Process harness: runs a real API, real worker processes and the demo receiver.

Used by the reliability tests and by ``demo/chaos.py``. Every component is a
separate OS process so crashes can be simulated with ``SIGKILL``.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable

import httpx

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(pred: Callable[[], bool], timeout: float, interval: float = 0.1, what: str = "condition") -> float:
    start = time.time()
    while time.time() - start < timeout:
        if pred():
            return time.time() - start
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {what}")


class Cluster:
    def __init__(self, workdir: str | None = None, **settings: str | float | int):
        self.dir = Path(workdir or tempfile.mkdtemp(prefix="relayqueue-"))
        self.dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(self.dir / "relayqueue.db")
        self.env = {
            **os.environ,
            "PYTHONPATH": str(ROOT),
            "RELAYQUEUE_DB_PATH": self.db_path,
            "RELAYQUEUE_ALLOW_PRIVATE_NETWORKS": "true",   # the demo receiver lives on localhost
            "RELAYQUEUE_ADMIN_TOKEN": "demo-admin-token",
            "RELAYQUEUE_LOG_LEVEL": "WARNING",
            "RELAYQUEUE_REQUEST_TIMEOUT": "2",
            "RELAYQUEUE_LEASE_SECONDS": "4",
            "RELAYQUEUE_BACKOFF_BASE": "0.25",
            "RELAYQUEUE_BACKOFF_MAX": "4",
            "RELAYQUEUE_MAX_ATTEMPTS": "12",
            "RELAYQUEUE_POLL_INTERVAL": "0.05",
            "RELAYQUEUE_HEARTBEAT_INTERVAL": "1",
        }
        for k, v in settings.items():
            self.env[f"RELAYQUEUE_{k.upper()}"] = str(v)
        self.api_port = free_port()
        self.receiver_port = free_port()
        self.api_url = f"http://127.0.0.1:{self.api_port}"
        self.receiver_url = f"http://127.0.0.1:{self.receiver_port}"
        self.api_proc: subprocess.Popen | None = None
        self.receiver_proc: subprocess.Popen | None = None
        self.workers: dict[str, subprocess.Popen] = {}
        self.events: list[tuple[float, str]] = []   # chaos timeline
        self.http = httpx.Client(timeout=10, trust_env=False)
        self.api_key: str | None = None

    # ------------------------------------------------------------ processes
    def _spawn(self, args: list[str], log: str) -> subprocess.Popen:
        f = open(self.dir / log, "ab")
        return subprocess.Popen([sys.executable, *args], env=self.env, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT)

    def start_api(self) -> None:
        self.api_proc = self._spawn(["-m", "relayqueue", "api", "--port", str(self.api_port)], "api.log")
        wait_until(lambda: self._ok(self.api_url + "/healthz"), 15, what="API")

    def start_receiver(self) -> None:
        self.receiver_proc = self._spawn(
            ["demo/receiver.py", "--port", str(self.receiver_port), "--db", str(self.dir / "receiver.db")],
            "receiver.log")
        wait_until(lambda: self._ok(self.receiver_url + "/_stats"), 15, what="receiver")
        self.mark("receiver up")

    def kill_receiver(self) -> None:
        if self.receiver_proc:
            self.receiver_proc.send_signal(signal.SIGKILL)
            self.receiver_proc.wait()
            self.receiver_proc = None
            self.mark("receiver killed")

    def start_worker(self, worker_id: str, concurrency: int = 32) -> None:
        env_backup = dict(self.env)
        self.env["RELAYQUEUE_WORKER_CONCURRENCY"] = str(concurrency)
        self.workers[worker_id] = self._spawn(["-m", "relayqueue", "worker", "--worker-id", worker_id],
                                              f"worker-{worker_id}.log")
        self.env = env_backup
        self.mark(f"worker {worker_id} started")

    def kill_worker(self, worker_id: str, sig: int = signal.SIGKILL) -> None:
        p = self.workers.pop(worker_id)
        p.send_signal(sig)
        p.wait(timeout=30)
        self.mark(f"worker {worker_id} {'killed (SIGKILL)' if sig == signal.SIGKILL else 'stopped'}")

    def mark(self, what: str) -> None:
        self.events.append((time.time(), what))

    def _ok(self, url: str) -> bool:
        try:
            return self.http.get(url, timeout=1).status_code == 200
        except httpx.HTTPError:
            return False

    def close(self) -> None:
        for wid in list(self.workers):
            self.kill_worker(wid, signal.SIGTERM)
        for p in (self.api_proc, self.receiver_proc):
            if p:
                p.terminate()
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    p.kill()
        self.http.close()

    def __enter__(self) -> "Cluster":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------ API helpers
    @property
    def headers(self) -> dict[str, str]:
        return {"authorization": f"Bearer {self.api_key}"}

    def create_account(self, name: str = "demo") -> str:
        r = self.http.post(self.api_url + "/v1/admin/accounts", json={"name": name},
                           headers={"x-admin-token": "demo-admin-token"})
        r.raise_for_status()
        self.api_key = r.json()["api_key"]["key"]
        return self.api_key

    def add_endpoint(self, hook: str, event_types: list[str] | None = None) -> dict:
        """Register ``/hooks/<hook>`` and teach the receiver the signing secret."""
        url = f"{self.receiver_url}/hooks/{hook}"
        r = self.http.post(self.api_url + "/v1/endpoints", headers=self.headers,
                           json={"url": url, "event_types": event_types or ["*"]})
        r.raise_for_status()
        ep = r.json()
        path = "/hooks/" + hook.split("?")[0]
        self.http.post(self.receiver_url + "/_secrets", json={path: ep["secret"]}).raise_for_status()
        return ep

    def publish(self, event_type: str, data: dict, idempotency_key: str | None = None) -> httpx.Response:
        h = dict(self.headers)
        if idempotency_key:
            h["idempotency-key"] = idempotency_key
        return self.http.post(self.api_url + "/v1/events", headers=h, json={"type": event_type, "data": data})

    def stats(self) -> dict:
        return self.http.get(self.api_url + "/v1/stats", headers=self.headers).json()

    def receiver_stats(self) -> dict:
        return self.http.get(self.receiver_url + "/_stats").json()

    def all_terminal(self) -> bool:
        d = self.stats()["deliveries"]
        return d["pending"] + d["in_flight"] + d["retrying"] == 0
