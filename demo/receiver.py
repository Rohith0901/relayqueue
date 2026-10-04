"""Demo webhook receiver with configurable failure behaviour.

Each path selects a behaviour (query parameters tune it):

    POST /hooks/ok                         always 200
    POST /hooks/fail-twice                 500, 500, then 200 (counted per event id)
    POST /hooks/slow?delay=3&times=0       sleeps `delay` seconds, then 200
                                           (times=N: only the first N requests per event are slow)
    POST /hooks/drop-response?times=1      processes the event, then closes the socket without
                                           answering (first `times` requests per event)
    POST /hooks/flaky?rate=0.3             random 503 with probability `rate`
    POST /hooks/down                       always 503
    POST /hooks/reject                     always 400 (permanent failure)

The receiver behaves like a well-written consumer: it verifies the HMAC
signature (when it knows the endpoint secret), and processes each event id
once — redeliveries are acknowledged with 200 but counted as duplicates.

    GET  /            live page of received webhooks
    GET  /events      JSON list of requests
    GET  /_stats      per-path counters (requests, processed, duplicates, ...)
    POST /_secrets    {"/hooks/ok": "whsec_..."}  register secrets for verification
    POST /_control    {"outage": true}  -> every hook answers 503 until switched off
                      {"healed": true}  -> `down` and `reject` hooks start answering 200
                                           (the receiver "fixed its bug": replay failed deliveries)
    POST /_reset      clear all state

    python demo/receiver.py --port 9000 [--db receiver.db] [--secret whsec_...]
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from relayqueue.signing import SignatureError, verify  # noqa: E402

MODES = ("ok", "fail-twice", "slow", "drop-response", "flaky", "down", "reject")

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT, received_at REAL, path TEXT, mode TEXT,
    event_id TEXT, delivery_id TEXT, attempt TEXT, event_type TEXT,
    status INTEGER, outcome TEXT, duplicate INTEGER, signature TEXT, body TEXT);
CREATE TABLE IF NOT EXISTS processed (
    path TEXT, event_id TEXT, first_seen_at REAL, event_created_at TEXT,
    PRIMARY KEY (path, event_id));
CREATE TABLE IF NOT EXISTS seen (path TEXT, event_id TEXT, n INTEGER, PRIMARY KEY (path, event_id));
CREATE TABLE IF NOT EXISTS secrets (path TEXT PRIMARY KEY, secret TEXT);
"""


class State:
    def __init__(self, db_path: str = ":memory:", default_secret: str | None = None):
        self.lock = threading.Lock()
        self.db = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        if db_path != ":memory:":
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(SCHEMA)
        self.default_secret = default_secret
        self.outage = False
        self.healed = False

    def reset(self) -> None:
        with self.lock:
            for t in ("requests", "processed", "seen", "secrets"):
                self.db.execute(f"DELETE FROM {t}")
            self.outage = False
            self.healed = False

    def secret_for(self, path: str) -> str | None:
        row = self.db.execute("SELECT secret FROM secrets WHERE path = ?", (path,)).fetchone()
        return row[0] if row else self.default_secret

    def bump(self, path: str, event_id: str) -> int:
        """How many times this event has hit this path (1-based)."""
        self.db.execute(
            "INSERT INTO seen VALUES (?, ?, 1) ON CONFLICT(path, event_id) DO UPDATE SET n = n + 1",
            (path, event_id))
        return self.db.execute("SELECT n FROM seen WHERE path=? AND event_id=?", (path, event_id)).fetchone()[0]

    def process(self, path: str, event_id: str, created_at: str | None) -> bool:
        """Idempotent processing: returns True if this is a duplicate."""
        cur = self.db.execute(
            "INSERT OR IGNORE INTO processed VALUES (?, ?, ?, ?)", (path, event_id, time.time(), created_at))
        return cur.rowcount == 0

    def record(self, **r) -> None:
        cols = ",".join(r)
        self.db.execute(f"INSERT INTO requests ({cols}) VALUES ({','.join('?' * len(r))})", tuple(r.values()))

    def stats(self) -> dict:
        with self.lock:
            out = {}
            for row in self.db.execute(
                "SELECT path, COUNT(*) requests, SUM(outcome='processed') processed, SUM(duplicate) duplicates,"
                " SUM(outcome='bad-signature') bad_signatures, SUM(outcome='dropped') dropped,"
                " SUM(status >= 500) errors, SUM(outcome='rejected') rejected, SUM(outcome='incomplete') incomplete"
                " FROM requests GROUP BY path"):
                out[row["path"]] = {k: row[k] or 0 for k in row.keys() if k != "path"}
            for row in self.db.execute("SELECT path, COUNT(*) n FROM processed GROUP BY path"):
                out.setdefault(row["path"], {})["unique_events"] = row["n"]
            totals = {k: sum(v.get(k, 0) for v in out.values()) for k in
                      ("requests", "processed", "duplicates", "bad_signatures", "dropped", "unique_events",
                       "incomplete")}
            return {"paths": out, "totals": totals, "outage": self.outage, "healed": self.healed}

    def events(self, limit: int = 200, after: int = 0) -> list[dict]:
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM requests WHERE id > ? ORDER BY id DESC LIMIT ?", (after, limit)).fetchall()
        return [dict(r) for r in rows]


def make_handler(state: State):
    class Handler(BaseHTTPRequestHandler):
        server_version = "RelayQueueDemoReceiver/1.0"
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def _send(self, code: int, body: str, ctype: str = "application/json", extra=None) -> None:
            data = body.encode()
            self.send_response(code)
            self.send_header("content-type", ctype)
            self.send_header("content-length", str(len(data)))
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        # ------------------------------------------------------------ GET
        def do_GET(self):
            u = urlsplit(self.path)
            q = parse_qs(u.query)
            if u.path == "/events":
                self._send(200, json.dumps(state.events(int(q.get("limit", ["200"])[0]),
                                                        int(q.get("after", ["0"])[0]))))
            elif u.path == "/_stats":
                self._send(200, json.dumps(state.stats()))
            elif u.path == "/":
                self._send(200, PAGE, "text/html; charset=utf-8")
            else:
                self._send(404, '{"error":"not found"}')

        # ------------------------------------------------------------ POST
        def do_POST(self):
            u = urlsplit(self.path)
            expected = int(self.headers.get("content-length") or 0)
            raw = self.rfile.read(expected)
            if len(raw) < expected:
                # The sender died mid-request (e.g. a SIGKILLed worker): nothing to process.
                with state.lock:
                    state.record(received_at=time.time(), path=u.path, mode="", event_id=self.headers.get("webhook-id"),
                                 status=None, outcome="incomplete", duplicate=0, body="")
                self.close_connection = True
                return
            if u.path == "/_secrets":
                with state.lock:
                    for p, s in json.loads(raw).items():
                        state.db.execute("INSERT OR REPLACE INTO secrets VALUES (?, ?)", (p, s))
                return self._send(200, '{"ok":true}')
            if u.path == "/_control":
                cfg = json.loads(raw or b"{}")
                if "outage" in cfg:
                    state.outage = bool(cfg["outage"])
                if "healed" in cfg:
                    state.healed = bool(cfg["healed"])
                return self._send(200, json.dumps({"outage": state.outage, "healed": state.healed}))
            if u.path == "/_reset":
                state.reset()
                return self._send(200, '{"ok":true}')
            if not u.path.startswith("/hooks/"):
                return self._send(404, '{"error":"unknown path"}')
            self.handle_hook(u.path, parse_qs(u.query), raw)

        def handle_hook(self, path: str, q: dict, raw: bytes) -> None:
            mode = path.split("/")[2] if len(path.split("/")) > 2 else "ok"
            body = raw.decode(errors="replace")
            try:
                payload = json.loads(body)
            except ValueError:
                payload = {}
            event_id = self.headers.get("webhook-id") or payload.get("id") or "?"
            rec = dict(received_at=time.time(), path=path, mode=mode, event_id=event_id,
                       delivery_id=self.headers.get("x-relayqueue-delivery-id"),
                       attempt=self.headers.get("x-relayqueue-attempt"),
                       event_type=payload.get("type"), duplicate=0, body=body[:2000])

            def param(name, default):
                return float(q.get(name, [default])[0])

            # 1. Authenticate the sender.
            with state.lock:
                secret = state.secret_for(path)
            sig = "unchecked"
            if secret:
                try:
                    verify(secret, dict(self.headers), raw)
                    sig = "valid"
                except SignatureError:
                    with state.lock:
                        state.record(**rec, status=401, outcome="bad-signature", signature="invalid")
                    return self._send(401, '{"error":"bad signature"}')
            rec["signature"] = sig

            with state.lock:
                n = state.bump(path, event_id)
                outage, healed = state.outage, state.healed

            # 2. Simulated failures that happen *before* processing.
            if outage:
                with state.lock:
                    state.record(**rec, status=503, outcome="outage")
                return self._send(503, '{"error":"outage"}', extra={"retry-after": "1"})
            if mode == "down" and not healed:
                with state.lock:
                    state.record(**rec, status=503, outcome="error")
                return self._send(503, '{"error":"down"}')
            if mode == "reject" and not healed:
                with state.lock:
                    state.record(**rec, status=400, outcome="rejected")
                return self._send(400, '{"error":"invalid payload"}')
            if mode == "fail-twice" and n <= int(param("times", 2)):
                with state.lock:
                    state.record(**rec, status=500, outcome="error")
                return self._send(500, '{"error":"simulated failure"}')
            if mode == "flaky" and random.random() < param("rate", 0.3):
                with state.lock:
                    state.record(**rec, status=503, outcome="error")
                return self._send(503, '{"error":"flaky"}')
            if mode == "slow":
                times = int(param("times", 0))
                if times == 0 or n <= times:
                    time.sleep(param("delay", 3))

            # 3. Process the event exactly once (idempotent on webhook-id).
            with state.lock:
                dup = state.process(path, event_id, payload.get("created_at"))
                rec["duplicate"] = int(dup)

            # 4. Simulated failure *after* processing: the response never arrives.
            if mode == "drop-response" and n <= int(param("times", 1)):
                with state.lock:
                    state.record(**rec, status=None, outcome="dropped")
                self.close_connection = True
                try:
                    self.wfile.flush()
                    self.connection.shutdown(2)
                except OSError:
                    pass
                return

            with state.lock:
                state.record(**rec, status=200, outcome="duplicate" if dup else "processed")
            self._send(200, json.dumps({"received": True, "duplicate": dup}))

    return Handler


class ReceiverServer(ThreadingHTTPServer):
    request_queue_size = 512        # default backlog (5) drops connections under load
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, state: State):
        self.state = state
        super().__init__(addr, make_handler(state))


PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Demo Receiver</title>
<style>
:root{--paper:#f3efe6;--card:#fbf9f4;--ink:#2c3328;--muted:#6f7566;--line:#e3dccd;
--moss:#4f6f45;--moss-bg:#e3ead9;--ochre:#b8862a;--ochre-bg:#f4e8c9;--clay:#b4553a;--clay-bg:#f3ddd3;
--river:#4d7387;--river-bg:#dde8ec;--stone:#8a8577;--stone-bg:#e8e5dd;--sage:#8fa77d}
@media (prefers-color-scheme:dark){:root{--paper:#151a14;--card:#1d231b;--ink:#e8e4d8;--muted:#9aa08f;--line:#2c3429;
--moss:#93b883;--moss-bg:#26331f;--ochre:#dcb257;--ochre-bg:#3a3020;--clay:#e08a6c;--clay-bg:#3d2620;
--river:#86afc2;--river-bg:#1f2f36;--stone:#a39e90;--stone-bg:#2a2a25;--sage:#6f8a5f}}
*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
.wrap{max-width:1100px;margin:0 auto;padding:24px 16px 60px}
h1{font:600 24px Georgia,serif;margin:0}.sub{color:var(--muted);font-size:13px}
header{display:flex;align-items:center;gap:12px;margin-bottom:18px;flex-wrap:wrap}
.leaf{width:30px;height:30px;border-radius:50% 0;background:linear-gradient(135deg,var(--sage),var(--moss));animation:sway 4s ease-in-out infinite;transform-origin:bottom left}
@keyframes sway{50%{transform:rotate(-8deg)}}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px 14px;animation:rise .5s both}
.card b{display:block;font:600 22px Georgia,serif}.card span{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.05em}
.panel{background:var(--card);border:1px solid var(--line);border-radius:16px;overflow:hidden}
.row{display:grid;grid-template-columns:80px 140px 1fr 70px 110px;gap:10px;padding:10px 16px;border-bottom:1px solid var(--line);align-items:center}
.row.new{animation:drop .55s cubic-bezier(.2,.7,.2,1) both}
@keyframes drop{from{opacity:0;transform:translateY(-10px);background:color-mix(in srgb,var(--sage) 25%,transparent)}to{opacity:1;transform:none}}
@keyframes rise{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
.mono{font:12px ui-monospace,Menlo,monospace}.muted{color:var(--muted)}
.pill{display:inline-block;padding:1px 9px;border-radius:999px;font-size:12px;font-weight:500}
.processed{color:var(--moss);background:var(--moss-bg)}.duplicate{color:var(--river);background:var(--river-bg)}
.error,.outage{color:var(--ochre);background:var(--ochre-bg)}.dropped{color:var(--stone);background:var(--stone-bg)}
.rejected,.bad-signature{color:var(--clay);background:var(--clay-bg)}.incomplete{color:var(--stone);background:var(--stone-bg)}
.mode{font-size:12px;color:var(--muted)}
.banner{display:none;background:var(--clay-bg);color:var(--clay);padding:10px 14px;border-radius:12px;margin-bottom:12px;animation:pulse 1.6s ease-in-out infinite}
@keyframes pulse{50%{opacity:.6}}
@media (max-width:640px){.row{grid-template-columns:70px 1fr 90px}.hide{display:none}}
@media (prefers-reduced-motion:reduce){*{animation:none!important}}
</style></head><body><div class="wrap">
<header><div class="leaf"></div><div><h1>Demo receiver</h1><div class="sub">Verifies signatures · processes each event id once</div></div></header>
<div class="banner" id="banner">Simulated outage in progress — every hook answers 503.</div>
<div class="cards" id="cards"></div>
<div class="panel" id="list"></div></div>
<script>
const esc=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
let last=0;const rows=[];
async function tick(){try{
 const s=await (await fetch("/_stats")).json();const t=s.totals;
 document.getElementById("banner").style.display=s.outage?"block":"none";
 document.getElementById("cards").innerHTML=[["Requests",t.requests],["Processed",t.unique_events],["Duplicates absorbed",t.duplicates],["Dropped responses",t.dropped],["Bad signatures",t.bad_signatures]]
  .map(([k,v],i)=>`<div class="card" style="animation-delay:${i*40}ms"><span>${k}</span><b>${v}</b></div>`).join("");
 const ev=await (await fetch("/events?limit=100&after="+last)).json();
 if(ev.length){last=ev[0].id;rows.unshift(...ev.map(e=>({...e,fresh:true})));rows.length=Math.min(rows.length,150);}
 document.getElementById("list").innerHTML=rows.length?rows.map((e,i)=>`<div class="row ${e.fresh?"new":""}" style="animation-delay:${e.fresh?Math.min(i,12)*30:0}ms">
  <span class="mono muted">${new Date(e.received_at*1000).toLocaleTimeString()}</span>
  <span><span class="pill ${esc(e.outcome)}">${esc(e.outcome)}</span></span>
  <span><div>${esc(e.event_type||"")}</div><div class="mono muted">${esc(e.event_id)}</div></span>
  <span class="hide muted">#${esc(e.attempt)}</span><span class="hide mode">${esc(e.path)}</span></div>`).join(""):`<div class="row"><span class="muted" style="grid-column:1/-1">Waiting for webhooks…</span></div>`;
 rows.forEach(r=>r.fresh=false);
}catch(e){}}
tick();setInterval(tick,1500);
</script></body></html>"""


def serve(host: str, port: int, db: str = ":memory:", secret: str | None = None) -> ReceiverServer:
    return ReceiverServer((host, port), State(db, secret))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--db", default=":memory:", help="sqlite file to persist state across restarts")
    ap.add_argument("--secret", default=None, help="default signing secret for all paths")
    args = ap.parse_args()
    srv = serve(args.host, args.port, args.db, args.secret)
    print(f"demo receiver listening on http://{args.host}:{args.port}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
