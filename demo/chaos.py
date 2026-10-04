"""Chaos benchmark: sustained load + worker crashes + a receiver outage.

Runs the real API, three worker processes and the demo receiver as separate
OS processes, publishes events at a steady rate to five endpoints with
different failure behaviours, and injects faults on a fixed schedule:

    t=  3s  SIGKILL worker w1                    (restarted at t=5)
    t= 10s  receiver process killed — outage     (restarted at t=16)
    t= 26s  SIGKILL *all* workers                (restarted at t=29)
    t= 33s  graceful SIGTERM of w2 (rolling)     (restarted right after)

10% of submissions are re-sent with the same Idempotency-Key to model a
producer retrying after a timeout.

Afterwards it measures, from the RelayQueue database *and* from the receiver's
own ground-truth log: delivery success rate, events lost, duplicates absorbed,
end-to-end latency, and recovery time after every fault.

    python demo/chaos.py [--rate 40] [--duration 38] [--out demo/results]
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sqlite3
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from demo.harness import Cluster, wait_until  # noqa: E402
from relayqueue.metrics import percentile  # noqa: E402

HOOKS = ["ok", "fail-twice", "slow?delay=0.4", "drop-response", "flaky?rate=0.2"]
EVENT_TYPES = ["order.created", "payment.completed", "account.updated"]


def run(rate: float, duration: float, out: Path, workers: int = 3) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    run_dir = out / "runs" / time.strftime("%Y%m%d-%H%M%S")
    cluster = Cluster(str(run_dir))
    samples: list[dict] = []
    submissions = {"sent": 0, "created": 0, "idempotent_replay": 0, "errors": 0}
    stop = threading.Event()

    try:
        cluster.start_api()
        cluster.start_receiver()
        cluster.create_account("chaos-demo")
        for h in HOOKS:
            cluster.add_endpoint(h)
        worker_ids = [f"w{i}" for i in range(1, workers + 1)]
        gen = {w: 1 for w in worker_ids}
        for w in worker_ids:
            cluster.start_worker(f"{w}-g1")

        t0 = time.time()
        cluster.mark("load start")

        # ------------------------------------------------------------ sampler
        def sampler():
            db = sqlite3.connect(cluster.db_path)
            while not stop.is_set():
                now = time.time()
                row = db.execute(
                    "SELECT SUM(status IN ('pending','retrying') AND next_attempt_at <= ?),"
                    " SUM(status='retrying' AND next_attempt_at > ?), SUM(status='in_flight'),"
                    " SUM(status='succeeded'), COUNT(*) FROM deliveries", (now, now)).fetchone()
                samples.append({"t": now - t0, "due": row[0] or 0, "scheduled": row[1] or 0,
                                "in_flight": row[2] or 0, "succeeded": row[3] or 0, "total": row[4] or 0})
                stop.wait(0.5)

        threading.Thread(target=sampler, daemon=True).start()

        # ------------------------------------------------------------ publisher
        def publisher():
            n, keys = 0, []
            interval = 1.0 / rate
            next_at = time.time()
            while time.time() - t0 < duration:
                if keys and random.random() < 0.10:
                    key, etype, data = random.choice(keys)     # producer retry
                else:
                    n += 1
                    etype = EVENT_TYPES[n % len(EVENT_TYPES)]
                    data = {"seq": n, "amount": round(random.uniform(5, 500), 2)}
                    key = f"evt-{n}"
                    keys.append((key, etype, data))
                try:
                    r = cluster.publish(etype, data, idempotency_key=key)
                    submissions["sent"] += 1
                    if r.status_code == 202:
                        submissions["created"] += 1
                    elif r.status_code == 200:
                        submissions["idempotent_replay"] += 1
                    else:
                        submissions["errors"] += 1
                except Exception:  # noqa: BLE001
                    submissions["errors"] += 1
                next_at += interval
                time.sleep(max(0, next_at - time.time()))

        pub = threading.Thread(target=publisher)
        pub.start()

        # ------------------------------------------------------------ chaos schedule
        faults: dict[str, dict] = {}

        def at(t):
            time.sleep(max(0, t0 + t - time.time()))

        at(3)
        faults["single_worker_crash"] = {"worker": "w1-g1", "killed_at": time.time()}
        cluster.kill_worker("w1-g1")
        at(5)
        gen["w1"] += 1
        cluster.start_worker(f"w1-g{gen['w1']}")
        faults["single_worker_crash"]["restarted_at"] = time.time()

        at(10)
        faults["receiver_outage"] = {"down_at": time.time()}
        cluster.kill_receiver()
        at(16)
        cluster.start_receiver()
        faults["receiver_outage"]["up_at"] = time.time()

        at(26)
        killed = [w for w in list(cluster.workers)]
        faults["all_workers_crash"] = {"workers": killed, "killed_at": time.time()}
        for w in killed:
            cluster.kill_worker(w)
        at(29)
        for w in worker_ids:
            gen[w] += 1
            cluster.start_worker(f"{w}-g{gen[w]}")
        faults["all_workers_crash"]["restarted_at"] = time.time()

        at(33)
        w2 = f"w2-g{gen['w2']}"
        faults["graceful_restart"] = {"worker": w2, "stopped_at": time.time()}
        cluster.kill_worker(w2, signal.SIGTERM)
        faults["graceful_restart"]["drained_in_s"] = round(time.time() - faults["graceful_restart"]["stopped_at"], 3)
        gen["w2"] += 1
        cluster.start_worker(f"w2-g{gen['w2']}")

        pub.join()
        cluster.mark("load end")
        load_end = time.time()
        drain_s = wait_until(cluster.all_terminal, 180, interval=0.25, what="all deliveries to finish")
        time.sleep(1.0)   # let receiver finish any trailing slow handlers
        stop.set()

        report = analyse(cluster, t0, load_end, drain_s, faults, submissions, samples, rate, duration)
    finally:
        stop.set()
        cluster.close()

    (out / "report.json").write_text(json.dumps(report, indent=2))
    (out / "report.md").write_text(render_markdown(report))
    try:
        plot(report, samples, out / "timeline.png")
    except Exception as exc:  # noqa: BLE001
        print("chart skipped:", exc)
    return report


# ---------------------------------------------------------------- analysis

def analyse(cluster, t0, load_end, drain_s, faults, submissions, samples, rate, duration) -> dict:
    db = sqlite3.connect(cluster.db_path)
    db.row_factory = sqlite3.Row
    events = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    endpoints = db.execute("SELECT COUNT(*) FROM endpoints").fetchone()[0]
    counts = dict(db.execute("SELECT status, COUNT(*) FROM deliveries GROUP BY status").fetchall())
    expected = events * endpoints
    succeeded = counts.get("succeeded", 0)

    rstats = cluster.receiver_stats() if cluster.receiver_proc else None
    rdb = sqlite3.connect(str(Path(cluster.dir) / "receiver.db"))
    unique_received = rdb.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
    per_path = {}
    for path, req, uniq_dup in rdb.execute(
            "SELECT path, COUNT(*), SUM(duplicate) FROM requests GROUP BY path").fetchall():
        uniq = rdb.execute("SELECT COUNT(*) FROM processed WHERE path=?", (path,)).fetchone()[0]
        per_path[path] = {"requests": req, "unique_processed": uniq, "duplicates_absorbed": uniq_dup or 0}
    bad_sigs = rdb.execute("SELECT COUNT(*) FROM requests WHERE outcome='bad-signature'").fetchone()[0]
    incomplete = rdb.execute("SELECT COUNT(*) FROM requests WHERE outcome='incomplete'").fetchone()[0]

    # what the database says was delivered must exist in the receiver's ledger
    delivered_pairs = {(r[0].split("/hooks/")[1].split("?")[0], r[1]) for r in db.execute(
        "SELECT ep.url, d.event_id FROM deliveries d JOIN endpoints ep ON ep.id=d.endpoint_id"
        " WHERE d.status='succeeded'")}
    received_pairs = {(p.split("/hooks/")[1], e) for p, e in rdb.execute("SELECT path, event_id FROM processed")}
    acked_but_missing = len(delivered_pairs - received_pairs)

    lat_rows = db.execute(
        "SELECT ep.url, (d.delivered_at - e.created_at) * 1000 AS ms, d.created_at FROM deliveries d"
        " JOIN events e ON e.id=d.event_id JOIN endpoints ep ON ep.id=d.endpoint_id"
        " WHERE d.status='succeeded'").fetchall()
    all_lat = [r["ms"] for r in lat_rows]

    def dist(vals):
        return {"p50": percentile(vals, 50), "p95": percentile(vals, 95), "p99": percentile(vals, 99),
                "max": round(max(vals), 1) if vals else None, "count": len(vals)}

    # Calm window: deliveries created between t=0..4s (before any fault) to the 'ok' endpoint.
    calm = [r["ms"] for r in lat_rows if r["url"].endswith("/hooks/ok") and r["created_at"] - t0 < 4.5]
    per_ep = {}
    for url in sorted({r["url"] for r in lat_rows}):
        per_ep[url.split("/hooks/")[1]] = dist([r["ms"] for r in lat_rows if r["url"] == url])

    attempts = dict(db.execute("SELECT outcome, COUNT(*) FROM attempts GROUP BY outcome").fetchall())
    first_try = db.execute(
        "SELECT COUNT(*) FROM deliveries WHERE status='succeeded' AND attempts=1 AND NOT EXISTS"
        " (SELECT 1 FROM attempts a WHERE a.delivery_id=deliveries.id AND a.outcome!='success')").fetchone()[0]

    # ---------------- recovery measurements
    rec = {}
    f = faults["single_worker_crash"]
    rows = db.execute("SELECT delivery_id, lease_expired_at, reclaimed_at FROM lease_recoveries"
                      " WHERE previous_owner=?", (f["worker"],)).fetchall()
    done = [db.execute("SELECT delivered_at FROM deliveries WHERE id=?", (r["delivery_id"],)).fetchone()[0]
            for r in rows]
    rec["single_worker_crash"] = {
        "orphaned_claims": len(rows),
        "all_reclaimed_after_s": round(max(r["reclaimed_at"] for r in rows) - f["killed_at"], 3) if rows else 0,
        "all_completed_after_s": round(max(d for d in done if d) - f["killed_at"], 3) if any(done) else None,
        "lost": sum(1 for d in done if not d),
    }

    f = faults["receiver_outage"]
    affected = [r[0] - f["up_at"] for r in db.execute(
        "SELECT delivered_at FROM deliveries WHERE created_at < ? AND delivered_at > ? AND status='succeeded'",
        (f["up_at"], f["up_at"]))]
    failed_during = db.execute(
        "SELECT COUNT(DISTINCT a.delivery_id) FROM attempts a WHERE a.started_at BETWEEN ? AND ?"
        " AND a.outcome='retry'", (f["down_at"], f["up_at"])).fetchone()[0]
    # backlog: queue depth before outage vs after restart
    pre = [s["due"] + s["scheduled"] + s["in_flight"] for s in samples
           if 1 < s["t"] < f["down_at"] - t0]
    baseline = (sorted(pre)[len(pre) // 2] if pre else 0)
    after = [s for s in samples if s["t"] >= f["up_at"] - t0]
    peak = max((s["due"] + s["scheduled"] + s["in_flight"] for s in samples
                if f["down_at"] - t0 <= s["t"] <= f["up_at"] - t0 + 3), default=0)
    back = next((s["t"] for s in after
                 if s["due"] + s["scheduled"] + s["in_flight"] <= max(baseline * 1.5, baseline + 10)), None)
    rec["receiver_outage"] = {
        "outage_s": round(f["up_at"] - f["down_at"], 2),
        "deliveries_hit_by_outage": failed_during,
        "queue_baseline": baseline,
        "queue_peak": peak,
        "queue_back_to_baseline_after_s": round(back - (f["up_at"] - t0), 2) if back is not None else None,
        "backlog_delivered_after_s": {"p50": round(percentile(affected, 50) or 0, 2),
                                      "p95": round(percentile(affected, 95) or 0, 2),
                                      "max": round(max(affected), 2) if affected else None,
                                      "count": len(affected)},
    }

    f = faults["all_workers_crash"]
    owners = tuple(f["workers"])
    rows = db.execute(f"SELECT reclaimed_at FROM lease_recoveries WHERE previous_owner IN ({','.join('?'*len(owners))})",
                      owners).fetchall()
    last_pre = db.execute("SELECT MAX(delivered_at) FROM deliveries WHERE created_at < ? AND status='succeeded'",
                          (f["killed_at"],)).fetchone()[0]
    rec["all_workers_crash"] = {
        "workers_killed": len(owners),
        "downtime_s": round(f["restarted_at"] - f["killed_at"], 2),
        "orphaned_claims": len(rows),
        "claims_reclaimed_after_restart_s": round(max(r[0] for r in rows) - f["restarted_at"], 3) if rows else 0,
        "pre_crash_backlog_delivered_after_restart_s": round(last_pre - f["restarted_at"], 3) if last_pre else None,
    }
    rec["graceful_restart"] = {
        "drain_s": faults["graceful_restart"]["drained_in_s"],
        "orphaned_claims": db.execute("SELECT COUNT(*) FROM lease_recoveries WHERE previous_owner=?",
                                      (faults["graceful_restart"]["worker"],)).fetchone()[0],
    }

    settings = {k.replace("RELAYQUEUE_", "").lower(): v for k, v in cluster.env.items()
                if k.startswith("RELAYQUEUE_") and k not in ("RELAYQUEUE_DB_PATH", "RELAYQUEUE_ADMIN_TOKEN")}
    return {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "config": {"rate_per_s": rate, "duration_s": duration, "endpoints": HOOKS, "workers": 3,
                   "settings": settings},
        "faults": {k: {kk: (round(vv - t0, 2) if isinstance(vv, float) and vv > 1e9 else vv)
                       for kk, vv in v.items()} for k, v in faults.items()},
        "submissions": {**submissions, "unique_events_stored": events},
        "deliveries": {"expected": expected, "succeeded": succeeded, "failed": counts.get("failed", 0),
                       "still_pending": expected - succeeded - counts.get("failed", 0),
                       "success_rate": round(succeeded / expected, 6) if expected else None,
                       "first_attempt_success_rate": round(first_try / succeeded, 4) if succeeded else None,
                       "drain_after_load_end_s": round(drain_s, 2)},
        "receiver_ground_truth": {
            "unique_events_processed": unique_received,
            "events_lost": expected - unique_received,
            "acked_but_not_received": acked_but_missing,
            "duplicate_deliveries_absorbed": sum(p["duplicates_absorbed"] for p in per_path.values()),
            "bad_signatures": bad_sigs,
            "incomplete_requests_from_killed_workers": incomplete,
            "per_path": per_path,
        },
        "attempts": attempts,
        "latency_ms": {"all": dist(all_lat), "calm_ok_endpoint": dist(calm), "per_endpoint": per_ep},
        "recovery": rec,
        "throughput": {"deliveries_per_s": round(succeeded / (load_end - t0 + drain_s), 1)},
    }


# ---------------------------------------------------------------- output

def _ms(v):
    if v is None:
        return "—"
    return f"{v:.0f} ms" if v < 1000 else f"{v / 1000:.2f} s"


def render_markdown(r: dict) -> str:
    d, g, lat, rec = r["deliveries"], r["receiver_ground_truth"], r["latency_ms"], r["recovery"]
    s = r["submissions"]
    lines = [
        "# RelayQueue chaos benchmark",
        "",
        f"_Generated {r['generated_at']} by `python demo/chaos.py`._",
        "",
        f"Load: **{r['config']['rate_per_s']:.0f} events/s for {r['config']['duration_s']:.0f}s**, fanned out to "
        f"{len(r['config']['endpoints'])} endpoints with different behaviours "
        f"({', '.join('`' + e + '`' for e in r['config']['endpoints'])}), 3 worker processes. "
        "Faults injected: one worker SIGKILLed, a 6s receiver outage (process killed), all workers "
        "SIGKILLed for 3s, and a graceful rolling restart.",
        "",
        "![timeline](timeline.png)",
        "",
        "## Headline numbers",
        "",
        "| Metric | Result |",
        "|---|---|",
        f"| Delivery success rate | **{d['success_rate'] * 100:.2f}%** ({d['succeeded']:,} / {d['expected']:,}) |",
        f"| Events lost (receiver ground truth) | **{g['events_lost']}** |",
        f"| Acked by RelayQueue but never received | **{g['acked_but_not_received']}** |",
        f"| Duplicate deliveries absorbed by receiver (same `webhook-id`) | {g['duplicate_deliveries_absorbed']:,} |",
        f"| Signature verification failures | {g['bad_signatures']} |",
        f"| Half-sent requests cut off by a SIGKILLed worker (discarded by receiver) | {g['incomplete_requests_from_killed_workers']} |",
        f"| Duplicate submissions collapsed by `Idempotency-Key` | {s['idempotent_replay']:,} of {s['sent']:,} POSTs → {s['unique_events_stored']:,} events |",
        f"| First-attempt success rate | {d['first_attempt_success_rate'] * 100:.1f}% |",
        f"| End-to-end latency, calm period (`ok` endpoint) p50 / p95 / p99 | {_ms(lat['calm_ok_endpoint']['p50'])} / {_ms(lat['calm_ok_endpoint']['p95'])} / {_ms(lat['calm_ok_endpoint']['p99'])} |",
        f"| End-to-end latency, whole run incl. retries p50 / p95 / p99 | {_ms(lat['all']['p50'])} / {_ms(lat['all']['p95'])} / {_ms(lat['all']['p99'])} |",
        f"| Throughput | {r['throughput']['deliveries_per_s']} deliveries/s |",
        "",
        "## Recovery",
        "",
        "| Fault | Measurement | Result |",
        "|---|---|---|",
        f"| Single worker SIGKILL | claims orphaned | {rec['single_worker_crash']['orphaned_claims']} |",
        f"| | all orphaned claims reclaimed after kill | {rec['single_worker_crash']['all_reclaimed_after_s']} s (lease = {r['config']['settings'].get('lease_seconds')} s) |",
        f"| | all orphaned deliveries completed after kill | {rec['single_worker_crash']['all_completed_after_s']} s |",
        f"| Receiver outage ({rec['receiver_outage']['outage_s']} s) | deliveries that hit the outage | {rec['receiver_outage']['deliveries_hit_by_outage']:,} |",
        f"| | queue depth: baseline → peak | {rec['receiver_outage']['queue_baseline']} → {rec['receiver_outage']['queue_peak']} |",
        f"| | queue back to baseline after receiver returned | {rec['receiver_outage']['queue_back_to_baseline_after_s']} s |",
        f"| | backlog delivered after receiver returned, p50 / p95 / max | {rec['receiver_outage']['backlog_delivered_after_s']['p50']} s / {rec['receiver_outage']['backlog_delivered_after_s']['p95']} s / {rec['receiver_outage']['backlog_delivered_after_s']['max']} s |",
        f"| All {rec['all_workers_crash']['workers_killed']} workers SIGKILL ({rec['all_workers_crash']['downtime_s']} s down) | claims orphaned | {rec['all_workers_crash']['orphaned_claims']} |",
        f"| | orphaned claims reclaimed after restart | {rec['all_workers_crash']['claims_reclaimed_after_restart_s']} s |",
        f"| | pre-crash backlog delivered after restart | {rec['all_workers_crash']['pre_crash_backlog_delivered_after_restart_s']} s |",
        f"| Graceful SIGTERM | drain time / orphaned claims | {rec['graceful_restart']['drain_s']} s / {rec['graceful_restart']['orphaned_claims']} |",
        "",
        "## Per endpoint",
        "",
        "| Endpoint behaviour | Requests | Unique processed | Duplicates absorbed | Latency p50 | p95 | p99 |",
        "|---|---|---|---|---|---|---|",
    ]
    for path, p in sorted(g["per_path"].items()):
        key = next((k for k in lat["per_endpoint"] if k.split("?")[0] == path.split("/hooks/")[1]), None)
        l = lat["per_endpoint"].get(key, {})
        lines.append(f"| `{key or path}` | {p['requests']:,} | {p['unique_processed']:,} | {p['duplicates_absorbed']:,} "
                     f"| {_ms(l.get('p50'))} | {_ms(l.get('p95'))} | {_ms(l.get('p99'))} |")
    lines += [
        "",
        "## Attempt outcomes",
        "",
        "| Outcome | Count |",
        "|---|---|",
        *[f"| {k} | {v:,} |" for k, v in sorted(r["attempts"].items())],
        "",
        "`abandoned` attempts are claims whose worker was killed before recording a result; each was "
        "reclaimed after its lease expired and redelivered with the same event id.",
        "",
        "## How to read this",
        "",
        "* **Success rate / events lost** are checked twice: RelayQueue's own records, and the receiver's "
        "independent ledger of event ids it processed.",
        "* **Duplicates absorbed** is the cost of at-least-once delivery: when a worker dies mid-request, "
        "the response is dropped, or a request times out after the receiver already did the work, the event "
        "is redelivered with the same `webhook-id`, and the receiver skips it.",
        "* After the outage, deliveries that already failed several times are waiting out their exponential "
        "backoff (capped at `backoff_max`), and `fail-twice` events still need two more failures, which "
        "is why the slowest backlog items arrive later than the queue returns to normal.",
        "* Latency \"whole run\" includes events deliberately delayed by retries (`fail-twice`, `flaky`, "
        "`drop-response`) and by the injected outages.",
        "",
    ]
    return "\n".join(lines)


def plot(r: dict, samples: list[dict], path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    moss, ochre, clay, river, ink, paper, line = "#4f6f45", "#c4952f", "#b4553a", "#4d7387", "#2c3328", "#f7f4ec", "#e3dccd"
    t = [s["t"] for s in samples]
    rate = [0.0] + [max(0, (b["succeeded"] - a["succeeded"]) / max(1e-6, b["t"] - a["t"]))
                    for a, b in zip(samples, samples[1:])]
    fig, ax = plt.subplots(figsize=(11, 4.2), dpi=150)
    fig.patch.set_facecolor(paper)
    ax.set_facecolor(paper)
    ax.fill_between(t, [s["in_flight"] + s["due"] + s["scheduled"] for s in samples], color=ochre, alpha=.25,
                    lw=0, label="queued (due + scheduled retries + in flight)")
    ax.plot(t, rate, color=moss, lw=2, label="delivered per second")
    f = r["faults"]
    spans = [(f["single_worker_crash"]["killed_at"], f["single_worker_crash"]["restarted_at"], "w1 killed", river),
             (f["receiver_outage"]["down_at"], f["receiver_outage"]["up_at"], "receiver outage", clay),
             (f["all_workers_crash"]["killed_at"], f["all_workers_crash"]["restarted_at"], "all workers killed", river)]
    top = max(rate + [s["in_flight"] + s["due"] + s["scheduled"] for s in samples]) * 1.12
    for a, b, label, c in spans:
        ax.axvspan(a, b, color=c, alpha=.12, lw=0)
        ax.text((a + b) / 2, top * .97, label, ha="center", va="top", fontsize=8, color=c)
    ax.axvline(f["graceful_restart"]["stopped_at"], color=river, ls=":", lw=1)
    ax.set_ylim(0, top)
    ax.set_xlabel("seconds since load start", color=ink)
    for s in ax.spines.values():
        s.set_color(line)
    ax.tick_params(colors=ink, labelsize=8)
    ax.grid(axis="y", color=line, lw=.6)
    ax.legend(loc="upper right", bbox_to_anchor=(1, .88), frameon=False, fontsize=8, labelcolor=ink)
    ax.set_title("Delivery throughput and backlog under injected faults", color=ink, fontsize=11, loc="left")
    fig.tight_layout()
    fig.savefig(path, facecolor=paper)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rate", type=float, default=40)
    ap.add_argument("--duration", type=float, default=38)
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent / "results"))
    args = ap.parse_args()
    if args.duration < 34:
        ap.error("--duration must be >= 34 (the fault schedule runs until t=33s)")
    report = run(args.rate, args.duration, Path(args.out))
    print(render_markdown(report))


if __name__ == "__main__":
    main()
