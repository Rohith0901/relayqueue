"""Start a complete local playground and keep traffic flowing.

    python demo/run_local.py            # Ctrl-C to stop

Launches the API (http://127.0.0.1:8000), two workers and the demo receiver
(http://127.0.0.1:9000), creates an account, registers one endpoint per
receiver behaviour and publishes a few events per second. Open the dashboard
and paste the printed API key.
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from demo.harness import Cluster  # noqa: E402

HOOKS = ["ok", "fail-twice", "slow?delay=1.5", "drop-response", "flaky?rate=0.3", "reject"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-port", type=int, default=8000)
    ap.add_argument("--receiver-port", type=int, default=9000)
    ap.add_argument("--rate", type=float, default=2.0, help="events per second")
    ap.add_argument("--workdir", default=str(Path(__file__).resolve().parent / ".playground"))
    args = ap.parse_args()

    c = Cluster(args.workdir, max_attempts=5, backoff_base=1, backoff_max=20)
    c.api_port, c.receiver_port = args.api_port, args.receiver_port
    c.api_url, c.receiver_url = f"http://127.0.0.1:{c.api_port}", f"http://127.0.0.1:{c.receiver_port}"
    try:
        c.start_api()
        c.start_receiver()
        key = c.create_account("Playground")
        for h in HOOKS:
            c.add_endpoint(h)
        c.start_worker("worker-a", concurrency=16)
        c.start_worker("worker-b", concurrency=16)
        print(f"""
  RelayQueue playground is running
  ─────────────────────────────────
  Dashboard : {c.api_url}/        (API key below)
  API docs  : {c.api_url}/docs
  Receiver  : {c.receiver_url}/
  API key   : {key}

  Publishing ~{args.rate:g} events/s. Ctrl-C to stop.
""", flush=True)
        n = 0
        types = ["order.created", "payment.completed", "account.updated", "invoice.paid"]
        while True:
            n += 1
            c.publish(random.choice(types), {"seq": n, "amount": round(random.uniform(5, 900), 2)},
                      idempotency_key=f"play-{int(time.time())}-{n}")
            time.sleep(random.expovariate(args.rate))
    except KeyboardInterrupt:
        pass
    finally:
        c.close()


if __name__ == "__main__":
    main()
