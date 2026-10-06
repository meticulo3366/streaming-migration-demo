#!/usr/bin/env python3
"""billing-service: a consumer group (`billing-service`) reading `orders`.

Commits offsets synchronously after every record, and appends every processed
order ID to /state/processed.log. On startup it prints the committed offset it
resumes from -- after cutover, that offset came from Confluent via Shadowing.
"""
import os
import signal

from confluent_kafka import Consumer

from common import BOOTSTRAP_SERVERS, CLUSTER, SR_URL, STATE_DIR, decode

GROUP = os.environ.get("GROUP_ID", "billing-service")
TOPIC = os.environ.get("TOPIC", "orders")
LOG_FILE = os.path.join(STATE_DIR, "processed.log")

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def main():
    os.makedirs(STATE_DIR, exist_ok=True)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 1000,
    })

    def on_assign(c, partitions):
        committed = c.committed(partitions, timeout=10)
        desc = ", ".join(
            f"p{tp.partition}@{tp.offset if tp.offset >= 0 else 'earliest'}" for tp in committed)
        print(f"[billing-service <- {CLUSTER}] group '{GROUP}' resuming from committed offsets: {desc}",
              flush=True)

    consumer.subscribe([TOPIC], on_assign=on_assign)
    print(f"[billing-service] connected to {CLUSTER} ({BOOTSTRAP_SERVERS}), SR={SR_URL}", flush=True)
    cache = {}
    with open(LOG_FILE, "a") as log:
        while running:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                print(f"[billing-service] error: {msg.error()}", flush=True)
                continue
            schema_id, order = decode(msg.value(), cache)
            # Record the work, then commit (at-least-once): a crash in between
            # would re-bill, never skip -- and the audit would catch it.
            log.write(f"{order['order_id']} {CLUSTER}\n")
            log.flush()
            consumer.commit(message=msg, asynchronous=False)
            print(f"[billing-service <- {CLUSTER}] billed {order['order_id']} "
                  f"${order['amount']:>7.2f}  schema_id={schema_id}  "
                  f"partition={msg.partition()} offset={msg.offset()}", flush=True)
    consumer.close()
    print("[billing-service] stopped cleanly", flush=True)


if __name__ == "__main__":
    main()
