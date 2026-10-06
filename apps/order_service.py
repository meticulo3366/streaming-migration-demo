#!/usr/bin/env python3
"""order-service: a live producer writing Avro orders to the `orders` topic.

Every order carries a monotonically increasing sequence number (ord-000001,
ord-000002, ...). The last delivered sequence is persisted to /state so that
when the service is restarted against Redpanda it continues the sequence --
which lets the demo prove there are no gaps and no duplicates across cutover.
"""
import os
import random
import signal
import sys
import time

from confluent_kafka import Producer

from common import BOOTSTRAP_SERVERS, CLUSTER, SR_URL, STATE_DIR, encode, latest_schema

TOPIC = os.environ.get("TOPIC", "orders")
RATE = float(os.environ.get("RATE", "4"))
SEQ_FILE = os.path.join(STATE_DIR, "order-service.seq")
LOG_FILE = os.path.join(STATE_DIR, "produced.log")

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def read_seq():
    try:
        with open(SEQ_FILE) as f:
            return int(f.read().strip() or 0)
    except FileNotFoundError:
        return 0


def wait_for_schema():
    while running:
        try:
            return latest_schema("orders-value")
        except Exception as e:  # registry or subject not ready yet
            print(f"[order-service] waiting for schema orders-value: {e}", flush=True)
            time.sleep(2)
    sys.exit(0)


def main():
    os.makedirs(STATE_DIR, exist_ok=True)
    schema_id, schema = wait_for_schema()
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
    })
    delivered = {"seq": read_seq()}
    log = open(LOG_FILE, "a")

    def on_delivery(err, msg):
        if err:
            print(f"[order-service] delivery failed: {err}", flush=True)
            return
        seq = int(msg.key().decode().split("-")[1])
        delivered["seq"] = max(delivered["seq"], seq)
        with open(SEQ_FILE, "w") as f:
            f.write(str(delivered["seq"]))
        log.write(f"{msg.key().decode()} {CLUSTER}\n")
        log.flush()
        print(f"[order-service -> {CLUSTER}] {msg.key().decode()}  "
              f"schema_id={schema_id}  partition={msg.partition()} offset={msg.offset()}", flush=True)

    seq = delivered["seq"]
    print(f"[order-service] connected to {CLUSTER} ({BOOTSTRAP_SERVERS}), SR={SR_URL}, "
          f"orders-value schema_id={schema_id}, continuing from seq {seq}", flush=True)
    while running:
        seq += 1
        order = {"order_id": f"ord-{seq:06d}",
                 "amount": round(random.uniform(5, 500), 2),
                 "currency": "USD"}
        producer.produce(TOPIC, key=order["order_id"], value=encode(schema_id, schema, order),
                         on_delivery=on_delivery)
        producer.poll(0)
        time.sleep(1.0 / RATE)
    remaining = producer.flush(15)
    log.close()
    print(f"[order-service] stopped cleanly; last delivered seq {delivered['seq']} "
          f"(unflushed={remaining})", flush=True)


if __name__ == "__main__":
    main()
