#!/usr/bin/env python3
"""Seed the Confluent source so it looks like a real production estate.

Idempotent -- safe to run repeatedly. Creates:
  * Schemas (Avro w/ evolution, Avro w/ reference, JSON Schema, Protobuf)
    plus subject-level compatibility overrides
  * Topics: orders (3 partitions), customers, shipping
  * Historical records on customers + shipping
  * ACLs for the app principals (these get shadowed too)
"""
import json
import time

import requests
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import (AclBinding, AclOperation, AclPermissionType, AdminClient,
                                   NewTopic, ResourcePatternType, ResourceType)

from common import BOOTSTRAP_SERVERS, SR_URL, encode, latest_schema

H = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def avro(name, fields):
    return json.dumps({"type": "record", "name": name, "namespace": "com.redpanda.demo",
                       "fields": fields})


SCHEMAS = [
    ("orders-value", {"schema": avro("Order", [
        {"name": "order_id", "type": "string"}, {"name": "amount", "type": "double"}])}),
    ("customers-value", {"schema": avro("Customer", [
        {"name": "customer_id", "type": "string"}, {"name": "email", "type": "string"}])}),
    ("orders-value", {"schema": avro("Order", [
        {"name": "order_id", "type": "string"}, {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string", "default": "USD"}])}),
    ("address-value", {"schema": avro("Address", [
        {"name": "street", "type": "string"}, {"name": "city", "type": "string"},
        {"name": "postal_code", "type": "string"}])}),
    ("shipping-value", {"schema": avro("Shipping", [
        {"name": "order_id", "type": "string"},
        {"name": "destination", "type": "com.redpanda.demo.Address"}]),
        "references": [{"name": "com.redpanda.demo.Address", "subject": "address-value",
                        "version": 1}]}),
    ("warehouse-events-value", {"schemaType": "JSON", "schema": json.dumps({
        "$schema": "http://json-schema.org/draft-07/schema#", "title": "WarehouseEvent",
        "type": "object", "properties": {"sku": {"type": "string"},
                                         "quantity": {"type": "integer"}},
        "required": ["sku", "quantity"]})}),
    ("inventory-events-value", {"schemaType": "PROTOBUF", "schema":
        'syntax = "proto3";\npackage com.redpanda.demo;\n'
        "message InventoryEvent {\n  string sku = 1;\n  int32 quantity = 2;\n}\n"}),
]
COMPAT = {"orders-value": "BACKWARD", "shipping-value": "FULL_TRANSITIVE"}
TOPICS = {"orders": 3, "customers": 1, "shipping": 1}
ACLS = [
    ("User:billing-service", ResourceType.TOPIC, "orders", ResourcePatternType.LITERAL, AclOperation.READ),
    ("User:billing-service", ResourceType.GROUP, "billing-service", ResourcePatternType.LITERAL, AclOperation.READ),
    ("User:order-service", ResourceType.TOPIC, "orders", ResourcePatternType.LITERAL, AclOperation.WRITE),
    ("User:analytics", ResourceType.TOPIC, "cust", ResourcePatternType.PREFIXED, AclOperation.READ),
]


def wait_for_registry():
    for _ in range(90):
        try:
            if requests.get(f"{SR_URL}/subjects", timeout=3).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise SystemExit(f"Schema Registry {SR_URL} not reachable")


def main():
    wait_for_registry()
    print("Registering schemas on Confluent Schema Registry...")
    for subject, body in SCHEMAS:
        r = requests.post(f"{SR_URL}/subjects/{subject}/versions", headers=H, json=body, timeout=10)
        r.raise_for_status()
        print(f"  {subject:24} id={r.json()['id']}")
    for subject, level in COMPAT.items():
        requests.put(f"{SR_URL}/config/{subject}", headers=H, json={"compatibility": level},
                     timeout=10).raise_for_status()
        print(f"  {subject:24} compatibility={level}")

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=10).topics
    new = [NewTopic(t, p, 1) for t, p in TOPICS.items() if t not in existing]
    for t, f in admin.create_topics(new).items() if new else []:
        f.result()
        print(f"  created topic {t}")

    # Historical data on topics that are *not* part of the live app.
    c = Consumer({"bootstrap.servers": BOOTSTRAP_SERVERS, "group.id": "seed-probe"})
    _, hi = c.get_watermark_offsets(TopicPartition("customers", 0), timeout=10)
    c.close()
    if hi == 0:
        p = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
        cid, cs = latest_schema("customers-value")
        sid, ss = latest_schema("shipping-value")
        for i in range(1, 51):
            p.produce("customers", key=f"cust-{i:04d}",
                      value=encode(cid, cs, {"customer_id": f"cust-{i:04d}",
                                             "email": f"customer{i}@example.com"}))
        for i in range(1, 26):
            p.produce("shipping", key=f"ord-{i:06d}", value=encode(sid, ss, {
                "order_id": f"ord-{i:06d}",
                "destination": {"street": f"{i} Panda Way", "city": "San Francisco",
                                "postal_code": "94105"}}))
        p.flush(15)
        print("  produced 50 customers + 25 shipments (historical data)")

    bindings = [AclBinding(rt, name, pt, principal, "*", op, AclPermissionType.ALLOW)
                for principal, rt, name, pt, op in ACLS]
    for b, f in admin.create_acls(bindings).items():
        f.result()
    print(f"  {len(bindings)} ACLs in place for order-service, billing-service, analytics")
    print("Seed complete.")


if __name__ == "__main__":
    main()
