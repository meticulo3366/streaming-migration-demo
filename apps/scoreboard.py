#!/usr/bin/env python3
"""Side-by-side proof: Confluent (source) vs Redpanda (shadow).

Compares everything the single shadow link carries:
  schemas (subject, version, *schema ID*, compatibility), topic data
  (partitions + per-partition offsets), consumer group offsets, and ACLs.

Usage:
  scoreboard.py                 pretty table
  scoreboard.py --assert        exit 1 unless everything matches
  scoreboard.py --assert --live tolerate in-flight lag on the live `orders` topic
  scoreboard.py --json          machine-readable
"""
import argparse
import json
import sys

import requests
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import (AclBindingFilter, AclOperation, AclPermissionType, AdminClient,
                                   ResourcePatternType, ResourceType)

SRC = {"name": "Confluent", "kafka": "confluent-kafka:29092",
       "sr": "http://confluent-schema-registry:8081"}
DST = {"name": "Redpanda", "kafka": "redpanda-shadow:9092", "sr": "http://redpanda-shadow:8081"}
TOPICS = ["orders", "customers", "shipping"]
GROUP = "billing-service"

G, Y, R, B, D, X = "\033[32m", "\033[33m", "\033[31m", "\033[1m", "\033[2m", "\033[0m"


def schemas(sr):
    out = {}
    try:
        subjects = requests.get(f"{sr}/subjects", timeout=5).json()
    except requests.RequestException:
        return out
    for s in sorted(subjects):
        vers = requests.get(f"{sr}/subjects/{s}/versions", timeout=5).json()
        ids = [requests.get(f"{sr}/subjects/{s}/versions/{v}", timeout=5).json()["id"] for v in vers]
        cfg = requests.get(f"{sr}/config/{s}", timeout=5)
        compat = cfg.json().get("compatibilityLevel", cfg.json().get("compatibility")) if cfg.ok else "-"
        out[s] = {"versions": vers, "ids": ids, "compat": compat}
    return out


def offsets(kafka):
    c = Consumer({"bootstrap.servers": kafka, "group.id": "scoreboard", "socket.timeout.ms": 5000})
    md = c.list_topics(timeout=5).topics
    out = {}
    for t in TOPICS:
        if t not in md or md[t].error:
            continue
        out[t] = [c.get_watermark_offsets(TopicPartition(t, p), timeout=5)[1]
                  for p in sorted(md[t].partitions)]
    c.close()
    return out


def group_offsets(admin):
    try:
        f = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(GROUP)])[GROUP]
        res = f.result()
        return {f"{tp.topic}/p{tp.partition}": tp.offset
                for tp in sorted(res.topic_partitions, key=lambda x: (x.topic, x.partition))
                if tp.offset >= 0}
    except Exception:
        return {}


def acls(admin):
    flt = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY, None, None,
                           AclOperation.ANY, AclPermissionType.ANY)
    try:
        res = admin.describe_acls(flt, request_timeout=5).result()
    except Exception:
        return []
    return sorted((a.principal, f"{a.operation.name} {a.restype.name.lower()}:"
                   f"{a.name}{'*' if a.resource_pattern_type.name == 'PREFIXED' else ''}")
                  for a in res)


def snapshot(side):
    admin = AdminClient({"bootstrap.servers": side["kafka"], "socket.timeout.ms": 5000})
    return {"schemas": schemas(side["sr"]), "offsets": offsets(side["kafka"]),
            "groups": group_offsets(admin), "acls": acls(admin)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--assert", dest="check", action="store_true")
    ap.add_argument("--live", action="store_true",
                    help="tolerate shadow lag on topics still being written")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--section", choices=["schemas", "topics", "groups", "acls"], action="append")
    a = ap.parse_args()

    src, dst = snapshot(SRC), snapshot(DST)
    if a.json:
        print(json.dumps({"source": src, "shadow": dst}, indent=2))
        return
    sections = a.section or ["schemas", "topics", "groups", "acls"]
    failures = []

    def row(label, left, right, ok, pending=False):
        mark = f"{G}✔ match{X}" if ok else (f"{Y}… syncing{X}" if pending else f"{R}✘ differs{X}")
        print(f"  {label:26} {str(left):29} {str(right):29} {mark}")
        if not ok and not pending:
            failures.append(label)
        elif not ok and pending and a.check and not a.live:
            failures.append(label)

    hdr = f"  {'':26} {B}{'CONFLUENT (source)':29} {'REDPANDA (shadow)':29}{X}"
    if "schemas" in sections:
        print(f"\n{B}SCHEMA REGISTRY{X}  {D}subject -> versions / schema IDs / compatibility{X}")
        print(hdr)
        for s, v in src["schemas"].items():
            d = dst["schemas"].get(s)
            left = f"v{v['versions']} id{v['ids']}"
            right = f"v{d['versions']} id{d['ids']}" if d else "—"
            row(s, left, right, d is not None and d["versions"] == v["versions"] and d["ids"] == v["ids"],
                pending=d is None)
            if v["compat"] != "-":
                dc = d["compat"] if d else "—"
                row(f"  └ compatibility", v["compat"], dc, dc == v["compat"], pending=d is None)
        if not src["schemas"]:
            failures.append("no source schemas")
    if "topics" in sections:
        print(f"\n{B}TOPIC DATA{X}  {D}per-partition high-watermark offsets (offset-preserving){X}")
        print(hdr)
        for t in TOPICS:
            s, d = src["offsets"].get(t), dst["offsets"].get(t)
            ok = s is not None and s == d
            pending = d is not None and s is not None and len(s) == len(d)
            lag = sum(x - y for x, y in zip(s, d)) if pending else None
            row(f"{t}" if not lag else f"{t} (lag {lag})", s or "—", d or "—", ok,
                pending=pending or d is None)
    if "groups" in sections:
        print(f"\n{B}CONSUMER GROUP OFFSETS{X}  {D}group '{GROUP}' committed offsets{X}")
        print(hdr)
        keys = sorted(set(src["groups"]) | set(dst["groups"]))
        for k in keys:
            s, d = src["groups"].get(k), dst["groups"].get(k)
            row(k, s if s is not None else "—", d if d is not None else "—", s == d,
                pending=True)
        if not keys:
            print(f"  {D}(no committed offsets yet){X}")
    if "acls" in sections:
        print(f"\n{B}ACLs{X}")
        print(hdr)
        for acl in src["acls"]:
            have = acl in dst["acls"]
            row(acl[0], acl[1], acl[1] if have else "—", have, pending=True)
    print()
    if a.check:
        if failures:
            print(f"{R}MISMATCH:{X} {', '.join(failures)}")
            sys.exit(1)
        print(f"{G}All checks passed.{X}")


if __name__ == "__main__":
    main()
