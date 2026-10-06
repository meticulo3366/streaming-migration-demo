#!/usr/bin/env python3
"""End-to-end audit of the cutover: every order produced was billed exactly once.

Reads /state/produced.log and /state/processed.log, which order-service and
billing-service append to on both clusters. Run it with the apps stopped (or
at least the producer stopped and the consumer drained) for an exact answer.
"""
import os
import sys
from collections import Counter

STATE = os.environ.get("STATE_DIR", "/state")
G, R, B, X = "\033[32m", "\033[31m", "\033[1m", "\033[0m"


def load(name):
    try:
        with open(os.path.join(STATE, name)) as f:
            return [l.split() for l in f if l.strip()]
    except FileNotFoundError:
        return []


produced, processed = load("produced.log"), load("processed.log")
p_ids = [r[0] for r in produced]
seqs = sorted(int(i.split("-")[1]) for i in p_ids)
billed = Counter(r[0] for r in processed)
by_cluster = Counter(r[1] for r in produced)
billed_by = Counter(r[1] for r in processed)

gaps = [s for s in range(1, seqs[-1] + 1) if s not in set(seqs)] if seqs else []
dupes = [k for k, v in billed.items() if v > 1]
missing = sorted(set(p_ids) - set(billed))

print(f"{B}CUTOVER AUDIT{X}  (every order produced -> billed exactly once?)")
print(f"  orders produced   on Confluent: {by_cluster.get('CONFLUENT', 0):5}   on Redpanda: {by_cluster.get('REDPANDA', 0):5}")
print(f"  orders billed     on Confluent: {billed_by.get('CONFLUENT', 0):5}   on Redpanda: {billed_by.get('REDPANDA', 0):5}")
print(f"  sequence gaps:        {len(gaps)}")
print(f"  duplicate billings:   {len(dupes)}")
print(f"  never billed:         {len(missing)}")
ok = bool(seqs) and not gaps and not dupes and not missing and by_cluster.get("REDPANDA", 0) > 0
print(f"  {G}✔ zero data loss, zero duplicates, consumers resumed exactly where they left off{X}" if ok
      else f"  {R}✘ audit failed {(gaps[:3], dupes[:3], missing[:3])}{X}")
sys.exit(0 if ok else 1)
