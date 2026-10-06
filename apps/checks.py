#!/usr/bin/env python3
"""Small exit-code checks used by demo.sh / test.sh wait loops.

  checks.py drained        billing-service has consumed everything on Confluent
  checks.py drained-redpanda  ...and on Redpanda (after cutover)
  checks.py topic-synced   shadow `orders` HWMs == source HWMs (offset-preserving)
  checks.py group-synced   shadow group offsets == source group offsets
  checks.py schemas-synced every source subject/version/ID present on Redpanda
  checks.py acls-synced    every source ACL present on Redpanda
"""
import sys

from confluent_kafka.admin import AdminClient

from scoreboard import DST, SRC, acls, group_offsets, offsets, schemas


def admin(side):
    return AdminClient({"bootstrap.servers": side["kafka"], "socket.timeout.ms": 5000})


def drained(side=SRC):
    hwm = offsets(side["kafka"]).get("orders", [])
    grp = group_offsets(admin(side))
    return bool(hwm) and all(grp.get(f"orders/p{i}") == h for i, h in enumerate(hwm))


def topic_synced():
    s, d = offsets(SRC["kafka"]), offsets(DST["kafka"])
    return bool(s.get("orders")) and s.get("orders") == d.get("orders")


def group_synced():
    s, d = group_offsets(admin(SRC)), group_offsets(admin(DST))
    return bool(s) and s == d


def group_shadowed():
    """Shadow group exists with real (non-zero) positions close to the live source."""
    s, d = group_offsets(admin(SRC)), group_offsets(admin(DST))
    return bool(s) and set(s) == set(d) and all(0 < d[k] and s[k] - d[k] <= 20 for k in s)


def schemas_synced():
    s, d = schemas(SRC["sr"]), schemas(DST["sr"])
    return bool(s) and all(k in d and d[k]["ids"] == v["ids"] and d[k]["compat"] == v["compat"]
                           for k, v in s.items())


def acls_synced():
    s, d = acls(admin(SRC)), acls(admin(DST))
    return bool(s) and set(map(tuple, s)) <= set(map(tuple, d))


CHECKS = {"drained": drained, "drained-redpanda": lambda: drained(DST), "topic-synced": topic_synced, "group-synced": group_synced, "group-shadowed": group_shadowed,
          "schemas-synced": schemas_synced, "acls-synced": acls_synced}

if __name__ == "__main__":
    try:
        ok = all(CHECKS[name]() for name in sys.argv[1:])
    except Exception as e:  # cluster not reachable yet, etc.
        print(f"check error: {e}", file=sys.stderr)
        ok = False
    sys.exit(0 if ok else 1)
