#!/usr/bin/env python3
"""Record-by-record proof: every record on Confluent exists on Redpanda,
byte-identical, at the same offset.

For each topic and partition, reads offsets [0, source high-watermark) from
BOTH clusters and compares a SHA-256 over (offset, timestamp, key, value,
headers) for every record. Works before or after cutover: after failover,
Redpanda holds the Confluent records as an exact prefix, plus new writes.

  verify_records.py            all shadowed topics
  verify_records.py orders     one topic
"""
import hashlib
import sys

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from scoreboard import DST, SRC, TOPICS

G, R, B, D, X = "\033[32m", "\033[31m", "\033[1m", "\033[2m", "\033[0m"


def digests(kafka, topic, partition, end):
    """Per-record SHA-256 for offsets [0, end) of one partition."""
    c = Consumer({"bootstrap.servers": kafka, "group.id": "verify-records",
                  "enable.auto.commit": False, "enable.partition.eof": True})
    c.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])
    out = {}
    while len(out) < end:
        msg = c.poll(5.0)
        if msg is None:
            break
        if msg.error():
            if msg.offset() is not None and len(out) >= end:
                break
            continue
        if msg.offset() >= end:
            break
        h = hashlib.sha256()
        for part in (str(msg.offset()), str(msg.timestamp()[1]), msg.key() or b"",
                     msg.value() or b"", repr(msg.headers() or [])):
            h.update(part if isinstance(part, bytes) else part.encode())
            h.update(b"\x00")
        out[msg.offset()] = h.hexdigest()
    c.close()
    return out


def main():
    topics = sys.argv[1:] or TOPICS
    probe = Consumer({"bootstrap.servers": SRC["kafka"], "group.id": "verify-records"})
    md = probe.list_topics(timeout=10).topics
    total = bad = 0
    print(f"{B}RECORD-BY-RECORD VERIFICATION{X}  "
          f"{D}SHA-256 of offset+timestamp+key+value+headers, Confluent vs Redpanda{X}")
    for t in topics:
        for p in sorted(md[t].partitions):
            lo, hi = probe.get_watermark_offsets(TopicPartition(t, p), timeout=10)
            src = digests(SRC["kafka"], t, p, hi)
            dst = digests(DST["kafka"], t, p, hi)
            missing = [o for o in src if o not in dst]
            differ = [o for o in src if o in dst and src[o] != dst[o]]
            short = len(src) != hi - lo
            ok = not missing and not differ and not short
            total += len(src)
            bad += len(missing) + len(differ) + (1 if short else 0)
            fp_src = hashlib.sha256("".join(src[o] for o in sorted(src)).encode()).hexdigest()[:12]
            fp_dst = hashlib.sha256("".join(dst.get(o, "") for o in sorted(src)).encode()).hexdigest()[:12]
            mark = f"{G}✔ identical{X}" if ok else f"{R}✘ missing={len(missing)} differ={len(differ)}{X}"
            print(f"  {t + '/p' + str(p):13} {len(src):5} records   "
                  f"Confluent {fp_src}   Redpanda {fp_dst}   {mark}")
    probe.close()
    if bad:
        print(f"{R}✘ {bad} discrepancies{X}")
        sys.exit(1)
    print(f"{G}✔ all {total} Confluent records present on Redpanda, byte-identical, same offsets{X}")


if __name__ == "__main__":
    main()
