# Confluent → Redpanda in 5 minutes, with Shadowing

A customer-ready SE demo of **push-button migration from Confluent to Redpanda**.
One shadow link carries the whole migration: **topic data, schemas, consumer
offsets and ACLs**. You then cut over a live application with zero data loss,
on camera, in about five minutes.

Built on the Redpanda lab
[Migrate from Confluent with Shadowing](https://docs.redpanda.com/labs/docker-compose/confluent-schema-registry-shadowing/),
with copy aligned to the blog post
[Push-button migration from Confluent to Redpanda with Shadowing](https://www.redpanda.com/blog/migrate-confluent-redpanda-shadowing).

## Quick start

```bash
make test     # ~60s: fresh env, full unattended demo, 19 assertions. PASS = safe to present
make reset    # ~30s: re-stage the "before" world (do this right before the call)
make demo     # the 5-minute guided demo; press Enter to advance
```

Open in a second terminal / browser tab before you start:

```bash
make logs     # live app logs; lines turn from CONFLUENT (blue) to REDPANDA (red) at cutover
make open     # Redpanda Console on the shadow (:8080) and on the Confluent source (:8088)
```

Talk track, timings and objection handling: **[TALK-TRACK.md](TALK-TRACK.md)**.

## Requirements

- Docker Desktop (or Docker Engine) with Compose v2, about 4 GB RAM available
- `make`, `curl`, `bash`. Nothing else: Python, rpk and the Kafka clients all run in containers
- Free ports: 8080, 8081, 8088, 9092, 28081, 29092, 29644
- First run pulls about 2 GB of images. Do it on good Wi-Fi, not on the customer's network

## Enterprise license

Shadowing is a Redpanda Enterprise feature. Put the license at
`license/redpanda.license` (it is git-ignored). `make up` applies it with
`rpk cluster license set`, and `make test` asserts that it is active.

- No license file, or an expired one: the demo still runs on Redpanda's
  built-in 30-day evaluation and prints a warning.
- To swap licenses, replace the file and run `make reset`.

## What's in the box

| Component | Role |
|---|---|
| `confluent-kafka`, `confluent-schema-registry` | The **source**: real Confluent Platform 8.3 (KRaft), with ACLs enabled |
| `order-service` | Live producer. Avro orders with sequence numbers, 4/s |
| `billing-service` | Live consumer group `billing-service`. Commits after every record |
| `redpanda-shadow` | The **destination**: Redpanda v26.2, Enterprise licensed, Kafka API + Schema Registry |
| `redpanda-console` (:8080) / `source-console` (:8088) | Redpanda Console on each side. Console runs on any Kafka |
| `tools` | Seeding, the side-by-side `scoreboard`, and the cutover `audit` |

The Confluent estate is seeded with Avro (with schema evolution), an Avro
**schema reference**, JSON Schema, Protobuf, subject-level compatibility
overrides, three topics with history, and four ACLs.

## The 5 acts

| # | Act | Proof point |
|---|---|---|
| 1 | Live Confluent estate | Real apps, real schemas, real ACLs |
| 2 | `rpk shadow create`: one link | Topics, schemas, offsets and ACLs all start syncing |
| 3 | Side-by-side scoreboard | **Same schema IDs**, same per-partition offsets, same group offsets, same ACLs. The shadow is read-only (produce → `POLICY_VIOLATION`, Schema Registry → `412`) |
| 4 | Cut over **one** app | Fail over only `orders` and re-point the apps (config only). The consumer resumes at the exact committed offsets. Other topics keep shadowing |
| 5 | Audit and finish | Every order billed exactly once (0 gaps, 0 dupes). Every Confluent record SHA-256 identical on Redpanda. Fail over the rest, delete the link, then evolve the schema on Redpanda (new ID 8 continues the sequence) |

## Make targets

```
make up          Stage Confluent + live apps + empty licensed Redpanda (idempotent)
make demo        Guided 5-minute demo
make reset       Wipe and re-stage (run between demos)
make test        Fresh env + unattended demo + assertions (exit 0 = ready)
make test-loop   Soak: N fresh runs back to back (make test-loop N=5)
make logs        Live app logs, colored by cluster
make scoreboard  Side-by-side Confluent vs Redpanda, any time
make verify      Record-by-record SHA-256 proof: every Confluent record is on Redpanda
make audit       Every order produced was billed exactly once (after cutover)
make status      rpk shadow status
make open        Open both Consoles
make down        Tear everything down
```

`make demo` and `make test` run **the same script** (`scripts/demo.sh`; the test
sets `AUTO=1`), so a green test means the exact demo path works. Test logs go
to `logs/`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `A shadow link already exists` | The stage has already been demoed. `make reset` |
| `Environment not running` | `make up` |
| Port already in use | Stop other local Kafka or Redpanda stacks (`docker ps`), then `make reset` |
| Wait step times out | `make status` and `make scoreboard` show what's lagging. `docker compose logs redpanda-shadow` |
| License warning | Check that `license/redpanda.license` exists and hasn't expired. The demo still works on the built-in trial |
| Anything weird | `make reset`. Every run starts from a clean slate |

## Demo vs. production

These settings are tuned so the demo stays interactive. Call them out if asked:

- Sync intervals are 3–15s. Production defaults are a 10s schema tail, a 5m full
  sync, and 30s for topics, offsets and ACLs.
- Everything is plaintext. Production links use TLS and SASL/SCRAM (or PLAIN)
  to Confluent, and HTTP Basic auth to Schema Registry. Confluent Cloud is
  supported via API key and secret.
- `unsupported_schema_feature_policy: REMOVE` strips Confluent-only rule sets
  and metadata. The default, `FAIL`, reports them so you can review them before
  cutover.
