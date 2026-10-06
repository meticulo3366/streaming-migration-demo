# Talk track: push-button migration from Confluent to Redpanda

**Audience:** platform and streaming teams running Confluent Platform or
Confluent Cloud. **Length:** 5 minutes, plus Q&A.
**Positioning source:** [redpanda.com/blog/migrate-confluent-redpanda-shadowing](https://www.redpanda.com/blog/migrate-confluent-redpanda-shadowing)

## Before the call (T–10 min)

1. `make test`, wait for **PASS**, then `make reset`
2. Terminal 1: `make demo`, sitting at the title card. Font size 16+, dark theme
3. Terminal 2: `make logs`
4. Browser: http://localhost:8088 (Confluent source) and http://localhost:8080 (Redpanda)

## The one-liner

> "One link carries the whole migration: topic data, schemas, consumer
> offsets and ACLs. You migrate one application at a time, on your own
> schedule. No MirrorMaker, no custom scripts, no cutover weekend."

## Script

### 0:00 Set up the pain (title card → Act 1)

> "Most teams we talk to want off Confluent, but the migration itself is what
> keeps them there. It usually means MirrorMaker for data, a separate tool for
> schemas, offset translation for consumers, rebuilding ACLs by hand, and then a
> big-bang cutover weekend. Hand-rolled migrations break on schema ID
> mismatches: every record on disk points at an ID the new registry doesn't
> have."

**Act 1:** point at terminal 2.
> "Here's a live Confluent Platform estate: an order service producing Avro, a
> billing service in a consumer group, and Avro, Protobuf and JSON schemas with
> references and compatibility rules. Plus ACLs. Real Confluent, not a mock."

### 0:45 One link (Act 2)

> "This is the entire migration config: one shadow link. It's created on
> Redpanda and pulls from Confluent. There's nothing to install on the Confluent
> side, no Connect cluster, no MirrorMaker."

Point at the four blocks: **topics, consumer offsets, ACLs, Schema Registry**.
> "Schema Registry runs in API mode. It talks to any Confluent-compatible
> registry over REST, so this works the same against Confluent Cloud."

Press Enter on `rpk shadow create`. The waits finish in seconds.

### 1:30 Proof (Act 3)

> "Side by side. Same subjects, same versions, and the same **schema IDs**.
> That's the one that matters: every record on disk embeds its schema ID, so
> those bytes decode on Redpanda unchanged. Same offsets on every partition.
> Same consumer group positions. Same ACLs."

Optionally flip to Console :8080 → Schema Registry → `shipping-value` to show the reference.

On the decoded record:
> "That's a record Confluent wrote, decoded by Redpanda Schema Registry."

On the read-only checks:
> "The shadow is read-only: produce is rejected and Schema Registry returns
> 412. It can't drift. You can run it against real production traffic for weeks
> to validate performance, or keep it indefinitely as a warm standby for DR."

### 2:45 Cut over one app (Act 4)

> "No big bang. We'll move just the orders pipeline. Customers and shipping
> keep shadowing untouched."

- Stop the producer, and the consumer drains.
> "Last records and final consumer offsets are shadowed in a couple of seconds."
- `rpk shadow failover --topic orders`
> "Only `orders` becomes a writable Redpanda topic."
- Re-point the apps.
> "That's a config change: bootstrap servers and registry URL. No code change."

Point at terminal 2: the lines flip from **CONFLUENT** to **REDPANDA**.
> "Look at the resume line. The billing group picked up at the *exact*
> committed offsets it left on Confluent. No reset to earliest, no reprocessing,
> no offset translation."

### 4:00 Zero loss, then finish (Act 5)

> "Every order produced across both clusters was billed exactly once. Zero
> gaps, zero duplicates."

> "When the last app has moved, you fail over the rest and delete the link.
> Redpanda is now the system of record. Here's a new schema version registered
> natively: ID 8, which continues right where Confluent's IDs left off. No
> collisions."

### 4:45 Close

> "One link. Offset-preserving, full-fidelity replication. One app at a time,
> on your schedule."

> "Your apps speak the same Kafka API on the other side. What changes is
> everything underneath: a single C++ binary with a smaller hardware footprint,
> no JVM to tune, Schema Registry built into the broker instead of a separate
> service to run, deployed where you choose
> (BYOC, Dedicated, Serverless or self-hosted). And our migration engineers
> work through it with you."

> "The next step is to point a shadow link at a copy of your own Confluent
> traffic. Want to set that up for next week?"

*Exec audience (optional bridge):* "This is the streaming foundation the
Redpanda Agentic Data Plane runs on. You migrate once and get governed agent
access to the same data later."

## Proving zero data loss (when they push back)

Three independent levels of evidence, from the storage layer up to the business result:

1. **Records:** `make verify`. It reads every record from both clusters and
   hashes offset, timestamp, key, value and headers with SHA-256. Matching
   fingerprints per partition mean the data is byte-identical at the same
   offsets. After cutover, Redpanda holds the Confluent records as an exact
   prefix, plus the new writes. The test suite also runs a *negative control*:
   one record written only to Confluent must be flagged. A check that can't
   fail proves nothing.
2. **Consumer positions:** `make scoreboard`. Committed group offsets match
   per partition before failover. The billing-service resume line prints the
   exact offsets it picked up at.
3. **Business outcome:** `make audit`. Every order carries a sequence number,
   and both apps keep an independent ledger. Every order produced on either
   cluster was billed exactly once: 0 gaps, 0 duplicates.

> "Don't take our word for it. Run your own consumer, or `kcat`, against both
> clusters and diff the output. Better still, do it on your own traffic during
> the shadow phase. That's what the shadow phase is for."

**Be candid:** replication is asynchronous. The guarantee in the demo comes
from the cutover sequence (stop producers → drain → confirm lag 0 → fail
over), not from synchronous replication. In a disaster failover without that
sequence, records still in flight are at risk. Say so before they ask.

## Discovery questions

- How many clusters, topics, and Schema Registry subjects? Any schema contexts or multi-tenant registries?
- Confluent Platform or Confluent Cloud? Which auth methods (SASL/SCRAM, mTLS, OAuth)?
- What did your last migration or upgrade look like? Was there a cutover weekend?
- Which application would you move first? (That's the pilot.)
- Do you have DR today? (Shadowing doubles as a warm standby.)

## Objection handling

| They say | You say |
|---|---|
| "We use Confluent Cloud." | API-mode Schema Registry replication uses the REST API with an API key and secret, and the Kafka side uses SASL. Same link, same flow. |
| "We use schema rule sets / data contracts." | The default policy `FAIL` lists every unsupported feature before you cut over. `REMOVE` strips them and imports the rest. You decide, with full visibility. |
| "What about exactly-once / transactions?" | Replication is offset-preserving. Validate transactional workloads end to end during the shadow phase — that is what it is for. |
| "Is replication synchronous?" | No, it's asynchronous. That's why cutover is stop → drain → fail over, which takes seconds per app, as you saw. |
| "Can we roll back?" | Until you fail over, Confluent is untouched and stays primary. Fail over per topic so each app has its own decision point. |
| "Do we need to change code?" | No. You change bootstrap servers and the registry URL. API compatibility is the baseline, not the pitch. The value is what you stop operating, and that migration becomes one link instead of a project. |
| "Licensing?" | Shadowing is a Redpanda Enterprise feature. This demo runs on an Enterprise license. |
| "How long for our estate?" | Backfill is bounded by data volume and bandwidth. Cutover is per app and takes minutes. Let's size it with your numbers. |

## Messaging guardrails

Use these phrasings: *push-button migration*, *one link carries the whole
migration*, *offset-preserving, full-fidelity replication*, *one application
at a time, on your own schedule*, *consumers resume where they left off*,
*no MirrorMaker, no custom scripts, no cutover weekend*.

Avoid promising specific migration durations or zero downtime for every
workload before sizing. Say "minutes per application" only after a stop, drain
and fail over sequence like the one shown.
