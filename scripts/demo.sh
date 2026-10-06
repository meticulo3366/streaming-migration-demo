#!/usr/bin/env bash
# The 5-minute customer demo: Confluent -> Redpanda with Shadowing.
#
#   make up     (before the call, ~1 min)   stage Confluent estate + live apps
#   make demo   (on the call, ~5 min)       this script; Enter advances each step
#
# AUTO=1 runs it unattended (that's what `make test` does), so the demo and
# the test suite exercise exactly the same code path.
source "$(dirname "$0")/lib.sh"

T0=$SECONDS

# ---------------------------------------------------------------- preflight
docker inspect -f '{{.State.Running}}' redpanda-shadow 2>/dev/null | grep -q true \
  || die "Environment not running. Run: make up"
if rpk shadow list 2>/dev/null | grep -q "$LINK"; then
  die "A shadow link already exists — this environment has been demoed. Run: make reset"
fi
check drained >/dev/null 2>&1 || wait_for "apps live on Confluent" 60 check drained

# Predicates for wait_for
three_topics_shadowed() { [[ $(docker exec redpanda-shadow rpk topic list | grep -c -E '^(orders|customers|shipping) ') == 3 ]]; }
orders_failed_over()    { [[ $(link_topic_state orders) == FAILED_OVER ]]; }
billing_on_redpanda()   { docker logs billing-service 2>&1 | grep -q 'REDPANDA\] billed'; }
all_failed_over()       { ! rpk shadow status "$LINK" | grep -q 'State: ACTIVE$'; }
sr_writable()           { [[ $(sr_post http://localhost:28081 orders-value "$V3") == 200 ]]; }

clear_screen() { [[ "$AUTO" == "1" ]] || printf '\033[2J\033[H'; }
clear_screen

cat <<EOF
${B}${RP}  ▶ Push-button migration from Confluent to Redpanda with Shadowing${X}
${D}    One link. Topics, schemas, consumer offsets and ACLs. No MirrorMaker,
    no custom scripts, no cutover weekend.${X}
EOF

# =================================================================== ACT 1
act 1 "Today: a live Confluent Platform estate" \
  "This is a typical Confluent estate: live producers and consumers, Avro, Protobuf and JSON schemas with references and compatibility rules, plus ACLs. Migrating this has traditionally meant MirrorMaker, a separate schema migration, offset translation and a risky big-bang cutover weekend."

run "docker logs --tail 3 order-service; docker logs --tail 3 billing-service"
run "curl -s localhost:8081/subjects; echo"
note "Redpanda Console works against any Kafka — Confluent source: http://localhost:8088"
pause

# =================================================================== ACT 2
act 2 "One link carries the whole migration" \
  "Shadowing is built into the Redpanda broker. One shadow link carries topic data, schemas, consumer offsets and ACLs — created on Redpanda, pulling from Confluent. Nothing to install or operate on the Confluent side."

show "cat config/shadow-link.yaml   # comments stripped"
grep -vE '^\s*#|^\s*$' config/shadow-link.yaml | sed 's/^/    /'
pause
run "rpk shadow create --config-file /config/shadow-link.yaml --no-confirm"
wait_for "schemas replicated (subjects, versions, IDs, compatibility)" 60 check schemas-synced
wait_for "ACLs replicated" 60 check acls-synced
wait_for "topic history backfilled (full history, offset-preserving)" 60 three_topics_shadowed
wait_for "consumer group offsets shadowed" 60 check group-shadowed
pause

# =================================================================== ACT 3
act 3 "Proof: full fidelity, side by side" \
  "Same subjects, same versions, and critically the same schema IDs — every record on disk embeds a schema ID, so it decodes on Redpanda unchanged. Same offsets per partition. Same consumer group positions. Same ACLs."

run "tools python scoreboard.py --live"
note "Live view: http://localhost:8080 (Redpanda Console → Topics / Schema Registry / Security)"
pause

say "Decode a record on Redpanda, using Redpanda Schema Registry:"
run "rpk topic consume orders -n 1 -o start --use-schema-registry=value -f '%k  →  %v  (offset %o)\n'"
say "The shadow is read-only, so it can never drift from the source:"
show "echo test | rpk topic produce customers"
echo test | rpk_i topic produce customers 2>&1 | tail -1 | sed 's/^/  /' || true
show "curl -X POST localhost:28081/subjects/rogue-value/versions ..."
code=$(sr_post http://localhost:28081 rogue-value '{"schema":"{\"type\":\"string\"}"}')
printf '  HTTP %s  %s\n' "$code" "${D}(writes to Schema Registry are disabled while shadowing)${X}"
[[ "$code" == "412" ]] || die "expected 412 from shadow Schema Registry, got $code"
note "Validate against real production traffic for as long as you like — or keep it as a warm standby / DR."
pause

# =================================================================== ACT 4
act 4 "Cut over one application — no big bang" \
  "Migrate one application at a time, on your own schedule. We move the orders pipeline now; customers and shipping keep shadowing untouched."

say "1) Stop order-service on Confluent, let billing-service drain"
run "docker compose stop order-service >/dev/null 2>&1"
wait_for "billing-service drained on Confluent" 60 check drained
run "docker compose stop billing-service >/dev/null 2>&1"
wait_for "last records + final consumer offsets shadowed to Redpanda" 60 check topic-synced group-synced
run "tools python scoreboard.py --section topics --section groups"

say "2) Fail over just the orders topic (others keep shadowing)"
run "rpk shadow failover $LINK --topic orders --no-confirm"
wait_for "orders is now a writable Redpanda topic" 30 orders_failed_over

say "3) Point the apps at Redpanda — a config change, no code change"
show "BOOTSTRAP_SERVERS=redpanda-shadow:9092  SR_URL=http://redpanda-shadow:8081  docker compose up -d order-service billing-service"
point_apps_at redpanda
wait_for "billing-service resumed on Redpanda" 60 billing_on_redpanda
sleep 2
run "docker logs billing-service 2>&1 | grep -m1 'REDPANDA\] group'"
run "docker logs --tail 4 billing-service"
note "The consumer group picked up at the exact committed offsets it left on Confluent — no reset, no reprocessing."
show "rpk shadow status $LINK   # topics"
rpk shadow status "$LINK" | awk '/^Name:/{print "    "$0}'
pause

# =================================================================== ACT 5
act 5 "Zero data loss — then finish the migration" \
  "Every order produced across both clusters was billed exactly once. When the last app has moved, fail over what's left, remove the link, and Redpanda is your system of record — schema IDs keep counting from where Confluent left off."

run "docker compose stop order-service >/dev/null 2>&1"
wait_for "billing-service drained on Redpanda" 60 check drained-redpanda
run "tools python audit.py"
say "And at the storage level — every Confluent record, hashed on both clusters:"
run "tools python verify_records.py"
docker compose start order-service >/dev/null 2>&1
pause

say "Finish: fail over remaining topics, retire the link"
run "rpk shadow failover $LINK --all --no-confirm"
wait_for "all topics failed over" 30 all_failed_over
run "rpk shadow delete $LINK --no-confirm"
say "Evolve the schema natively on Redpanda (v3 adds 'channel'):"
V3='{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.redpanda.demo\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"},{\"name\":\"channel\",\"type\":\"string\",\"default\":\"web\"}]}"}'
show "curl -X POST localhost:28081/subjects/orders-value/versions -d '{...v3...}'"
wait_for "v3 registered on Redpanda (HTTP 200)" 30 sr_writable
run "curl -s localhost:28081/subjects/orders-value/versions/latest | sed 's/,\"schema\".*/}/'; echo"

ELAPSED=$((SECONDS - T0))
cat <<EOF

${RP}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${X}
${B}What you just saw${X}
  ${G}✔${X} One link carried topics, schemas, consumer offsets and ACLs
  ${G}✔${X} Offset-preserving, full-fidelity replication — same schema IDs, same offsets
  ${G}✔${X} One application cut over while the rest kept shadowing — no big bang
  ${G}✔${X} Consumers resumed exactly where they left off — zero loss, zero duplicates
  ${G}✔${X} No MirrorMaker, no custom scripts, no cutover weekend

${B}What changes underneath:${X} same Kafka API for your apps, on a single C++ binary
with Schema Registry built in, deployed where you choose (BYOC, Dedicated,
Serverless, self-hosted). Redpanda migration engineers work through it with you.

${B}Next step:${X} run Shadowing against a copy of your own Confluent traffic.
${D}Blog: redpanda.com/blog/migrate-confluent-redpanda-shadowing   Lab: docs.redpanda.com/labs${X}
${D}(demo runtime ${ELAPSED}s)${X}
EOF
