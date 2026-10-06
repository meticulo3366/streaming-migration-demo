#!/usr/bin/env bash
# Full end-to-end test: fresh environment -> unattended demo -> assertions.
# Exit code 0 = demo is safe to give. Log: logs/test-<timestamp>.log
#
#   make test               one run
#   make test-loop N=5      soak: N back-to-back fresh runs
source "$(dirname "$0")/lib.sh"

mkdir -p logs
LOG="logs/test-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee "$LOG") 2>&1

PASS=0; FAIL=0; FAILED=()
assert() {  # assert "<description>" <command...>
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then ok "$desc"; PASS=$((PASS+1))
  else printf '%s\n' "${R}✘ ${desc}${X}"; FAIL=$((FAIL+1)); FAILED+=("$desc"); fi
}

T0=$SECONDS
say "[1/3] Fresh environment (make reset)"
scripts/up.sh --fresh
T_UP=$((SECONDS - T0))

say "[2/3] Unattended demo run (AUTO=1 make demo)"
T1=$SECONDS
AUTO=1 scripts/demo.sh
T_DEMO=$((SECONDS - T1))

say "[3/3] Assertions"
assert "demo refuses to re-run without reset (guards against a stale stage)" \
  bash -c '! AUTO=1 scripts/demo.sh'

assert "Enterprise license applied and valid" \
  bash -c "docker exec redpanda-shadow rpk cluster license info | grep -q 'License status: *valid' &&
           ! docker exec redpanda-shadow rpk cluster license info | grep -q 'Built-In Evaluation'"

assert "shadow link removed after migration" \
  bash -c "! docker exec redpanda-shadow rpk shadow list -X admin.hosts=redpanda-shadow:9644 | grep -q $LINK"

docker compose stop order-service >/dev/null 2>&1
wait_for "drain on Redpanda for final audit" 60 check drained-redpanda
assert "audit: every order billed exactly once across cutover (no gaps, no dupes)" tools python audit.py
assert "record-by-record: every Confluent record byte-identical on Redpanda at same offset" \
  tools python verify_records.py
assert "orders produced on BOTH clusters (cutover really happened)" \
  bash -c "grep -q CONFLUENT state/produced.log && grep -q REDPANDA state/produced.log"

assert "Redpanda group offsets advanced past Confluent's (not overwritten by sync)" \
  tools python -c "
from checks import admin, group_offsets, SRC, DST
s, d = group_offsets(admin(SRC)), group_offsets(admin(DST))
assert s and all(d[k] > s[k] for k in s), (s, d)"

assert "schema IDs on Redpanda identical to Confluent for all migrated versions" \
  tools python -c "
from scoreboard import schemas, SRC, DST
s, d = schemas(SRC['sr']), schemas(DST['sr'])
assert s and all(d[k]['ids'][:len(v['ids'])] == v['ids'] for k, v in s.items()), (s, d)"

assert "compatibility settings migrated (BACKWARD, FULL_TRANSITIVE)" \
  bash -c "curl -s localhost:28081/config/orders-value | grep -q BACKWARD &&
           curl -s localhost:28081/config/shipping-value | grep -q FULL_TRANSITIVE"

assert "schema references preserved (shipping-value -> address-value)" \
  bash -c "curl -s localhost:28081/subjects/shipping-value/versions/1 | grep -q '\"subject\":\"address-value\"'"

assert "Protobuf + JSON Schema subjects migrated" \
  bash -c "curl -s localhost:28081/subjects/inventory-events-value/versions/1 | grep -q PROTOBUF &&
           curl -s localhost:28081/subjects/warehouse-events-value/versions/1 | grep -q JSON"

assert "new schema on Redpanda continues ID sequence (no collision with migrated IDs)" \
  bash -c "[[ \$(curl -s localhost:28081/subjects/orders-value/versions/latest | sed -n 's/.*\"id\":\([0-9]*\).*/\1/p') -gt 7 ]]"

assert "historical data decodes on Redpanda (shipping uses a schema reference)" \
  bash -c "docker exec redpanda-shadow rpk topic consume shipping -n 1 -o start --use-schema-registry=value | grep -q 'Panda Way'"

assert "customers history fully migrated (50 records + 0 lost)" \
  bash -c "[[ \$(docker exec redpanda-shadow rpk topic describe customers -p | awk 'NR==2{print \$NF}') -ge 50 ]]"

assert "ACLs present on Redpanda" \
  bash -c "docker exec redpanda-shadow rpk security acl list | grep -q billing-service"

# Negative control: the link is gone, so a record written only to Confluent
# must be reported as missing. Proves the verifier can actually fail.
tools python -c "
from confluent_kafka import Producer
p = Producer({'bootstrap.servers': 'confluent-kafka:29092'}); p.produce('customers', b'negative-control'); p.flush(10)" >/dev/null
assert "negative control: verifier detects a record missing on Redpanda" \
  bash -c "! docker exec tools python verify_records.py customers"

assert "Redpanda Console (shadow) up on :8080" curl -sf -o /dev/null localhost:8080
assert "Redpanda Console (source) up on :8088" curl -sf -o /dev/null localhost:8088
assert "demo fits the 5-minute slot when run unattended (${T_DEMO}s < 240s)" test "$T_DEMO" -lt 240

docker compose start order-service >/dev/null 2>&1

say "Result"
printf '  setup %ss · demo %ss · total %ss · log %s\n' "$T_UP" "$T_DEMO" "$((SECONDS - T0))" "$LOG"
if (( FAIL == 0 )); then
  printf '%s\n' "${G}${B}PASS${X}${G} — ${PASS}/${PASS} checks. Demo is ready. Run 'make reset' before going live.${X}"
else
  printf '%s\n' "${R}${B}FAIL${X}${R} — ${FAIL} of $((PASS+FAIL)) checks failed:${X}"
  printf '  - %s\n' "${FAILED[@]}"
  exit 1
fi
