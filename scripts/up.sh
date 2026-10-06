#!/usr/bin/env bash
# Stage the "before" world: a Confluent Platform estate with live apps, and an
# empty, Enterprise-licensed Redpanda cluster waiting next to it.
# Idempotent. `--fresh` wipes everything first.
source "$(dirname "$0")/lib.sh"

if [[ "${1:-}" == "--fresh" ]]; then
  say "Resetting to a clean slate..."
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf state
fi
mkdir -p state

say "Preflight"
command -v docker >/dev/null || die "docker not found"
docker info >/dev/null 2>&1 || die "Docker is not running"
docker compose version >/dev/null 2>&1 || die "docker compose v2 required"
command -v curl >/dev/null || die "curl not found"
ok "docker, compose, curl"

LICENSE="license/redpanda.license"
if [[ -s "$LICENSE" ]]; then
  exp=$(cut -d. -f1 "$LICENSE" | base64 -d 2>/dev/null | sed -n 's/.*"expiry":\([0-9]*\).*/\1/p' || true)
  if [[ -n "$exp" && "$exp" -lt "$(date +%s)" ]]; then
    warn "License in $LICENSE has EXPIRED — falling back to Redpanda's built-in 30-day trial"
  else
    ok "Enterprise license found ($LICENSE)"
  fi
else
  warn "No $LICENSE — Shadowing will run on Redpanda's built-in 30-day evaluation"
fi

say "Starting Confluent Platform + Redpanda (images are cached after first run)..."
docker compose up -d --build --wait \
  confluent-kafka confluent-schema-registry redpanda-shadow redpanda-console source-console tools
ok "clusters healthy"

if [[ -s "$LICENSE" ]]; then
  docker exec redpanda-shadow rpk cluster license set --path /license/redpanda.license >/dev/null
  org=$(docker exec redpanda-shadow rpk cluster license info | sed -n 's/^Organization: *//p')
  ok "Redpanda Enterprise license applied (${org})"
fi

say "Seeding the Confluent estate (schemas, topics, history, ACLs)..."
tools python seed.py | sed 's/^/  /'

say "Starting live applications on Confluent..."
point_apps_at confluent
wait_for "order-service producing + billing-service consuming" 60 check drained
sleep 5   # build a little history so the backfill is visible
ok "apps live on Confluent"

cat <<EOF

${B}Ready.${X}  Open these before the call:
  Redpanda Console (shadow):   http://localhost:8080
  Console on Confluent source: http://localhost:8088
  Live app logs:               make logs     (second terminal)
Then run:                      make demo
EOF
