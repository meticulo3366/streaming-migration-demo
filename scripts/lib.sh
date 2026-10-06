#!/usr/bin/env bash
# Shared helpers for up.sh / demo.sh / test.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LINK="confluent-migration"
ADMIN="-X admin.hosts=redpanda-shadow:9644"
AUTO="${AUTO:-0}"           # 1 = no pauses (used by test.sh)

if [[ -t 1 ]]; then
  B=$'\033[1m'; D=$'\033[2m'; C=$'\033[36m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; X=$'\033[0m'
  RP=$'\033[38;5;196m'
else
  B=""; D=""; C=""; G=""; Y=""; R=""; X=""; RP=""
fi

say()   { printf '\n%s\n' "${B}$*${X}"; }
note()  { printf '%s\n' "${D}$*${X}"; }
ok()    { printf '%s\n' "${G}✔ $*${X}"; }
warn()  { printf '%s\n' "${Y}! $*${X}"; }
die()   { printf '%s\n' "${R}✘ $*${X}" >&2; exit 1; }

# Section banner with the talk-track line for the presenter.
act() {
  local n="$1" title="$2" line="$3"
  printf '\n%s\n' "${RP}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${X}"
  printf '%s\n' "${B}${RP}ACT ${n}${X}${B}  ${title}${X}"
  printf '%s\n' "${D}“${line}”${X}"
  printf '%s\n' "${RP}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${X}"
}

pause() {
  [[ "$AUTO" == "1" ]] && return 0
  printf '\n%s' "${D}⏎  press Enter to continue${X}"
  read -r _ </dev/tty || true
}

# Show the command the audience should see, then run it.
show() { printf '%s\n' "${C}\$ $*${X}"; }
run()  { show "$@"; eval "$@"; }

rpk()   { docker exec redpanda-shadow rpk "$@" $ADMIN; }
rpk_i() { docker exec -i redpanda-shadow rpk "$@" $ADMIN; }
tools() { docker exec tools "$@"; }

# wait_for "<description>" <timeout-seconds> <command...>
wait_for() {
  local desc="$1" timeout="$2"; shift 2
  local start=$SECONDS
  printf '%s' "${D}  waiting: ${desc} ${X}"
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS - start >= timeout )); then
      printf '\n'; die "timed out after ${timeout}s: ${desc}"
    fi
    printf '%s' "${D}.${X}"; sleep 1
  done
  if [[ -t 1 ]]; then printf '\r\033[K'; else printf ' '; fi
  printf '%s\n' "${G}  ✔ ${desc} ($((SECONDS - start))s)${X}"
}

check() { tools python checks.py "$@"; }

link_topic_state() {  # link_topic_state <topic> -> ACTIVE | FAILED_OVER | ...
  rpk shadow status "$LINK" 2>/dev/null | sed -n "s/^Name: $1, State: \([A-Z_]*\).*/\1/p"
}

sr_post() {  # sr_post <base-url> <subject> <json-body>  -> prints HTTP status
  curl -s -o /dev/null -w "%{http_code}" -X POST "$1/subjects/$2/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" -d "$3"
}

# Re-point the orders apps at a cluster: "confluent" or "redpanda".
point_apps_at() {
  if [[ "$1" == "redpanda" ]]; then
    export ORDERS_BOOTSTRAP=redpanda-shadow:9092 ORDERS_SR=http://redpanda-shadow:8081 ORDERS_CLUSTER=redpanda
  else
    unset ORDERS_BOOTSTRAP ORDERS_SR ORDERS_CLUSTER
  fi
  docker compose up -d --no-deps order-service billing-service >/dev/null 2>&1
}
