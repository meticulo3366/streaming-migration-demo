#!/usr/bin/env bash
# Live app logs for a second terminal. Survives the cutover (containers are
# recreated when re-pointed at Redpanda) and colors lines by cluster.
cd "$(dirname "$0")/.."
BLUE=$'\033[34m'; RED=$'\033[38;5;196m'; X=$'\033[0m'
while true; do
  docker compose logs -f --tail 3 --no-log-prefix order-service billing-service 2>/dev/null \
    | sed -u -e "s/CONFLUENT/${BLUE}CONFLUENT${X}/" -e "s/REDPANDA/${RED}REDPANDA${X}/"
  sleep 1
done
