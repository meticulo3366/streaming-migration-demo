.DEFAULT_GOAL := help
.PHONY: help up demo reset test test-loop logs status scoreboard verify audit open down

help: ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36mmake %-10s\033[0m %s\n", $$1, $$2}'

up: ## Stage the "before" world: Confluent + live apps + empty licensed Redpanda (~1 min)
	@scripts/up.sh

demo: ## Run the 5-minute guided demo (Enter advances each step)
	@scripts/demo.sh

reset: ## Wipe everything and re-stage from scratch (run between demos)
	@scripts/up.sh --fresh

test: ## Fresh env + unattended demo + assertions. Exit 0 = safe to present
	@scripts/test.sh

N ?= 3
test-loop: ## Soak test: N back-to-back fresh runs (make test-loop N=5)
	@for i in $$(seq 1 $(N)); do echo "=== run $$i/$(N) ==="; scripts/test.sh || exit 1; done; echo "All $(N) runs passed."

logs: ## Tail live app logs, colored by cluster (use a second terminal)
	@scripts/logs.sh

status: ## Shadow link status
	@docker exec redpanda-shadow rpk shadow status confluent-migration -X admin.hosts=redpanda-shadow:9644

scoreboard: ## Side-by-side Confluent vs Redpanda comparison
	@docker exec tools python scoreboard.py --live

verify: ## Record-by-record proof: SHA-256 of every record on both clusters
	@docker exec tools python verify_records.py

audit: ## App-level proof: every order produced was billed exactly once
	@docker exec tools python audit.py

open: ## Open both Consoles in the browser
	@open http://localhost:8088 http://localhost:8080 2>/dev/null || xdg-open http://localhost:8080

down: ## Stop and remove everything
	@docker compose down -v --remove-orphans
	@rm -rf state
