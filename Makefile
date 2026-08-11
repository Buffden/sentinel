.DEFAULT_GOAL := help

.PHONY: up down reset ps logs help

up: ## Start all infrastructure services in detached mode
	docker compose up -d

down: ## Stop and remove containers (named volumes are preserved)
	docker compose down

reset: ## WARNING: destroy containers AND all named volumes — all local data is lost
	docker compose down --volumes

ps: ## Show container status and health
	docker compose ps

logs: ## Tail logs; filter by service with SERVICE=<name>  e.g. make logs SERVICE=timescaledb
	docker compose logs -f $(SERVICE)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
