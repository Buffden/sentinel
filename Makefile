.DEFAULT_GOAL := help

.PHONY: up down reset ps logs migrate topics neo4j-schema help

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

migrate: ## Apply database migrations in order (run after make up)
	@bash infra/scripts/migrate.sh

topics: ## Provision canonical Kafka topics (run after make up)
	@bash infra/kafka/topics.sh

neo4j-schema: ## Apply canonical Neo4j constraints and indexes (run after make up)
	@bash infra/neo4j/apply-schema.sh

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
