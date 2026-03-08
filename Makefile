# Makefile for Livestream Assistant Agent
# ============================================
# A scalable event-driven AI Agent pipeline
# ============================================

.PHONY: help setup install env up down restart logs \
        kafka-up kafka-down cdc-up cdc-down streaming-up streaming-down \
        api-start api-cdc-start ksql-init \
        clean clean-volumes status health \
        dev-up dev-down full-up full-down

# Default target
.DEFAULT_GOAL := help

# ============================================
# VARIABLES
# ============================================
DOCKER_COMPOSE := docker compose
PYTHON := python
KSQL_SERVER := http://localhost:8088
KAFKA_CONNECT_URL := http://localhost:8083

# Colors for output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# ============================================
# HELP
# ============================================
help: ## Show this help message
	@echo "$(CYAN)Livestream Assistant Agent - Makefile$(NC)"
	@echo "============================================"
	@echo ""
	@echo "$(GREEN)Quick Start (Full Pipeline):$(NC)"
	@echo "  make setup          - First time setup (install deps + create env files)"
	@echo "  make full-pipeline  - Start the complete pipeline in correct order"
	@echo "  make full-down      - Stop everything"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-18s$(NC) %s\n", $$1, $$2}'

# ============================================
# SETUP & INSTALLATION
# ============================================
setup: env install ## First time setup (create env files + install dependencies)
	@echo "$(GREEN)Setup complete!$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. Edit .env and .env.docker with your configuration"
	@echo "  2. Run 'make full-pipeline' to start everything"

install: ## Install Python dependencies
	@echo "$(CYAN)Installing Python dependencies...$(NC)"
	uv sync

env: ## Create .env files from examples (if not exist)
	@if [ ! -f .env ]; then \
		echo "$(CYAN)Creating .env from .env.example...$(NC)"; \
		cp .env.example .env; \
	else \
		echo "$(YELLOW).env already exists, skipping...$(NC)"; \
	fi
	@if [ ! -f .env.docker ]; then \
		echo "$(CYAN)Creating .env.docker from .env.docker.example...$(NC)"; \
		cp .env.docker.example .env.docker; \
	else \
		echo "$(YELLOW).env.docker already exists, skipping...$(NC)"; \
	fi

# ============================================
# FULL PIPELINE (Recommended Order)
# ============================================
full-pipeline: ## Start complete pipeline in correct order
	@echo "$(CYAN)============================================$(NC)"
	@echo "$(CYAN)Starting Full Pipeline$(NC)"
	@echo "$(CYAN)============================================$(NC)"
	@$(MAKE) kafka-up
	@$(MAKE) cdc-up
	@$(MAKE) wait-kafka-connect
	@echo ""
	@echo "$(GREEN)Infrastructure ready!$(NC)"
	@echo "$(YELLOW)Now start the API servers in separate terminals:$(NC)"
	@echo "  Terminal 1: make api-cdc-start"
	@echo "  Terminal 2: make api-start"
	@echo ""
	@echo "$(YELLOW)Then run:$(NC)"
	@echo "  make streaming-pipeline"

streaming-pipeline: ## Start streaming pipeline (after APIs are running)
	@echo "$(CYAN)Starting streaming pipeline...$(NC)"
	@$(MAKE) ksql-up
	@$(MAKE) wait-ksql
	@$(MAKE) ksql-init
	@sleep 5
	@$(MAKE) streaming-services-up
	@echo "$(GREEN)Streaming pipeline started!$(NC)"

# ============================================
# KAFKA CLUSTER
# ============================================
kafka-up: ## Start Kafka cluster (3 nodes + Schema Registry + UI)
	@echo "$(CYAN)Starting Kafka cluster...$(NC)"
	$(DOCKER_COMPOSE) --profile kafka up -d
	@$(MAKE) wait-kafka

kafka-down: ## Stop Kafka cluster
	@echo "$(CYAN)Stopping Kafka cluster...$(NC)"
	$(DOCKER_COMPOSE) --profile kafka down

wait-kafka: ## Wait for Kafka to be healthy
	@echo "$(CYAN)Waiting for Kafka brokers to be healthy...$(NC)"
	@until docker exec kafka-kraft-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092 > /dev/null 2>&1; do \
		echo "  Waiting for kafka-1..."; \
		sleep 5; \
	done
	@until docker exec kafka-kraft-2 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-2:9092 > /dev/null 2>&1; do \
		echo "  Waiting for kafka-2..."; \
		sleep 5; \
	done
	@until docker exec kafka-kraft-3 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-3:9092 > /dev/null 2>&1; do \
		echo "  Waiting for kafka-3..."; \
		sleep 5; \
	done
	@echo "$(GREEN)Kafka cluster is ready!$(NC)"

# ============================================
# CDC PIPELINE
# ============================================
cdc-up: ## Start CDC pipeline (PostgreSQL + Kafka Connect + Debezium)
	@echo "$(CYAN)Starting CDC pipeline...$(NC)"
	$(DOCKER_COMPOSE) --profile cdc up -d

cdc-down: ## Stop CDC pipeline
	@echo "$(CYAN)Stopping CDC pipeline...$(NC)"
	$(DOCKER_COMPOSE) --profile cdc down

wait-kafka-connect: ## Wait for Kafka Connect to be healthy
	@echo "$(CYAN)Waiting for Kafka Connect to be healthy...$(NC)"
	@until curl -s -o /dev/null -w "%{http_code}" $(KAFKA_CONNECT_URL)/connectors | grep -q "200"; do \
		echo "  Waiting for Kafka Connect..."; \
		sleep 5; \
	done
	@echo "$(GREEN)Kafka Connect is ready!$(NC)"

# ============================================
# API SERVICES (Run in separate terminals)
# ============================================
api-start: ## Start Feature & Sentiment API (port 8000)
	@echo "$(CYAN)Starting Feature & Sentiment API on port 8000...$(NC)"
	$(PYTHON) main.py

api-cdc-start: ## Start CDC Producer API (port 8001)
	@echo "$(CYAN)Starting CDC Producer API on port 8001...$(NC)"
	$(PYTHON) main_cdc.py

# ============================================
# KSQLDB
# ============================================
ksql-up: ## Start ksqlDB server
	@echo "$(CYAN)Starting ksqlDB server...$(NC)"
	$(DOCKER_COMPOSE) up -d ksqldb-server

ksql-down: ## Stop ksqlDB server
	@echo "$(CYAN)Stopping ksqlDB server...$(NC)"
	$(DOCKER_COMPOSE) stop ksqldb-server

wait-ksql: ## Wait for ksqlDB to be healthy
	@echo "$(CYAN)Waiting for ksqlDB to be healthy...$(NC)"
	@until curl -s $(KSQL_SERVER)/info > /dev/null 2>&1; do \
		echo "  Waiting for ksqlDB..."; \
		sleep 5; \
	done
	@echo "$(GREEN)ksqlDB is ready!$(NC)"

ksql-init: ## Initialize ksqlDB streams and tables
	@echo "$(CYAN)Initializing ksqlDB...$(NC)"
	./src/streaming/ksql/init_ksql.sh $(KSQL_SERVER)

ksql-cli: ## Open ksqlDB CLI
	@echo "$(CYAN)Opening ksqlDB CLI...$(NC)"
	$(DOCKER_COMPOSE) --profile streaming-cli run --rm ksqldb-cli ksql $(KSQL_SERVER)

# ============================================
# STREAMING SERVICES
# ============================================
streaming-up: ## Start all streaming services (ksqlDB + enrichment + LLM)
	@echo "$(CYAN)Starting streaming services...$(NC)"
	$(DOCKER_COMPOSE) --profile streaming up -d

streaming-down: ## Stop all streaming services
	@echo "$(CYAN)Stopping streaming services...$(NC)"
	$(DOCKER_COMPOSE) --profile streaming down

streaming-services-up: ## Start streaming enrichment and LLM services only
	@echo "$(CYAN)Starting streaming enrichment and LLM services...$(NC)"
	$(DOCKER_COMPOSE) up -d streaming-enrichment streaming-llm-insight

# ============================================
# DEVELOPMENT MODE (Lightweight)
# ============================================
dev-up: ## Start minimal dev stack (single Kafka + Redis)
	@echo "$(CYAN)Starting development stack...$(NC)"
	$(DOCKER_COMPOSE) --profile dev up -d

dev-down: ## Stop development stack
	@echo "$(CYAN)Stopping development stack...$(NC)"
	$(DOCKER_COMPOSE) --profile dev down

# ============================================
# FULL STACK (All at once - use with caution)
# ============================================
full-up: ## Start everything (may have timing issues)
	@echo "$(YELLOW)Warning: This starts all services at once.$(NC)"
	@echo "$(YELLOW)For proper startup order, use 'make full-pipeline' instead.$(NC)"
	$(DOCKER_COMPOSE) --profile full up -d

full-down: ## Stop all services
	@echo "$(CYAN)Stopping all services...$(NC)"
	$(DOCKER_COMPOSE) --profile kafka down
	$(DOCKER_COMPOSE) --profile cdc down
	$(DOCKER_COMPOSE) --profile streaming down
	$(DOCKER_COMPOSE) --profile dev down

# ============================================
# MONITORING & STATUS
# ============================================
status: ## Show status of all containers
	@echo "$(CYAN)Container Status:$(NC)"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(kafka|redis|postgres|ksql|streaming|debezium|schema)" || echo "No containers running"

health: ## Check health of all services
	@echo "$(CYAN)Service Health Check:$(NC)"
	@echo ""
	@echo "Kafka Brokers:"
	@curl -s http://localhost:19092 > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) kafka-1 (19092)" || echo "  $(RED)✗$(NC) kafka-1 (19092)"
	@curl -s http://localhost:29092 > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) kafka-2 (29092)" || echo "  $(RED)✗$(NC) kafka-2 (29092)"
	@curl -s http://localhost:39092 > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) kafka-3 (39092)" || echo "  $(RED)✗$(NC) kafka-3 (39092)"
	@echo ""
	@echo "Services:"
	@curl -s http://localhost:8081/subjects > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) Schema Registry (8081)" || echo "  $(RED)✗$(NC) Schema Registry (8081)"
	@curl -s http://localhost:8080 > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) Kafka UI (8080)" || echo "  $(RED)✗$(NC) Kafka UI (8080)"
	@curl -s http://localhost:8083/connectors > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) Kafka Connect (8083)" || echo "  $(RED)✗$(NC) Kafka Connect (8083)"
	@curl -s http://localhost:8088/info > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) ksqlDB (8088)" || echo "  $(RED)✗$(NC) ksqlDB (8088)"
	@echo ""
	@echo "APIs:"
	@curl -s http://localhost:8000/health > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) Feature API (8000)" || echo "  $(RED)✗$(NC) Feature API (8000)"
	@curl -s http://localhost:8001/health > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) CDC API (8001)" || echo "  $(RED)✗$(NC) CDC API (8001)"
	@echo ""
	@echo "Databases:"
	@docker exec cdc-postgresql pg_isready -U cdc > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) PostgreSQL (5432)" || echo "  $(RED)✗$(NC) PostgreSQL (5432)"
	@docker exec online-store-redis redis-cli ping > /dev/null 2>&1 && echo "  $(GREEN)✓$(NC) Redis (6379)" || echo "  $(RED)✗$(NC) Redis (6379)"

logs: ## Show logs for all containers
	$(DOCKER_COMPOSE) --profile kafka --profile cdc --profile streaming logs -f

logs-kafka: ## Show Kafka logs
	$(DOCKER_COMPOSE) logs -f kafka-1 kafka-2 kafka-3

logs-streaming: ## Show streaming service logs
	$(DOCKER_COMPOSE) logs -f streaming-enrichment streaming-llm-insight

logs-cdc: ## Show CDC logs
	$(DOCKER_COMPOSE) logs -f kafka-connect cdc-producer

# ============================================
# CLEANUP
# ============================================
clean: ## Stop all containers and remove orphans
	@echo "$(CYAN)Cleaning up containers...$(NC)"
	$(DOCKER_COMPOSE) --profile kafka down --remove-orphans
	$(DOCKER_COMPOSE) --profile cdc down --remove-orphans
	$(DOCKER_COMPOSE) --profile streaming down --remove-orphans
	$(DOCKER_COMPOSE) --profile dev down --remove-orphans

clean-volumes: ## Remove all volumes (WARNING: deletes all data)
	@echo "$(RED)WARNING: This will delete all data!$(NC)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	$(DOCKER_COMPOSE) --profile kafka down -v
	$(DOCKER_COMPOSE) --profile cdc down -v
	$(DOCKER_COMPOSE) --profile streaming down -v
	$(DOCKER_COMPOSE) --profile dev down -v
	@echo "$(GREEN)All volumes removed.$(NC)"

# ============================================
# UTILITY TARGETS
# ============================================
topics-list: ## List all Kafka topics
	@echo "$(CYAN)Kafka Topics:$(NC)"
	@docker exec kafka-kraft-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list

topics-create: ## Create required topics manually
	@echo "$(CYAN)Creating topics...$(NC)"
	@docker exec kafka-kraft-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic streaming.enriched_events --partitions 3 --replication-factor 3 --if-not-exists
	@docker exec kafka-kraft-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic streaming.combined_stats --partitions 3 --replication-factor 3 --if-not-exists
	@docker exec kafka-kraft-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --topic streaming.llm_insights --partitions 3 --replication-factor 3 --if-not-exists
	@echo "$(GREEN)Topics created!$(NC)"

connectors-status: ## Show Debezium connector status
	@echo "$(CYAN)Kafka Connect Connectors:$(NC)"
	@curl -s $(KAFKA_CONNECT_URL)/connectors | python3 -m json.tool 2>/dev/null || echo "Kafka Connect not available"
