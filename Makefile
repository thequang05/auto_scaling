# =============================================================================
# DATA LAKEHOUSE - MAKEFILE
# =============================================================================
# Automation commands for the Data Lakehouse project
# =============================================================================

.PHONY: help build up down restart logs clean \
        ingest-bronze transform-silver aggregate-gold \
        sync-clickhouse setup-superset pipeline-full \
        spark-shell spark-sql clickhouse-client \
        test lint format

# Default target
.DEFAULT_GOAL := help

# =============================================================================
# VARIABLES
# =============================================================================
DOCKER_COMPOSE = docker-compose -f docker/docker-compose.yml
SPARK_MASTER = spark://spark-master:7077
DATA_DIR = data/raw

# Colors for output
GREEN = \033[0;32m
YELLOW = \033[0;33m
RED = \033[0;31m
NC = \033[0m # No Color

# =============================================================================
# HELP
# =============================================================================
help: ## Show this help message
	@echo ""
	@echo "$(GREEN)╔═══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║          DATA LAKEHOUSE - AUTOMATION COMMANDS                 ║$(NC)"
	@echo "$(GREEN)╚═══════════════════════════════════════════════════════════════╝$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

# =============================================================================
# INFRASTRUCTURE
# =============================================================================
build: ## Build all Docker images
	@echo "$(GREEN)Building Docker images...$(NC)"
	$(DOCKER_COMPOSE) build

up: ## Start all services
	@echo "$(GREEN)Starting all services...$(NC)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)Services started! Waiting for initialization...$(NC)"
	@sleep 30
	@echo ""
	@echo "$(GREEN)╔═══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║                    SERVICES READY                              ║$(NC)"
	@echo "$(GREEN)╠═══════════════════════════════════════════════════════════════╣$(NC)"
	@echo "$(GREEN)║  MinIO Console:     http://localhost:9001                      ║$(NC)"
	@echo "$(GREEN)║  Spark Master UI:   http://localhost:8080                      ║$(NC)"
	@echo "$(GREEN)║  ClickHouse HTTP:   http://localhost:8123                      ║$(NC)"
	@echo "$(GREEN)║  Superset:          http://localhost:8088                      ║$(NC)"
	@echo "$(GREEN)║  Iceberg REST:      http://localhost:8181                      ║$(NC)"
	@echo "$(GREEN)╚═══════════════════════════════════════════════════════════════╝$(NC)"

down: ## Stop all services
	@echo "$(RED)Stopping all services...$(NC)"
	$(DOCKER_COMPOSE) down

restart: down up ## Restart all services

logs: ## Show logs from all services
	$(DOCKER_COMPOSE) logs -f

logs-spark: ## Show Spark logs
	$(DOCKER_COMPOSE) logs -f spark-master spark-worker

logs-clickhouse: ## Show ClickHouse logs
	$(DOCKER_COMPOSE) logs -f clickhouse

logs-superset: ## Show Superset logs
	$(DOCKER_COMPOSE) logs -f superset

status: ## Show status of all services
	$(DOCKER_COMPOSE) ps

clean: ## Remove all containers, volumes, and data
	@echo "$(RED)Cleaning up...$(NC)"
	$(DOCKER_COMPOSE) down -v --remove-orphans
	docker volume rm lakehouse-minio-data lakehouse-spark-logs \
		lakehouse-clickhouse-data lakehouse-clickhouse-logs \
		lakehouse-superset-db lakehouse-superset-cache \
		lakehouse-superset-home 2>/dev/null || true
	@echo "$(GREEN)Cleanup complete!$(NC)"

# =============================================================================
# DATA PIPELINE
# =============================================================================
ingest-bronze: ## Ingest raw data to Bronze Layer
	@echo "$(GREEN)Ingesting data to Bronze Layer...$(NC)"
	docker exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		/opt/spark-apps/jobs/bronze/ingest_events.py
	@echo "$(GREEN)Bronze Layer ingestion complete!$(NC)"

transform-silver: ## Transform Bronze → Silver Layer
	@echo "$(GREEN)Transforming data to Silver Layer...$(NC)"
	docker exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		/opt/spark-apps/jobs/silver/clean_events.py
	@echo "$(GREEN)Silver Layer transformation complete!$(NC)"

aggregate-gold: ## Aggregate Silver → Gold Layer
	@echo "$(GREEN)Aggregating data to Gold Layer...$(NC)"
	docker exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		/opt/spark-apps/jobs/gold/aggregate_sales.py
	@echo "$(GREEN)Gold Layer aggregation complete!$(NC)"

sync-clickhouse: ## Sync Gold data to ClickHouse
	@echo "$(GREEN)Syncing data to ClickHouse...$(NC)"
	docker exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		--packages com.github.housepower:clickhouse-spark-runtime-3.4_2.12:0.7.3 \
		/opt/spark-apps/jobs/serving/sync_to_clickhouse.py
	@echo "$(GREEN)ClickHouse sync complete!$(NC)"

demo-schema-evolution: ## Demo Schema Evolution feature
	@echo "$(GREEN)Running Schema Evolution demo...$(NC)"
	docker exec spark-master spark-submit \
		--master $(SPARK_MASTER) \
		/opt/spark-apps/jobs/bronze/schema_evolution_demo.py

pipeline-full: ingest-bronze transform-silver aggregate-gold sync-clickhouse ## Run full data pipeline
	@echo "$(GREEN)Full pipeline completed!$(NC)"

# =============================================================================
# DBT
# =============================================================================
dbt-run: ## Run dbt models
	@echo "$(GREEN)Running dbt models...$(NC)"
	cd dbt && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	@echo "$(GREEN)Running dbt tests...$(NC)"
	cd dbt && dbt test --profiles-dir .

dbt-docs: ## Generate dbt documentation
	@echo "$(GREEN)Generating dbt docs...$(NC)"
	cd dbt && dbt docs generate --profiles-dir .
	cd dbt && dbt docs serve --profiles-dir . --port 8001

# =============================================================================
# SUPERSET
# =============================================================================
setup-superset: ## Setup Superset with ClickHouse connection
	@echo "$(GREEN)Setting up Superset...$(NC)"
	pip install requests
	python superset/setup_superset.py

# =============================================================================
# CLICKHOUSE
# =============================================================================
clickhouse-migrate: ## Run ClickHouse migrations
	@echo "$(GREEN)Running ClickHouse migrations...$(NC)"
	docker exec -i clickhouse clickhouse-client \
		--password clickhouse123 \
		< clickhouse/migrations/001_create_iceberg_tables.sql
	@echo "$(GREEN)Migrations complete!$(NC)"

clickhouse-client: ## Open ClickHouse client
	docker exec -it clickhouse clickhouse-client --password clickhouse123

# =============================================================================
# SPARK
# =============================================================================
spark-shell: ## Open Spark shell (Scala)
	docker exec -it spark-master spark-shell \
		--master $(SPARK_MASTER)

spark-pyspark: ## Open PySpark shell
	docker exec -it spark-master pyspark \
		--master $(SPARK_MASTER)

spark-sql: ## Open Spark SQL shell
	docker exec -it spark-master spark-sql \
		--master $(SPARK_MASTER)

# =============================================================================
# DATA MANAGEMENT
# =============================================================================
download-data: ## Download sample dataset from Kaggle
	@echo "$(GREEN)Downloading e-commerce dataset...$(NC)"
	@echo "Please download the dataset manually from:"
	@echo "https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop"
	@echo ""
	@echo "Then place the CSV files in: $(DATA_DIR)/"
	@mkdir -p $(DATA_DIR)

upload-data: ## Upload raw data to MinIO
	@echo "$(GREEN)Uploading data to MinIO...$(NC)"
	docker exec minio-setup mc cp --recursive /opt/data/raw/ minio/lakehouse-bronze/raw/

# =============================================================================
# DEVELOPMENT
# =============================================================================
lint: ## Run linters
	@echo "$(GREEN)Running linters...$(NC)"
	python -m pylint spark/jobs/
	python -m black --check spark/jobs/

format: ## Format code
	@echo "$(GREEN)Formatting code...$(NC)"
	python -m black spark/jobs/

test: ## Run tests
	@echo "$(GREEN)Running tests...$(NC)"
	python -m pytest tests/

# =============================================================================
# QUICK START
# =============================================================================
quickstart: build up ## Quick start: build and run all services
	@echo ""
	@echo "$(GREEN)Quickstart complete! Services are running.$(NC)"
	@echo "Next steps:"
	@echo "  1. Download data:     make download-data"
	@echo "  2. Run pipeline:      make pipeline-full"
	@echo "  3. Setup Superset:    make setup-superset"
	@echo ""

demo: quickstart pipeline-full setup-superset ## Run complete demo
	@echo ""
	@echo "$(GREEN)╔═══════════════════════════════════════════════════════════════╗$(NC)"
	@echo "$(GREEN)║                    DEMO COMPLETE!                              ║$(NC)"
	@echo "$(GREEN)╠═══════════════════════════════════════════════════════════════╣$(NC)"
	@echo "$(GREEN)║  Access Superset at: http://localhost:8088                     ║$(NC)"
	@echo "$(GREEN)║  Username: admin    Password: admin                            ║$(NC)"
	@echo "$(GREEN)╚═══════════════════════════════════════════════════════════════╝$(NC)"
