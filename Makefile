# Data Platform Makefile

.PHONY: help build up down logs clean test status

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all Docker images
	@echo "Building Docker images..."
	docker-compose build

up: ## Start all services
	@echo "Starting data platform..."
	docker-compose up -d
	@echo "Services starting... This may take 2-3 minutes."
	@echo "Access URLs:"
	@echo "  Spark Master UI:  http://localhost:8080"
	@echo "  MinIO Console:    http://localhost:9001"
	@echo "  Spark Worker 1:   http://localhost:8081"
	@echo "  Spark Worker 2:   http://localhost:8082"

down: ## Stop all services
	@echo "Stopping data platform..."
	docker-compose down

logs: ## Show logs for all services
	docker-compose logs -f

clean: ## Stop services and remove volumes
	@echo "Cleaning up data platform..."
	docker-compose down -v
	docker system prune -f

test: ## Run integration tests
	@echo "Running integration tests..."
	./scripts/integration-tests.sh

status: ## Show service status
	@echo "Service Status:"
	docker-compose ps

kafka-topics: ## List Kafka topics
	@echo "Kafka Topics:"
	docker-compose exec kafka-broker-1 kafka-topics.sh --bootstrap-server kafka-broker-1:29092 --list

create-topic: ## Create a test topic (usage: make create-topic TOPIC=my-topic)
	@if [ -z "$(TOPIC)" ]; then echo "Usage: make create-topic TOPIC=topic-name"; exit 1; fi
	docker-compose exec kafka-broker-1 kafka-topics.sh \
		--bootstrap-server kafka-broker-1:29092 \
		--create --if-not-exists \
		--topic $(TOPIC) \
		--partitions 3 \
		--replication-factor 2

produce: ## Start Kafka producer (usage: make produce TOPIC=my-topic)
	@if [ -z "$(TOPIC)" ]; then echo "Usage: make produce TOPIC=topic-name"; exit 1; fi
	docker-compose exec kafka-broker-1 kafka-console-producer.sh \
		--bootstrap-server kafka-broker-1:29092 \
		--topic $(TOPIC)


spark-shell: ## Open Spark shell
	docker-compose exec spark-master spark-shell \
		--master spark://spark-master:7077

spark-submit: ## Submit Spark application (usage: make spark-submit APP=path/to/app.py)
	@if [ -z "$(APP)" ]; then echo "Usage: make spark-submit APP=path/to/app.py"; exit 1; fi
	docker-compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		$(APP)

minio-client: ## Open MinIO client
	docker run --rm -it --network b-data-platform_data-platform \
		--entrypoint /bin/sh \
		minio/mc:latest \
		-c "mc alias set minio http://minio:9000 minioadmin minioadmin123 && /bin/sh"


restart: down up ## Restart all services

rebuild: clean build up ## Clean, rebuild and start

dev: build up logs ## Start development environment