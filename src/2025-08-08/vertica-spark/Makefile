# Use .env for defaults
include .env
export

.PHONY: help up down reset ps logs logs-vertica logs-spark sh-vertica sh-spark vsql spark-jars init-logs clean

help: ## Show targets
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## / - /'

up: ## Start all services
	docker compose up -d

down: ## Stop services (keep data)
	docker compose down

reset: ## Stop and remove volumes (DESTROYS DB)
	docker compose down -v

ps: ## List containers
	docker compose ps

logs: ## Tail all logs
	docker compose logs -f

logs-vertica: ## Tail Vertica logs
	docker compose logs -f vertica

logs-spark: ## Tail Spark logs
	docker compose logs -f spark

init-logs: ## Tail init logs
	docker compose logs -f vertica-init

sh-vertica: ## Shell into Vertica container
	docker exec -it vertica bash

sh-spark: ## Shell into Spark container
	docker exec -it spark bash

vsql: ## Open vsql on VMart
	docker exec -it vertica vsql -U dbadmin -d $(VERTICA_DB)

spark-jars: ## List jars mounted in Spark
	docker exec -it spark ls -l /opt/spark/jars-extra

clean: ## Remove __pycache__, .ipynb_checkpoints, etc.
	find . -name "__pycache__" -type d -prune -exec rm -rf {} \; || true
	find . -name ".ipynb_checkpoints" -type d -prune -exec rm -rf {} \; || true

