# Trader Intelligence - Docker Workflow

SHELL := /bin/bash
COMPOSE := docker compose
DB_CONTAINER := base-intel-db
DB_USER := trader
DB_NAME := base_mainnet

.PHONY: up down restart status logs schema psql run help

help:
	@echo "Common commands:"
	@echo "  make up       - start Postgres via Docker"
	@echo "  make schema   - apply database/schema1.sql into the container"
	@echo "  make run      - run the pipeline with .env"
	@echo "  make status   - show compose services"
	@echo "  make logs     - tail Postgres logs"
	@echo "  make psql     - open psql shell in the DB container"
	@echo "  make down     - stop containers"

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

status:
	$(COMPOSE) ps

logs:
	docker logs -f $(DB_CONTAINER)

schema:
	docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME) < database/schema1.sql

psql:
	docker exec -it $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME)

run:
	/bin/bash -lc 'set -a && source .env && set +a && python3 pipeline/base_pipeline_best.py'
