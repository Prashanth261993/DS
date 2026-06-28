# FluxTape developer Makefile. Requires: docker, go, cargo, npm (+ make).
# On Windows without make, use ./dev.ps1.

KAFKA ?= fluxtape-redpanda
PG    ?= fluxtape-timescaledb

.PHONY: up down reset schema topics init ingestion processor bar-sink trade-sink api web build test

## --- infra ---
up:            ## start backing services
	docker compose up -d
down:          ## stop services (keep data)
	docker compose down
reset:         ## stop and wipe data volumes
	docker compose down -v
stop-services: ## kill local service processes (not infra)
	-pkill -f services/ingestion || true
	-pkill -f services/processor || true
	-pkill -f services/bar-sink || true
	-pkill -f services/trade-sink || true
	-pkill -f services/api || true
	-pkill -f "vite" || true

schema:        ## apply DB schema
	@docker exec -i $(PG) psql -U fluxtape -d fluxtape < infra/db/schema.sql
topics:        ## create kafka topics
	-docker exec $(KAFKA) rpk topic create trades -p 3
	-docker exec $(KAFKA) rpk topic create bars_1s -p 3
init: up schema topics   ## first-time setup

## --- services (run each in its own terminal) ---
ingestion:
	cargo run --manifest-path services/ingestion/Cargo.toml
processor:
	cd services/processor && go run .
bar-sink:
	cd services/bar-sink && go run .
trade-sink:
	cd services/trade-sink && go run .
api:
	cd services/api && go run .
web:
	cd web && npm run dev

## --- build (used for Phase 4b deploy) ---
build:         ## build all binaries + web bundle
	cargo build --release --manifest-path services/ingestion/Cargo.toml
	cd services/processor && go build -o ../../bin/processor .
	cd services/bar-sink && go build -o ../../bin/bar-sink .
	cd services/trade-sink && go build -o ../../bin/trade-sink .
	cd services/api && go build -o ../../bin/api .
	cd web && npm ci && npm run build
test:
	cd services/processor && go test ./...

## --- deploy (Phase 4b: fly.io + cloudflare) ---
deploy-fly:    ## deploy all services to fly.io (run fly auth login first)
	cd services/ingestion && fly deploy
	cd services/processor && fly deploy
	cd services/bar-sink && fly deploy
	cd services/trade-sink && fly deploy
	cd services/api && fly deploy
deploy-web:    ## build + deploy web to cloudflare pages
	cd web && npm ci && npm run build && npx wrangler pages deploy dist --project-name fluxtape
prod-up:       ## build+run full stack on a VM (Oracle always-free)
	docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
prod-down:
	docker compose -f docker-compose.yml -f docker-compose.prod.yml down
