.PHONY: build run dev stop clean test help

# Default target
help: ## Show this help
	@echo ""
	@echo "  Mini-Kafka — Distributed Log Broker"
	@echo ""
	@echo "  Usage:"
	@echo "    make run         Build and run everything via Docker Compose"
	@echo "    make stop        Stop all containers"
	@echo "    make dev         Run broker + dashboard locally (no Docker)"
	@echo "    make build       Build Go binary + Next.js production build"
	@echo "    make test        Run Go tests"
	@echo "    make clean       Remove data, containers, and build artifacts"
	@echo ""

# ─── Docker ──────────────────────────────────────────────

run: ## Build and run both services via Docker Compose
	docker-compose up --build

run-detached: ## Build and run in background
	docker-compose up --build -d

stop: ## Stop all containers
	docker-compose down

clean: ## Remove containers, volumes, and local data
	docker-compose down -v
	rm -rf broker/data
	rm -rf dashboard/.next dashboard/node_modules

# ─── Local Development ───────────────────────────────────

build-broker: ## Build the Go broker binary
	cd broker && go build -o bin/broker .

build-dashboard: ## Build the Next.js dashboard
	cd dashboard && npm install && npm run build

build: build-broker build-dashboard ## Build everything locally

test: ## Run Go tests
	cd broker && go test -v -race ./...

dev-broker: ## Run broker locally
	cd broker && go run . 

dev-dashboard: ## Run dashboard dev server
	cd dashboard && npm install && npm run dev

dev: ## Run both broker and dashboard locally (requires two terminals)
	@echo ""
	@echo "  Run these in separate terminals:"
	@echo ""
	@echo "    Terminal 1:  make dev-broker"
	@echo "    Terminal 2:  make dev-dashboard"
	@echo ""

# ─── Quick Test ──────────────────────────────────────────

create-test-topic: ## Create a test topic via HTTP API
	curl -s -X POST http://localhost:8080/api/topics \
		-H "Content-Type: application/json" \
		-d '{"name":"test-events","partitions":3}' | python3 -m json.tool

produce-test-message: ## Produce a test message via HTTP API
	curl -s -X POST http://localhost:8080/api/produce \
		-H "Content-Type: application/json" \
		-d '{"topic":"test-events","key":"user-123","value":"order placed at $(shell date)"}' | python3 -m json.tool

smoke-test: create-test-topic ## Run a quick smoke test
	@echo "Producing 10 test messages..."
	@for i in $$(seq 1 10); do \
		curl -s -X POST http://localhost:8080/api/produce \
			-H "Content-Type: application/json" \
			-d "{\"topic\":\"test-events\",\"key\":\"key-$$i\",\"value\":\"message $$i\"}" > /dev/null; \
	done
	@echo "Done. Check dashboard at http://localhost:3000"
	@echo ""
	@curl -s http://localhost:8080/api/stats | python3 -m json.tool
