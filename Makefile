SHELL := /bin/bash
export PATH := $(shell go env GOPATH)/bin:$(PATH)
.PHONY: help up down logs test lint build fmt migrate-up migrate-down migrate-version bench clean

help: ## Display available make targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start PostgreSQL, RabbitMQ, and Redis containers and wait for healthiness
	docker compose up -d --wait

down: ## Stop local infrastructure containers (preserves persistent volumes)
	docker compose down

logs: ## Tail output from local infrastructure containers
	docker compose logs -f

test: ## Run unit tests with race detector
	go test -v -race -p 1 ./...

lint: ## Run static analysis (golangci-lint, go vet, and formatting check)
	@if command -v golangci-lint >/dev/null 2>&1; then \
		echo "Running golangci-lint..."; \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, falling back to go vet and gofmt check..."; \
		go vet ./...; \
		DIFF=$$(gofmt -l .); \
		if [ -n "$$DIFF" ]; then \
			echo "The following files require formatting with 'make fmt':"; \
			echo "$$DIFF"; \
			exit 1; \
		fi; \
		echo "Static checks passed."; \
	fi

build: ## Build all platform executables into ./bin
	@mkdir -p bin
	go build -trimpath -o bin/api ./cmd/api
	go build -trimpath -o bin/worker ./cmd/worker
	go build -trimpath -o bin/outbox-relay ./cmd/outbox-relay
	go build -trimpath -o bin/migrate ./cmd/migrate
	go build -trimpath -o bin/bench ./cmd/bench
	@echo "Built binaries in ./bin:"
	@ls -la bin/

bench: ## Execute the automated performance benchmark harness
	go run ./cmd/bench

fmt: ## Format Go source files with gofmt
	gofmt -s -w .

migrate-up: ## Apply all pending database migrations
	go run ./cmd/migrate up

migrate-down: ## Rollback 1 migration step
	go run ./cmd/migrate down

migrate-version: ## Print current schema migration version
	go run ./cmd/migrate version

clean: ## Clean build artifacts
	rm -rf bin/ coverage.txt coverage.out
