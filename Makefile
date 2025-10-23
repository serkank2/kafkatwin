.PHONY: help build run test clean docker-build docker-up docker-down docker-logs install-tools lint fmt vet tidy

# Variables
APP_NAME := kafkatwin
VERSION := 2.0.0
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -ldflags "-w -s -X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)"

# Directories
BIN_DIR := bin
CMD_DIR := cmd/proxy
COVERAGE_DIR := coverage

# Colors for output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

## help: Display this help message
help:
	@echo "$(CYAN)KafkaTwin Multi-Cluster Kafka Proxy - Makefile Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Development Commands:$(NC)"
	@echo "  make build           - Build the application binary"
	@echo "  make run             - Run the application locally"
	@echo "  make test            - Run all tests"
	@echo "  make test-coverage   - Run tests with coverage report"
	@echo "  make bench           - Run benchmarks"
	@echo ""
	@echo "$(GREEN)Code Quality:$(NC)"
	@echo "  make lint            - Run golangci-lint"
	@echo "  make fmt             - Format code with gofmt"
	@echo "  make vet             - Run go vet"
	@echo "  make tidy            - Tidy go.mod and go.sum"
	@echo "  make check           - Run all checks (fmt, vet, lint)"
	@echo ""
	@echo "$(GREEN)Docker Commands:$(NC)"
	@echo "  make docker-build    - Build Docker image"
	@echo "  make docker-up       - Start all services with docker-compose"
	@echo "  make docker-down     - Stop all services"
	@echo "  make docker-logs     - View logs from all services"
	@echo "  make docker-clean    - Remove all containers and volumes"
	@echo ""
	@echo "$(GREEN)Utility Commands:$(NC)"
	@echo "  make clean           - Clean build artifacts"
	@echo "  make install-tools   - Install development tools"
	@echo "  make version         - Show version information"
	@echo ""

## build: Build the application binary
build:
	@echo "$(CYAN)Building $(APP_NAME) v$(VERSION)...$(NC)"
	@mkdir -p $(BIN_DIR)
	@go build $(LDFLAGS) -o $(BIN_DIR)/$(APP_NAME) ./$(CMD_DIR)
	@echo "$(GREEN)✓ Build complete: $(BIN_DIR)/$(APP_NAME)$(NC)"

## build-linux: Build for Linux
build-linux:
	@echo "$(CYAN)Building $(APP_NAME) for Linux...$(NC)"
	@mkdir -p $(BIN_DIR)
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BIN_DIR)/$(APP_NAME)-linux-amd64 ./$(CMD_DIR)
	@echo "$(GREEN)✓ Build complete: $(BIN_DIR)/$(APP_NAME)-linux-amd64$(NC)"

## run: Run the application locally
run: build
	@echo "$(CYAN)Running $(APP_NAME)...$(NC)"
	@./$(BIN_DIR)/$(APP_NAME) -config config.yaml

## test: Run all tests
test:
	@echo "$(CYAN)Running tests...$(NC)"
	@go test -v -race ./...
	@echo "$(GREEN)✓ Tests passed$(NC)"

## test-coverage: Run tests with coverage report
test-coverage:
	@echo "$(CYAN)Running tests with coverage...$(NC)"
	@mkdir -p $(COVERAGE_DIR)
	@go test -v -race -coverprofile=$(COVERAGE_DIR)/coverage.out -covermode=atomic ./...
	@go tool cover -html=$(COVERAGE_DIR)/coverage.out -o $(COVERAGE_DIR)/coverage.html
	@go tool cover -func=$(COVERAGE_DIR)/coverage.out
	@echo "$(GREEN)✓ Coverage report: $(COVERAGE_DIR)/coverage.html$(NC)"

## bench: Run benchmarks
bench:
	@echo "$(CYAN)Running benchmarks...$(NC)"
	@go test -bench=. -benchmem ./...

## lint: Run golangci-lint
lint:
	@echo "$(CYAN)Running linter...$(NC)"
	@if command -v golangci-lint > /dev/null; then \
		golangci-lint run ./...; \
		echo "$(GREEN)✓ Linting complete$(NC)"; \
	else \
		echo "$(YELLOW)⚠ golangci-lint not installed. Run 'make install-tools'$(NC)"; \
	fi

## fmt: Format code with gofmt
fmt:
	@echo "$(CYAN)Formatting code...$(NC)"
	@gofmt -w -s .
	@echo "$(GREEN)✓ Formatting complete$(NC)"

## vet: Run go vet
vet:
	@echo "$(CYAN)Running go vet...$(NC)"
	@go vet ./...
	@echo "$(GREEN)✓ Vet complete$(NC)"

## tidy: Tidy go.mod and go.sum
tidy:
	@echo "$(CYAN)Tidying modules...$(NC)"
	@go mod tidy
	@echo "$(GREEN)✓ Tidy complete$(NC)"

## check: Run all checks
check: fmt vet lint
	@echo "$(GREEN)✓ All checks passed$(NC)"

## docker-build: Build Docker image
docker-build:
	@echo "$(CYAN)Building Docker image...$(NC)"
	@docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(APP_NAME):$(VERSION) \
		-t $(APP_NAME):latest \
		.
	@echo "$(GREEN)✓ Docker image built: $(APP_NAME):$(VERSION)$(NC)"

## docker-up: Start all services with docker-compose
docker-up:
	@echo "$(CYAN)Starting services with docker-compose...$(NC)"
	@BUILD_TIME=$(BUILD_TIME) docker-compose up -d
	@echo "$(GREEN)✓ Services started$(NC)"
	@echo "$(YELLOW)Access points:$(NC)"
	@echo "  - KafkaTwin Proxy:    http://localhost:9092"
	@echo "  - Admin Web UI:       http://localhost:8090"
	@echo "  - Health Check:       http://localhost:8080/health"
	@echo "  - Prometheus Metrics: http://localhost:9090/metrics"
	@echo "  - Prometheus UI:      http://localhost:9091"
	@echo "  - Grafana:            http://localhost:3000 (admin/admin)"

## docker-down: Stop all services
docker-down:
	@echo "$(CYAN)Stopping services...$(NC)"
	@docker-compose down
	@echo "$(GREEN)✓ Services stopped$(NC)"

## docker-logs: View logs from all services
docker-logs:
	@docker-compose logs -f

## docker-logs-proxy: View KafkaTwin proxy logs
docker-logs-proxy:
	@docker-compose logs -f kafkatwin

## docker-clean: Remove all containers and volumes
docker-clean:
	@echo "$(CYAN)Cleaning Docker resources...$(NC)"
	@docker-compose down -v --remove-orphans
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

## clean: Clean build artifacts
clean:
	@echo "$(CYAN)Cleaning build artifacts...$(NC)"
	@rm -rf $(BIN_DIR)
	@rm -rf $(COVERAGE_DIR)
	@rm -f *.log
	@echo "$(GREEN)✓ Clean complete$(NC)"

## install-tools: Install development tools
install-tools:
	@echo "$(CYAN)Installing development tools...$(NC)"
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "$(GREEN)✓ Tools installed$(NC)"

## version: Show version information
version:
	@echo "$(CYAN)Version Information:$(NC)"
	@echo "  Version:     $(VERSION)"
	@echo "  Git Commit:  $(GIT_COMMIT)"
	@echo "  Build Time:  $(BUILD_TIME)"

## deps: Download dependencies
deps:
	@echo "$(CYAN)Downloading dependencies...$(NC)"
	@go mod download
	@echo "$(GREEN)✓ Dependencies downloaded$(NC)"

## install: Install the binary
install: build
	@echo "$(CYAN)Installing $(APP_NAME)...$(NC)"
	@cp $(BIN_DIR)/$(APP_NAME) /usr/local/bin/
	@echo "$(GREEN)✓ Installed to /usr/local/bin/$(APP_NAME)$(NC)"

## uninstall: Uninstall the binary
uninstall:
	@echo "$(CYAN)Uninstalling $(APP_NAME)...$(NC)"
	@rm -f /usr/local/bin/$(APP_NAME)
	@echo "$(GREEN)✓ Uninstalled$(NC)"

# Default target
.DEFAULT_GOAL := help
