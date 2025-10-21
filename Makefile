.PHONY: build run test clean docker-build docker-run help

BINARY_NAME=kafkatwin
VERSION?=1.0.0
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}"

## help: Display this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'

## build: Build the binary
build:
	@echo "Building ${BINARY_NAME}..."
	go build ${LDFLAGS} -o ${BINARY_NAME} ./cmd/proxy

## run: Run the application
run: build
	@echo "Running ${BINARY_NAME}..."
	./${BINARY_NAME} -config configs/example-config.yaml

## test: Run tests
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.out ./...

## test-coverage: Run tests with coverage report
test-coverage: test
	@echo "Generating coverage report..."
	go tool cover -html=coverage.out -o coverage.html

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f ${BINARY_NAME}
	rm -f coverage.out coverage.html
	go clean

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

## fmt: Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

## lint: Lint code
lint:
	@echo "Linting code..."
	golangci-lint run

## docker-build: Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t ${BINARY_NAME}:${VERSION} -t ${BINARY_NAME}:latest .

## docker-run: Run Docker container
docker-run:
	@echo "Running Docker container..."
	docker run -p 9092:9092 -p 8080:8080 -p 9090:9090 \
		-v $(PWD)/configs/example-config.yaml:/app/config.yaml \
		${BINARY_NAME}:latest

## install: Install the binary
install: build
	@echo "Installing ${BINARY_NAME}..."
	cp ${BINARY_NAME} /usr/local/bin/

## uninstall: Uninstall the binary
uninstall:
	@echo "Uninstalling ${BINARY_NAME}..."
	rm -f /usr/local/bin/${BINARY_NAME}
