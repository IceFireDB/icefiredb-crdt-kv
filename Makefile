# Makefile for IceFireDB CRDT-KV

# Variables
BINARY_NAME := icefiredb-crdt-kv
EXAMPLE_BINARY := kvdb-example
BUILD_DIR := ./build
EXAMPLE_DIR := ./example/kvdb

# Go parameters
GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOGET := $(GOCMD) get
GOMOD := $(GOCMD) mod

# Default target
all: build test

# Build the main library
build:
	@echo "Building library..."
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) ./...

# Build the example application
example:
	@echo "Building example application..."
	mkdir -p $(BUILD_DIR)
	cd $(EXAMPLE_DIR) && $(GOBUILD) -o ../../$(BUILD_DIR)/$(EXAMPLE_BINARY)

# Run all tests
test:
	@echo "Running all tests..."
	$(GOTEST) -v -race ./...

# Run unit tests only (skip integration tests)
test-unit:
	@echo "Running unit tests..."
	$(GOTEST) -v -race -run "TestCRDTKeyValueDB_Mock|TestCRDTKeyValueDB_Fast|TestPrintVal|TestMainFunctionExists|TestPubSubHandleType|TestGenerateCID|TestNewP2P|TestP2P_Close|TestP2P_MultipleInstances|TestP2P_NetworkOperations|TestPubSubHandleType_String|TestP2P_ContextCancellation" ./...

# Run integration tests only
test-integration:
	@echo "Running integration tests..."
	$(GOTEST) -v -race -run "Integration|MultiNode|Concurrent" ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run tests with coverage and show summary
test-coverage-summary:
	@echo "Running tests with coverage summary..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -func=coverage.out

# Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

# Install dependencies
deps:
	@echo "Installing dependencies..."
	$(GOMOD) tidy

# Format code
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

# Vet code
vet:
	@echo "Vetting code..."
	$(GOCMD) vet ./...

# Lint and check code quality
lint: fmt vet

# Run the example application
run-example: example
	@echo "Running example application..."
	./$(BUILD_DIR)/$(EXAMPLE_BINARY)

# Build and run the example
build-run: example run-example

# Show help
help:
	@echo "Available targets:"
	@echo "  all                    - Build and test everything"
	@echo "  build                  - Build the main library"
	@echo "  example                - Build the example application"
	@echo "  test                   - Run all tests with race detection"
	@echo "  test-unit              - Run unit tests only (skip integration)"
	@echo "  test-integration       - Run integration tests only"
	@echo "  test-coverage          - Run tests with HTML coverage report"
	@echo "  test-coverage-summary  - Run tests with coverage summary"
	@echo "  clean                  - Clean build artifacts"
	@echo "  deps                   - Install dependencies"
	@echo "  fmt                    - Format code"
	@echo "  vet                    - Vet code"
	@echo "  lint                   - Lint and check code quality"
	@echo "  run-example            - Run the example application"
	@echo "  build-run              - Build and run the example"
	@echo "  help                   - Show this help message"

.PHONY: all build example test test-coverage test-coverage-summary clean deps fmt vet lint run-example build-run help