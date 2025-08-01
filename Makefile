# Makefile for the Gorilla project

.PHONY: help build test lint fmt vet run-demo install-tools setup coverage coverage-html

# Variables
BINARY_NAME=gorilla-cli
CLI_PATH=./cmd/gorilla-cli

# Version information
VERSION ?= dev
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_TAG ?= $(shell git describe --tags --exact-match 2>/dev/null || echo "unknown")
GO_VERSION ?= $(shell go version | cut -d' ' -f3)

# Build flags
LDFLAGS := -ldflags "-X github.com/paveg/gorilla/internal/version.Version=$(VERSION) \
	-X github.com/paveg/gorilla/internal/version.BuildDate=$(BUILD_DATE) \
	-X github.com/paveg/gorilla/internal/version.GitCommit=$(GIT_COMMIT) \
	-X github.com/paveg/gorilla/internal/version.GitTag=$(GIT_TAG) \
	-X github.com/paveg/gorilla/internal/version.GoVersion=$(GO_VERSION)"

# Default target executed when you run `make`
all: help

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build            Build the ${BINARY_NAME} binary to the project root."
	@echo "  release          Build a release binary with version information."
	@echo "  test             Run all Go tests."
	@echo "  lint             Run golangci-lint to check the code."
	@echo "  fmt              Format Go source files with gofmt."
	@echo "  vet              Run go vet to inspect the code."
	@echo "  run-demo         Build and run the CLI with the --demo flag."
	@echo "  coverage         Run tests with coverage and generate report."
	@echo "  coverage-html    Run tests with coverage and open HTML report."
	@echo "  install-tools    Install required development tools (lefthook, golangci-lint)."
	@echo "  setup            Install tools and set up git hooks using lefthook."

build:
	@echo "Building ${BINARY_NAME}..."
	@go build -o ${BINARY_NAME} ${CLI_PATH}

release:
	@echo "Building release ${BINARY_NAME} with version information..."
	@go build $(LDFLAGS) -o ${BINARY_NAME} ${CLI_PATH}

test:
	@echo "Running tests..."
	@go test -short ./...

test-all:
	@echo "Running all tests (including long-running tests)..."
	@go test ./...

lint:
	@echo "Running linter..."
	@golangci-lint run ./...

fmt:
	@echo "Formatting code..."
	@gofmt -w .

vet:
	@echo "Running go vet..."
	@go vet ./...

run-demo: build
	@echo "Running demo..."
	@./${BINARY_NAME} --demo

coverage:
	@echo "Running tests with coverage..."
	@./scripts/coverage.sh

coverage-html: coverage
	@echo "Opening coverage report in browser..."
	@if command -v open >/dev/null 2>&1; then \
		open coverage.html; \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open coverage.html; \
	else \
		echo "Please open coverage.html in your browser"; \
	fi

install-tools:
	@echo "Installing Go development tools..."
	@go install github.com/evilmartians/lefthook@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install github.com/wadey/gocovmerge@latest
	@echo "\nNOTE: Please install markdownlint-cli separately if you haven't already."
	@echo "e.g., 'npm install -g markdownlint-cli' or 'brew install markdownlint-cli'"

setup: install-tools
	@echo "Setting up git hooks with lefthook..."
	@lefthook install
