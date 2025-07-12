# Makefile for the Gorilla project

.PHONY: help build test lint fmt vet run-demo install-tools setup

# Variables
BINARY_NAME=gorilla-cli
CLI_PATH=./cmd/gorilla-cli

# Default target executed when you run `make`
all: help

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  build            Build the ${BINARY_NAME} binary to the project root."
	@echo "  test             Run all Go tests."
	@echo "  lint             Run golangci-lint to check the code."
	@echo "  fmt              Format Go source files with gofmt."
	@echo "  vet              Run go vet to inspect the code."
	@echo "  run-demo         Build and run the CLI with the --demo flag."
	@echo "  install-tools    Install required development tools (lefthook, golangci-lint)."
	@echo "  setup            Install tools and set up git hooks using lefthook."

build:
	@echo "Building ${BINARY_NAME}..."
	@go build -o ${BINARY_NAME} ${CLI_PATH}

test:
	@echo "Running tests..."
	@go test ./...

lint:
	@echo "Running linter..."
	@golangci-lint run

fmt:
	@echo "Formatting code..."
	@gofmt -w .

vet:
	@echo "Running go vet..."
	@go vet ./...

run-demo: build
	@echo "Running demo..."
	@./${BINARY_NAME} --demo

install-tools:
	@echo "Installing Go development tools..."
	@go install github.com/evilmartians/lefthook@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "\nNOTE: Please install markdownlint-cli separately if you haven't already."
	@echo "e.g., 'npm install -g markdownlint-cli' or 'brew install markdownlint-cli'"

setup: install-tools
	@echo "Setting up git hooks with lefthook..."
	@lefthook install
