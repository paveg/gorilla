#!/bin/bash

# Coverage script for Gorilla DataFrame Library
# This script runs tests with coverage and generates reports

set -e

echo "🦍 Gorilla DataFrame Library - Coverage Report"
echo "============================================="

# Clean up any existing coverage files
rm -f coverage.out coverage.html unit-coverage.out integration-coverage.out

# Run unit tests with coverage
echo "📊 Running unit tests with coverage..."
go test -race -coverprofile=unit-coverage.out -covermode=atomic ./...

# Run integration tests with coverage if they exist
if go test -list -run "Integration" ./internal/dataframe | grep -q "Test"; then
    echo "🔗 Running integration tests with coverage..."
    go test -v -coverprofile=integration-coverage.out -covermode=atomic ./internal/dataframe -run "Integration"
fi

# Combine coverage files
echo "📈 Combining coverage reports..."
if [ -f "integration-coverage.out" ]; then
    # Use gocovmerge if available, otherwise use the unit coverage
    if command -v gocovmerge &> /dev/null; then
        gocovmerge unit-coverage.out integration-coverage.out > coverage.out
    else
        echo "⚠️  gocovmerge not found, using unit test coverage only"
        echo "   Install with: go install github.com/wadey/gocovmerge@latest"
        cp unit-coverage.out coverage.out
    fi
else
    cp unit-coverage.out coverage.out
fi

# Generate HTML report
echo "🌐 Generating HTML coverage report..."
go tool cover -html=coverage.out -o coverage.html

# Display coverage summary
echo "📋 Coverage Summary:"
echo "=================="
go tool cover -func=coverage.out | tail -1

# Display per-package coverage
echo ""
echo "📦 Per-package coverage:"
echo "======================="
go tool cover -func=coverage.out | grep -E "^(github.com/paveg/gorilla/|total:)" | sort

# Check if coverage meets minimum threshold
COVERAGE=$(go tool cover -func=coverage.out | grep "total:" | awk '{print $3}' | sed 's/%//')
THRESHOLD=80

echo ""
if (( $(echo "$COVERAGE >= $THRESHOLD" | bc -l) )); then
    echo "✅ Coverage ($COVERAGE%) meets minimum threshold ($THRESHOLD%)"
else
    echo "❌ Coverage ($COVERAGE%) below minimum threshold ($THRESHOLD%)"
    echo "   Consider adding more tests to improve coverage"
fi

echo ""
echo "📄 Coverage report generated: coverage.html"
echo "🔍 Open coverage.html in your browser to see detailed coverage"

# Clean up intermediate files
rm -f unit-coverage.out integration-coverage.out

echo ""
echo "🎉 Coverage analysis complete!"