//nolint:testpackage // requires internal access to unexported types and functions
package monitoring

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMonitoringServer(t *testing.T) {
	t.Run("create monitoring server", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		server := NewMonitoringServer(collector, 8080)

		assert.NotNil(t, server)
		assert.Equal(t, collector, server.collector)
		assert.NotNil(t, server.server)
		assert.Equal(t, ":8080", server.server.Addr)
	})
}

func TestMetricsEndpoint(t *testing.T) {
	t.Run("empty metrics", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		server := NewMonitoringServer(collector, 8080)

		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		w := httptest.NewRecorder()

		server.handleMetrics(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var metrics []OperationMetrics
		err := json.Unmarshal(w.Body.Bytes(), &metrics)
		require.NoError(t, err)
		assert.Empty(t, metrics)
	})

	t.Run("metrics with data", func(t *testing.T) {
		collector := NewMetricsCollector(true)

		// Add some test metrics
		err := collector.RecordOperation("test_op", func() error {
			time.Sleep(1 * time.Millisecond)
			return nil
		})
		require.NoError(t, err)

		server := NewMonitoringServer(collector, 8080)

		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		w := httptest.NewRecorder()

		server.handleMetrics(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var metrics []OperationMetrics
		err = json.Unmarshal(w.Body.Bytes(), &metrics)
		require.NoError(t, err)
		require.Len(t, metrics, 1)
		assert.Equal(t, "test_op", metrics[0].Operation)
	})

	t.Run("invalid method", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		server := NewMonitoringServer(collector, 8080)

		req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
		w := httptest.NewRecorder()

		server.handleMetrics(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}

func TestHealthEndpoint(t *testing.T) {
	t.Run("health check", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		server := NewMonitoringServer(collector, 8080)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()

		server.handleHealth(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, "ok", response["status"])
		assert.NotEmpty(t, response["timestamp"])
	})

	t.Run("invalid method", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		server := NewMonitoringServer(collector, 8080)

		req := httptest.NewRequest(http.MethodDelete, "/health", nil)
		w := httptest.NewRecorder()

		server.handleHealth(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}

func TestDashboardEndpoint(t *testing.T) {
	t.Run("dashboard HTML", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		server := NewMonitoringServer(collector, 8080)

		req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
		w := httptest.NewRecorder()

		server.handleDashboard(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "text/html", w.Header().Get("Content-Type"))
		assert.Contains(t, w.Body.String(), "Gorilla DataFrame Monitoring")
		assert.Contains(t, w.Body.String(), "<html>")
	})

	t.Run("invalid method", func(t *testing.T) {
		collector := NewMetricsCollector(true)
		server := NewMonitoringServer(collector, 8080)

		req := httptest.NewRequest(http.MethodPut, "/dashboard", nil)
		w := httptest.NewRecorder()

		server.handleDashboard(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
	})
}

func TestServerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Run("full server workflow", func(t *testing.T) {
		collector := NewMetricsCollector(true)

		// Record some operations
		operations := []string{"filter", "sort", "groupby"}
		for _, op := range operations {
			err := collector.RecordOperation(op, func() error {
				time.Sleep(1 * time.Millisecond)
				return nil
			})
			require.NoError(t, err)
		}

		server := NewMonitoringServer(collector, 8080)

		// Test metrics endpoint
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		w := httptest.NewRecorder()
		server.handleMetrics(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var metrics []OperationMetrics
		err := json.Unmarshal(w.Body.Bytes(), &metrics)
		require.NoError(t, err)
		assert.Len(t, metrics, 3)

		// Test health endpoint
		req = httptest.NewRequest(http.MethodGet, "/health", nil)
		w = httptest.NewRecorder()
		server.handleHealth(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		// Test dashboard endpoint
		req = httptest.NewRequest(http.MethodGet, "/dashboard", nil)
		w = httptest.NewRecorder()
		server.handleDashboard(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "Gorilla DataFrame Monitoring")
	})
}
