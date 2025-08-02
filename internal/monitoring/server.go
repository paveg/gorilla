package monitoring

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Server provides HTTP endpoints for monitoring DataFrame operations.
type Server struct {
	collector *MetricsCollector
	server    *http.Server
}

// NewMonitoringServer creates a new monitoring server.
func NewMonitoringServer(collector *MetricsCollector, port int) *Server {
	mux := http.NewServeMux()

	server := &Server{
		collector: collector,
		server: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second, //nolint:mnd // Standard timeout value
		},
	}

	// Register endpoints
	mux.HandleFunc("/metrics", server.handleMetrics)
	mux.HandleFunc("/health", server.handleHealth)
	mux.HandleFunc("/dashboard", server.handleDashboard)

	return server
}

// Start starts the monitoring server.
func (ms *Server) Start() error {
	return ms.server.ListenAndServe()
}

// Stop stops the monitoring server.
func (ms *Server) Stop() error {
	return ms.server.Close()
}

// handleMetrics serves the metrics endpoint.
func (ms *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	metrics := ms.collector.GetMetrics()
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
		return
	}
}

// handleHealth serves the health check endpoint.
func (ms *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	response := map[string]interface{}{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"enabled":   ms.collector.IsEnabled(),
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode health status", http.StatusInternalServerError)
		return
	}
}

// handleDashboard serves a simple HTML dashboard.
func (ms *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/html")

	html := generateDashboardHTML(ms.collector)
	if _, err := w.Write([]byte(html)); err != nil {
		http.Error(w, "Failed to write dashboard", http.StatusInternalServerError)
		return
	}
}

// generateDashboardHTML creates a simple HTML dashboard.
//
//nolint:funlen // HTML template generation requires inline content
func generateDashboardHTML(collector *MetricsCollector) string {
	metrics := collector.GetMetrics()
	summary := collector.GetSummary()

	html := `<!DOCTYPE html>
<html>
<head>
    <title>Gorilla DataFrame Monitoring</title>
    <meta charset="UTF-8">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
        .summary { background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }
        .metrics-table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        .metrics-table th, .metrics-table td { 
            border: 1px solid #ddd; 
            padding: 8px; 
            text-align: left; 
        }
        .metrics-table th { background-color: #f2f2f2; }
        .status { padding: 4px 8px; border-radius: 3px; }
        .enabled { background-color: #d4edda; color: #155724; }
        .disabled { background-color: #f8d7da; color: #721c24; }
    </style>
</head>
<body>
    <h1 class="header">Gorilla DataFrame Monitoring Dashboard</h1>
    
    <div class="summary">
        <h2>Summary</h2>
        <p><strong>Status:</strong> <span class="status %s">%s</span></p>
        <p><strong>Total Operations:</strong> %d</p>
        <p><strong>Total Duration:</strong> %v</p>
        <p><strong>Average Duration:</strong> %v</p>
        <p><strong>Total Memory Used:</strong> %d bytes</p>
        <p><strong>Total Rows Processed:</strong> %d</p>
    </div>

    <h2>Recent Operations</h2>
    <table class="metrics-table">
        <thead>
            <tr>
                <th>Operation</th>
                <th>Duration</th>
                <th>Memory Used</th>
                <th>Rows Processed</th>
                <th>Parallel</th>
            </tr>
        </thead>
        <tbody>`

	statusClass := "enabled"
	statusText := "Enabled"
	if !collector.IsEnabled() {
		statusClass = "disabled"
		statusText = "Disabled"
	}

	//nolint:staticcheck // Dynamic HTML template formatting
	html = fmt.Sprintf(html,
		statusClass, statusText,
		summary.TotalOperations,
		summary.TotalDuration,
		summary.AverageDuration,
		summary.TotalMemory,
		summary.TotalRows,
	)

	// Add metrics rows
	for _, metric := range metrics {
		parallelText := "No"
		if metric.Parallel {
			parallelText = "Yes"
		}
		html += fmt.Sprintf(`
            <tr>
                <td>%s</td>
                <td>%v</td>
                <td>%d</td>
                <td>%d</td>
                <td>%s</td>
            </tr>`,
			metric.Operation,
			metric.Duration,
			metric.MemoryUsed,
			metric.RowsProcessed,
			parallelText,
		)
	}

	html += `
        </tbody>
    </table>

    <h2>Operations by Type</h2>
    <ul>`

	for operation, count := range summary.OperationCounts {
		html += fmt.Sprintf(`<li><strong>%s:</strong> %d operations</li>`, operation, count)
	}

	html += `
    </ul>

    <script>
        // Auto-refresh every 30 seconds
        setTimeout(function() {
            window.location.reload();
        }, 30000);
    </script>
</body>
</html>`

	return html
}
