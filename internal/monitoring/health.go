package monitoring

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// HealthStatus represents the health status
type HealthStatus string

const (
	StatusHealthy   HealthStatus = "healthy"
	StatusUnhealthy HealthStatus = "unhealthy"
	StatusDegraded  HealthStatus = "degraded"
)

// HealthCheck represents a health check result
type HealthCheck struct {
	Name      string       `json:"name"`
	Status    HealthStatus `json:"status"`
	Message   string       `json:"message,omitempty"`
	Timestamp time.Time    `json:"timestamp"`
}

// HealthResponse represents the overall health response
type HealthResponse struct {
	Status HealthStatus  `json:"status"`
	Checks []HealthCheck `json:"checks"`
}

// HealthChecker interface for components that can be health checked
type HealthChecker interface {
	HealthCheck() HealthCheck
}

// HealthServer represents the health check HTTP server
type HealthServer struct {
	server   *http.Server
	port     int
	checkers map[string]HealthChecker
	mu       sync.RWMutex
}

// NewHealthServer creates a new health server
func NewHealthServer(port int) *HealthServer {
	return &HealthServer{
		port:     port,
		checkers: make(map[string]HealthChecker),
	}
}

// RegisterChecker registers a health checker
func (h *HealthServer) RegisterChecker(name string, checker HealthChecker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checkers[name] = checker
}

// Start starts the health server
func (h *HealthServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", h.handleLiveness)
	mux.HandleFunc("/health/ready", h.handleReadiness)
	mux.HandleFunc("/health", h.handleHealth)

	h.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", h.port),
		Handler: mux,
	}

	GetLogger().Info("Starting health server", zap.Int("port", h.port))

	go func() {
		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			GetLogger().Error("Health server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the health server
func (h *HealthServer) Stop() error {
	if h.server != nil {
		return h.server.Close()
	}
	return nil
}

// handleLiveness handles liveness probe
func (h *HealthServer) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleReadiness handles readiness probe
func (h *HealthServer) handleReadiness(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	response := h.performHealthChecks()

	w.Header().Set("Content-Type", "application/json")

	if response.Status == StatusHealthy {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(response)
}

// handleHealth handles detailed health check
func (h *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	response := h.performHealthChecks()

	w.Header().Set("Content-Type", "application/json")

	if response.Status == StatusHealthy {
		w.WriteHeader(http.StatusOK)
	} else if response.Status == StatusDegraded {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	json.NewEncoder(w).Encode(response)
}

// performHealthChecks performs all registered health checks
func (h *HealthServer) performHealthChecks() HealthResponse {
	checks := make([]HealthCheck, 0, len(h.checkers))
	overallStatus := StatusHealthy
	unhealthyCount := 0

	for _, checker := range h.checkers {
		check := checker.HealthCheck()
		checks = append(checks, check)

		if check.Status == StatusUnhealthy {
			unhealthyCount++
		}
	}

	// Determine overall status
	if unhealthyCount == len(h.checkers) && len(h.checkers) > 0 {
		overallStatus = StatusUnhealthy
	} else if unhealthyCount > 0 {
		overallStatus = StatusDegraded
	}

	return HealthResponse{
		Status: overallStatus,
		Checks: checks,
	}
}
