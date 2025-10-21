package monitoring

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	// Request metrics
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafkatwin_requests_total",
			Help: "Total number of requests",
		},
		[]string{"operation", "cluster", "status"},
	)

	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafkatwin_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "cluster"},
	)

	// Produce metrics
	ProduceRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafkatwin_produce_requests_total",
			Help: "Total number of produce requests",
		},
		[]string{"topic", "cluster", "status"},
	)

	ProduceBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafkatwin_produce_bytes_total",
			Help: "Total bytes produced",
		},
		[]string{"topic", "cluster"},
	)

	ProduceLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafkatwin_produce_latency_seconds",
			Help:    "Produce request latency",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"topic", "cluster"},
	)

	// Fetch metrics
	FetchRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafkatwin_fetch_requests_total",
			Help: "Total number of fetch requests",
		},
		[]string{"topic", "cluster", "status"},
	)

	FetchBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafkatwin_fetch_bytes_total",
			Help: "Total bytes fetched",
		},
		[]string{"topic", "cluster"},
	)

	FetchLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafkatwin_fetch_latency_seconds",
			Help:    "Fetch request latency",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"topic", "cluster"},
	)

	// Cluster health metrics
	ClusterHealthStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafkatwin_cluster_health_status",
			Help: "Cluster health status (1=healthy, 0=unhealthy)",
		},
		[]string{"cluster"},
	)

	ClusterConnectionsActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafkatwin_cluster_connections_active",
			Help: "Number of active connections to cluster",
		},
		[]string{"cluster"},
	)

	// Consumer group metrics
	ConsumerGroupMembers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafkatwin_consumer_group_members",
			Help: "Number of members in consumer group",
		},
		[]string{"group"},
	)

	ConsumerGroupRebalanceTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafkatwin_consumer_group_rebalance_total",
			Help: "Total number of rebalances",
		},
		[]string{"group"},
	)

	ConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafkatwin_consumer_lag",
			Help: "Consumer lag",
		},
		[]string{"group", "topic", "partition", "cluster"},
	)

	// Connection metrics
	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "kafkatwin_active_connections",
			Help: "Number of active client connections",
		},
	)

	// Error metrics
	ErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafkatwin_errors_total",
			Help: "Total number of errors",
		},
		[]string{"operation", "cluster", "error_type"},
	)
)

// MetricsServer represents the metrics HTTP server
type MetricsServer struct {
	server *http.Server
	port   int
	path   string
}

// NewMetricsServer creates a new metrics server
func NewMetricsServer(port int, path string) *MetricsServer {
	return &MetricsServer{
		port: port,
		path: path,
	}
}

// Start starts the metrics server
func (m *MetricsServer) Start() error {
	mux := http.NewServeMux()
	mux.Handle(m.path, promhttp.Handler())

	m.server = &http.Server{
		Addr:    formatAddr(m.port),
		Handler: mux,
	}

	GetLogger().Info("Starting metrics server",
		zap.Int("port", m.port),
		zap.String("path", m.path),
	)

	go func() {
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			GetLogger().Error("Metrics server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the metrics server
func (m *MetricsServer) Stop() error {
	if m.server != nil {
		return m.server.Close()
	}
	return nil
}

// RecordProduceRequest records a produce request
func RecordProduceRequest(topic, cluster, status string, bytes int64, duration time.Duration) {
	ProduceRequestsTotal.WithLabelValues(topic, cluster, status).Inc()
	ProduceBytesTotal.WithLabelValues(topic, cluster).Add(float64(bytes))
	ProduceLatency.WithLabelValues(topic, cluster).Observe(duration.Seconds())
}

// RecordFetchRequest records a fetch request
func RecordFetchRequest(topic, cluster, status string, bytes int64, duration time.Duration) {
	FetchRequestsTotal.WithLabelValues(topic, cluster, status).Inc()
	FetchBytesTotal.WithLabelValues(topic, cluster).Add(float64(bytes))
	FetchLatency.WithLabelValues(topic, cluster).Observe(duration.Seconds())
}

// RecordError records an error
func RecordError(operation, cluster, errorType string) {
	ErrorsTotal.WithLabelValues(operation, cluster, errorType).Inc()
}

// UpdateClusterHealth updates cluster health status
func UpdateClusterHealth(cluster string, healthy bool) {
	if healthy {
		ClusterHealthStatus.WithLabelValues(cluster).Set(1)
	} else {
		ClusterHealthStatus.WithLabelValues(cluster).Set(0)
	}
}

// UpdateActiveConnections updates active connections count
func UpdateActiveConnections(delta int) {
	if delta > 0 {
		ActiveConnections.Add(float64(delta))
	} else {
		ActiveConnections.Sub(float64(-delta))
	}
}

func formatAddr(port int) string {
	return fmt.Sprintf(":%d", port)
}
