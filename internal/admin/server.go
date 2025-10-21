package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/metadata"
	"github.com/serkank2/kafkatwin/internal/monitoring"
	"github.com/serkank2/kafkatwin/internal/ratelimit"
	"github.com/serkank2/kafkatwin/internal/schema"
	"github.com/serkank2/kafkatwin/internal/transformation"
)

// Server represents the admin API server
type Server struct {
	router              *mux.Router
	server              *http.Server
	clusterManager      *cluster.Manager
	metadataManager     *metadata.Manager
	schemaRegistry      *schema.Registry
	transformEngine     *transformation.Engine
	rateLimiter         *ratelimit.Limiter
	port                int
}

// Config contains admin server configuration
type Config struct {
	Port            int
	EnableAuth      bool
	AllowedOrigins  []string
}

// NewServer creates a new admin API server
func NewServer(
	config Config,
	clusterMgr *cluster.Manager,
	metaMgr *metadata.Manager,
	schemaReg *schema.Registry,
	transformEng *transformation.Engine,
	rateLim *ratelimit.Limiter,
) *Server {
	s := &Server{
		router:          mux.NewRouter(),
		clusterManager:  clusterMgr,
		metadataManager: metaMgr,
		schemaRegistry:  schemaReg,
		transformEngine: transformEng,
		rateLimiter:     rateLim,
		port:            config.Port,
	}

	s.setupRoutes()

	return s
}

// setupRoutes sets up API routes
func (s *Server) setupRoutes() {
	// Enable CORS
	s.router.Use(corsMiddleware)
	s.router.Use(loggingMiddleware)

	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Cluster management
	api.HandleFunc("/clusters", s.listClusters).Methods("GET")
	api.HandleFunc("/clusters/{id}", s.getCluster).Methods("GET")
	api.HandleFunc("/clusters/{id}/health", s.getClusterHealth).Methods("GET")

	// Metadata
	api.HandleFunc("/topics", s.listTopics).Methods("GET")
	api.HandleFunc("/topics/{topic}", s.getTopic).Methods("GET")
	api.HandleFunc("/topics/{topic}/partitions", s.getTopicPartitions).Methods("GET")

	// Schema Registry
	api.HandleFunc("/schemas/subjects", s.listSchemaSubjects).Methods("GET")
	api.HandleFunc("/schemas/subjects/{subject}/versions/latest", s.getLatestSchema).Methods("GET")
	api.HandleFunc("/schemas/subjects/{subject}/versions/{version}", s.getSchemaByVersion).Methods("GET")
	api.HandleFunc("/schemas/subjects/{subject}", s.registerSchema).Methods("POST")
	api.HandleFunc("/schemas/subjects/{subject}", s.deleteSubject).Methods("DELETE")

	// Transformation rules
	api.HandleFunc("/transformations", s.listAllTransformations).Methods("GET")
	api.HandleFunc("/transformations/{topic}", s.listTransformations).Methods("GET")
	api.HandleFunc("/transformations/{topic}", s.addTransformation).Methods("POST")
	api.HandleFunc("/transformations/{topic}/{rule_id}", s.deleteTransformation).Methods("DELETE")

	// Rate limiting & quotas
	api.HandleFunc("/quotas", s.listQuotas).Methods("GET")
	api.HandleFunc("/quotas/{client_id}", s.getQuota).Methods("GET")
	api.HandleFunc("/quotas/{client_id}", s.setQuota).Methods("PUT")
	api.HandleFunc("/quotas/{client_id}", s.deleteQuota).Methods("DELETE")

	// System info
	api.HandleFunc("/info", s.getSystemInfo).Methods("GET")
	api.HandleFunc("/stats", s.getStats).Methods("GET")
}

// Start starts the admin API server
func (s *Server) Start() error {
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}

	monitoring.Info("Starting admin API server", zap.Int("port", s.port))

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			monitoring.Error("Admin API server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the admin API server
func (s *Server) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

// Cluster handlers

func (s *Server) listClusters(w http.ResponseWriter, r *http.Request) {
	clusters := s.clusterManager.GetAllClusters()

	response := make([]map[string]interface{}, 0, len(clusters))
	for _, c := range clusters {
		response = append(response, map[string]interface{}{
			"id":      c.ID,
			"healthy": c.IsHealthy(),
			"config":  c.Config,
		})
	}

	respondJSON(w, http.StatusOK, response)
}

func (s *Server) getCluster(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	clusterID := vars["id"]

	cluster, err := s.clusterManager.GetCluster(clusterID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	response := map[string]interface{}{
		"id":      cluster.ID,
		"healthy": cluster.IsHealthy(),
		"config":  cluster.Config,
		"health":  cluster.Health.GetStats(),
	}

	respondJSON(w, http.StatusOK, response)
}

func (s *Server) getClusterHealth(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	clusterID := vars["id"]

	cluster, err := s.clusterManager.GetCluster(clusterID)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, cluster.Health.GetStats())
}

// Metadata handlers

func (s *Server) listTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.metadataManager.GetAllTopics()
	respondJSON(w, http.StatusOK, topics)
}

func (s *Server) getTopic(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topic := vars["topic"]

	meta, err := s.metadataManager.GetTopicMetadata(topic)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, meta)
}

func (s *Server) getTopicPartitions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topic := vars["topic"]

	meta, err := s.metadataManager.GetTopicMetadata(topic)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, meta.Partitions)
}

// Schema Registry handlers

func (s *Server) listSchemaSubjects(w http.ResponseWriter, r *http.Request) {
	if s.schemaRegistry == nil {
		respondError(w, http.StatusServiceUnavailable, "Schema Registry not configured")
		return
	}

	subjects, err := s.schemaRegistry.ListSubjects()
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, subjects)
}

func (s *Server) getLatestSchema(w http.ResponseWriter, r *http.Request) {
	if s.schemaRegistry == nil {
		respondError(w, http.StatusServiceUnavailable, "Schema Registry not configured")
		return
	}

	vars := mux.Vars(r)
	subject := vars["subject"]

	schema, err := s.schemaRegistry.GetLatestSchema(subject)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, schema)
}

func (s *Server) getSchemaByVersion(w http.ResponseWriter, r *http.Request) {
	if s.schemaRegistry == nil {
		respondError(w, http.StatusServiceUnavailable, "Schema Registry not configured")
		return
	}

	vars := mux.Vars(r)
	subject := vars["subject"]
	versionStr := vars["version"]

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, "Invalid version")
		return
	}

	schema, err := s.schemaRegistry.GetSchemaByVersion(subject, version)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, schema)
}

func (s *Server) registerSchema(w http.ResponseWriter, r *http.Request) {
	if s.schemaRegistry == nil {
		respondError(w, http.StatusServiceUnavailable, "Schema Registry not configured")
		return
	}

	vars := mux.Vars(r)
	subject := vars["subject"]

	var req struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	schema, err := s.schemaRegistry.RegisterSchema(subject, req.Schema, req.SchemaType)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, schema)
}

func (s *Server) deleteSubject(w http.ResponseWriter, r *http.Request) {
	if s.schemaRegistry == nil {
		respondError(w, http.StatusServiceUnavailable, "Schema Registry not configured")
		return
	}

	vars := mux.Vars(r)
	subject := vars["subject"]

	if err := s.schemaRegistry.DeleteSubject(subject); err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"message": "Subject deleted"})
}

// Transformation handlers

func (s *Server) listAllTransformations(w http.ResponseWriter, r *http.Request) {
	if s.transformEngine == nil {
		respondError(w, http.StatusServiceUnavailable, "Transformation engine not configured")
		return
	}

	rules := s.transformEngine.GetAllRules()
	respondJSON(w, http.StatusOK, rules)
}

func (s *Server) listTransformations(w http.ResponseWriter, r *http.Request) {
	if s.transformEngine == nil {
		respondError(w, http.StatusServiceUnavailable, "Transformation engine not configured")
		return
	}

	vars := mux.Vars(r)
	topic := vars["topic"]

	rules := s.transformEngine.GetRules(topic)
	respondJSON(w, http.StatusOK, rules)
}

func (s *Server) addTransformation(w http.ResponseWriter, r *http.Request) {
	if s.transformEngine == nil {
		respondError(w, http.StatusServiceUnavailable, "Transformation engine not configured")
		return
	}

	vars := mux.Vars(r)
	topic := vars["topic"]

	var rule transformation.Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	rule.Topic = topic

	if err := s.transformEngine.AddRule(&rule); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	respondJSON(w, http.StatusCreated, rule)
}

func (s *Server) deleteTransformation(w http.ResponseWriter, r *http.Request) {
	if s.transformEngine == nil {
		respondError(w, http.StatusServiceUnavailable, "Transformation engine not configured")
		return
	}

	vars := mux.Vars(r)
	topic := vars["topic"]
	ruleID := vars["rule_id"]

	if err := s.transformEngine.RemoveRule(topic, ruleID); err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"message": "Transformation rule deleted"})
}

// Quota handlers

func (s *Server) listQuotas(w http.ResponseWriter, r *http.Request) {
	if s.rateLimiter == nil {
		respondError(w, http.StatusServiceUnavailable, "Rate limiter not configured")
		return
	}

	// Note: This would need to be implemented in the rate limiter
	respondJSON(w, http.StatusOK, map[string]interface{}{})
}

func (s *Server) getQuota(w http.ResponseWriter, r *http.Request) {
	if s.rateLimiter == nil {
		respondError(w, http.StatusServiceUnavailable, "Rate limiter not configured")
		return
	}

	vars := mux.Vars(r)
	clientID := vars["client_id"]

	quota := s.rateLimiter.GetQuota(clientID)
	if quota == nil {
		respondError(w, http.StatusNotFound, "Quota not found")
		return
	}

	respondJSON(w, http.StatusOK, quota.GetStats())
}

func (s *Server) setQuota(w http.ResponseWriter, r *http.Request) {
	if s.rateLimiter == nil {
		respondError(w, http.StatusServiceUnavailable, "Rate limiter not configured")
		return
	}

	vars := mux.Vars(r)
	clientID := vars["client_id"]

	var quota ratelimit.Quota
	if err := json.NewDecoder(r.Body).Decode(&quota); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	s.rateLimiter.SetQuota(clientID, &quota)
	respondJSON(w, http.StatusOK, quota.GetStats())
}

func (s *Server) deleteQuota(w http.ResponseWriter, r *http.Request) {
	if s.rateLimiter == nil {
		respondError(w, http.StatusServiceUnavailable, "Rate limiter not configured")
		return
	}

	vars := mux.Vars(r)
	clientID := vars["client_id"]

	s.rateLimiter.RemoveQuota(clientID)
	respondJSON(w, http.StatusOK, map[string]string{"message": "Quota deleted"})
}

// System handlers

func (s *Server) getSystemInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]interface{}{
		"version":   "1.0.0",
		"clusters":  len(s.clusterManager.GetAllClusters()),
		"topics":    len(s.metadataManager.GetAllTopics()),
	}

	respondJSON(w, http.StatusOK, info)
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{
		"metadata": s.metadataManager.GetCacheStats(),
	}

	respondJSON(w, http.StatusOK, stats)
}

// Helper functions

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		monitoring.Debug("Admin API request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
		)

		next.ServeHTTP(w, r)
	})
}
