package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// Registry represents a Schema Registry client
type Registry struct {
	baseURL    string
	httpClient *http.Client
	cache      *SchemaCache
	mu         sync.RWMutex
}

// SchemaCache caches schemas by ID and subject-version
type SchemaCache struct {
	byID      map[int]*Schema
	bySubject map[string]map[int]*Schema // subject -> version -> schema
	mu        sync.RWMutex
}

// Schema represents a schema
type Schema struct {
	ID         int       `json:"id"`
	Subject    string    `json:"subject"`
	Version    int       `json:"version"`
	Schema     string    `json:"schema"`
	SchemaType string    `json:"schemaType"` // AVRO, JSON, PROTOBUF
	References []SchemaReference `json:"references,omitempty"`
	CachedAt   time.Time `json:"-"`
}

// SchemaReference represents a schema reference
type SchemaReference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// RegistryConfig contains Schema Registry configuration
type RegistryConfig struct {
	URL         string
	Timeout     time.Duration
	CacheTTL    time.Duration
	BasicAuth   *BasicAuth
}

// BasicAuth contains basic authentication credentials
type BasicAuth struct {
	Username string
	Password string
}

// NewRegistry creates a new Schema Registry client
func NewRegistry(config RegistryConfig) *Registry {
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = 5 * time.Minute
	}

	return &Registry{
		baseURL: config.URL,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
		cache: &SchemaCache{
			byID:      make(map[int]*Schema),
			bySubject: make(map[string]map[int]*Schema),
		},
	}
}

// GetSchemaByID retrieves a schema by its ID
func (r *Registry) GetSchemaByID(schemaID int) (*Schema, error) {
	// Check cache first
	r.cache.mu.RLock()
	if schema, exists := r.cache.byID[schemaID]; exists {
		if time.Since(schema.CachedAt) < 5*time.Minute {
			r.cache.mu.RUnlock()
			return schema, nil
		}
	}
	r.cache.mu.RUnlock()

	// Fetch from registry
	url := fmt.Sprintf("%s/schemas/ids/%d", r.baseURL, schemaID)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error: %s", string(body))
	}

	var result struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode schema: %w", err)
	}

	schema := &Schema{
		ID:         schemaID,
		Schema:     result.Schema,
		SchemaType: result.SchemaType,
		CachedAt:   time.Now(),
	}

	// Cache the schema
	r.cache.mu.Lock()
	r.cache.byID[schemaID] = schema
	r.cache.mu.Unlock()

	monitoring.Debug("Schema fetched from registry",
		zap.Int("schema_id", schemaID),
		zap.String("type", schema.SchemaType),
	)

	return schema, nil
}

// GetLatestSchema retrieves the latest schema version for a subject
func (r *Registry) GetLatestSchema(subject string) (*Schema, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", r.baseURL, subject)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error: %s", string(body))
	}

	var schema Schema
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, fmt.Errorf("failed to decode schema: %w", err)
	}

	schema.Subject = subject
	schema.CachedAt = time.Now()

	// Cache the schema
	r.cache.mu.Lock()
	r.cache.byID[schema.ID] = &schema
	if r.cache.bySubject[subject] == nil {
		r.cache.bySubject[subject] = make(map[int]*Schema)
	}
	r.cache.bySubject[subject][schema.Version] = &schema
	r.cache.mu.Unlock()

	monitoring.Debug("Latest schema fetched",
		zap.String("subject", subject),
		zap.Int("version", schema.Version),
		zap.Int("id", schema.ID),
	)

	return &schema, nil
}

// GetSchemaByVersion retrieves a specific version of a schema
func (r *Registry) GetSchemaByVersion(subject string, version int) (*Schema, error) {
	// Check cache first
	r.cache.mu.RLock()
	if versions, exists := r.cache.bySubject[subject]; exists {
		if schema, exists := versions[version]; exists {
			if time.Since(schema.CachedAt) < 5*time.Minute {
				r.cache.mu.RUnlock()
				return schema, nil
			}
		}
	}
	r.cache.mu.RUnlock()

	url := fmt.Sprintf("%s/subjects/%s/versions/%d", r.baseURL, subject, version)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error: %s", string(body))
	}

	var schema Schema
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, fmt.Errorf("failed to decode schema: %w", err)
	}

	schema.Subject = subject
	schema.CachedAt = time.Now()

	// Cache the schema
	r.cache.mu.Lock()
	r.cache.byID[schema.ID] = &schema
	if r.cache.bySubject[subject] == nil {
		r.cache.bySubject[subject] = make(map[int]*Schema)
	}
	r.cache.bySubject[subject][version] = &schema
	r.cache.mu.Unlock()

	return &schema, nil
}

// RegisterSchema registers a new schema
func (r *Registry) RegisterSchema(subject string, schema string, schemaType string) (*Schema, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", r.baseURL, subject)

	payload := map[string]interface{}{
		"schema":     schema,
		"schemaType": schemaType,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal schema: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("schema registry error: %s", string(respBody))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	monitoring.Info("Schema registered",
		zap.String("subject", subject),
		zap.Int("id", result.ID),
	)

	// Fetch the full schema details
	return r.GetSchemaByID(result.ID)
}

// CheckCompatibility checks if a schema is compatible with the subject
func (r *Registry) CheckCompatibility(subject string, schema string, schemaType string) (bool, error) {
	url := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest", r.baseURL, subject)

	payload := map[string]interface{}{
		"schema":     schema,
		"schemaType": schemaType,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal schema: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to check compatibility: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.IsCompatible, nil
}

// ListSubjects lists all subjects in the registry
func (r *Registry) ListSubjects() ([]string, error) {
	url := fmt.Sprintf("%s/subjects", r.baseURL)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to list subjects: %w", err)
	}
	defer resp.Body.Close()

	var subjects []string
	if err := json.NewDecoder(resp.Body).Decode(&subjects); err != nil {
		return nil, fmt.Errorf("failed to decode subjects: %w", err)
	}

	return subjects, nil
}

// DeleteSubject deletes a subject and all its versions
func (r *Registry) DeleteSubject(subject string) error {
	url := fmt.Sprintf("%s/subjects/%s", r.baseURL, subject)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete subject: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("schema registry error: %s", string(body))
	}

	// Remove from cache
	r.cache.mu.Lock()
	delete(r.cache.bySubject, subject)
	r.cache.mu.Unlock()

	monitoring.Info("Subject deleted", zap.String("subject", subject))

	return nil
}

// ClearCache clears the schema cache
func (r *Registry) ClearCache() {
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()

	r.cache.byID = make(map[int]*Schema)
	r.cache.bySubject = make(map[string]map[int]*Schema)

	monitoring.Info("Schema cache cleared")
}
