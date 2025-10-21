package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the main configuration structure
type Config struct {
	Server         ServerConfig         `yaml:"server"`
	Clusters       []ClusterConfig      `yaml:"clusters"`
	Producer       ProducerConfig       `yaml:"producer"`
	Consumer       ConsumerConfig       `yaml:"consumer"`
	Security       SecurityConfig       `yaml:"security"`
	Monitoring     MonitoringConfig     `yaml:"monitoring"`
	Performance    PerformanceConfig    `yaml:"performance"`
	SchemaRegistry SchemaRegistryConfig `yaml:"schema_registry"`
	Transformation TransformationConfig `yaml:"transformation"`
	RateLimit      RateLimitConfig      `yaml:"rate_limit"`
	AdminAPI       AdminAPIConfig       `yaml:"admin_api"`
	MultiDC        MultiDCConfig        `yaml:"multi_dc"`
}

// ServerConfig contains proxy server settings
type ServerConfig struct {
	ListenAddress string    `yaml:"listen_address"`
	Port          int       `yaml:"port"`
	TLS           TLSConfig `yaml:"tls"`
}

// ClusterConfig contains backend Kafka cluster settings
type ClusterConfig struct {
	ID              string        `yaml:"id"`
	BootstrapServers []string     `yaml:"bootstrap_servers"`
	Priority        int           `yaml:"priority"`
	Weight          int           `yaml:"weight"`
	Timeout         TimeoutConfig `yaml:"timeout"`
	Security        ClusterSecurityConfig `yaml:"security"`
}

// TimeoutConfig contains timeout settings
type TimeoutConfig struct {
	Connection time.Duration `yaml:"connection"`
	Request    time.Duration `yaml:"request"`
	Metadata   time.Duration `yaml:"metadata"`
}

// ClusterSecurityConfig contains cluster-specific security settings
type ClusterSecurityConfig struct {
	Protocol string            `yaml:"protocol"` // PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
	SASL     SASLConfig        `yaml:"sasl"`
	TLS      TLSConfig         `yaml:"tls"`
}

// SASLConfig contains SASL authentication settings
type SASLConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Mechanism string `yaml:"mechanism"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

// TLSConfig contains TLS settings
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	CAFile             string `yaml:"ca_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// ProducerConfig contains producer-specific settings
type ProducerConfig struct {
	AckPolicy       string        `yaml:"ack_policy"` // ALL_CLUSTERS, MAJORITY, ANY, QUORUM
	QuorumCount     int           `yaml:"quorum_count"`
	Timeout         time.Duration `yaml:"timeout"`
	MaxRetries      int           `yaml:"max_retries"`
	RetryBackoff    time.Duration `yaml:"retry_backoff"`
	EnableIdempotency bool        `yaml:"enable_idempotency"`
	MaxInFlight     int           `yaml:"max_in_flight"`
}

// ConsumerConfig contains consumer-specific settings
type ConsumerConfig struct {
	MaxPollRecords  int           `yaml:"max_poll_records"`
	FetchMinBytes   int           `yaml:"fetch_min_bytes"`
	FetchMaxBytes   int           `yaml:"fetch_max_bytes"`
	FetchMaxWait    time.Duration `yaml:"fetch_max_wait"`
	SessionTimeout  time.Duration `yaml:"session_timeout"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	OffsetStorage   OffsetStorageConfig `yaml:"offset_storage"`
}

// OffsetStorageConfig contains offset storage settings
type OffsetStorageConfig struct {
	Type     string            `yaml:"type"` // memory, redis, etcd, kafka
	Servers  []string          `yaml:"servers"`
	Topic    string            `yaml:"topic"` // for kafka type
	Database int               `yaml:"database"` // for redis type
	Prefix   string            `yaml:"prefix"`
}

// SecurityConfig contains proxy security settings
type SecurityConfig struct {
	Authentication AuthenticationConfig `yaml:"authentication"`
	Authorization  AuthorizationConfig  `yaml:"authorization"`
	TLS            TLSConfig            `yaml:"tls"`
	AuditLog       AuditLogConfig       `yaml:"audit_log"`
}

// AuthenticationConfig contains authentication settings
type AuthenticationConfig struct {
	Enabled   bool       `yaml:"enabled"`
	Mechanism string     `yaml:"mechanism"` // SASL, mTLS
	SASL      SASLConfig `yaml:"sasl"`
}

// AuthorizationConfig contains authorization settings
type AuthorizationConfig struct {
	Enabled bool   `yaml:"enabled"`
	Type    string `yaml:"type"` // simple, acl
	ACLFile string `yaml:"acl_file"`
}

// AuditLogConfig contains audit log settings
type AuditLogConfig struct {
	Enabled bool   `yaml:"enabled"`
	File    string `yaml:"file"`
}

// MonitoringConfig contains monitoring settings
type MonitoringConfig struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Logging LoggingConfig `yaml:"logging"`
	Tracing TracingConfig `yaml:"tracing"`
	Health  HealthConfig  `yaml:"health"`
}

// MetricsConfig contains metrics settings
type MetricsConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Port     int    `yaml:"port"`
	Path     string `yaml:"path"`
	Provider string `yaml:"provider"` // prometheus, statsd
}

// LoggingConfig contains logging settings
type LoggingConfig struct {
	Level      string `yaml:"level"` // DEBUG, INFO, WARN, ERROR, FATAL
	Format     string `yaml:"format"` // json, text
	Output     string `yaml:"output"` // stdout, file
	File       string `yaml:"file"`
	MaxSize    int    `yaml:"max_size"`    // megabytes
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`     // days
	Sampling   SamplingConfig `yaml:"sampling"`
}

// SamplingConfig contains log sampling settings
type SamplingConfig struct {
	Enabled bool `yaml:"enabled"`
	Initial int  `yaml:"initial"`
	Thereafter int `yaml:"thereafter"`
}

// TracingConfig contains tracing settings
type TracingConfig struct {
	Enabled  bool    `yaml:"enabled"`
	Provider string  `yaml:"provider"` // jaeger, zipkin, otlp
	Endpoint string  `yaml:"endpoint"`
	SampleRate float64 `yaml:"sample_rate"`
}

// HealthConfig contains health check settings
type HealthConfig struct {
	Enabled       bool          `yaml:"enabled"`
	Port          int           `yaml:"port"`
	Interval      time.Duration `yaml:"interval"`
	Timeout       time.Duration `yaml:"timeout"`
	UnhealthyThreshold int      `yaml:"unhealthy_threshold"`
	HealthyThreshold   int      `yaml:"healthy_threshold"`
}

// PerformanceConfig contains performance tuning settings
type PerformanceConfig struct {
	ConnectionPool ConnectionPoolConfig `yaml:"connection_pool"`
	Cache          CacheConfig          `yaml:"cache"`
	Batching       BatchingConfig       `yaml:"batching"`
	Workers        WorkerConfig         `yaml:"workers"`
}

// ConnectionPoolConfig contains connection pool settings
type ConnectionPoolConfig struct {
	MinConnections int           `yaml:"min_connections"`
	MaxConnections int           `yaml:"max_connections"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`
	MaxLifetime    time.Duration `yaml:"max_lifetime"`
}

// CacheConfig contains cache settings
type CacheConfig struct {
	MetadataTTL time.Duration `yaml:"metadata_ttl"`
	OffsetTTL   time.Duration `yaml:"offset_ttl"`
	MaxSize     int           `yaml:"max_size"` // entries
}

// BatchingConfig contains batching settings
type BatchingConfig struct {
	Enabled    bool          `yaml:"enabled"`
	MaxSize    int           `yaml:"max_size"`
	LingerTime time.Duration `yaml:"linger_time"`
}

// WorkerConfig contains worker pool settings
type WorkerConfig struct {
	MinWorkers int `yaml:"min_workers"`
	MaxWorkers int `yaml:"max_workers"`
}

// SchemaRegistryConfig contains Schema Registry settings
type SchemaRegistryConfig struct {
	Enabled  bool          `yaml:"enabled"`
	URL      string        `yaml:"url"`
	Timeout  time.Duration `yaml:"timeout"`
	CacheTTL time.Duration `yaml:"cache_ttl"`
	Auth     struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"auth"`
}

// TransformationConfig contains message transformation settings
type TransformationConfig struct {
	Enabled bool `yaml:"enabled"`
}

// RateLimitConfig contains rate limiting settings
type RateLimitConfig struct {
	Enabled                  bool          `yaml:"enabled"`
	DefaultRequestsPerSecond int64         `yaml:"default_requests_per_second"`
	DefaultBytesPerSecond    int64         `yaml:"default_bytes_per_second"`
	BurstSize                int64         `yaml:"burst_size"`
	QuotaCheckInterval       time.Duration `yaml:"quota_check_interval"`
}

// AdminAPIConfig contains Admin API settings
type AdminAPIConfig struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"`
	WebUI   bool   `yaml:"web_ui"`
}

// MultiDCConfig contains multi-datacenter settings
type MultiDCConfig struct {
	Enabled             bool   `yaml:"enabled"`
	Strategy            string `yaml:"strategy"` // active-active, active-passive, regional-active, preferred
	LocalDC             string `yaml:"local_dc"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	LatencyThreshold    time.Duration `yaml:"latency_threshold"`
	PreferLocalReads    bool          `yaml:"prefer_local_reads"`
	Datacenters         []DatacenterConfig `yaml:"datacenters"`
}

// DatacenterConfig contains datacenter configuration
type DatacenterConfig struct {
	ID         string   `yaml:"id"`
	Name       string   `yaml:"name"`
	Region     string   `yaml:"region"`
	Priority   int      `yaml:"priority"`
	ClusterIDs []string `yaml:"cluster_ids"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Apply environment variable overrides
	applyEnvOverrides(&config)

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	if len(c.Clusters) == 0 {
		return fmt.Errorf("at least one cluster must be configured")
	}

	for i, cluster := range c.Clusters {
		if cluster.ID == "" {
			return fmt.Errorf("cluster[%d]: ID is required", i)
		}
		if len(cluster.BootstrapServers) == 0 {
			return fmt.Errorf("cluster[%d]: bootstrap_servers is required", i)
		}
	}

	return nil
}

// applyEnvOverrides applies environment variable overrides to configuration
func applyEnvOverrides(config *Config) {
	if val := os.Getenv("PROXY_LISTEN_ADDRESS"); val != "" {
		config.Server.ListenAddress = val
	}
	if val := os.Getenv("PROXY_PORT"); val != "" {
		var port int
		if _, err := fmt.Sscanf(val, "%d", &port); err == nil {
			config.Server.Port = port
		}
	}
	if val := os.Getenv("LOG_LEVEL"); val != "" {
		config.Monitoring.Logging.Level = val
	}
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddress: "0.0.0.0",
			Port:          9092,
			TLS: TLSConfig{
				Enabled: false,
			},
		},
		Producer: ProducerConfig{
			AckPolicy:         "MAJORITY",
			QuorumCount:       2,
			Timeout:           30 * time.Second,
			MaxRetries:        3,
			RetryBackoff:      100 * time.Millisecond,
			EnableIdempotency: false,
			MaxInFlight:       5,
		},
		Consumer: ConsumerConfig{
			MaxPollRecords:    500,
			FetchMinBytes:     1,
			FetchMaxBytes:     52428800, // 50MB
			FetchMaxWait:      500 * time.Millisecond,
			SessionTimeout:    10 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			OffsetStorage: OffsetStorageConfig{
				Type:   "memory",
				Prefix: "kafkatwin",
			},
		},
		Security: SecurityConfig{
			Authentication: AuthenticationConfig{
				Enabled: false,
			},
			Authorization: AuthorizationConfig{
				Enabled: false,
			},
			TLS: TLSConfig{
				Enabled: false,
			},
			AuditLog: AuditLogConfig{
				Enabled: false,
			},
		},
		Monitoring: MonitoringConfig{
			Metrics: MetricsConfig{
				Enabled:  true,
				Port:     9090,
				Path:     "/metrics",
				Provider: "prometheus",
			},
			Logging: LoggingConfig{
				Level:      "INFO",
				Format:     "json",
				Output:     "stdout",
				MaxSize:    100,
				MaxBackups: 3,
				MaxAge:     7,
				Sampling: SamplingConfig{
					Enabled:    false,
					Initial:    100,
					Thereafter: 100,
				},
			},
			Tracing: TracingConfig{
				Enabled:    false,
				SampleRate: 0.1,
			},
			Health: HealthConfig{
				Enabled:            true,
				Port:               8080,
				Interval:           10 * time.Second,
				Timeout:            5 * time.Second,
				UnhealthyThreshold: 3,
				HealthyThreshold:   2,
			},
		},
		Performance: PerformanceConfig{
			ConnectionPool: ConnectionPoolConfig{
				MinConnections: 2,
				MaxConnections: 10,
				IdleTimeout:    5 * time.Minute,
				MaxLifetime:    30 * time.Minute,
			},
			Cache: CacheConfig{
				MetadataTTL: 5 * time.Minute,
				OffsetTTL:   1 * time.Minute,
				MaxSize:     10000,
			},
			Batching: BatchingConfig{
				Enabled:    false,
				MaxSize:    1000,
				LingerTime: 10 * time.Millisecond,
			},
			Workers: WorkerConfig{
				MinWorkers: 10,
				MaxWorkers: 100,
			},
		},
	}
}
