package config

import (
	"os"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Server.Port != 9092 {
		t.Errorf("Expected default port 9092, got %d", cfg.Server.Port)
	}

	if cfg.Producer.AckPolicy != "MAJORITY" {
		t.Errorf("Expected default ack policy MAJORITY, got %s", cfg.Producer.AckPolicy)
	}

	if cfg.Consumer.MaxPollRecords != 500 {
		t.Errorf("Expected default max poll records 500, got %d", cfg.Consumer.MaxPollRecords)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct{
		name      string
		cfg       *Config
		shouldErr bool
	}{
		{
			name: "Valid config",
			cfg: &Config{
				Server: ServerConfig{Port: 9092},
				Clusters: []ClusterConfig{
					{
						ID:               "cluster1",
						BootstrapServers: []string{"localhost:9092"},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "Invalid port",
			cfg: &Config{
				Server: ServerConfig{Port: 99999},
				Clusters: []ClusterConfig{
					{
						ID:               "cluster1",
						BootstrapServers: []string{"localhost:9092"},
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "No clusters",
			cfg: &Config{
				Server:   ServerConfig{Port: 9092},
				Clusters: []ClusterConfig{},
			},
			shouldErr: true,
		},
		{
			name: "Cluster without ID",
			cfg: &Config{
				Server: ServerConfig{Port: 9092},
				Clusters: []ClusterConfig{
					{
						ID:               "",
						BootstrapServers: []string{"localhost:9092"},
					},
				},
			},
			shouldErr: true,
		},
		{
			name: "Cluster without bootstrap servers",
			cfg: &Config{
				Server: ServerConfig{Port: 9092},
				Clusters: []ClusterConfig{
					{
						ID:               "cluster1",
						BootstrapServers: []string{},
					},
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.shouldErr && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.shouldErr && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configContent := `
server:
  listen_address: "0.0.0.0"
  port: 9092

clusters:
  - id: "cluster-1"
    bootstrap_servers:
      - "kafka1:9092"
    priority: 100
    weight: 100

producer:
  ack_policy: "MAJORITY"
  timeout: 30s

consumer:
  max_poll_records: 500
`

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	tmpFile.Close()

	cfg, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.Server.Port != 9092 {
		t.Errorf("Expected port 9092, got %d", cfg.Server.Port)
	}

	if len(cfg.Clusters) != 1 {
		t.Errorf("Expected 1 cluster, got %d", len(cfg.Clusters))
	}

	if cfg.Clusters[0].ID != "cluster-1" {
		t.Errorf("Expected cluster ID cluster-1, got %s", cfg.Clusters[0].ID)
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	// Set environment variables
	os.Setenv("PROXY_PORT", "19092")
	os.Setenv("LOG_LEVEL", "DEBUG")
	defer func() {
		os.Unsetenv("PROXY_PORT")
		os.Unsetenv("LOG_LEVEL")
	}()

	cfg := &Config{
		Server: ServerConfig{
			Port: 9092,
		},
		Monitoring: MonitoringConfig{
			Logging: LoggingConfig{
				Level: "INFO",
			},
		},
	}

	applyEnvOverrides(cfg)

	if cfg.Server.Port != 19092 {
		t.Errorf("Expected port to be overridden to 19092, got %d", cfg.Server.Port)
	}

	if cfg.Monitoring.Logging.Level != "DEBUG" {
		t.Errorf("Expected log level to be overridden to DEBUG, got %s", cfg.Monitoring.Logging.Level)
	}
}

func TestTimeoutConfig(t *testing.T) {
	timeout := TimeoutConfig{
		Connection: 30 * time.Second,
		Request:    10 * time.Second,
		Metadata:   5 * time.Second,
	}

	if timeout.Connection != 30*time.Second {
		t.Errorf("Expected connection timeout 30s, got %v", timeout.Connection)
	}
}

func TestProducerConfig(t *testing.T) {
	cfg := ProducerConfig{
		AckPolicy:         "QUORUM",
		QuorumCount:       3,
		Timeout:           30 * time.Second,
		MaxRetries:        5,
		RetryBackoff:      100 * time.Millisecond,
		EnableIdempotency: true,
		MaxInFlight:       1,
	}

	if cfg.AckPolicy != "QUORUM" {
		t.Errorf("Expected QUORUM ack policy, got %s", cfg.AckPolicy)
	}

	if cfg.QuorumCount != 3 {
		t.Errorf("Expected quorum count 3, got %d", cfg.QuorumCount)
	}

	if !cfg.EnableIdempotency {
		t.Error("Expected idempotency to be enabled")
	}
}

func TestConsumerConfig(t *testing.T) {
	cfg := ConsumerConfig{
		MaxPollRecords:    1000,
		FetchMinBytes:     1,
		FetchMaxBytes:     52428800,
		FetchMaxWait:      500 * time.Millisecond,
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		OffsetStorage: OffsetStorageConfig{
			Type:   "redis",
			Servers: []string{"redis:6379"},
			Database: 0,
			Prefix: "kafkatwin",
		},
	}

	if cfg.OffsetStorage.Type != "redis" {
		t.Errorf("Expected redis offset storage, got %s", cfg.OffsetStorage.Type)
	}

	if cfg.MaxPollRecords != 1000 {
		t.Errorf("Expected max poll records 1000, got %d", cfg.MaxPollRecords)
	}
}
