package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/admin"
	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/consumer"
	"github.com/serkank2/kafkatwin/internal/coordination"
	"github.com/serkank2/kafkatwin/internal/metadata"
	"github.com/serkank2/kafkatwin/internal/monitoring"
	"github.com/serkank2/kafkatwin/internal/multidc"
	"github.com/serkank2/kafkatwin/internal/producer"
	"github.com/serkank2/kafkatwin/internal/protocol"
	"github.com/serkank2/kafkatwin/internal/ratelimit"
	"github.com/serkank2/kafkatwin/internal/schema"
	"github.com/serkank2/kafkatwin/internal/transformation"
)

var (
	configFile = flag.String("config", "config.yaml", "Path to configuration file")
	version    = "2.0.0"
	buildTime  = "unknown"
)

func main() {
	flag.Parse()

	fmt.Printf("🔄 KafkaTwin Multi-Cluster Proxy v%s (built %s)\n", version, buildTime)

	// Load configuration
	cfg, err := loadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	if err := monitoring.InitLogger(cfg.Monitoring.Logging); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer monitoring.Sync()

	monitoring.Info("Starting KafkaTwin proxy",
		zap.String("version", version),
		zap.String("build_time", buildTime),
	)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	app, err := initializeApp(cfg)
	if err != nil {
		monitoring.Fatal("Failed to initialize application", zap.Error(err))
	}

	// Start metrics server
	if cfg.Monitoring.Metrics.Enabled {
		metricsServer := monitoring.NewMetricsServer(
			cfg.Monitoring.Metrics.Port,
			cfg.Monitoring.Metrics.Path,
		)
		if err := metricsServer.Start(); err != nil {
			monitoring.Fatal("Failed to start metrics server", zap.Error(err))
		}
		defer metricsServer.Stop()
		monitoring.Info("Metrics server started", zap.Int("port", cfg.Monitoring.Metrics.Port))
	}

	// Start health server
	if cfg.Monitoring.Health.Enabled {
		healthServer := monitoring.NewHealthServer(cfg.Monitoring.Health.Port)

		// Register health checkers
		for _, cluster := range app.ClusterManager.GetAllClusters() {
			healthServer.RegisterChecker(cluster.ID, cluster)
		}

		if err := healthServer.Start(); err != nil {
			monitoring.Fatal("Failed to start health server", zap.Error(err))
		}
		defer healthServer.Stop()
		monitoring.Info("Health server started", zap.Int("port", cfg.Monitoring.Health.Port))
	}

	// Start Admin API server
	if app.AdminServer != nil {
		if err := app.AdminServer.Start(); err != nil {
			monitoring.Fatal("Failed to start admin API server", zap.Error(err))
		}
		defer app.AdminServer.Stop()
		monitoring.Info("Admin API server started", zap.Int("port", cfg.AdminAPI.Port))
	}

	// Start protocol server
	if err := app.ProtocolServer.Start(); err != nil {
		monitoring.Fatal("Failed to start protocol server", zap.Error(err))
	}
	defer app.ProtocolServer.Stop()

	monitoring.Info("KafkaTwin proxy started successfully",
		zap.String("listen_address", cfg.Server.ListenAddress),
		zap.Int("port", cfg.Server.Port),
		zap.Int("clusters", len(cfg.Clusters)),
		zap.Bool("schema_registry", cfg.SchemaRegistry.Enabled),
		zap.Bool("transformations", cfg.Transformation.Enabled),
		zap.Bool("rate_limiting", cfg.RateLimit.Enabled),
		zap.Bool("admin_api", cfg.AdminAPI.Enabled),
		zap.Bool("multi_dc", cfg.MultiDC.Enabled),
	)

	// Wait for shutdown signal
	waitForShutdown(ctx, app)

	monitoring.Info("KafkaTwin proxy stopped")
}

// Application holds all application components
type Application struct {
	Config              *config.Config
	ClusterManager      *cluster.Manager
	MetadataManager     *metadata.Manager
	ProducerHandler     *producer.Handler
	ConsumerHandler     *consumer.Handler
	CoordinationService *coordination.Service
	ProtocolServer      *protocol.Server
	SchemaRegistry      *schema.Registry
	TransformEngine     *transformation.Engine
	RateLimiter         *ratelimit.Limiter
	AdminServer         *admin.Server
	MultiDCManager      *multidc.Manager
}

// loadConfig loads configuration from file
func loadConfig(filename string) (*config.Config, error) {
	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		// Use default config if file doesn't exist
		fmt.Printf("Configuration file not found, using defaults\n")
		return config.DefaultConfig(), nil
	}

	return config.LoadConfig(filename)
}

// initializeApp initializes all application components
func initializeApp(cfg *config.Config) (*Application, error) {
	monitoring.Info("Initializing application components")

	app := &Application{
		Config: cfg,
	}

	// Initialize cluster manager
	clusterManager, err := cluster.NewManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster manager: %w", err)
	}
	app.ClusterManager = clusterManager

	// Initialize metadata manager
	metadataManager := metadata.NewManager(clusterManager, cfg.Performance.Cache)
	app.MetadataManager = metadataManager

	// Initialize Schema Registry (if enabled)
	if cfg.SchemaRegistry.Enabled {
		schemaRegistry := schema.NewRegistry(schema.RegistryConfig{
			URL:      cfg.SchemaRegistry.URL,
			Timeout:  cfg.SchemaRegistry.Timeout,
			CacheTTL: cfg.SchemaRegistry.CacheTTL,
		})
		app.SchemaRegistry = schemaRegistry
		monitoring.Info("Schema Registry initialized", zap.String("url", cfg.SchemaRegistry.URL))
	}

	// Initialize Transformation Engine (if enabled)
	if cfg.Transformation.Enabled {
		transformEngine := transformation.NewEngine()
		app.TransformEngine = transformEngine
		monitoring.Info("Transformation Engine initialized")
	}

	// Initialize Rate Limiter (if enabled)
	if cfg.RateLimit.Enabled {
		rateLimiter := ratelimit.NewLimiter(ratelimit.Config{
			DefaultRequestsPerSecond: cfg.RateLimit.DefaultRequestsPerSecond,
			DefaultBytesPerSecond:    cfg.RateLimit.DefaultBytesPerSecond,
			BurstSize:                cfg.RateLimit.BurstSize,
			QuotaCheckInterval:       cfg.RateLimit.QuotaCheckInterval,
		})
		app.RateLimiter = rateLimiter
		monitoring.Info("Rate Limiter initialized")
	}

	// Initialize Multi-DC Manager (if enabled)
	if cfg.MultiDC.Enabled {
		multiDCManager := multidc.NewManager(multidc.Config{
			Strategy:            multidc.ReplicationStrategy(cfg.MultiDC.Strategy),
			LocalDC:             cfg.MultiDC.LocalDC,
			HealthCheckInterval: cfg.MultiDC.HealthCheckInterval,
			LatencyThreshold:    cfg.MultiDC.LatencyThreshold,
			PreferLocalReads:    cfg.MultiDC.PreferLocalReads,
		})

		// Register datacenters
		for _, dcCfg := range cfg.MultiDC.Datacenters {
			dc := &multidc.Datacenter{
				ID:       dcCfg.ID,
				Name:     dcCfg.Name,
				Region:   dcCfg.Region,
				Priority: dcCfg.Priority,
				Clusters: make([]*cluster.Cluster, 0),
			}

			// Add clusters to datacenter
			for _, clusterID := range dcCfg.ClusterIDs {
				if c, err := clusterManager.GetCluster(clusterID); err == nil {
					dc.Clusters = append(dc.Clusters, c)
				}
			}

			if err := multiDCManager.RegisterDatacenter(dc); err != nil {
				monitoring.Warn("Failed to register datacenter", zap.String("dc", dcCfg.ID), zap.Error(err))
			}
		}

		app.MultiDCManager = multiDCManager
		monitoring.Info("Multi-DC Manager initialized", zap.Int("datacenters", len(cfg.MultiDC.Datacenters)))
	}

	// Initialize producer handler
	producerHandler := producer.NewHandler(clusterManager, metadataManager, cfg.Producer)
	app.ProducerHandler = producerHandler

	// Initialize consumer handler
	consumerHandler, err := consumer.NewHandler(clusterManager, metadataManager, cfg.Consumer)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer handler: %w", err)
	}
	app.ConsumerHandler = consumerHandler

	// Initialize coordination service
	coordinationService := coordination.NewService(clusterManager, metadataManager, cfg.Consumer)
	app.CoordinationService = coordinationService

	// Initialize protocol server
	protocolServer := protocol.NewServer(
		cfg,
		producerHandler,
		consumerHandler,
		coordinationService,
	)
	app.ProtocolServer = protocolServer

	// Initialize Admin API server (if enabled)
	if cfg.AdminAPI.Enabled {
		adminServer := admin.NewServer(
			admin.Config{
				Port: cfg.AdminAPI.Port,
			},
			clusterManager,
			metadataManager,
			app.SchemaRegistry,
			app.TransformEngine,
			app.RateLimiter,
		)
		app.AdminServer = adminServer
	}

	monitoring.Info("Application components initialized successfully")

	return app, nil
}

// waitForShutdown waits for shutdown signal and performs graceful shutdown
func waitForShutdown(ctx context.Context, app *Application) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		monitoring.Info("Received shutdown signal", zap.String("signal", sig.String()))
	case <-ctx.Done():
		monitoring.Info("Context cancelled")
	}

	// Perform graceful shutdown
	monitoring.Info("Starting graceful shutdown")

	// Stop admin server
	if app.AdminServer != nil {
		if err := app.AdminServer.Stop(); err != nil {
			monitoring.Error("Error stopping admin server", zap.Error(err))
		}
	}

	// Stop accepting new connections
	if err := app.ProtocolServer.Stop(); err != nil {
		monitoring.Error("Error stopping protocol server", zap.Error(err))
	}

	// Close metadata manager
	app.MetadataManager.Close()

	// Close cluster manager
	if err := app.ClusterManager.Close(); err != nil {
		monitoring.Error("Error closing cluster manager", zap.Error(err))
	}

	monitoring.Info("Graceful shutdown completed")
}
