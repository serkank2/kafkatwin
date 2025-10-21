package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/consumer"
	"github.com/serkank2/kafkatwin/internal/coordination"
	"github.com/serkank2/kafkatwin/internal/metadata"
	"github.com/serkank2/kafkatwin/internal/monitoring"
	"github.com/serkank2/kafkatwin/internal/producer"
	"github.com/serkank2/kafkatwin/internal/protocol"
)

var (
	configFile = flag.String("config", "config.yaml", "Path to configuration file")
	version    = "1.0.0"
	buildTime  = "unknown"
)

func main() {
	flag.Parse()

	fmt.Printf("KafkaTwin Multi-Cluster Proxy v%s (built %s)\n", version, buildTime)

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

	// Initialize cluster manager
	clusterManager, err := cluster.NewManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster manager: %w", err)
	}

	// Initialize metadata manager
	metadataManager := metadata.NewManager(clusterManager, cfg.Performance.Cache)

	// Initialize producer handler
	producerHandler := producer.NewHandler(clusterManager, metadataManager, cfg.Producer)

	// Initialize consumer handler
	consumerHandler, err := consumer.NewHandler(clusterManager, metadataManager, cfg.Consumer)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer handler: %w", err)
	}

	// Initialize coordination service
	coordinationService := coordination.NewService(clusterManager, metadataManager, cfg.Consumer)

	// Initialize protocol server
	protocolServer := protocol.NewServer(
		cfg,
		producerHandler,
		consumerHandler,
		coordinationService,
	)

	monitoring.Info("Application components initialized successfully")

	return &Application{
		Config:              cfg,
		ClusterManager:      clusterManager,
		MetadataManager:     metadataManager,
		ProducerHandler:     producerHandler,
		ConsumerHandler:     consumerHandler,
		CoordinationService: coordinationService,
		ProtocolServer:      protocolServer,
	}, nil
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
