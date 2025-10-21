package protocol

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/consumer"
	"github.com/serkank2/kafkatwin/internal/coordination"
	"github.com/serkank2/kafkatwin/internal/monitoring"
	"github.com/serkank2/kafkatwin/internal/producer"
)

// Server represents the protocol server
type Server struct {
	config              *config.Config
	listener            net.Listener
	producerHandler     *producer.Handler
	consumerHandler     *consumer.Handler
	coordinationService *coordination.Service
	connections         map[string]*Connection
	connMu              sync.RWMutex
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
}

// Connection represents a client connection
type Connection struct {
	ID        string
	Conn      net.Conn
	ClientID  string
	CreatedAt time.Time
	LastUsed  time.Time
	mu        sync.Mutex
}

// NewServer creates a new protocol server
func NewServer(
	cfg *config.Config,
	producerHandler *producer.Handler,
	consumerHandler *consumer.Handler,
	coordinationService *coordination.Service,
) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		config:              cfg,
		producerHandler:     producerHandler,
		consumerHandler:     consumerHandler,
		coordinationService: coordinationService,
		connections:         make(map[string]*Connection),
		ctx:                 ctx,
		cancel:              cancel,
	}
}

// Start starts the protocol server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Server.ListenAddress, s.config.Server.Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.listener = listener

	monitoring.Info("Protocol server started", zap.String("address", addr))

	// Start accepting connections
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop()
	}()

	return nil
}

// acceptLoop accepts incoming connections
func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				monitoring.Error("Failed to accept connection", zap.Error(err))
				continue
			}
		}

		// Handle connection in a goroutine
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

// handleConnection handles a client connection
func (s *Server) handleConnection(netConn net.Conn) {
	defer netConn.Close()

	connID := fmt.Sprintf("%s-%d", netConn.RemoteAddr().String(), time.Now().UnixNano())

	connection := &Connection{
		ID:        connID,
		Conn:      netConn,
		CreatedAt: time.Now(),
		LastUsed:  time.Now(),
	}

	// Register connection
	s.connMu.Lock()
	s.connections[connID] = connection
	s.connMu.Unlock()

	// Update metrics
	monitoring.UpdateActiveConnections(1)

	monitoring.Info("Client connected",
		zap.String("conn_id", connID),
		zap.String("remote_addr", netConn.RemoteAddr().String()),
	)

	// Deregister connection on exit
	defer func() {
		s.connMu.Lock()
		delete(s.connections, connID)
		s.connMu.Unlock()

		monitoring.UpdateActiveConnections(-1)

		monitoring.Info("Client disconnected", zap.String("conn_id", connID))
	}()

	// Note: In a real implementation, we would implement the Kafka wire protocol here.
	// Since this is complex and Sarama already handles it, we're creating a placeholder.
	// A production implementation would:
	// 1. Read Kafka protocol messages from the connection
	// 2. Parse the request type (Produce, Fetch, Metadata, etc.)
	// 3. Route to appropriate handler
	// 4. Return response in Kafka wire format

	// For now, we'll just keep the connection open
	buffer := make([]byte, 4096)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			netConn.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, err := netConn.Read(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				return
			}

			connection.mu.Lock()
			connection.LastUsed = time.Now()
			connection.mu.Unlock()
		}
	}
}

// Stop stops the protocol server
func (s *Server) Stop() error {
	monitoring.Info("Stopping protocol server")

	s.cancel()

	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	s.connMu.Lock()
	for _, conn := range s.connections {
		conn.Conn.Close()
	}
	s.connMu.Unlock()

	s.wg.Wait()

	monitoring.Info("Protocol server stopped")

	return nil
}

// GetActiveConnections returns the number of active connections
func (s *Server) GetActiveConnections() int {
	s.connMu.RLock()
	defer s.connMu.RUnlock()
	return len(s.connections)
}
