package coordination

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/metadata"
	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// Service manages consumer group coordination
type Service struct {
	clusterManager  *cluster.Manager
	metadataManager *metadata.Manager
	groups          map[string]*ConsumerGroup
	config          config.ConsumerConfig
	mu              sync.RWMutex
}

// ConsumerGroup represents a consumer group
type ConsumerGroup struct {
	ID              string
	Members         map[string]*GroupMember
	State           GroupState
	GenerationID    int32
	ProtocolType    string
	ProtocolName    string
	Leader          string
	Assignment      map[string]*MemberAssignment
	LastHeartbeat   map[string]time.Time
	Created         time.Time
	mu              sync.RWMutex
}

// GroupMember represents a member of a consumer group
type GroupMember struct {
	ID              string
	GroupInstanceID string
	Metadata        []byte
	Assignment      []byte
	JoinedAt        time.Time
}

// GroupState represents the state of a consumer group
type GroupState string

const (
	StateEmpty               GroupState = "Empty"
	StatePreparingRebalance  GroupState = "PreparingRebalance"
	StateCompletingRebalance GroupState = "CompletingRebalance"
	StateStable              GroupState = "Stable"
	StateDead                GroupState = "Dead"
)

// MemberAssignment represents partition assignment for a member
type MemberAssignment struct {
	Topics     map[string][]int32 // topic -> partitions
	ClusterIDs []string
	Version    int16
}

// NewService creates a new coordination service
func NewService(clusterMgr *cluster.Manager, metaMgr *metadata.Manager, cfg config.ConsumerConfig) *Service {
	s := &Service{
		clusterManager:  clusterMgr,
		metadataManager: metaMgr,
		groups:          make(map[string]*ConsumerGroup),
		config:          cfg,
	}

	// Start background tasks
	go s.heartbeatMonitor()

	return s
}

// JoinGroup handles a consumer joining a group
func (s *Service) JoinGroup(ctx context.Context, groupID string, memberID string, protocolType string, protocols []string) (*JoinGroupResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get or create group
	group, exists := s.groups[groupID]
	if !exists {
		group = &ConsumerGroup{
			ID:            groupID,
			Members:       make(map[string]*GroupMember),
			State:         StateEmpty,
			ProtocolType:  protocolType,
			LastHeartbeat: make(map[string]time.Time),
			Created:       time.Now(),
		}
		s.groups[groupID] = group

		monitoring.Info("Consumer group created", zap.String("group", groupID))
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	// Add member
	member := &GroupMember{
		ID:       memberID,
		JoinedAt: time.Now(),
	}
	group.Members[memberID] = member
	group.LastHeartbeat[memberID] = time.Now()

	// Trigger rebalance if in stable state
	if group.State == StateStable {
		group.State = StatePreparingRebalance
		monitoring.ConsumerGroupRebalanceTotal.WithLabelValues(groupID).Inc()
		monitoring.Info("Rebalance triggered", zap.String("group", groupID), zap.String("reason", "new member"))
	}

	// Select leader (first member)
	if group.Leader == "" {
		group.Leader = memberID
	}

	// Increment generation
	group.GenerationID++

	// Update metrics
	monitoring.ConsumerGroupMembers.WithLabelValues(groupID).Set(float64(len(group.Members)))

	response := &JoinGroupResponse{
		GenerationID: group.GenerationID,
		ProtocolName: group.ProtocolName,
		LeaderID:     group.Leader,
		MemberID:     memberID,
		Members:      make([]GroupMemberInfo, 0),
	}

	// If this is the leader, include all member info
	if memberID == group.Leader {
		for mid, m := range group.Members {
			response.Members = append(response.Members, GroupMemberInfo{
				MemberID: mid,
				Metadata: m.Metadata,
			})
		}
	}

	monitoring.Debug("Member joined group",
		zap.String("group", groupID),
		zap.String("member", memberID),
		zap.Int32("generation", group.GenerationID),
	)

	return response, nil
}

// SyncGroup handles consumer group synchronization
func (s *Service) SyncGroup(ctx context.Context, groupID string, memberID string, generationID int32, assignments map[string][]byte) (*SyncGroupResponse, error) {
	s.mu.RLock()
	group, exists := s.groups[groupID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if group.GenerationID != generationID {
		return nil, fmt.Errorf("generation mismatch: expected %d, got %d", group.GenerationID, generationID)
	}

	// If this is the leader, store assignments
	if memberID == group.Leader && assignments != nil {
		for mid, assignment := range assignments {
			if member, exists := group.Members[mid]; exists {
				member.Assignment = assignment
			}
		}
		group.State = StateStable
	}

	// Wait for assignment to be available
	var assignment []byte
	if member, exists := group.Members[memberID]; exists {
		assignment = member.Assignment
	}

	response := &SyncGroupResponse{
		Assignment: assignment,
	}

	monitoring.Debug("Member synced group",
		zap.String("group", groupID),
		zap.String("member", memberID),
	)

	return response, nil
}

// Heartbeat handles consumer heartbeat
func (s *Service) Heartbeat(ctx context.Context, groupID string, memberID string, generationID int32) error {
	s.mu.RLock()
	group, exists := s.groups[groupID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if _, exists := group.Members[memberID]; !exists {
		return fmt.Errorf("member %s not found in group", memberID)
	}

	if group.GenerationID != generationID {
		return fmt.Errorf("generation mismatch")
	}

	group.LastHeartbeat[memberID] = time.Now()

	return nil
}

// LeaveGroup handles a consumer leaving a group
func (s *Service) LeaveGroup(ctx context.Context, groupID string, memberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	delete(group.Members, memberID)
	delete(group.LastHeartbeat, memberID)

	// Update metrics
	monitoring.ConsumerGroupMembers.WithLabelValues(groupID).Set(float64(len(group.Members)))

	// Trigger rebalance
	if len(group.Members) > 0 {
		group.State = StatePreparingRebalance
		monitoring.ConsumerGroupRebalanceTotal.WithLabelValues(groupID).Inc()
		monitoring.Info("Rebalance triggered", zap.String("group", groupID), zap.String("reason", "member left"))
	} else {
		group.State = StateEmpty
	}

	monitoring.Info("Member left group",
		zap.String("group", groupID),
		zap.String("member", memberID),
	)

	return nil
}

// heartbeatMonitor monitors heartbeats and removes dead members
func (s *Service) heartbeatMonitor() {
	ticker := time.NewTicker(s.config.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.checkHeartbeats()
	}
}

// checkHeartbeats checks for expired heartbeats
func (s *Service) checkHeartbeats() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	timeout := s.config.SessionTimeout

	for groupID, group := range s.groups {
		group.mu.Lock()

		deadMembers := make([]string, 0)
		for memberID, lastHeartbeat := range group.LastHeartbeat {
			if now.Sub(lastHeartbeat) > timeout {
				deadMembers = append(deadMembers, memberID)
			}
		}

		for _, memberID := range deadMembers {
			delete(group.Members, memberID)
			delete(group.LastHeartbeat, memberID)

			monitoring.Warn("Member timed out",
				zap.String("group", groupID),
				zap.String("member", memberID),
			)
		}

		if len(deadMembers) > 0 {
			// Update metrics
			monitoring.ConsumerGroupMembers.WithLabelValues(groupID).Set(float64(len(group.Members)))

			// Trigger rebalance
			if len(group.Members) > 0 {
				group.State = StatePreparingRebalance
				monitoring.ConsumerGroupRebalanceTotal.WithLabelValues(groupID).Inc()
			} else {
				group.State = StateEmpty
			}
		}

		group.mu.Unlock()
	}
}

// GetGroup returns information about a consumer group
func (s *Service) GetGroup(groupID string) (*ConsumerGroup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, exists := s.groups[groupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	return group, nil
}

// ListGroups returns all consumer groups
func (s *Service) ListGroups() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups := make([]string, 0, len(s.groups))
	for groupID := range s.groups {
		groups = append(groups, groupID)
	}

	return groups
}

// JoinGroupResponse represents the response to a join group request
type JoinGroupResponse struct {
	GenerationID int32
	ProtocolName string
	LeaderID     string
	MemberID     string
	Members      []GroupMemberInfo
}

// GroupMemberInfo represents member information
type GroupMemberInfo struct {
	MemberID string
	Metadata []byte
}

// SyncGroupResponse represents the response to a sync group request
type SyncGroupResponse struct {
	Assignment []byte
}
