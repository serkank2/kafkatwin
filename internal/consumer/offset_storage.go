package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/serkank2/kafkatwin/internal/config"
)

// OffsetStorage manages consumer offsets across clusters
type OffsetStorage interface {
	// Commit commits offsets for a consumer group
	Commit(ctx context.Context, group string, offsets map[string]map[int32]*OffsetInfo) error

	// Fetch fetches offsets for a consumer group
	Fetch(ctx context.Context, group string, topic string) (map[int32]*OffsetInfo, error)

	// Delete deletes offsets for a consumer group
	Delete(ctx context.Context, group string) error
}

// OffsetInfo contains offset information for a partition
type OffsetInfo struct {
	VirtualOffset  int64            // Virtual offset presented to consumer
	ClusterOffsets map[string]int64 // Real offsets per cluster
	Metadata       string
	Timestamp      time.Time
}

// MemoryOffsetStorage is an in-memory offset storage
type MemoryOffsetStorage struct {
	offsets map[string]map[string]map[int32]*OffsetInfo // group -> topic -> partition -> offset
	mu      sync.RWMutex
}

// NewOffsetStorage creates a new offset storage based on configuration
func NewOffsetStorage(cfg config.OffsetStorageConfig) (OffsetStorage, error) {
	switch cfg.Type {
	case "memory":
		return NewMemoryOffsetStorage(), nil
	default:
		return nil, fmt.Errorf("unsupported offset storage type: %s", cfg.Type)
	}
}

// NewMemoryOffsetStorage creates a new in-memory offset storage
func NewMemoryOffsetStorage() *MemoryOffsetStorage {
	return &MemoryOffsetStorage{
		offsets: make(map[string]map[string]map[int32]*OffsetInfo),
	}
}

// Commit commits offsets
func (m *MemoryOffsetStorage) Commit(ctx context.Context, group string, offsets map[string]map[int32]*OffsetInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.offsets[group] == nil {
		m.offsets[group] = make(map[string]map[int32]*OffsetInfo)
	}

	for topic, partitions := range offsets {
		if m.offsets[group][topic] == nil {
			m.offsets[group][topic] = make(map[int32]*OffsetInfo)
		}

		for partition, offset := range partitions {
			offset.Timestamp = time.Now()
			m.offsets[group][topic][partition] = offset
		}
	}

	return nil
}

// Fetch fetches offsets
func (m *MemoryOffsetStorage) Fetch(ctx context.Context, group string, topic string) (map[int32]*OffsetInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.offsets[group] == nil || m.offsets[group][topic] == nil {
		return make(map[int32]*OffsetInfo), nil
	}

	// Return a copy
	result := make(map[int32]*OffsetInfo)
	for partition, offset := range m.offsets[group][topic] {
		offsetCopy := *offset
		result[partition] = &offsetCopy
	}

	return result, nil
}

// Delete deletes offsets for a group
func (m *MemoryOffsetStorage) Delete(ctx context.Context, group string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.offsets, group)
	return nil
}

// GetAllGroups returns all consumer groups
func (m *MemoryOffsetStorage) GetAllGroups() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	groups := make([]string, 0, len(m.offsets))
	for group := range m.offsets {
		groups = append(groups, group)
	}

	return groups
}

// MarshalJSON serializes offset info to JSON
func (o *OffsetInfo) MarshalJSON() ([]byte, error) {
	type Alias OffsetInfo
	return json.Marshal(&struct {
		*Alias
		Timestamp string `json:"timestamp"`
	}{
		Alias:     (*Alias)(o),
		Timestamp: o.Timestamp.Format(time.RFC3339),
	})
}

// UnmarshalJSON deserializes offset info from JSON
func (o *OffsetInfo) UnmarshalJSON(data []byte) error {
	type Alias OffsetInfo
	aux := &struct {
		*Alias
		Timestamp string `json:"timestamp"`
	}{
		Alias: (*Alias)(o),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.Timestamp != "" {
		t, err := time.Parse(time.RFC3339, aux.Timestamp)
		if err != nil {
			return err
		}
		o.Timestamp = t
	}

	return nil
}

// OffsetMapper maps between virtual and real offsets
type OffsetMapper struct {
	mu sync.RWMutex
}

// NewOffsetMapper creates a new offset mapper
func NewOffsetMapper() *OffsetMapper {
	return &OffsetMapper{}
}

// ToVirtualOffset converts cluster offsets to a virtual offset
func (om *OffsetMapper) ToVirtualOffset(clusterOffsets map[string]int64) int64 {
	// Simple strategy: use the maximum offset across clusters
	var maxOffset int64
	for _, offset := range clusterOffsets {
		if offset > maxOffset {
			maxOffset = offset
		}
	}
	return maxOffset
}

// ToClusterOffsets converts a virtual offset to cluster offsets
func (om *OffsetMapper) ToClusterOffsets(virtualOffset int64, previousOffsets map[string]int64) map[string]int64 {
	// Simple strategy: use the virtual offset for all clusters
	result := make(map[string]int64)
	for clusterID := range previousOffsets {
		result[clusterID] = virtualOffset
	}
	return result
}

// MergeOffsets merges offsets from multiple fetch operations
func (om *OffsetMapper) MergeOffsets(offsets []map[string]int64) map[string]int64 {
	result := make(map[string]int64)

	for _, offsetMap := range offsets {
		for clusterID, offset := range offsetMap {
			if existing, exists := result[clusterID]; !exists || offset > existing {
				result[clusterID] = offset
			}
		}
	}

	return result
}
