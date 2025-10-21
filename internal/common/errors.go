package common

import "errors"

var (
	// ErrClusterNotFound is returned when a cluster is not found
	ErrClusterNotFound = errors.New("cluster not found")

	// ErrClusterUnhealthy is returned when a cluster is unhealthy
	ErrClusterUnhealthy = errors.New("cluster is unhealthy")

	// ErrAllClustersUnhealthy is returned when all clusters are unhealthy
	ErrAllClustersUnhealthy = errors.New("all clusters are unhealthy")

	// ErrInsufficientAcks is returned when insufficient acks are received
	ErrInsufficientAcks = errors.New("insufficient acknowledgments")

	// ErrTimeout is returned when an operation times out
	ErrTimeout = errors.New("operation timed out")

	// ErrInvalidOffset is returned when an invalid offset is provided
	ErrInvalidOffset = errors.New("invalid offset")

	// ErrNoPartitions is returned when no partitions are available
	ErrNoPartitions = errors.New("no partitions available")

	// ErrInvalidConfig is returned when configuration is invalid
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrConnectionFailed is returned when connection to cluster fails
	ErrConnectionFailed = errors.New("connection failed")

	// ErrMetadataNotFound is returned when metadata is not found
	ErrMetadataNotFound = errors.New("metadata not found")

	// ErrConsumerGroupNotFound is returned when consumer group is not found
	ErrConsumerGroupNotFound = errors.New("consumer group not found")

	// ErrRebalanceInProgress is returned when rebalance is in progress
	ErrRebalanceInProgress = errors.New("rebalance in progress")
)
