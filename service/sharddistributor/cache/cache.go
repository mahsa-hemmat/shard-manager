package cache

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination=cache_mock.go ShardCache

import (
	"context"

	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

// ShardCache answers shard and executor lookups from a cached view of the namespace,
// which it keeps current in the background from a Store. Lookups that miss the cache
// trigger a refresh, so a caller never misses a shard purely because the cache is cold.
type ShardCache interface {
	// GetShardOwner retrieves the owner of a specific shard within a namespace.
	// It returns store.ErrShardNotFound if the shard does not exist, and store.ErrShardDrained
	// if the shard is drained.
	GetShardOwner(ctx context.Context, namespace, shardID string) (*store.ShardOwner, error)

	// GetExecutor retrieves an executor within a namespace.
	GetExecutor(ctx context.Context, namespace string, executorID string) (*store.ShardOwner, error)

	// GetShardAssignments returns a snapshot of assignments and drained shards.
	GetShardAssignments(namespace string) (store.AssignmentSnapshot, error)

	// Subscribe signals when the namespace's assignments, executor metadata or
	// drained shards change. The returned function unsubscribes.
	Subscribe(namespace string) (<-chan struct{}, func(), error)

	// GetRangeForShardKey returns the assignment that owns the given key in a
	// range-based namespace. Returns ErrRangeIndexNotReady if the range index cannot be built.
	// Callers should fail closed rather than fall back to the discrete path.
	// The returned assignment is from the cached snapshot and must not be
	// modified. Use GetRangeAssignments for a mutable copy.
	GetRangeForShardKey(ctx context.Context, namespace string, key []byte) (*store.RangeAssignment, error)

	// GetRangeAssignments returns a deep copy of the namespace's range cover.
	// The result is owned by the caller.
	GetRangeAssignments(ctx context.Context, namespace string) ([]*store.RangeAssignment, error)
}
