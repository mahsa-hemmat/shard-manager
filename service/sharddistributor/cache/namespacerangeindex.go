package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cadence-workflow/shard-manager/common/log"
	"github.com/cadence-workflow/shard-manager/common/log/tag"
	"github.com/cadence-workflow/shard-manager/common/metrics"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

// ErrRangeIndexNotReady is returned when a range-mode namespace has no valid
// range index to serve. Callers must fail closed: there is no fallback to the
// discrete path.
var ErrRangeIndexNotReady = errors.New("range index not ready")

// namespaceRangeIndex holds the range cover for one namespace as an immutable
// *store.RangeIndex snapshot. Readers look up the current pointer and search it
// without holding a lock, so a rebuild never blocks a request.
//
// A failed rebuild keeps the previous snapshot. The range boundaries remain
// valid, so a stale snapshot is still a complete cover even if ownership
// information is temporarily out of date.
type namespaceRangeIndex struct {
	mu sync.RWMutex

	namespace     string
	index         *store.RangeIndex
	executorStore store.Store
	stopCh        chan struct{}
	logger        log.Logger
	metricsClient metrics.Client

	// refreshSF serializes rebuilds. This prevents an older snapshot from
	// overwriting a newer one since rebuilds never overlap and each reads at a
	// revision at least as new as the previous rebuild.
	refreshSF      singleflight.Group
	refreshTimeout time.Duration
}

func newNamespaceRangeIndex(
	namespace string,
	executorStore store.Store,
	stopCh chan struct{},
	logger log.Logger,
	metricsClient metrics.Client,
) *namespaceRangeIndex {
	return &namespaceRangeIndex{
		namespace:      namespace,
		executorStore:  executorStore,
		stopCh:         stopCh,
		logger:         logger.WithTags(tag.ShardNamespace(namespace)),
		metricsClient:  metricsClient,
		refreshTimeout: refreshOperationTimeout,
	}
}

func (n *namespaceRangeIndex) Start(wg *sync.WaitGroup) error {
	changeCh, err := n.executorStore.SubscribeToNamespaceChanges(n.namespace)
	if err != nil {
		return fmt.Errorf("subscribe to namespace changes: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		n.refreshLoop(changeCh)
	}()

	return nil
}

func (n *namespaceRangeIndex) refreshLoop(changeCh <-chan struct{}) {
	for {
		select {
		case <-n.stopCh:
			return

		case _, ok := <-changeCh:
			if !ok {
				return
			}

			// The wait is bounded by the flight's refreshTimeout, so a stuck etcd
			// read cannot keep this loop from observing stopCh. Failed rebuilds
			// are already logged and counted, and the previous snapshot is
			// retained.
			_, _ = n.rebuild(context.Background())
		}
	}
}

// GetRangeForKey returns the assignment for the given key in the namespace.
// It points into the immutable snapshot and must not be modified.
func (n *namespaceRangeIndex) GetRangeForKey(ctx context.Context, key []byte) (*store.RangeAssignment, error) {
	index, err := n.snapshot(ctx)
	if err != nil {
		return nil, err
	}

	assignment, ok := index.GetRangeForKey(key)
	if !ok {
		return nil, fmt.Errorf("range index for namespace %s has no range covering key %x", n.namespace, key)
	}
	return assignment, nil
}

// GetRangeAssignments returns the current cover as deep copies. Callers own the
// returned assignments; nothing is shared with the underlying snapshot.
func (n *namespaceRangeIndex) GetRangeAssignments(ctx context.Context) ([]*store.RangeAssignment, error) {
	index, err := n.snapshot(ctx)
	if err != nil {
		return nil, err
	}

	ranges := index.Ranges()
	assignments := make([]*store.RangeAssignment, 0, len(ranges))
	for _, assignment := range ranges {
		assignments = append(assignments, assignment.Clone())
	}
	return assignments, nil
}

func (n *namespaceRangeIndex) snapshot(ctx context.Context) (*store.RangeIndex, error) {
	n.mu.RLock()
	index := n.index
	n.mu.RUnlock()
	if index != nil {
		return index, nil
	}

	// No snapshot yet: build one on demand so the first request succeeds while
	// etcd is healthy instead of failing until a watch signal arrives.
	return n.rebuild(ctx)
}

// rebuild runs rebuilds through singleflight. The actual rebuild uses a
// detached, bounded context so one caller's cancellation does not affect other
// waiters, and a hung etcd read cannot hold the flight key indefinitely.
func (n *namespaceRangeIndex) rebuild(ctx context.Context) (*store.RangeIndex, error) {
	result := n.refreshSF.DoChan(refreshSingleFlightKey, func() (any, error) {
		refreshCtx, cancel := context.WithTimeout(context.Background(), n.refreshTimeout)
		defer cancel()
		return n.rebuildOnce(refreshCtx)
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-result:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*store.RangeIndex), nil
	}
}

func (n *namespaceRangeIndex) rebuildOnce(ctx context.Context) (*store.RangeIndex, error) {
	assignments, err := n.executorStore.ListRanges(ctx, n.namespace)
	if err != nil {
		return n.retain(err)
	}

	index, err := store.NewRangeIndex(assignments)
	if err != nil {
		return n.retain(err)
	}

	n.mu.Lock()
	n.index = index
	n.mu.Unlock()
	return index, nil
}

// retain records a failed rebuild and returns the previous snapshot when one
// is available. The cause is logged and wrapped in the returned error so store
// failures remain distinguishable from an invalid cover.
func (n *namespaceRangeIndex) retain(cause error) (*store.RangeIndex, error) {
	n.mu.RLock()
	previous := n.index
	n.mu.RUnlock()

	n.metricsClient.Scope(metrics.ShardDistributorRangeIndexScope).
		Tagged(metrics.NamespaceTag(n.namespace)).
		IncCounter(metrics.ShardDistributorRangeIndexRebuildFailures)
	n.logger.Error("range index rebuild failed", tag.Error(cause))

	if previous == nil {
		return nil, fmt.Errorf("%w: %w", ErrRangeIndexNotReady, cause)
	}
	return previous, nil
}
