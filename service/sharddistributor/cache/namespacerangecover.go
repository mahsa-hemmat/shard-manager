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

// ErrRangeCoverNotReady is returned when a range-mode namespace has no valid
// cover to serve. Callers must fail closed: there is no fallback to the
// discrete path.
var ErrRangeCoverNotReady = errors.New("range cover not ready")

// namespaceRangeCover holds the range cover for one namespace as an immutable
// *store.RangeCover snapshot. Readers look up the current pointer and search it
// without holding a lock, so a rebuild never blocks a request.
//
// A failed rebuild keeps the previous snapshot. The range boundaries remain
// valid, so a stale snapshot is still a complete cover even if ownership
// information is temporarily out of date.
type namespaceRangeCover struct {
	mu sync.RWMutex

	namespace     string
	cover         *store.RangeCover
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

func newNamespaceRangeCover(
	namespace string,
	executorStore store.Store,
	stopCh chan struct{},
	logger log.Logger,
	metricsClient metrics.Client,
) *namespaceRangeCover {
	return &namespaceRangeCover{
		namespace:      namespace,
		executorStore:  executorStore,
		stopCh:         stopCh,
		logger:         logger.WithTags(tag.ShardNamespace(namespace)),
		metricsClient:  metricsClient,
		refreshTimeout: refreshOperationTimeout,
	}
}

func (n *namespaceRangeCover) Start(wg *sync.WaitGroup) error {
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

func (n *namespaceRangeCover) refreshLoop(changeCh <-chan struct{}) {
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
func (n *namespaceRangeCover) GetRangeForKey(ctx context.Context, key []byte) (*store.RangeAssignment, error) {
	cover, err := n.snapshot(ctx)
	if err != nil {
		return nil, err
	}

	// A valid cover answers every key, so a miss here is not possible.
	return cover.GetRangeForKey(key), nil
}

func (n *namespaceRangeCover) snapshot(ctx context.Context) (*store.RangeCover, error) {
	n.mu.RLock()
	cover := n.cover
	n.mu.RUnlock()
	if cover != nil {
		return cover, nil
	}

	// No snapshot yet: build one on demand so the first request succeeds while
	// etcd is healthy instead of failing until a watch signal arrives.
	return n.rebuild(ctx)
}

// rebuild runs rebuilds through singleflight. The actual rebuild uses a
// detached, bounded context so one caller's cancellation does not affect other
// waiters, and a hung etcd read cannot hold the flight key indefinitely.
func (n *namespaceRangeCover) rebuild(ctx context.Context) (*store.RangeCover, error) {
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
		return res.Val.(*store.RangeCover), nil
	}
}

func (n *namespaceRangeCover) rebuildOnce(ctx context.Context) (*store.RangeCover, error) {
	assignments, err := n.executorStore.ListRanges(ctx, n.namespace)
	if err != nil {
		return n.retain(err)
	}

	cover, err := store.NewRangeCover(assignments)
	if err != nil {
		return n.retain(err)
	}

	n.mu.Lock()
	n.cover = cover
	n.mu.Unlock()
	return cover, nil
}

// retain records a failed rebuild and returns the previous snapshot when one
// is available. The cause is logged and wrapped in the returned error so store
// failures remain distinguishable from an invalid cover.
func (n *namespaceRangeCover) retain(cause error) (*store.RangeCover, error) {
	n.mu.RLock()
	previous := n.cover
	n.mu.RUnlock()

	n.metricsClient.Scope(metrics.ShardDistributorRangeCoverScope).
		Tagged(metrics.NamespaceTag(n.namespace)).
		IncCounter(metrics.ShardDistributorRangeCoverRebuildFailures)
	n.logger.Error("range cover rebuild failed", tag.Error(cause))

	if previous == nil {
		return nil, fmt.Errorf("%w: %w", ErrRangeCoverNotReady, cause)
	}
	return previous, nil
}
