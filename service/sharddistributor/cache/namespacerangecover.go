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

// ErrRangeCoverNotReady is returned when a range namespace has no valid cover to serve.
var ErrRangeCoverNotReady = errors.New("range cover not ready")

// namespaceRangeCover holds the range cover for one namespace as an immutable
// *store.RangeCover snapshot. A failed rebuild retains the previous snapshot,
// which stays a complete cover while boundaries do not move.
type namespaceRangeCover struct {
	mu sync.RWMutex

	namespace     string
	cover         *store.RangeCover
	executorStore store.Store
	logger        log.Logger
	metricsClient metrics.Client

	// refreshSF serializes rebuilds so an older read cannot overwrite a newer cover.
	refreshSF      singleflight.Group
	refreshTimeout time.Duration
}

func newNamespaceRangeCover(
	namespace string,
	executorStore store.Store,
	logger log.Logger,
	metricsClient metrics.Client,
) *namespaceRangeCover {
	return &namespaceRangeCover{
		namespace:      namespace,
		executorStore:  executorStore,
		logger:         logger.WithTags(tag.ShardNamespace(namespace)),
		metricsClient:  metricsClient,
		refreshTimeout: refreshOperationTimeout,
	}
}

// GetRangeForKey returns the assignment for key from the immutable snapshot.
func (n *namespaceRangeCover) GetRangeForKey(ctx context.Context, key []byte) (*store.RangeAssignment, error) {
	cover, err := n.snapshot(ctx)
	if err != nil {
		return nil, err
	}

	return cover.GetRangeForKey(key), nil
}

// refresh updates the cached cover. An unbootstrapped empty namespace is not an
// error for the background loop; lookups still fail closed with ErrRangeCoverNotReady.
func (n *namespaceRangeCover) refresh(ctx context.Context) error {
	_, err := n.rebuild(ctx)
	if errors.Is(err, ErrRangeCoverNotReady) {
		return nil
	}
	return err
}

func (n *namespaceRangeCover) snapshot(ctx context.Context) (*store.RangeCover, error) {
	if cover := n.currentCover(); cover != nil {
		return cover, nil
	}

	// Build on demand when cold so a request before the first watch signal succeeds.
	return n.rebuild(ctx)
}

func (n *namespaceRangeCover) currentCover() *store.RangeCover {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.cover
}

// rebuild runs rebuilds through singleflight under a detached, bounded context so one
// caller's cancellation cannot fail other waiters and a hung read cannot hold the key.
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
	if len(assignments) == 0 && n.currentCover() == nil {
		return nil, ErrRangeCoverNotReady
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

// retain records a failed rebuild and returns the previous snapshot when available.
func (n *namespaceRangeCover) retain(cause error) (*store.RangeCover, error) {
	previous := n.currentCover()

	n.metricsClient.Scope(metrics.ShardDistributorRangeCoverScope).
		Tagged(metrics.NamespaceTag(n.namespace)).
		IncCounter(metrics.ShardDistributorRangeCoverRebuildFailures)
	n.logger.Error("range cover rebuild failed", tag.Error(cause))

	if previous == nil {
		return nil, fmt.Errorf("%w: %w", ErrRangeCoverNotReady, cause)
	}
	return previous, nil
}
