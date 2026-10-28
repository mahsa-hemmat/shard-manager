package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/cadence-workflow/shard-manager/common/log/testlogger"
	"github.com/cadence-workflow/shard-manager/common/metrics"
	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

const testRangeNamespace = "range-namespace"

// testBound builds an 8-byte big-endian bound with the given first byte and
// zeroes elsewhere.
func testBound(first byte) []byte {
	return []byte{first, 0, 0, 0, 0, 0, 0, 0}
}

// testCover returns a valid two-range cover: [00.., 80..) owned by lowOwner and
// [80.., ∞) owned by highOwner.
func testCover(t *testing.T, lowOwner, highOwner string) []*store.RangeAssignment {
	t.Helper()

	low, err := types.NewRange(testBound(0x00), testBound(0x80))
	require.NoError(t, err)
	high, err := types.NewFinalRange(testBound(0x80))
	require.NoError(t, err)

	return []*store.RangeAssignment{
		{Range: low, ExecutorID: lowOwner, Status: types.AssignmentStatusREADY},
		{Range: high, ExecutorID: highOwner, Status: types.AssignmentStatusREADY},
	}
}

func newTestRangeIndex(t *testing.T, executorStore store.Store, metricsClient metrics.Client) *namespaceRangeIndex {
	t.Helper()
	return newNamespaceRangeIndex(testRangeNamespace, executorStore, make(chan struct{}), testlogger.New(t), metricsClient)
}

// expectRebuildFailureMetric registers the counters a single failed rebuild must
// report: the failure counter on a scope tagged with the namespace.
func expectRebuildFailureMetric(ctrl *gomock.Controller) metrics.Client {
	metricsClient := metrics.NewMockClient(ctrl)
	scope := metrics.NewMockScope(ctrl)
	metricsClient.EXPECT().Scope(metrics.ShardDistributorRangeIndexScope).Return(scope)
	scope.EXPECT().Tagged(metrics.NamespaceTag(testRangeNamespace)).Return(scope)
	scope.EXPECT().IncCounter(metrics.ShardDistributorRangeIndexRebuildFailures)
	return metricsClient
}

func TestNamespaceRangeIndex_GetRangeForKey(t *testing.T) {
	const namespace = testRangeNamespace

	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().
		ListRanges(gomock.Any(), namespace).
		Return(testCover(t, "executor-low", "executor-high"), nil).
		Times(1)

	index := newTestRangeIndex(t, executorStore, metrics.NewNoopMetricsClient())

	tests := []struct {
		name string
		key  []byte
		want string
	}{
		{"low bound is inclusive", testBound(0x00), "executor-low"},
		{"interior of the lower range", testBound(0x7F), "executor-low"},
		{"shared boundary belongs to the upper range", testBound(0x80), "executor-high"},
		{"maximum fingerprint stays in the final range", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, "executor-high"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assignment, err := index.GetRangeForKey(context.Background(), tt.key)
			require.NoError(t, err)
			assert.Equal(t, tt.want, assignment.ExecutorID)
		})
	}
}

// A namespace with no valid index has nothing safe to serve, so a range lookup
// must fail closed rather than guess or fall back to the discrete path.
func TestNamespaceRangeIndex_FailsClosedWithoutSnapshot(t *testing.T) {
	notAtMinimum, err := types.NewRange(testBound(0x10), testBound(0x80))
	require.NoError(t, err)
	final, err := types.NewFinalRange(testBound(0x80))
	require.NoError(t, err)

	tests := []struct {
		name  string
		cover []*store.RangeAssignment
		err   error
	}{
		{
			name: "list ranges fails",
			err:  errors.New("etcd unavailable"),
		},
		{
			name:  "empty cover",
			cover: nil,
		},
		{
			name: "cover does not start at the minimum bound",
			cover: []*store.RangeAssignment{
				{Range: notAtMinimum, ExecutorID: "executor-1"},
				{Range: final, ExecutorID: "executor-2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			executorStore := store.NewMockStore(ctrl)
			executorStore.EXPECT().
				ListRanges(gomock.Any(), testRangeNamespace).
				Return(tt.cover, tt.err)

			index := newTestRangeIndex(t, executorStore, expectRebuildFailureMetric(ctrl))

			_, err := index.GetRangeForKey(context.Background(), testBound(0x00))
			require.ErrorIs(t, err, ErrRangeIndexNotReady)
		})
	}
}

// A failed rebuild keeps the previous cover. The stale snapshot remains a
// complete cover even if ownership information is temporarily out of date.
func TestNamespaceRangeIndex_RetainsLastKnownGood(t *testing.T) {
	invalidCover := func(t *testing.T) []*store.RangeAssignment {
		t.Helper()
		notAtMinimum, err := types.NewRange(testBound(0x10), testBound(0x80))
		require.NoError(t, err)
		final, err := types.NewFinalRange(testBound(0x80))
		require.NoError(t, err)
		return []*store.RangeAssignment{
			{Range: notAtMinimum, ExecutorID: "executor-1"},
			{Range: final, ExecutorID: "executor-2"},
		}
	}

	tests := []struct {
		name  string
		cover []*store.RangeAssignment
		err   error
	}{
		{
			name: "list ranges fails on rebuild",
			err:  errors.New("etcd unavailable"),
		},
		{
			name:  "rebuilt cover is invalid",
			cover: invalidCover(t),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			executorStore := store.NewMockStore(ctrl)
			gomock.InOrder(
				executorStore.EXPECT().
					ListRanges(gomock.Any(), testRangeNamespace).
					Return(testCover(t, "executor-low", "executor-high"), nil),
				executorStore.EXPECT().
					ListRanges(gomock.Any(), testRangeNamespace).
					Return(tt.cover, tt.err),
			)

			metricsClient := metrics.NewMockClient(ctrl)
			index := newTestRangeIndex(t, executorStore, metricsClient)

			assignment, err := index.GetRangeForKey(context.Background(), testBound(0x00))
			require.NoError(t, err)
			assert.Equal(t, "executor-low", assignment.ExecutorID)

			// The failed rebuild reports the failure and returns the previous
			// snapshot instead of an error.
			scope := metrics.NewMockScope(ctrl)
			metricsClient.EXPECT().Scope(metrics.ShardDistributorRangeIndexScope).Return(scope)
			scope.EXPECT().Tagged(metrics.NamespaceTag(testRangeNamespace)).Return(scope)
			scope.EXPECT().IncCounter(metrics.ShardDistributorRangeIndexRebuildFailures)

			_, err = index.rebuild(context.Background())
			require.NoError(t, err)

			assignment, err = index.GetRangeForKey(context.Background(), testBound(0x80))
			require.NoError(t, err)
			assert.Equal(t, "executor-high", assignment.ExecutorID)
		})
	}
}

// GetRangeAssignments returns deep copies, so caller modifications cannot
// affect the snapshot used by lookups.
func TestNamespaceRangeIndex_GetRangeAssignmentsCopies(t *testing.T) {
	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().
		ListRanges(gomock.Any(), testRangeNamespace).
		Return(testCover(t, "executor-low", "executor-high"), nil)

	index := newTestRangeIndex(t, executorStore, metrics.NewNoopMetricsClient())

	assignments, err := index.GetRangeAssignments(context.Background())
	require.NoError(t, err)
	require.Len(t, assignments, 2)

	assignments[0].ExecutorID = "mutated"
	assignments[0].Range.LowInclusive[0] = 0xEE
	assignments[0].Range.HighExclusive[0] = 0xEE

	again, err := index.GetRangeAssignments(context.Background())
	require.NoError(t, err)
	require.Len(t, again, 2)
	assert.Equal(t, "executor-low", again[0].ExecutorID)
	assert.Equal(t, byte(0x00), again[0].Range.LowInclusive[0])
	assert.Equal(t, byte(0x80), again[0].Range.HighExclusive[0])
}

// A successful rebuild replaces the retained cover and restores fresh
// ownership information.
func TestNamespaceRangeIndex_RecoversAfterFailedRebuild(t *testing.T) {
	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	gomock.InOrder(
		executorStore.EXPECT().
			ListRanges(gomock.Any(), testRangeNamespace).
			Return(testCover(t, "executor-low", "executor-high"), nil),
		executorStore.EXPECT().
			ListRanges(gomock.Any(), testRangeNamespace).
			Return(nil, errors.New("etcd unavailable")),
		executorStore.EXPECT().
			ListRanges(gomock.Any(), testRangeNamespace).
			Return(testCover(t, "executor-recovered-low", "executor-recovered-high"), nil),
	)

	metricsClient := metrics.NewMockClient(ctrl)
	index := newTestRangeIndex(t, executorStore, metricsClient)

	assignment, err := index.GetRangeForKey(context.Background(), testBound(0x00))
	require.NoError(t, err)
	assert.Equal(t, "executor-low", assignment.ExecutorID)

	// The failed rebuild reports the failure and keeps the previous cover.
	scope := metrics.NewMockScope(ctrl)
	metricsClient.EXPECT().Scope(metrics.ShardDistributorRangeIndexScope).Return(scope)
	scope.EXPECT().Tagged(metrics.NamespaceTag(testRangeNamespace)).Return(scope)
	scope.EXPECT().IncCounter(metrics.ShardDistributorRangeIndexRebuildFailures)
	_, err = index.rebuild(context.Background())
	require.NoError(t, err)

	assignment, err = index.GetRangeForKey(context.Background(), testBound(0x00))
	require.NoError(t, err)
	assert.Equal(t, "executor-low", assignment.ExecutorID, "the retained cover still answers")

	// The next successful rebuild swaps in the new cover.
	_, err = index.rebuild(context.Background())
	require.NoError(t, err)

	assignment, err = index.GetRangeForKey(context.Background(), testBound(0x00))
	require.NoError(t, err)
	assert.Equal(t, "executor-recovered-low", assignment.ExecutorID)
}

// GetRangeAssignments fails closed like the lookup path when there is no cover
// to serve.
func TestNamespaceRangeIndex_GetRangeAssignmentsFailsClosedWithoutSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().
		ListRanges(gomock.Any(), testRangeNamespace).
		Return(nil, errors.New("etcd unavailable"))

	index := newTestRangeIndex(t, executorStore, expectRebuildFailureMetric(ctrl))

	assignments, err := index.GetRangeAssignments(context.Background())
	require.ErrorIs(t, err, ErrRangeIndexNotReady)
	assert.Nil(t, assignments)
}

// A cancelled caller sees its own context error while the flight completes:
// the joiner behind it must not inherit the cancellation.
func TestNamespaceRangeIndex_RebuildSurvivesCallerCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().
		ListRanges(gomock.Any(), testRangeNamespace).
		DoAndReturn(func(context.Context, string) ([]*store.RangeAssignment, error) {
			close(started)
			<-release
			return testCover(t, "executor-low", "executor-high"), nil
		}).
		Times(1)

	index := newTestRangeIndex(t, executorStore, metrics.NewNoopMetricsClient())

	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() {
		_, err := index.GetRangeForKey(ctx, testBound(0x00))
		cancelled <- err
	}()

	<-started
	cancel()
	require.ErrorIs(t, <-cancelled, context.Canceled)

	// A second caller joins the same flight and is served by it, not by the
	// cancellation of the first caller.
	assignment := make(chan *store.RangeAssignment, 1)
	lookupErr := make(chan error, 1)
	go func() {
		a, err := index.GetRangeForKey(context.Background(), testBound(0x80))
		assignment <- a
		lookupErr <- err
	}()

	close(release)
	require.NoError(t, <-lookupErr)
	assert.Equal(t, "executor-high", (<-assignment).ExecutorID)
}

// A hung etcd read must not hold the flight forever. The rebuild times out
// and reports the failure.
func TestNamespaceRangeIndex_RebuildTimeoutFailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().
		ListRanges(gomock.Any(), testRangeNamespace).
		DoAndReturn(func(ctx context.Context, namespace string) ([]*store.RangeAssignment, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})

	index := newTestRangeIndex(t, executorStore, expectRebuildFailureMetric(ctrl))
	index.refreshTimeout = 20 * time.Millisecond

	_, err := index.GetRangeForKey(context.Background(), testBound(0x00))
	require.ErrorIs(t, err, ErrRangeIndexNotReady)
}

// The holder only learns about a new cover from the namespace watch, so a
// signal must trigger a rebuild.
func TestNamespaceRangeIndex_RefreshLoopRebuildsOnSignal(t *testing.T) {
	changeCh := make(chan struct{}, 1)
	built := make(chan struct{}, 2)
	cover := testCover(t, "executor-low", "executor-high")

	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().
		SubscribeToNamespaceChanges(testRangeNamespace).
		Return((<-chan struct{})(changeCh), nil)
	executorStore.EXPECT().
		ListRanges(gomock.Any(), testRangeNamespace).
		Do(func(context.Context, string) { built <- struct{}{} }).
		Return(cover, nil).
		Times(2)

	index := newTestRangeIndex(t, executorStore, metrics.NewNoopMetricsClient())

	var wg sync.WaitGroup
	require.NoError(t, index.Start(&wg))

	for range 2 {
		changeCh <- struct{}{}
		select {
		case <-built:
		case <-time.After(time.Second):
			t.Fatal("watch signal did not trigger a rebuild")
		}
	}

	close(changeCh)
	wg.Wait()

	assignment, err := index.GetRangeForKey(context.Background(), testBound(0x80))
	require.NoError(t, err)
	assert.Equal(t, "executor-high", assignment.ExecutorID)
}
