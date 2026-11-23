package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/cadence-workflow/shard-manager/common/clock"
	"github.com/cadence-workflow/shard-manager/common/log/testlogger"
	"github.com/cadence-workflow/shard-manager/common/metrics"
	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
)

const testRangeNamespace = "range-namespace"

func testBound(first byte) []byte {
	return []byte{first, 0, 0, 0, 0, 0, 0, 0}
}

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

func testInvalidCover(t *testing.T) []*store.RangeAssignment {
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

func newTestRangeCover(t *testing.T, executorStore store.Store, metricsClient metrics.Client) *namespaceRangeCover {
	t.Helper()
	return newNamespaceRangeCover(testRangeNamespace, executorStore, testlogger.New(t), metricsClient)
}

func expectRebuildFailureMetric(ctrl *gomock.Controller, metricsClient *metrics.MockClient) {
	scope := metrics.NewMockScope(ctrl)
	metricsClient.EXPECT().Scope(metrics.ShardDistributorRangeCoverScope).Return(scope)
	scope.EXPECT().Tagged(metrics.NamespaceTag(testRangeNamespace)).Return(scope)
	scope.EXPECT().IncCounter(metrics.ShardDistributorRangeCoverRebuildFailures)
}

func TestNamespaceRangeCover_GetRangeForKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().
		ListRanges(gomock.Any(), testRangeNamespace).
		Return(testCover(t, "executor-low", "executor-high"), nil).
		Times(1)

	cover := newTestRangeCover(t, executorStore, metrics.NewNoopMetricsClient())

	for _, tt := range []struct {
		name string
		key  []byte
		want string
	}{
		{"low bound is inclusive", testBound(0x00), "executor-low"},
		{"interior of the lower range", testBound(0x7F), "executor-low"},
		{"shared boundary belongs to the upper range", testBound(0x80), "executor-high"},
		{"maximum fingerprint stays in the final range", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, "executor-high"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assignment, err := cover.GetRangeForKey(context.Background(), tt.key)
			require.NoError(t, err)
			assert.Equal(t, tt.want, assignment.ExecutorID)
		})
	}
}

func TestNamespaceRangeCover_FailsClosedWithoutSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name       string
		cover      []*store.RangeAssignment
		err        error
		wantMetric bool
	}{
		{"list ranges fails", nil, errors.New("etcd unavailable"), true},
		{"unbootstrapped empty namespace does not count as a failure", []*store.RangeAssignment{}, nil, false},
		{"cover does not start at the minimum bound", testInvalidCover(t), nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			executorStore := store.NewMockStore(ctrl)
			executorStore.EXPECT().ListRanges(gomock.Any(), testRangeNamespace).Return(tt.cover, tt.err)

			metricsClient := metrics.NewMockClient(ctrl)
			if tt.wantMetric {
				expectRebuildFailureMetric(ctrl, metricsClient)
			}

			cover := newTestRangeCover(t, executorStore, metricsClient)
			_, err := cover.GetRangeForKey(context.Background(), testBound(0x00))
			require.ErrorIs(t, err, ErrRangeCoverNotReady)
		})
	}
}

func TestNamespaceRangeCover_RetainsLastKnownGoodAndRecovers(t *testing.T) {
	for _, tt := range []struct {
		name  string
		cover []*store.RangeAssignment
		err   error
	}{
		{"list ranges fails on rebuild", nil, errors.New("etcd unavailable")},
		{"cover disappears after bootstrap", []*store.RangeAssignment{}, nil},
		{"rebuilt cover is invalid", testInvalidCover(t), nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			executorStore := store.NewMockStore(ctrl)
			gomock.InOrder(
				executorStore.EXPECT().ListRanges(gomock.Any(), testRangeNamespace).Return(testCover(t, "executor-low", "executor-high"), nil),
				executorStore.EXPECT().ListRanges(gomock.Any(), testRangeNamespace).Return(tt.cover, tt.err),
				executorStore.EXPECT().ListRanges(gomock.Any(), testRangeNamespace).Return(testCover(t, "executor-recovered-low", "executor-recovered-high"), nil),
			)

			metricsClient := metrics.NewMockClient(ctrl)
			cover := newTestRangeCover(t, executorStore, metricsClient)

			assignment, err := cover.GetRangeForKey(context.Background(), testBound(0x00))
			require.NoError(t, err)
			assert.Equal(t, "executor-low", assignment.ExecutorID)

			expectRebuildFailureMetric(ctrl, metricsClient)
			require.NoError(t, cover.refresh(context.Background()))

			assignment, err = cover.GetRangeForKey(context.Background(), testBound(0x80))
			require.NoError(t, err)
			assert.Equal(t, "executor-high", assignment.ExecutorID)

			require.NoError(t, cover.refresh(context.Background()))

			assignment, err = cover.GetRangeForKey(context.Background(), testBound(0x00))
			require.NoError(t, err)
			assert.Equal(t, "executor-recovered-low", assignment.ExecutorID)
		})
	}
}

func TestNamespaceRangeCover_RebuildSurvivesCallerCancellation(t *testing.T) {
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

	cover := newTestRangeCover(t, executorStore, metrics.NewNoopMetricsClient())

	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() {
		_, err := cover.GetRangeForKey(ctx, testBound(0x00))
		cancelled <- err
	}()

	<-started
	cancel()
	require.ErrorIs(t, <-cancelled, context.Canceled)

	assignment := make(chan *store.RangeAssignment, 1)
	lookupErr := make(chan error, 1)
	go func() {
		a, err := cover.GetRangeForKey(context.Background(), testBound(0x80))
		assignment <- a
		lookupErr <- err
	}()

	close(release)
	require.NoError(t, <-lookupErr)
	assert.Equal(t, "executor-high", (<-assignment).ExecutorID)
}

func TestNamespaceRangeCover_RebuildTimeoutFailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().
		ListRanges(gomock.Any(), testRangeNamespace).
		DoAndReturn(func(ctx context.Context, namespace string) ([]*store.RangeAssignment, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})

	metricsClient := metrics.NewMockClient(ctrl)
	expectRebuildFailureMetric(ctrl, metricsClient)

	cover := newTestRangeCover(t, executorStore, metricsClient)
	cover.refreshTimeout = 20 * time.Millisecond

	_, err := cover.GetRangeForKey(context.Background(), testBound(0x00))
	require.ErrorIs(t, err, ErrRangeCoverNotReady)
}

func TestShardCache_RangeModeRefreshesCoverFromNamespaceWatch(t *testing.T) {
	changeCh := make(chan struct{}, 1)
	built := make(chan struct{}, 2)

	ctrl := gomock.NewController(t)
	executorStore := store.NewMockStore(ctrl)
	executorStore.EXPECT().SubscribeToNamespaceChanges(testRangeNamespace).Return((<-chan struct{})(changeCh), nil).Times(1)
	executorStore.EXPECT().GetAssignmentState(gomock.Any(), testRangeNamespace).Return(&store.AssignmentState{Revision: 1}, nil).Times(2)
	gomock.InOrder(
		executorStore.EXPECT().
			ListRanges(gomock.Any(), testRangeNamespace).
			Do(func(context.Context, string) { built <- struct{}{} }).
			Return([]*store.RangeAssignment{}, nil),
		executorStore.EXPECT().
			ListRanges(gomock.Any(), testRangeNamespace).
			Do(func(context.Context, string) { built <- struct{}{} }).
			Return(testCover(t, "executor-low", "executor-high"), nil),
	)

	c := newShardCache(testRangeNamespace, executorStore, testlogger.New(t), clock.NewMockedTimeSource(), ShardCacheOptions{
		MetricsClient: metrics.NewNoopMetricsClient(),
		RangeMode:     true,
	})
	require.NoError(t, c.Start())
	defer c.Stop()

	for range 2 {
		changeCh <- struct{}{}
		select {
		case <-built:
		case <-time.After(time.Second):
			t.Fatal("watch signal did not refresh the range cover")
		}
	}

	require.Eventually(t, func() bool {
		return c.rangeCover.currentCover() != nil
	}, time.Second, time.Millisecond)

	assignment, err := c.GetRangeForShardKey(context.Background(), testBound(0x80))
	require.NoError(t, err)
	assert.Equal(t, "executor-high", assignment.ExecutorID)
}
