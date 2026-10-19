// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package proto

import (
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharddistributorv1 "github.com/cadence-workflow/shard-manager/.gen/proto/sharddistributor/v1"
	"github.com/cadence-workflow/shard-manager/common/types"
	"github.com/cadence-workflow/shard-manager/common/types/mapper/testutils"
)

// bound8 builds an 8-byte big-endian bound with the given first byte and zeroes
// elsewhere.
func bound8(first byte) []byte {
	return []byte{first, 0, 0, 0, 0, 0, 0, 0}
}

// finiteRange builds a finite [low, high) range from first-byte bounds.
func finiteRange(low, high byte) *types.Range {
	return &types.Range{LowInclusive: bound8(low), HighExclusive: bound8(high)}
}

// finalRange builds the final range [low, ∞).
func finalRange(low byte) *types.Range {
	return &types.Range{LowInclusive: bound8(low), HighExclusive: types.RangeUpperBoundInfinity}
}

func TestRangeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		r    *types.Range
	}{
		{"minimum-bound", finiteRange(0x00, 0x40)},
		{"mid", finiteRange(0x40, 0x80)},
		{"max-fingerprint-high", &types.Range{LowInclusive: bound8(0xC0), HighExclusive: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}}},
		{"final", finalRange(0xC0)},
		{"nil", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.r, toShardDistributorRange(fromShardDistributorRange(tc.r)))
		})
	}
}

func TestRangeFinalEncoding(t *testing.T) {
	final := finalRange(0xC0)

	proto := fromShardDistributorRange(final)
	require.NotNil(t, proto)
	assert.True(t, final.IsFinalRange())
	// A final range's infinite upper bound is empty on the wire.
	assert.Empty(t, proto.HighExclusive)

	// An empty high_exclusive decodes back to the final range.
	round := toShardDistributorRange(&sharddistributorv1.Range{LowInclusive: final.LowInclusive})
	require.NotNil(t, round)
	assert.True(t, round.IsFinalRange())
	assert.Equal(t, types.RangeUpperBoundInfinity, round.HighExclusive)
}

func TestRangeAssignmentRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		a    *types.RangeAssignment
	}{
		{
			name: "finite",
			a:    &types.RangeAssignment{Range: finiteRange(0x00, 0x40), ExecutorID: "executor-1", Status: types.AssignmentStatusREADY},
		},
		{
			name: "final",
			a:    &types.RangeAssignment{Range: finalRange(0xC0), ExecutorID: "executor-2", Status: types.AssignmentStatusINVALID},
		},
		{
			name: "nil-range",
			a:    &types.RangeAssignment{Range: nil, ExecutorID: "executor-3", Status: types.AssignmentStatusREADY},
		},
		{"nil", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.a, toShardDistributorRangeAssignment(fromShardDistributorRangeAssignment(tc.a)))
		})
	}
}

func TestRangeLoadReportRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		r    *types.RangeLoadReport
	}{
		{
			name: "with-top-tasklists",
			r: &types.RangeLoadReport{
				RangeLoad:    1.5,
				TopTasklists: map[string]float64{"tasklist-a": 0.9, "tasklist-b": 0.6},
			},
		},
		{"empty-map", &types.RangeLoadReport{RangeLoad: 0.5, TopTasklists: map[string]float64{}}},
		{"nil-map", &types.RangeLoadReport{RangeLoad: 0.5, TopTasklists: nil}},
		{"nil", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := toShardDistributorRangeLoadReport(fromShardDistributorRangeLoadReport(tc.r))
			assert.Equal(t, tc.r, result)
			if tc.r == nil {
				assert.Nil(t, result)
				return
			}
			// A nil map must stay nil, a non-nil map must stay non-nil: the
			// mapper preserves the nil-vs-empty distinction.
			if tc.r.TopTasklists == nil {
				assert.Nil(t, result.TopTasklists)
			} else {
				assert.NotNil(t, result.TopTasklists)
			}
		})
	}
}

// TestRangeMapperDoesNotAlias asserts the range mappers copy bounds rather than
// sharing backing arrays, and that decoding an empty high_exclusive never hands
// out the shared types.RangeUpperBoundInfinity slice.
func TestRangeMapperDoesNotAlias(t *testing.T) {
	internal := finiteRange(0x00, 0x40)
	proto := fromShardDistributorRange(internal)

	require.NotNil(t, proto)
	proto.LowInclusive[0] = 0xEE
	proto.HighExclusive[0] = 0xEE
	assert.Equal(t, byte(0x00), internal.LowInclusive[0], "proto must not share the internal low bound")
	assert.Equal(t, byte(0x40), internal.HighExclusive[0], "proto must not share the internal high bound")

	decoded := toShardDistributorRange(&sharddistributorv1.Range{LowInclusive: []byte{0x01, 0x02}})
	require.NotNil(t, decoded)
	assert.True(t, decoded.IsFinalRange())
	decoded.HighExclusive[0] = 0x00
	assert.Equal(t, byte(0xFF), types.RangeUpperBoundInfinity[0], "decoded final range must not share the global sentinel")
}

// TestRangeLoadReportDoesNotAlias asserts the top-N map is deep-copied in both
// directions, so mutating one side never changes the other.
func TestRangeLoadReportDoesNotAlias(t *testing.T) {
	internal := &types.RangeLoadReport{
		RangeLoad:    1.5,
		TopTasklists: map[string]float64{"tasklist-a": 0.9},
	}

	proto := fromShardDistributorRangeLoadReport(internal)
	require.NotNil(t, proto)
	proto.TopTasklists["tasklist-a"] = 0.1
	proto.TopTasklists["tasklist-b"] = 0.2
	assert.Equal(t, map[string]float64{"tasklist-a": 0.9}, internal.TopTasklists,
		"mutating the proto map must not change the internal map")

	decoded := toShardDistributorRangeLoadReport(proto)
	require.NotNil(t, decoded)
	decoded.TopTasklists["tasklist-a"] = 7.7
	decoded.TopTasklists["tasklist-c"] = 3.3
	assert.Equal(t, map[string]float64{"tasklist-a": 0.1, "tasklist-b": 0.2}, proto.TopTasklists,
		"mutating the decoded map must not change the proto map")
}

// protoWireFieldCount counts the fields that carry a protobuf tag, i.e. the
// declared wire fields. The gogo-generated structs add unexported XXX_* helper
// fields that a plain NumField count would wrongly include.
func protoWireFieldCount(proto interface{}) int {
	typ := reflect.TypeOf(proto)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	count := 0
	for i := 0; i < typ.NumField(); i++ {
		if _, ok := typ.Field(i).Tag.Lookup("protobuf"); ok {
			count++
		}
	}
	return count
}

// TestRangeFieldCountGuards pins each internal type's field count to its proto
// message's wire field count. A new field on either side — including tag 8
// range_load_reports on HeartbeatRequest — forces the mapper to be revisited.
func TestRangeFieldCountGuards(t *testing.T) {
	tests := []struct {
		name     string
		internal interface{}
		proto    interface{}
	}{
		{"ExecutorHeartbeatRequest", types.ExecutorHeartbeatRequest{}, sharddistributorv1.HeartbeatRequest{}},
		{"ExecutorHeartbeatResponse", types.ExecutorHeartbeatResponse{}, sharddistributorv1.HeartbeatResponse{}},
		{"WatchNamespaceStateResponse", types.WatchNamespaceStateResponse{}, sharddistributorv1.WatchNamespaceStateResponse{}},
		{"GetFullNamespaceStateResponse", types.GetFullNamespaceStateResponse{}, sharddistributorv1.GetFullNamespaceStateResponse{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t,
				reflect.TypeOf(tc.internal).NumField(),
				protoWireFieldCount(tc.proto),
				"field count mismatch; update the mapper when a field is added to either side",
			)
		})
	}
}

// RangeFuzzer normalizes an empty or nil HighExclusive to the final-range
// sentinel. On the wire an empty high_exclusive means "extends to the end of
// the key space", so an empty/nil bound round-trips to the sentinel rather than
// to itself.
func RangeFuzzer(r *types.Range, c fuzz.Continue) {
	c.FuzzNoCustom(r)
	if len(r.HighExclusive) == 0 {
		r.HighExclusive = types.RangeUpperBoundInfinity
	}
}

func TestRangeFuzz(t *testing.T) {
	testutils.RunMapperFuzzTest(t, fromShardDistributorRange, toShardDistributorRange,
		testutils.WithCustomFuncs(RangeFuzzer),
	)
}

func TestRangeAssignmentFuzz(t *testing.T) {
	testutils.RunMapperFuzzTest(t, fromShardDistributorRangeAssignment, toShardDistributorRangeAssignment,
		testutils.WithCustomFuncs(RangeFuzzer, AssignmentStatusFuzzer),
	)
}

func TestRangeLoadReportFuzz(t *testing.T) {
	testutils.RunMapperFuzzTest(t, fromShardDistributorRangeLoadReport, toShardDistributorRangeLoadReport)
}
