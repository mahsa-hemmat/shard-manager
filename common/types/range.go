package types

import (
	"bytes"
	"encoding/hex"
	"errors"
)

// Range represents a contiguous interval of the key space [LowInclusive, HighExclusive).
// Ranges are the unit of ownership in the range-based model, replacing discrete shard assignments.
type Range struct {
	// LowInclusive is the inclusive lower bound in the ordered key space.
	LowInclusive []byte
	// HighExclusive is the exclusive upper bound in the ordered key space.
	HighExclusive []byte
}

// Errors returned when constructing an invalid range.
var (
	ErrRangeInvalid   = errors.New("range is invalid: LowInclusive must be < HighExclusive")
	ErrRangeNilBounds = errors.New("range bounds cannot be nil")
)

// NewRange creates a new Range with the given bounds.
func NewRange(lowInclusive, highExclusive []byte) (*Range, error) {
	if lowInclusive == nil || highExclusive == nil {
		return nil, ErrRangeNilBounds
	}

	r := &Range{
		LowInclusive:  bytes.Clone(lowInclusive),
		HighExclusive: bytes.Clone(highExclusive),
	}

	if !r.IsValid() {
		return nil, ErrRangeInvalid
	}

	return r, nil
}

// RangeUpperBoundInfinity is the exclusive upper bound of a final range, which
// extends to the end of the key space. It is one byte longer than a fingerprint
// so that byte comparison orders it after every finite fingerprint
// while it remains a valid upper bound for the half-open interval.
var RangeUpperBoundInfinity = bytes.Repeat([]byte{0xFF}, 9)

// NewFinalRange creates the final range [lowInclusive, infinity), covering every
// key greater than or equal to lowInclusive.
func NewFinalRange(lowInclusive []byte) (*Range, error) {
	if lowInclusive == nil {
		return nil, ErrRangeNilBounds
	}

	r := &Range{
		LowInclusive:  bytes.Clone(lowInclusive),
		HighExclusive: bytes.Clone(RangeUpperBoundInfinity),
	}

	if !r.IsValid() {
		return nil, ErrRangeInvalid
	}

	return r, nil
}

// IsValid reports whether the range is well-formed: non-nil, with non-nil bounds
// and LowInclusive strictly less than HighExclusive.
func (r *Range) IsValid() bool {
	if r == nil || r.LowInclusive == nil || r.HighExclusive == nil {
		return false
	}
	return bytes.Compare(r.LowInclusive, r.HighExclusive) < 0
}

// Contains reports whether key lies in the half-open interval
// [LowInclusive, HighExclusive). A nil key or an invalid range returns false.
func (r *Range) Contains(key []byte) bool {
	if !r.IsValid() || key == nil {
		return false
	}
	return bytes.Compare(r.LowInclusive, key) <= 0 && bytes.Compare(key, r.HighExclusive) < 0
}

// Overlaps reports whether the two ranges share at least one key.
func (r *Range) Overlaps(other *Range) bool {
	if !r.IsValid() || !other.IsValid() {
		return false
	}
	return bytes.Compare(r.LowInclusive, other.HighExclusive) < 0 &&
		bytes.Compare(other.LowInclusive, r.HighExclusive) < 0
}

// Adjacent reports whether the two ranges touch at exactly one boundary, with no
// gap and no overlap.
func (r *Range) Adjacent(other *Range) bool {
	if !r.IsValid() || !other.IsValid() {
		return false
	}
	return bytes.Equal(r.HighExclusive, other.LowInclusive) ||
		bytes.Equal(other.HighExclusive, r.LowInclusive)
}

// Compare orders valid ranges by their lower bounds. It returns:
//
//	-1 if r < other (r.LowInclusive < other.LowInclusive)
//	 0 if r == other (same bounds)
//	+1 if r > other (r.LowInclusive > other.LowInclusive)
func (r *Range) Compare(other *Range) int {
	if !r.IsValid() || !other.IsValid() {
		return 0
	}
	return bytes.Compare(r.LowInclusive, other.LowInclusive)
}

// IsFinalRange reports whether the range extends to the end of the key space.
func (r *Range) IsFinalRange() bool {
	return r != nil && bytes.Equal(r.HighExclusive, RangeUpperBoundInfinity)
}

func (r *Range) GetLowInclusive() []byte {
	if r == nil {
		return nil
	}
	return r.LowInclusive
}

func (r *Range) GetHighExclusive() []byte {
	if r == nil {
		return nil
	}
	return r.HighExclusive
}

// String returns a bounded hexadecimal representation used for logs.
func (r *Range) String() string {
	if !r.IsValid() {
		return "<invalid range>"
	}

	return "[" + hex.EncodeToString(r.LowInclusive) + ", " + hex.EncodeToString(r.HighExclusive) + ")"
}

// RangeAssignment is the RPC-layer ownership record for one range. It mirrors
// the wire type and carries no ModRevision; the storage-layer counterpart with
// an optimistic-lock revision is store.RangeAssignment.
type RangeAssignment struct {
	Range      *Range
	ExecutorID string
	Status     AssignmentStatus
}

func (v *RangeAssignment) GetRange() (o *Range) {
	if v != nil {
		return v.Range
	}
	return
}

func (v *RangeAssignment) GetExecutorID() (o string) {
	if v != nil {
		return v.ExecutorID
	}
	return
}

func (v *RangeAssignment) GetStatus() (o AssignmentStatus) {
	if v != nil {
		return v.Status
	}
	return
}

// RangeLoadReport is the load report for a single range in a range-mode heartbeat.
type RangeLoadReport struct {
	// RangeLoad is the total load falling inside the range.
	RangeLoad float64
	// TopTasklists holds the highest-load tasklists in the range, keyed by
	// tasklist name. It is an unordered bounded top-N: the bound is
	// not encoded here, and map iteration order is unspecified.
	// A consumer that needs rank must sort by load with a deterministic tie-break.
	TopTasklists map[string]float64
}

func (v *RangeLoadReport) GetRangeLoad() (o float64) {
	if v != nil {
		return v.RangeLoad
	}
	return
}

func (v *RangeLoadReport) GetTopTasklists() (o map[string]float64) {
	if v != nil {
		return v.TopTasklists
	}
	return
}
