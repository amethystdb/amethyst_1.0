package metadata

import (
	"amethyst/internal/common"
	"sort"
	"sync"
)

// smol change only added isobsolete to this file instead of obsolete func
type Tracker interface {
	RegisterSegment(meta *common.SegmentMeta)
	GetSegmentsForKey(key string) []*common.SegmentMeta
	GetAllSegments() []*common.SegmentMeta
	GetOverlappingSegments(target *common.SegmentMeta) []*common.SegmentMeta

	MarkObsolete(id string)
	UpdateStats(id string, reads int64, writes int64)
}

type tracker struct {
	mu       sync.RWMutex //RWMutex for better read performance
	segments map[string]*common.SegmentMeta
	ordered  []*common.SegmentMeta
}

// NewTracker creates a new MetadataTracker.
func NewTracker() Tracker {
	return &tracker{
		segments: make(map[string]*common.SegmentMeta),
		ordered:  make([]*common.SegmentMeta, 0),
	}
}

func (t *tracker) RegisterSegment(meta *common.SegmentMeta) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var overlaps int64
	for _, other := range t.segments {
		if other.IsObsolete() {
			continue
		}
		//two segments overlap if they don't sit entirely to the left or right of each other
		//core metric of Adaptive transition proof
		if !(meta.MaxKey < other.MinKey || meta.MinKey > other.MaxKey) {
			overlaps++
		}
	}

	//FSM to detects "Tiered" behavior (high overlap) and transition to "Leveled" (zero overlap)
	meta.OverlapCount = overlaps

	t.segments[meta.ID] = meta
	t.ordered = append([]*common.SegmentMeta{meta}, t.ordered...)
}

// this fixes the "MissingFieldOrMethod" error in your screenshot
func (t *tracker) GetOverlappingSegments(target *common.SegmentMeta) []*common.SegmentMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var overlaps []*common.SegmentMeta
	for _, seg := range t.segments {
		if seg.ID == target.ID || seg.IsObsolete() {
			continue
		}
		//if ranges touch then overlap
		if !(target.MaxKey < seg.MinKey || target.MinKey > seg.MaxKey) {
			overlaps = append(overlaps, seg)
		}
	}
	return overlaps
}

func (t *tracker) GetSegmentsForKey(key string) []*common.SegmentMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]*common.SegmentMeta, 0)
	for _, seg := range t.ordered {
		if seg.IsObsolete() {
			continue
		}
		if key >= seg.MinKey && key <= seg.MaxKey {
			result = append(result, seg)
		}
	}
	// Sort by Level (ascending), then CreatedAt (descending)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Level != result[j].Level {
			return result[i].Level < result[j].Level
		}
		return result[i].CreatedAt > result[j].CreatedAt
	})
	return result
}

func (t *tracker) GetAllSegments() []*common.SegmentMeta {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*common.SegmentMeta, 0, len(t.ordered))
	for _, seg := range t.ordered {
		if !seg.IsObsolete() {
			result = append(result, seg)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Level != result[j].Level {
			return result[i].Level < result[j].Level
		}
		return result[i].CreatedAt > result[j].CreatedAt
	})

	return result
}

func (t *tracker) MarkObsolete(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if seg, ok := t.segments[id]; ok {
		seg.MarkObsolete()
	}
}

func (t *tracker) UpdateStats(id string, reads int64, writes int64) {
	t.mu.RLock() //to only need read access to find the segment
	seg, ok := t.segments[id]
	t.mu.RUnlock()
	if ok {
		seg.AddReads(reads) //use atomics internally
		seg.AddWrites(writes)
	}
}
