package common

import (
	"sync"
	"sync/atomic"
)

type CompactionType int

const (
	TIERED CompactionType = iota
	LEVELED
)

type SegmentMeta struct {
	ID     string
	Level  int 
	Offset int64
	Length int64

	MinKey string
	MaxKey string

	Strategy CompactionType

	// Stats - use atomic operations for thread safety
	readCount    int64
	writeCount   int64
	OverlapCount int64

	CreatedAt     int64
	lastRewriteAt int64

	// Protected by mutex
	obsolete bool
	mu       sync.RWMutex

	// Atomic flag for compaction tracking
	InCompaction atomic.Bool

	SparseIndex       interface{}
	DataStartOffset   int64
	SparseIndexOffset int64
}

// thread safe methods for updating stats
func (s *SegmentMeta) IncrementReads() {
	atomic.AddInt64(&s.readCount, 1)
}

func (s *SegmentMeta) IncrementWrites() {
	atomic.AddInt64(&s.writeCount, 1)
}

func (s *SegmentMeta) AddReads(count int64) {
	atomic.AddInt64(&s.readCount, count)
}

func (s *SegmentMeta) AddWrites(count int64) {
	atomic.AddInt64(&s.writeCount, count)
}

func (s *SegmentMeta) GetReadCount() int64 {
	return atomic.LoadInt64(&s.readCount)
}

func (s *SegmentMeta) GetWriteCount() int64 {
	return atomic.LoadInt64(&s.writeCount)
}

// thread safe obsolete marking
func (s *SegmentMeta) MarkObsolete() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.obsolete = true
}

func (s *SegmentMeta) IsObsolete() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.obsolete
}

// Size returns the on-disk size of the segment in bytes for compaction decision
func (s *SegmentMeta) Size() int64 {
	return s.Length
}

// ReadWriteRatio returns reads / writes, guarding against divide-by-zero.
// If WriteCount == 0, treat ratio as ReadCount.
// This is one of the parameters used by the fsm, from the parameters of the segment
func (s *SegmentMeta) ReadWriteRatio() float64 {
	reads := s.GetReadCount()
	writes := s.GetWriteCount()
	if writes == 0 {
		return float64(reads)
	}
	return float64(reads) / float64(writes)
}

// CooldownExpired returns true if enough time has passed since last rewrite.
func (s *SegmentMeta) CooldownExpired(now int64, minInterval int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return now-s.lastRewriteAt >= minInterval
}

// UpdateLastRewriteAt updates the last compaction timestamp
func (s *SegmentMeta) UpdateLastRewriteAt(timestamp int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRewriteAt = timestamp
}

type WALEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
}

// memtable sorted key entry
type KVEntry struct {
	Key       string
	Value     []byte
	Tombstone bool
}
