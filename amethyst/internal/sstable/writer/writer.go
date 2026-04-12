package writer

import (
	"amethyst/internal/common"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Global counters for physical and compaction writes
var GlobalPhysicalWriteBytes int64
var GlobalCompactionWriteBytes int64

type SSTableWriter interface {
	// Updated to accept the sorted slice from Memtable
	WriteSegment(
		sortedData []common.KVEntry,
		strategy common.CompactionType,
		level int,
	) (*common.SegmentMeta, error)
}

type writer struct {
	fileMgr      segmentfile.SegmentFileManager
	indexBuilder sparseindex.Builder
}

func NewWriter(fileMgr segmentfile.SegmentFileManager, indexBuilder sparseindex.Builder) *writer {
	return &writer{
		fileMgr:      fileMgr,
		indexBuilder: indexBuilder,
	}
}

func (w *writer) WriteSegment(
	sortedData []common.KVEntry,
	strategy common.CompactionType,
	level int, // <-- ADDED
) (*common.SegmentMeta, error) {
	segmentID := uuid.New().String()
	now := time.Now().Unix()

	buf := make([]byte, 0, 1024)

	// Helper to write length-prefixed strings
	writeString := func(s string) {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(s)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(s)...)
	}

	// 1. Header: ID, MinKey, MaxKey
	writeString(segmentID)

	if len(sortedData) == 0 {
		return nil, nil // Or handle empty flush appropriately
	}

	minKey := sortedData[0].Key
	maxKey := sortedData[len(sortedData)-1].Key

	writeString(minKey)
	writeString(maxKey)

	// 2. Metadata: Strategy and Record Count
	buf = append(buf, byte(strategy))
	tmp8 := make([]byte, 8)
	binary.BigEndian.PutUint64(tmp8, uint64(len(sortedData)))
	buf = append(buf, tmp8...)

	// 3. Actual Data Entries
	// We track keys and offsets specifically for the Sparse Index
	keysForIndex := make([]string, 0, len(sortedData))
	offsetsForIndex := make([]int64, 0, len(sortedData))
	dataStartOffset := int64(len(buf))

	for _, entry := range sortedData {
		// Calculate offset relative to data start for the index
		offsetsForIndex = append(offsetsForIndex, int64(len(buf))-dataStartOffset)
		keysForIndex = append(keysForIndex, entry.Key)

		tmp := make([]byte, 9)
		binary.BigEndian.PutUint32(tmp[0:4], uint32(len(entry.Key)))
		binary.BigEndian.PutUint32(tmp[4:8], uint32(len(entry.Value)))

		if entry.Tombstone {
			tmp[8] = 1
		} else {
			tmp[8] = 0
		}

		buf = append(buf, tmp...)
		buf = append(buf, []byte(entry.Key)...)
		buf = append(buf, entry.Value...)
	}

	// 4. Build and Serialize Sparse Index
	sparse := w.indexBuilder.Build(keysForIndex, offsetsForIndex)
	sparseOffset := int64(len(buf))

	for i, k := range sparse.Keys {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(len(k)))
		buf = append(buf, tmp...)
		buf = append(buf, []byte(k)...)

		tmp8_idx := make([]byte, 8)
		binary.BigEndian.PutUint64(tmp8_idx, uint64(sparse.Offsets[i]))
		buf = append(buf, tmp8_idx...)
	}

	// 5. Footer: Pointer to where the Sparse Index starts
	tmp8_footer := make([]byte, 8)
	binary.BigEndian.PutUint64(tmp8_footer, uint64(sparseOffset))
	buf = append(buf, tmp8_footer...)

	// 6. Final Disk Write
	offset, length, err := w.fileMgr.Append(buf)
	if err != nil {
		return nil, err
	}
	if level == 0 {
		// Count this as a direct Flush from Memtable
		atomic.AddInt64(&GlobalPhysicalWriteBytes, length)
	} else {
		// Count this as background Compaction work
		atomic.AddInt64(&GlobalCompactionWriteBytes, length)
	}

	meta := &common.SegmentMeta{
		ID:       segmentID,
		Level:    level, // <-- ADDED
		Offset:   offset,
		Length:   length,
		MinKey:   minKey,
		MaxKey:   maxKey,
		Strategy: strategy,
		// readCount, writeCount are zero-initialized (private fields)
		OverlapCount: 0, // Will be updated by Tracker
		CreatedAt:    now,
		// lastRewriteAt is set via method after creation
		// obsolete is false by default (private field)
		SparseIndex:       sparse,
		DataStartOffset:   dataStartOffset,
		SparseIndexOffset: sparseOffset,
	}

	// Set lastRewriteAt using the thread-safe method
	meta.UpdateLastRewriteAt(now)

	return meta, nil
}
