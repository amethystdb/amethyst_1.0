package compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/metadata"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"fmt"
	"log"
	"sort"
)

type Executor interface {
	Execute(plan *Plan) (*common.SegmentMeta, error)
}

type executor struct {
	meta   metadata.Tracker
	reader reader.SSTableReader
	writer writer.SSTableWriter
}

func NewExecutor(
	meta metadata.Tracker,
	reader reader.SSTableReader,
	writer writer.SSTableWriter,
) *executor {
	return &executor{
		meta:   meta,
		reader: reader,
		writer: writer,
	}
}

func (e *executor) Execute(plan *Plan) (*common.SegmentMeta, error) {
	//validate ALL input segments BEFORE starting
	for i, seg := range plan.Inputs {
		if seg == nil {
			return nil, fmt.Errorf("input segment %d is nil", i)
		}
		if seg.IsObsolete() {
			return nil, fmt.Errorf("input segment %d (%s) is already obsolete", i, seg.ID)
		}
		if seg.Offset < 0 || seg.Length <= 0 {
			return nil, fmt.Errorf("input segment %d (%s) has invalid bounds: offset=%d, length=%d",
				i, seg.ID, seg.Offset, seg.Length)
		}
	}

	for _, seg := range plan.Inputs {
		seg.InCompaction.Store(true)
	}

	//ensure we always clear InCompaction flag
	defer func() {
		for _, seg := range plan.Inputs {
			seg.InCompaction.Store(false)
		}
	}()

	merged := make(map[string][]byte)

	// Scan all input segments. Higher index (newer) will overwrite older values.
	for _, seg := range plan.Inputs {
		//double-check segment is still valid before scanning
		if seg.IsObsolete() || !seg.InCompaction.Load() {
			//skip if it was obsoleted by another goroutine
			log.Printf("  [WARN] Skipping segment %s (obsolete=%v, inCompaction=%v)", seg.ID, seg.IsObsolete(), seg.InCompaction.Load())
			continue
		}

		data, err := e.reader.Scan(seg)
		if err != nil {
			//log but continue - don't fail entire compaction
			log.Printf("  [WARN] Failed to scan segment %s: %v", seg.ID, err)
			continue
		}
		for k, v := range data {
			if v != nil {
				merged[k] = v
			}
		}

		// Track that this segment is being rewritten
		e.meta.UpdateStats(seg.ID, 0, 1)
	}

	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	finalEntries := make([]common.KVEntry, 0, len(keys))
	for _, k := range keys {
		val := merged[k]
		finalEntries = append(finalEntries, common.KVEntry{
			Key:       k,
			Value:     val,
			Tombstone: val == nil, // nil from Scan correctly becomes a Tombstone here
		})
	}

	newSeg, err := e.writer.WriteSegment(finalEntries, plan.OutputStrategy, plan.TargetLevel)
	if err != nil {
		return nil, err
	}

	if newSeg != nil {
		e.meta.RegisterSegment(newSeg)
	}

	// Mark ALL inputs obsolete and clear InCompaction flag
	for _, seg := range plan.Inputs {
		seg.MarkObsolete()
		//removed seg.InCompaction.Store(false)
		e.meta.MarkObsolete(seg.ID)
	}

	// Improved logging for Suchi to see the merge happening
	log.Printf("ADAPTIVE MERGE: %d segs merged into 1 @ Level %d (Strategy: %v, Reason: %s)",
		len(plan.Inputs), newSeg.Level, plan.OutputStrategy, plan.Reason)

	return newSeg, nil
}
