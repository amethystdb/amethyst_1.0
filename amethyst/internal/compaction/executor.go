package compaction

import (
	"amethyst/internal/common"
	"amethyst/internal/metadata"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
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
	merged := make(map[string][]byte)

	// Scan all input segments. Higher index (newer) will overwrite older values.
	for _, seg := range plan.Inputs {
		data, err := e.reader.Scan(seg)
		if err != nil {
			return nil, err
		}
		for k, v := range data {
			merged[k] = v
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

	newSeg, err := e.writer.WriteSegment(finalEntries, plan.OutputStrategy)
	if err != nil {
		return nil, err
	}

	if newSeg != nil {
		e.meta.RegisterSegment(newSeg)
	}

	// Mark ALL inputs obsolete. This is what drops your segment count to 1.
	for _, seg := range plan.Inputs {
		e.meta.MarkObsolete(seg.ID)
	}

	// Improved logging for Suchi to see the merge happening
	log.Printf("ADAPTIVE MERGE: %d segments -> 1 (Strategy: %v, Reason: %s)",
		len(plan.Inputs), plan.OutputStrategy, plan.Reason)

	return newSeg, nil
}
