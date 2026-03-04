package engine

import (
	"amethyst/internal/common"
	"amethyst/internal/memtable"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sstable/writer"
	"amethyst/internal/wal"
	"fmt"
)

type Engine struct {
	wal    wal.WAL
	mem    memtable.Memtable
	sfm    segmentfile.SegmentFileManager
	writer writer.SSTableWriter
}

// initializes pipe
func NewEngine(w wal.WAL, m memtable.Memtable, s segmentfile.SegmentFileManager, sw writer.SSTableWriter) *Engine {
	return &Engine{
		wal:    w,
		mem:    m,
		sfm:    s,
		writer: sw,
	}
}

// handles the WAL -> Memtable flow
func (e *Engine) Put(key string, value []byte) error {
	// Log to WAL for durability
	if err := e.wal.LogPut(key, value); err != nil {
		return fmt.Errorf("WAL log failure: %w", err)
	}

	//Insert into Memtable
	e.mem.Put(key, value)

	//Check if Memtable reached its limit
	if e.mem.ShouldFlush() {
		return e.ExecuteFlush()
	}

	return nil
}

// handles the Memtable -> SSTable -> Truncate flow
func (e *Engine) ExecuteFlush() error {
	fmt.Println("Threshold reached: Starting Flush Plumbing...")

	//Get sorted data from Memtable
	data := e.mem.Flush()

	//Hand off to the SSTable Writer (The disk storage logic)
	//TIERED default for new flushes?
	// Pass 0 as the level
	_, err := e.writer.WriteSegment(data, common.TIERED, 0)
	if err != nil {
		return fmt.Errorf("SSTable write failure: %w", err)
	}

	// only truncate WAL after disk write is confirmed
	if err := e.wal.Truncate(); err != nil {
		return fmt.Errorf("WAL cleanup failure: %w", err)
	}

	fmt.Println("Flush complete: Memtable cleared and WAL truncated.")
	return nil
}
