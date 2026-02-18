package main

import (
	"amethyst/internal/adaptive"
	"amethyst/internal/common"
	"amethyst/internal/compaction"
	"amethyst/internal/memtable"
	"amethyst/internal/metadata"
	"amethyst/internal/segmentfile"
	"amethyst/internal/sparseindex"
	"amethyst/internal/sstable/reader"
	"amethyst/internal/sstable/writer"
	"amethyst/internal/wal"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	workloadFlag  = flag.String("workload", "shift", "Workload type")
	numKeysFlag   = flag.Int("keys", 10000000, "Number of keys")
	valueSizeFlag = flag.Int("value-size", 256, "Value size in bytes")
	engineFlag    = flag.String("engine", "adaptive", "Engine name for output")
)

// Results structure for JSON output
type Results struct {
	Engine             string        `json:"engine"`
	Workload           string        `json:"workload"`
	NumKeys            int           `json:"num_keys"`
	ValueSize          int           `json:"value_size"`
	WriteAmplification float64       `json:"write_amplification"`
	ReadAmplification  float64       `json:"read_amplification"`
	SpaceAmplification float64       `json:"space_amplification"`
	CompactionCount    int           `json:"compaction_count"`
	TotalDurationSec   float64       `json:"total_duration_sec"`
	LogicalBytes       int64         `json:"logical_bytes"`
	PhysicalBytes      int64         `json:"physical_bytes"`
	TotalReads         int64         `json:"total_reads"`
	SegmentScans       int64         `json:"segment_scans"`
	LiveDataBytes      int64         `json:"live_data_bytes"`
	TotalDiskBytes     int64         `json:"total_disk_bytes"`
	Phases             []PhaseResult `json:"phases,omitempty"`
}

type PhaseResult struct {
	Name     string  `json:"name"`
	Duration float64 `json:"duration_sec"`
	WA       float64 `json:"wa"`
	RA       float64 `json:"ra"`
	LatP50   float64 `json:"lat_p50_us"`
	LatP95   float64 `json:"lat_p95_us"`
	LatP99   float64 `json:"lat_p99_us"`
}

// LatencyTracker collects latency samples
type LatencyTracker struct {
	ch      chan int64
	samples []int64
	mu      sync.Mutex
}

func NewLatencyTracker(capacity int) *LatencyTracker {
	return &LatencyTracker{
		ch:      make(chan int64, capacity),
		samples: make([]int64, 0, capacity),
	}
}

func (lt *LatencyTracker) Record(d time.Duration) {
	select {
	case lt.ch <- d.Nanoseconds():
	default:
		// Buffer full: skip to avoid blocking
	}
}

func (lt *LatencyTracker) drain() {
	for {
		select {
		case v := <-lt.ch:
			lt.samples = append(lt.samples, v)
		default:
			return
		}
	}
}

func (lt *LatencyTracker) Finalize() {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.drain()
	sort.Slice(lt.samples, func(i, j int) bool {
		return lt.samples[i] < lt.samples[j]
	})
}

func (lt *LatencyTracker) Percentile(p float64) float64 {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	n := len(lt.samples)
	if n == 0 {
		return 0
	}
	idx := int(float64(n) * p)
	if idx >= n {
		idx = n - 1
	}
	return float64(lt.samples[idx]) / 1000.0 // microseconds
}

// Zipfian distribution generator
func zipfian(n int, s float64) int {
	sum := 0.0
	for i := 1; i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), s)
	}

	r := rand.Float64() * sum
	partialSum := 0.0

	for i := 1; i <= n; i++ {
		partialSum += 1.0 / math.Pow(float64(i), s)
		if partialSum >= r {
			return i - 1
		}
	}
	return n - 1
}

func main() {
	flag.Parse()

	// Validate inputs
	if *numKeysFlag <= 0 {
		fmt.Fprintf(os.Stderr, "Error: --keys must be > 0\n")
		os.Exit(1)
	}
	if *valueSizeFlag <= 0 {
		fmt.Fprintf(os.Stderr, "Error: --value-size must be > 0\n")
		os.Exit(1)
	}

	// Clean slate
	os.Remove("wal.log")
	os.Remove("sstable.data")

	fmt.Printf("╔════════════════════════════════════════╗\n")
	fmt.Printf("║  AMETHYST BENCHMARK                    ║\n")
	fmt.Printf("╚════════════════════════════════════════╝\n")
	fmt.Printf("Engine:   %s\n", *engineFlag)
	fmt.Printf("Workload: %s\n", *workloadFlag)
	fmt.Printf("Keys:     %d\n", *numKeysFlag)
	fmt.Printf("Value:    %d bytes\n", *valueSizeFlag)
	fmt.Println()

	// Initialize components
	w, err := wal.NewDiskWAL("wal.log")
	if err != nil {
		panic(err)
	}

	mem := memtable.NewMemtable(100000) // 100k entries for 10M scale
	meta := metadata.NewTracker()

	fileMgr, err := segmentfile.NewSegmentFileManager("sstable.data")
	if err != nil {
		panic(err)
	}

	indexBuilder := sparseindex.NewBuilder(16)
	sstWriter := writer.NewWriter(fileMgr, indexBuilder)
	sstReader := reader.NewReader(fileMgr)

	// FIXED: Create FSMController directly
	fsm := adaptive.NewFSMController()
	director := compaction.NewDirector(meta, fsm)
	executor := compaction.NewExecutor(meta, sstReader, sstWriter)

	// Metrics tracking
	var logicalBytes int64 = 0
	var physicalBytes int64 = 0
	var userBytes int64 = 0
	var totalReads int64 = 0
	var totalSegmentScans int64 = 0
	var compactionCount int64 = 0
	var phases []PhaseResult

	// Track unique live keys for space amplification
	liveKeys := make(map[string]int)
	var liveKeysMutex sync.RWMutex

	// Background compaction goroutine
	stopCompaction := make(chan struct{})

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				fmt.Printf("  [DEBUG] Checking for compaction...\n")

				// CRITICAL: Lock metadata before planning
				plan := director.MaybePlan()

				if plan != nil {
					fmt.Printf("  [BG Compaction] %d segs → Strategy=%v, Reason=%s\n",
						len(plan.Inputs), plan.OutputStrategy, plan.Reason)

					// CRITICAL: Validate inputs before execution
					validInputs := true
					for _, seg := range plan.Inputs {
						if seg == nil || seg.IsObsolete() {
							validInputs = false
							fmt.Printf("  [BG Compaction] SKIPPED: Invalid or obsolete segment\n")
							break
						}
					}

					if validInputs {
						newSeg, err := executor.Execute(plan)
						if err == nil && newSeg != nil {
							atomic.AddInt64(&physicalBytes, newSeg.Length)
							atomic.AddInt64(&compactionCount, 1)
							fmt.Printf("  [BG Compaction] COMPLETED: +%d bytes to physicalBytes\n", newSeg.Length)
						} else if err != nil {
							fmt.Printf("  [BG Compaction] ERROR: %v\n", err)
						}
					}
				} else {
					fmt.Printf("  [DEBUG] No compaction needed\n")
				}

			case <-stopCompaction:
				return
			}
		}
	}()

	startTime := time.Now()

	// Run workload
	switch *workloadFlag {
	case "shift":
		phases = runShift(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes, &userBytes,
			&totalReads, &totalSegmentScans, &compactionCount, liveKeys, &liveKeysMutex)

	case "pure-write":
		runPureWrite(w, mem, meta, sstWriter, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes, &userBytes, &compactionCount,
			liveKeys, &liveKeysMutex)

	case "pure-read":
		runPureRead(w, mem, meta, sstWriter, sstReader,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes,
			&totalReads, &totalSegmentScans, liveKeys, &liveKeysMutex)

	case "mixed":
		runMixed(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes, &userBytes,
			&totalReads, &totalSegmentScans, &compactionCount, liveKeys, &liveKeysMutex)

	case "read-heavy":
		runReadHeavy(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes, &userBytes,
			&totalReads, &totalSegmentScans, &compactionCount, liveKeys, &liveKeysMutex)

	case "write-heavy":
		runWriteHeavy(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes, &userBytes,
			&totalReads, &totalSegmentScans, &compactionCount, liveKeys, &liveKeysMutex)

	case "zipfian":
		runZipfian(w, mem, meta, sstWriter, sstReader, director, executor,
			*numKeysFlag, *valueSizeFlag, &logicalBytes, &physicalBytes, &userBytes,
			&totalReads, &totalSegmentScans, &compactionCount, liveKeys, &liveKeysMutex)

	default:
		fmt.Printf("Unknown workload: %s\n", *workloadFlag)
		os.Exit(1)
	}

	// Stop background compaction
	close(stopCompaction)
	time.Sleep(5 * time.Second)
	fmt.Println("Background compaction stopped")

	totalDuration := time.Since(startTime)

	// Calculate final metrics
	finalPhysicalBytes := atomic.LoadInt64(&physicalBytes)
	finalUserBytes := atomic.LoadInt64(&userBytes)
	finalTotalReads := atomic.LoadInt64(&totalReads)
	finalTotalSegmentScans := atomic.LoadInt64(&totalSegmentScans)
	finalLogicalBytes := atomic.LoadInt64(&logicalBytes)
	finalCompactionCount := atomic.LoadInt64(&compactionCount)

	wa := 0.0
	if finalUserBytes > 0 {
		wa = float64(finalPhysicalBytes) / float64(finalUserBytes)
	}
	ra := 0.0
	if finalTotalReads > 0 {
		ra = float64(finalTotalSegmentScans) / float64(finalTotalReads)
	}

	// Space amplification - GET ACTUAL FILE SIZE
	liveKeysMutex.RLock()
	logicalDataSize := int64(0)
	for key, valSize := range liveKeys {
		logicalDataSize += int64(len(key) + valSize)
	}
	liveKeysMutex.RUnlock()

	// Get actual file size on disk
	fileInfo, err := os.Stat("sstable.data")
	physicalDiskSize := int64(0)
	if err == nil {
		physicalDiskSize = fileInfo.Size()
	} else {
		// Fallback to sum of segment sizes
		allSegs := meta.GetAllSegments()
		for _, seg := range allSegs {
			if !seg.IsObsolete() {
				physicalDiskSize += seg.Length
			}
		}
	}

	sa := 0.0
	if logicalDataSize > 0 {
		sa = float64(physicalDiskSize) / float64(logicalDataSize)
	}

	// Sanitize all metrics
	if math.IsNaN(wa) || math.IsInf(wa, 0) {
		wa = 0.0
	}
	if math.IsNaN(ra) || math.IsInf(ra, 0) {
		ra = 0.0
	}
	if math.IsNaN(sa) || math.IsInf(sa, 0) {
		sa = 0.0
	}

	// Create results
	results := Results{
		Engine:             *engineFlag,
		Workload:           *workloadFlag,
		NumKeys:            *numKeysFlag,
		ValueSize:          *valueSizeFlag,
		WriteAmplification: wa,
		ReadAmplification:  ra,
		SpaceAmplification: sa,
		CompactionCount:    int(finalCompactionCount),
		TotalDurationSec:   totalDuration.Seconds(),
		LogicalBytes:       finalLogicalBytes,
		PhysicalBytes:      finalPhysicalBytes,
		TotalReads:         finalTotalReads,
		SegmentScans:       finalTotalSegmentScans,
		LiveDataBytes:      logicalDataSize,
		TotalDiskBytes:     physicalDiskSize,
		Phases:             phases,
	}

	// Print summary
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════╗\n")
	fmt.Printf("║  RESULTS                               ║\n")
	fmt.Printf("╚════════════════════════════════════════╝\n")
	fmt.Printf("Write Amplification:  %.2f\n", wa)
	fmt.Printf("Read Amplification:   %.2f\n", ra)
	fmt.Printf("Space Amplification:  %.2f\n", sa)
	fmt.Printf("Compaction Count:     %d\n", finalCompactionCount)
	fmt.Printf("Total Duration:       %.2fs\n", totalDuration.Seconds())
	fmt.Printf("Throughput:           %.0f ops/sec\n",
		float64(*numKeysFlag)/totalDuration.Seconds())
	fmt.Println()

	// Save to JSON
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		panic(err)
	}

	filename := fmt.Sprintf("results_%s_%s.json", *engineFlag, *workloadFlag)
	err = os.WriteFile(filename, data, 0644)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Results saved to: %s\n", filename)
}

// ========================================
// WORKLOAD IMPLEMENTATIONS
// ========================================

func runShift(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, userBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int64, liveKeys map[string]int, liveKeysMutex *sync.RWMutex) []PhaseResult {

	var phases []PhaseResult

	// PHASE 1: Write (multiple rounds to create overlapping segments)
	fmt.Println("=== PHASE 1: Write ===")
	phase1Start := time.Now()
	writeLatency := NewLatencyTracker(numKeys + 100000)

	// Round 1: Sequential write to populate all keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		t := time.Now()
		w.LogPut(key, val)
		mem.Put(key, val)
		writeLatency.Record(time.Since(t))
		atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
		atomic.AddInt64(userBytes, int64(len(key)+len(val)))

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			atomic.AddInt64(physicalBytes, seg.Length)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Written: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}

	// Round 2: Random overwrites to create overlapping segments
	// This simulates a real workload where updates hit existing keys
	overwriteCount := numKeys / 2
	for i := 0; i < overwriteCount; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
		val := make([]byte, valueSize)
		rand.Read(val)

		t := time.Now()
		w.LogPut(key, val)
		mem.Put(key, val)
		writeLatency.Record(time.Since(t))
		atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
		atomic.AddInt64(userBytes, int64(len(key)+len(val)))

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			atomic.AddInt64(physicalBytes, seg.Length)
			meta.RegisterSegment(seg)
			w.Truncate()
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.TIERED)
		atomic.AddInt64(physicalBytes, seg.Length)
		meta.RegisterSegment(seg)
	}

	phase1Duration := time.Since(phase1Start)
	phase1WA := float64(atomic.LoadInt64(physicalBytes)) / float64(atomic.LoadInt64(logicalBytes))
	writeLatency.Finalize()
	phases = append(phases, PhaseResult{
		Name:     "write",
		Duration: phase1Duration.Seconds(),
		WA:       phase1WA,
		RA:       0,
		LatP50:   writeLatency.Percentile(0.50),
		LatP95:   writeLatency.Percentile(0.95),
		LatP99:   writeLatency.Percentile(0.99),
	})

	fmt.Printf("  Segments: %d\n", len(meta.GetAllSegments()))
	fmt.Printf("  Duration: %v\n", phase1Duration)

	// PHASE 2: Read
	fmt.Println("\n=== PHASE 2: Read (3x) ===")
	time.Sleep(2 * time.Second)

	phase2Start := time.Now()
	//numReads := numKeys * 3
	numReads := numKeys
	var phase2SegmentScans int64 = 0
	readLatency := NewLatencyTracker(numReads)

	for i := 0; i < numReads; i++ {
		safeKeyRange := (numKeys * 9) / 10
		key := fmt.Sprintf("key-%010d", rand.Intn(safeKeyRange))

		t := time.Now()
		atomic.AddInt64(totalReads, 1)
		segs := meta.GetSegmentsForKey(key)
		phase2SegmentScans += int64(len(segs)) // RA = total candidate segments / total reads

		// Debug: show segment counts for first 10 reads
		if i < 10 {
			fmt.Printf("  [DEBUG READ] Key=%s, Segments to check=%d\n", key, len(segs))
		}

		for _, seg := range segs {
			_, ok := sstReader.Get(seg, key)
			meta.UpdateStats(seg.ID, 1, 0)
			if ok {
				break
			}
		}
		readLatency.Record(time.Since(t))

		if i > 0 && i%500000 == 0 {
			currentRA := float64(phase2SegmentScans) / float64(atomic.LoadInt64(totalReads))
			fmt.Printf("  Reads: %d/%d (%.1f%%) RA=%.2f\r", i, numReads, float64(i)*100/float64(numReads), currentRA)
		}
	}
	fmt.Println()

	// Calculate Phase 2 metrics
	phase2Duration := time.Since(phase2Start)
	phase2RA := float64(phase2SegmentScans) / float64(numReads)
	phase2WA := float64(atomic.LoadInt64(physicalBytes)) / float64(atomic.LoadInt64(logicalBytes))
	atomic.AddInt64(totalSegmentScans, phase2SegmentScans)
	readLatency.Finalize()

	phases = append(phases, PhaseResult{
		Name:     "read",
		Duration: phase2Duration.Seconds(),
		WA:       phase2WA,
		RA:       phase2RA,
		LatP50:   readLatency.Percentile(0.50),
		LatP95:   readLatency.Percentile(0.95),
		LatP99:   readLatency.Percentile(0.99),
	})

	fmt.Printf("  Current RA: %.2f\n", phase2RA)
	fmt.Printf("  Duration: %v\n", phase2Duration)

	// Let background compaction handle it
	time.Sleep(10 * time.Second)

	// PHASE 3: Write again
	fmt.Println("\n=== PHASE 3: Write (50%) ===")
	phase3Start := time.Now()
	write2Latency := NewLatencyTracker(numKeys/2 + 1000)

	for i := 0; i < numKeys/2; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
		val := make([]byte, valueSize)
		rand.Read(val)

		t := time.Now()
		w.LogPut(key, val)
		mem.Put(key, val)
		write2Latency.Record(time.Since(t))
		atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
		atomic.AddInt64(userBytes, int64(len(key)+len(val)))

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			atomic.AddInt64(physicalBytes, seg.Length)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Written: %d/%d (%.1f%%)\r", i, numKeys/2, float64(i)*100/float64(numKeys/2))
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.TIERED)
		atomic.AddInt64(physicalBytes, seg.Length)
		meta.RegisterSegment(seg)
	}

	phase3Duration := time.Since(phase3Start)
	phase3WA := float64(atomic.LoadInt64(physicalBytes)) / float64(atomic.LoadInt64(logicalBytes))
	write2Latency.Finalize()

	fmt.Printf("  Segments: %d\n", len(meta.GetAllSegments()))
	fmt.Printf("  Duration: %v\n", phase3Duration)

	// Let background compaction work on the new overlapping segments
	time.Sleep(10 * time.Second)

	// PHASE 4: Read again (after overwrites created overlapping segments)
	fmt.Println("\n=== PHASE 4: Read After Overwrites ===")
	phase4Start := time.Now()
	numReads4 := numKeys * 3
	var phase4SegmentScans int64 = 0
	var phase4Reads int64 = 0
	read2Latency := NewLatencyTracker(numReads4)

	for i := 0; i < numReads4; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))

		t := time.Now()
		phase4Reads++
		atomic.AddInt64(totalReads, 1)
		segs := meta.GetSegmentsForKey(key)
		phase4SegmentScans += int64(len(segs))

		// Debug: show segment counts for first 10 reads
		if i < 10 {
			fmt.Printf("  [DEBUG READ] Key=%s, Segments to check=%d\n", key, len(segs))
		}

		for _, seg := range segs {
			_, ok := sstReader.Get(seg, key)
			meta.UpdateStats(seg.ID, 1, 0)
			if ok {
				break
			}
		}
		read2Latency.Record(time.Since(t))

		if i > 0 && i%500000 == 0 {
			currentRA := float64(phase4SegmentScans) / float64(phase4Reads)
			fmt.Printf("  Reads: %d/%d (%.1f%%) RA=%.2f\r", i, numReads4, float64(i)*100/float64(numReads4), currentRA)
		}
	}
	fmt.Println()

	atomic.AddInt64(totalSegmentScans, phase4SegmentScans)

	phase4Duration := time.Since(phase4Start)
	phase4RA := float64(phase4SegmentScans) / float64(phase4Reads)
	phase4WA := float64(atomic.LoadInt64(physicalBytes)) / float64(atomic.LoadInt64(logicalBytes))
	read2Latency.Finalize()

	fmt.Printf("  Current RA: %.2f (with overlapping segments)\n", phase4RA)
	fmt.Printf("  Duration: %v\n", phase4Duration)

	phases = append(phases, PhaseResult{
		Name:     "write2",
		Duration: phase3Duration.Seconds(),
		WA:       phase3WA,
		RA:       phase2RA,
		LatP50:   write2Latency.Percentile(0.50),
		LatP95:   write2Latency.Percentile(0.95),
		LatP99:   write2Latency.Percentile(0.99),
	})
	phases = append(phases, PhaseResult{
		Name:     "read_after_overwrites",
		Duration: phase4Duration.Seconds(),
		WA:       phase4WA,
		RA:       phase4RA,
		LatP50:   read2Latency.Percentile(0.50),
		LatP95:   read2Latency.Percentile(0.95),
		LatP99:   read2Latency.Percentile(0.99),
	})

	// Let background compaction catch FSM transitions from the new read load
	time.Sleep(10 * time.Second)

	return phases
}

func runPureWrite(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, userBytes *int64, compactionCount *int64,
	liveKeys map[string]int, liveKeysMutex *sync.RWMutex) {

	fmt.Println("=== PURE WRITE WORKLOAD ===")

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
		atomic.AddInt64(userBytes, int64(len(key)+len(val)))

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			atomic.AddInt64(physicalBytes, seg.Length)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.TIERED)
		atomic.AddInt64(physicalBytes, seg.Length)
		meta.RegisterSegment(seg)
	}

	// Let background compaction handle it
	time.Sleep(10 * time.Second)
}

func runPureRead(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	numKeys, valueSize int, logicalBytes, physicalBytes, totalReads, totalSegmentScans *int64,
	liveKeys map[string]int, liveKeysMutex *sync.RWMutex) {

	fmt.Println("=== PURE READ WORKLOAD ===")

	// Phase 1: Populate
	fmt.Println("Populating data...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Populated: %d/%d (%.1f%%)\r", i, numKeys, float64(i)*100/float64(numKeys))
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.TIERED)
		meta.RegisterSegment(seg)
	}

	// Reset counters
	atomic.StoreInt64(logicalBytes, 0)
	atomic.StoreInt64(physicalBytes, 0)

	// Phase 2: Read
	fmt.Println("Reading (3x)...")
	numReads := numKeys * 3

	for i := 0; i < numReads; i++ {
		key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))

		atomic.AddInt64(totalReads, 1)
		segs := meta.GetSegmentsForKey(key)
		atomic.AddInt64(totalSegmentScans, int64(len(segs)))

		for _, seg := range segs {
			_, ok := sstReader.Get(seg, key)
			meta.UpdateStats(seg.ID, 1, 0)
			if ok {
				break
			}
		}

		if i > 0 && i%500000 == 0 {
			fmt.Printf("  Progress: %d/%d (%.1f%%)\r", i, numReads, float64(i)*100/float64(numReads))
		}
	}
	fmt.Println()
}

func runMixed(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, userBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int64, liveKeys map[string]int, liveKeysMutex *sync.RWMutex) {

	fmt.Println("=== MIXED WORKLOAD (50/50) ===")

	// Populate
	fmt.Println("Populating...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
		atomic.AddInt64(userBytes, int64(len(key)+len(val)))

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			atomic.AddInt64(physicalBytes, seg.Length)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.TIERED)
		atomic.AddInt64(physicalBytes, seg.Length)
		meta.RegisterSegment(seg)
	}

	// Mixed operations
	fmt.Println("Running mixed operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		if rand.Float32() < 0.5 {
			// Write
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			val := make([]byte, valueSize)
			rand.Read(val)

			w.LogPut(key, val)
			mem.Put(key, val)
			atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
			atomic.AddInt64(userBytes, int64(len(key)+len(val)))

			liveKeysMutex.Lock()
			liveKeys[key] = len(val)
			liveKeysMutex.Unlock()

			if mem.ShouldFlush() {
				data := mem.Flush()
				seg, _ := sstWriter.WriteSegment(data, common.TIERED)
				atomic.AddInt64(physicalBytes, seg.Length)
				meta.RegisterSegment(seg)
				w.Truncate()
			}
		} else {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))

			atomic.AddInt64(totalReads, 1)
			segs := meta.GetSegmentsForKey(key)
			atomic.AddInt64(totalSegmentScans, int64(len(segs)))

			for _, seg := range segs {
				_, ok := sstReader.Get(seg, key)
				meta.UpdateStats(seg.ID, 1, 0)
				if ok {
					break
				}
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		newSeg, err := executor.Execute(plan)
		if err == nil && newSeg != nil {
			atomic.AddInt64(physicalBytes, newSeg.Length)
			atomic.AddInt64(compactionCount, 1)
		}
	}
}

func runReadHeavy(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, userBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int64, liveKeys map[string]int, liveKeysMutex *sync.RWMutex) {

	fmt.Println("=== READ-HEAVY WORKLOAD (95% reads) ===")

	// Populate
	fmt.Println("Populating...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
		atomic.AddInt64(userBytes, int64(len(key)+len(val)))

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			atomic.AddInt64(physicalBytes, seg.Length)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.TIERED)
		atomic.AddInt64(physicalBytes, seg.Length)
		meta.RegisterSegment(seg)
	}

	// Operations (95% read)
	fmt.Println("Running read-heavy operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		if rand.Float32() < 0.95 {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))

			atomic.AddInt64(totalReads, 1)
			segs := meta.GetSegmentsForKey(key)
			atomic.AddInt64(totalSegmentScans, int64(len(segs)))

			for _, seg := range segs {
				_, ok := sstReader.Get(seg, key)
				meta.UpdateStats(seg.ID, 1, 0)
				if ok {
					break
				}
			}
		} else {
			// Write
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			val := make([]byte, valueSize)
			rand.Read(val)

			w.LogPut(key, val)
			mem.Put(key, val)
			atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
			atomic.AddInt64(userBytes, int64(len(key)+len(val)))

			liveKeysMutex.Lock()
			liveKeys[key] = len(val)
			liveKeysMutex.Unlock()

			if mem.ShouldFlush() {
				data := mem.Flush()
				seg, _ := sstWriter.WriteSegment(data, common.TIERED)
				atomic.AddInt64(physicalBytes, seg.Length)
				meta.RegisterSegment(seg)
				w.Truncate()
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		newSeg, err := executor.Execute(plan)
		if err == nil && newSeg != nil {
			atomic.AddInt64(physicalBytes, newSeg.Length)
			atomic.AddInt64(compactionCount, 1)
		}
	}
}

func runWriteHeavy(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, userBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int64, liveKeys map[string]int, liveKeysMutex *sync.RWMutex) {

	fmt.Println("=== WRITE-HEAVY WORKLOAD (95% writes) ===")

	// Populate
	fmt.Println("Populating...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
		atomic.AddInt64(userBytes, int64(len(key)+len(val)))

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			atomic.AddInt64(physicalBytes, seg.Length)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.TIERED)
		atomic.AddInt64(physicalBytes, seg.Length)
		meta.RegisterSegment(seg)
	}

	// Operations (95% write)
	fmt.Println("Running write-heavy operations...")
	numOps := numKeys * 2

	for i := 0; i < numOps; i++ {
		if rand.Float32() < 0.95 {
			// Write
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))
			val := make([]byte, valueSize)
			rand.Read(val)

			w.LogPut(key, val)
			mem.Put(key, val)
			atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
			atomic.AddInt64(userBytes, int64(len(key)+len(val)))

			liveKeysMutex.Lock()
			liveKeys[key] = len(val)
			liveKeysMutex.Unlock()

			if mem.ShouldFlush() {
				data := mem.Flush()
				seg, _ := sstWriter.WriteSegment(data, common.TIERED)
				atomic.AddInt64(physicalBytes, seg.Length)
				meta.RegisterSegment(seg)
				w.Truncate()
			}
		} else {
			// Read
			key := fmt.Sprintf("key-%010d", rand.Intn(numKeys))

			atomic.AddInt64(totalReads, 1)
			segs := meta.GetSegmentsForKey(key)
			atomic.AddInt64(totalSegmentScans, int64(len(segs)))

			for _, seg := range segs {
				_, ok := sstReader.Get(seg, key)
				meta.UpdateStats(seg.ID, 1, 0)
				if ok {
					break
				}
			}
		}

		if i > 0 && i%200000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numOps)
		}
	}
	fmt.Println()

	// Compaction
	time.Sleep(2 * time.Second)
	if plan := director.MaybePlan(); plan != nil {
		newSeg, err := executor.Execute(plan)
		if err == nil && newSeg != nil {
			atomic.AddInt64(physicalBytes, newSeg.Length)
			atomic.AddInt64(compactionCount, 1)
		}
	}
}

func runZipfian(w wal.WAL, mem memtable.Memtable, meta metadata.Tracker,
	sstWriter writer.SSTableWriter, sstReader reader.SSTableReader,
	director compaction.Director, executor compaction.Executor,
	numKeys, valueSize int, logicalBytes, physicalBytes, userBytes, totalReads, totalSegmentScans *int64,
	compactionCount *int64, liveKeys map[string]int, liveKeysMutex *sync.RWMutex) {

	fmt.Println("=== ZIPFIAN WORKLOAD (hot keys, s=1.5) ===")

	// Phase 1: Write
	fmt.Println("Populating data...")
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%010d", i)
		val := make([]byte, valueSize)
		rand.Read(val)

		w.LogPut(key, val)
		mem.Put(key, val)
		atomic.AddInt64(logicalBytes, int64(len(key)+valueSize))
		atomic.AddInt64(userBytes, int64(len(key)+len(val)))

		liveKeysMutex.Lock()
		liveKeys[key] = len(val)
		liveKeysMutex.Unlock()

		if mem.ShouldFlush() {
			data := mem.Flush()
			seg, _ := sstWriter.WriteSegment(data, common.TIERED)
			atomic.AddInt64(physicalBytes, seg.Length)
			meta.RegisterSegment(seg)
			w.Truncate()
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numKeys)
		}
	}
	fmt.Println()

	// Final flush
	if mem.ShouldFlush() {
		data := mem.Flush()
		seg, _ := sstWriter.WriteSegment(data, common.TIERED)
		atomic.AddInt64(physicalBytes, seg.Length)
		meta.RegisterSegment(seg)
	}

	// Phase 2: Zipfian reads
	fmt.Println("Reading with Zipfian distribution (s=1.5)...")
	numReads := numKeys * 3
	topHotKeyAccesses := 0

	for i := 0; i < numReads; i++ {
		keyIdx := zipfian(numKeys, 1.5)

		if keyIdx < numKeys/10 {
			topHotKeyAccesses++
		}

		key := fmt.Sprintf("key-%010d", keyIdx)

		atomic.AddInt64(totalReads, 1)
		segs := meta.GetSegmentsForKey(key)
		atomic.AddInt64(totalSegmentScans, int64(len(segs)))

		for _, seg := range segs {
			_, ok := sstReader.Get(seg, key)
			meta.UpdateStats(seg.ID, 1, 0)
			if ok {
				break
			}
		}

		if i > 0 && i%500000 == 0 {
			fmt.Printf("  Progress: %d/%d\r", i, numReads)
		}
	}
	fmt.Println()

	// Report hot key stats
	if numReads > 0 {
		fmt.Printf("  Hot key distribution: Top 10%% of keys = %d%% of accesses\n",
			topHotKeyAccesses*100/numReads)
	}

	// Let background compaction handle it
	time.Sleep(10 * time.Second)
}
