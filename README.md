
# <img src="amethyst_logo.svg" alt="Amethyst Logo" height="40"> Amethyst

Amethyst is a prototype LSM-tree storage engine written in Go that introduces **segment-level adaptive compaction**. Instead of committing to a single global compaction policy (leveled or tiered) upfront, Amethyst attaches a compaction strategy to each SSTable segment individually and switches strategies on the fly as access patterns change.

## Motivation

LSM-trees traditionally pick one of two compaction strategies before a workload begins:

- **Tiered** – batches many overlapping segments together before merging. Great for write-heavy workloads, but hurts read latency.
- **Leveled** – keeps each level sorted and non-overlapping. Great for reads, but amplifies write cost.

Real workloads shift between read-heavy and write-heavy phases, making any static choice suboptimal. Amethyst lets individual segments migrate between the two strategies as conditions change, converging toward the best policy for whatever the current workload looks like.

## Architecture

Amethyst follows the standard LSM write path (WAL → Memtable → SSTable flush) with an added metadata and control layer on top.

- **Write Path:**  `Client → WAL → Memtable → [flush] → SSTable (tagged with strategy)`
- **Read Path:**   `Client → Memtable → Metadata Query → SSTable scan (newest → oldest)`
- **Background:**  `Compaction Director → FSM Controller → Compaction Executor`

### Key Components

| Package | Responsibility |
|---------|----------------|
| `internal/engine` | Orchestrates the WAL → Memtable → flush pipeline |
| `internal/wal` | Append-only write-ahead log for crash durability |
| `internal/memtable` | In-memory sorted buffer; flushes when full |
| `internal/metadata` | **Metadata Tracker** — per-segment registry (key range, file offset, strategy, read/write counters, overlap info) |
| `internal/adaptive` | **FSM Controller** — observes per-segment access history over a sliding window and signals strategy switches |
| `internal/compaction` | **Director** selects segments to compact; **Executor** performs multi-way merge and rewrites under the new strategy |
| `internal/sstable` | SSTable reader and writer |
| `internal/sparseindex` | Sparse index (stride = 16) for efficient key lookup within segments |
| `internal/segmentfile` | Append-only file manager with mmap support |
| `cmd/amethystd` | Entry point and workload runner |

### Segment Lifecycle

1. **Creation** — flushed from the Memtable, tagged `tiered` by default.
2. **Use** — serves reads; read/write counters are updated on every access.
3. **Monitoring** — the FSM Controller compares read vs. write counts over a sliding window. A `tiered` segment with sustained read pressure is flagged `need-leveled`; a `leveled` segment with heavy writes is flagged `need-tiered`.
4. **Rewriting** — the Compaction Director selects flagged segments plus any overlapping segments and hands them to the Executor. The Executor performs a multi-way merge, drops obsolete entries, and writes a new SSTable under the target strategy. A cooldown period prevents thrashing.
5. **Obsolescence** — old input segments are marked obsolete and skipped during reads. Space reclamation is deferred (not implemented in this prototype).

## Getting Started

### Prerequisites

- Go 1.21+

### Build

```bash
cd ws-impl/amethyst
go build ./...
```

### Run

```bash
go run ./cmd/amethystd [flags]
```

# Specifically for our results in shift workload we used:
```bash
Navigate to the workspace directory
cd cmd/amethystd

# Run the benchmark workload using a specific engine baseline label
go run main.go --engine=leveldb --workload=shift
```

#### Flags:

| Flag | Default | Description |
|------|---------|-------------|
| `-workload` | `shift` | Workload type |
| `-keys` | `1000000` | Number of keys |
| `-value-size` | `256` | Value size in bytes |
| `-engine` | `adaptive` | Engine label used in output |

### Workload Phases

The `shift` workload executes four sequential phases:

1. **PHASE 1: Write** — sequential writes to populate all keys + random overwrites (50% of keys) to create overlapping segments
2. **PHASE 2: Read** — point lookups over a safe key range (90% of keyspace)
3. **PHASE 3: Write (50%)** — additional random writes scattered over the keyspace
4. **PHASE 4: Read After Overwrites** — another round of point lookups (3x keys) to observe compaction effects

Results are emitted as JSON, including throughput, read/write amplification, and per-phase breakdowns.

## Design Notes

- **Single compaction thread** — the prototype uses one background compaction goroutine. Ingestion and reads proceed concurrently with compaction.
- **No Bloom filters** — intentionally omitted so that any performance improvement is attributable to the adaptive metadata logic alone.
- **Sparse index** — every 16th key is indexed per segment, balancing memory use against scan cost.
- **Atomic counters** — `SegmentMeta` read/write stats use `sync/atomic` so the hot read path does not take a lock.
- **Cooldown guard** — a minimum interval between strategy switches (`MinSwitchInterval = 2s`) prevents the FSM from oscillating under mixed workloads.

## Limitations

- Space reclamation for obsolete segments is not implemented.
- Bloom filters, parallel sub-compactions, and concurrent flushes are excluded.
- Evaluated on a single node; distributed considerations are out of scope.
- FSM thresholds and cooldowns are heuristic and not auto-tuned.


## License

MIT License - see [LICENSE](LICENSE) file for details.
