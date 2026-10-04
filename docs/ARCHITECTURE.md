# VegaDB Architecture

VegaDB is a 100% self-contained, high-performance, single-node Key-Value storage engine implemented in Go. It is based on the **WiscKey** architecture (*Lu et al., FAST 2016*), decoupling keys from values to minimize write and read amplification on modern solid-state storage.

---

## 1. High-Level System Architecture

```
                       +-----------------------+
                       |    Client Requests    |
                       |      (gRPC / REST)    |
                       +-----------+-----------+
                                   |
                                   v
                       +-----------------------+
                       |     Service Layer     |
                       |  (Concurrency Limiter |
                       |   & Metrics Registry) |
                       +-----------+-----------+
                                   |
                                   v
                       +-----------------------+
                       |     WiscKey Engine    |
                       +-----------+-----------+
                                   |
         +-------------------------+-------------------------+
         |                                                   |
         v                                                   v
+------------------+                               +------------------+
|  Value Log (vLog)|                               |     LSM Tree     |
| (Append-Only)    |                               |  (Keys + 17B Rec)|
+------------------+                               +------------------+
| Segmented files  |                               | Memtable (Arena) |
| Cached pread fds |                               | Frozen Memtable  |
| CRC32C protected |                               | Level 0 SSTables |
+------------------+                               | Level 1 SSTables |
                                                   +------------------+
```

---

## 2. Core Components

### 2.1 Concurrency & Group Commit
- Writes are enqueued onto a lock-free request channel (`reqCh`).
- A dedicated `writerLoop` drains all queued requests into a batch (up to 4096 entries or maximum frame limit).
- Frames are serialized into a pooled buffer and appended to the active vLog segment in a single sequential I/O operation.
- If `Sync == SyncAlways`, an `fsync` is issued once per batch, amortizing durability overhead across concurrent writers.
- Individual client callers wait on pooled token channels and are released upon batch completion.

### 2.2 In-Memory Memtable & Monotonic Arena Allocator
- Memtables are implemented as lock-free concurrent SkipLists using 4096-node fixed slabs and an xorshift32 pseudo-random number generator for $O(\log n)$ inserts and point queries.
- Key strings and nodes are allocated using a monotonic bump allocator (`internal/arena`), avoiding per-key heap allocations and garbage collection pressure.
- Memtable sizes are tracked via `ApproxSize()`. When the active memtable reaches `MemtableSize` (default 64 MiB), it is frozen into `readState.imm` and a background flush is signaled.

### 2.3 Value Log (vLog)
- Values are written strictly sequentially to segmented log files named `%08d.vlog`.
- Log files roll automatically when reaching `VlogSegmentSize` (default 1 GiB).
- Concurrent reads execute via `pread` system calls on cached read-only file descriptors (`sync.RWMutex`), completely bypassing write locks and OS seek locks.
- Frames are protected by Castagnoli CRC32 (`crc32.MakeTable(crc32.Castagnoli)`).
- Crash recovery replays frames from `manifestState.VlogHead` to reconstruct the uncommitted active memtable, truncating torn tails if incomplete writes occurred at the crash boundary.

### 2.4 SSTable Storage (Level 0 and Level 1)
- Flushed memtables write Level 0 SSTables (`%08d.sst`).
- SSTables consist of 4 KiB data blocks (prefixed with 4-byte CRC32C), an index block mapping block start keys to file offsets, a Bloom filter block with 7 xxhash probes (1% false positive rate), and a fixed 48-byte footer.
- Compaction runs in the background when Level 0 accumulates 4 or more SSTables, merging L0 tables and overlapping L1 tables into non-overlapping L1 SSTables and dropping tombstones.

### 2.5 Atomic Manifest Management
- Global database state (active SSTables, file sequence counter, and vLog head position) is tracked in `MANIFEST`.
- State transitions are committed via `MANIFEST.tmp` write $\to$ `fsync` $\to$ atomic rename $\to$ directory `fsync`, ensuring zero state corruption across sudden power outages.

### 2.6 Pipelined Parallel Range Scans
- Range queries scan the LSM tree to collect ordered keys and 17-byte vLog pointers.
- A sliding window (64 items) prefetches value payloads concurrently across background workers (`ScanPrefetch`), saturating NVMe/SSD random-read bandwidth while preserving strict sorted key delivery.
