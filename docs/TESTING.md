# VegaDB Testing Strategy

VegaDB incorporates a multi-tiered verification strategy designed to guarantee data integrity, zero race conditions, crash resilience, and performance stability.

---

## 1. Test Layers

### 1.1 Unit & Race Testing
Every internal package maintains dedicated tests executed with the Go race detector enabled:
- `internal/bufpool`: Verification of power-of-two size classes, memory poisoning with `0xDE`, and double-release panic guards.
- `internal/arena`: Monotonic chunk allocation, memory alignment, and reset verification.
- `internal/memtable`: Concurrent put/get, frozen snapshot iterators, and zero-allocation point queries.
- `internal/vlog`: CRC32 validation, automatic segment rolling, `pread` caching, and torn-tail truncation upon crash.
- `internal/bloom`: 7-probe xxhash double-hashing, 1% false positive rate verification.
- `internal/sstable`: Block builder, index encoding, Bloom filter search, refcount lifecycle, and 48-byte footer integrity.
- `internal/manifest`: Temp-file write, atomic rename, and directory fsync verification.
- `internal/lsm`: Level 0 flush, Level 0 $\to$ Level 1 compaction, and multi-version merge iterators.
- `pkg/client`: Connection pooling, error translation, and CRUD streaming.

### 1.2 Model-Based Randomized Testing
`TestEngineRandomizedModel` in `internal/engine/engine_test.go` generates pseudo-random sequences of thousands of interleaved operations (Put, Get, Delete) across wide value size ranges (0 B to 50 KB) and tiny memtables (32 KiB) to force heavy flush and compaction activity. Correctness is continuously cross-checked against an in-memory oracle model.

### 1.3 Crash Recovery & Failpoint Simulation
`TestEngineCrashRecovery` and `TestEngineFailpoints` in `internal/engine/crash_test.go`:
- Executes 50 independent crash iterations under random SIGKILL timings against child processes.
- Simulates intentional system crashes at critical failpoint injection sites:
  1. `flush_after_sst_before_manifest`: Crash after writing an SSTable but before updating the manifest.
  2. `flush_after_manifest`: Crash immediately after updating the manifest.
  3. `compaction_after_output_before_manifest`: Crash after generating new L1 SSTables before updating manifest.
  4. `vlog_mid_batch`: Crash during incomplete vLog batch appends.
- Verifies that every acknowledged write survives restart with zero data corruption.

### 1.4 Docker Integration Smoke Test
`scripts/docker_smoke.sh` performs end-to-end container verification:
- Builds distroless non-root Docker container image.
- Verifies container health check transition to `healthy`.
- Performs REST PUT/GET and gRPC PUT/GET operations.
- Restarts container and asserts data persistence on disk.
- Stops container cleanly.
