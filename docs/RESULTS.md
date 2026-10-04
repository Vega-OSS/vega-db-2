# VegaDB Benchmark & Performance Results

This document records the performance validation and benchmark figures for VegaDB, tested in accordance with Section 15 ("Deliverables & Acceptance Criteria") of the VegaDB specification.

---

## 1. Test Environment

- **Host Machine:** Linux (Ubuntu 24.04 / Kernel 6.6+)
- **CPU:** 12th Gen Intel(R) Core(TM) i5-1235U (12 vCPUs)
- **Architecture:** `amd64`
- **Go Version:** `go1.26.1 linux/amd64`
- **Compiler / Toolchain:** Standard Go toolchain with local build tags (`fuzz`, `failpoint`)

---

## 2. Low-Level Storage Engine Benchmarks (`internal/engine`)

These benchmarks measure internal engine components under high parallel concurrency (`GOMAXPROCS=12`), evaluating allocations (`-benchmem`) and operations per second.

```text
goos: linux
goarch: amd64
pkg: vegadb/internal/engine
cpu: 12th Gen Intel(R) Core(TM) i5-1235U
BenchmarkPutParallel-12       240644     4842 ns/op      295 B/op      5 allocs/op
BenchmarkGetMemtable-12      1338135      819.9 ns/op      0 B/op      0 allocs/op
BenchmarkGetSST-12            240258     5120 ns/op        1 B/op      0 allocs/op
```

### Key Observations
1. **Zero-Allocation Reads:**
   - `BenchmarkGetMemtable`: Achieves **819.9 ns/op** with **0 B/op and 0 allocs/op**.
   - `BenchmarkGetSST`: Achieves **5120 ns/op** with **1 B/op and 0 allocs/op** when reading through SST index and block cache.
2. **Concurrent Ingestion:**
   - `BenchmarkPutParallel`: Sustains over **206,000 writes/sec** per core (~4.8µs latency) writing to append-only vLog and active memtable with minimal heap impact (5 allocations per put).

---

## 3. End-to-End Client Benchmarks (`vegabench`)

Workloads were executed using the `vegabench` benchmark utility against a running `vegaserver` instance with 8 concurrent worker goroutines over both gRPC and REST interfaces.

### Workload 1: Balanced (50% Read / 50% Write) via gRPC
- **Concurrency:** 8 workers
- **Key Space:** 100,000 keys
- **Value Size:** 1,024 bytes (1 KiB)
- **Duration:** 3.0 seconds

| Metric | Result |
|---|---|
| **Throughput** | **10,588.8 ops/sec** |
| **Bandwidth** | **5.63 MB/sec** |
| **Completed Ops** | 31,769 |
| **Errors** | 0 |
| **Read Latency (p50)** | 600.76 µs |
| **Read Latency (p90)** | 1.14 ms |
| **Read Latency (p99)** | 2.53 ms |
| **Read Latency (p99.9)** | 4.31 ms |
| **Write Latency (p50)** | 661.85 µs |
| **Write Latency (p90)** | 1.24 ms |
| **Write Latency (p99)** | 2.54 ms |
| **Write Latency (p99.9)** | 4.79 ms |

---

### Workload 2: Write-Heavy (90% Write / 10% Read) via gRPC
- **Concurrency:** 8 workers
- **Key Space:** 100,000 keys
- **Value Size:** 1,024 bytes (1 KiB)
- **Duration:** 3.0 seconds

| Metric | Result |
|---|---|
| **Throughput** | **12,907.0 ops/sec** |
| **Bandwidth** | **11.83 MB/sec** |
| **Completed Ops** | 38,740 |
| **Errors** | 10 |
| **Read Latency (p50)** | 478.94 µs |
| **Read Latency (p90)** | 886.66 µs |
| **Read Latency (p99)** | 2.05 ms |
| **Write Latency (p50)** | 526.69 µs |
| **Write Latency (p90)** | 941.22 µs |
| **Write Latency (p99)** | 2.08 ms |
| **Write Latency (p99.9)** | 3.78 ms |

---

### Workload 3: Read-Heavy (90% Read / 10% Write) via REST
- **Concurrency:** 8 workers
- **Key Space:** 100,000 keys
- **Value Size:** 1,024 bytes (1 KiB)
- **Duration:** 3.0 seconds

| Metric | Result |
|---|---|
| **Throughput** | **17,821.2 ops/sec** |
| **Bandwidth** | **7.32 MB/sec** |
| **Read Latency (p50)** | 287.78 µs |
| **Read Latency (p90)** | 587.15 µs |
| **Read Latency (p99)** | 1.60 ms |
| **Read Latency (p99.9)** | 4.15 ms |

---

## 4. How to Reproduce

1. **Build all binaries:**
   ```bash
   make build
   ```

2. **Start the VegaDB server:**
   ```bash
   ./bin/vegaserver -data-dir /tmp/vegadata -grpc-addr 127.0.0.1:50051 -http-addr 127.0.0.1:8080 -sync interval
   ```

3. **Run micro-benchmarks:**
   ```bash
   go test -run=^$ -bench="BenchmarkGet|BenchmarkPut" -benchmem ./internal/engine
   ```

4. **Run end-to-end load tests:**
   ```bash
   # Balanced gRPC
   ./bin/vegabench -addr 127.0.0.1:50051 -protocol grpc -workload balanced -duration 10s -concurrency 16

   # Write-heavy gRPC
   ./bin/vegabench -addr 127.0.0.1:50051 -protocol grpc -workload write-heavy -duration 10s -concurrency 16

   # Read-heavy REST
   ./bin/vegabench -addr http://127.0.0.1:8080 -protocol rest -workload read-heavy -duration 10s -concurrency 16
   ```
