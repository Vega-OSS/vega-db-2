# VegaDB

A high-performance single-node key-value storage engine implementing WiscKey.

![CI](https://github.com/Vega-OSS/vega-db/actions/workflows/ci.yml/badge.svg)

## What It Is and Why

Traditional LSM-tree storage engines repeatedly rewrite both keys and values during background compactions, resulting in high write amplification when values are large. 
VegaDB implements the WiscKey design (Lu et al., FAST 2016), storing values once in an append-only value log (`vLog`) and keeping only key-to-pointer references inside the LSM tree.
By decoupling keys from values, compaction reorganizes only compact keys and 17-byte pointers, conserving disk bandwidth and maximizing write throughput.

## Features

- **WiscKey Architecture:** Keys and value pointers in LSM tree; values in append-only segmented vLog.
- **vLog as WAL:** Value log acts as write-ahead log; no redundant writes.
- **Low Allocation:** Size-class buffer pooling (`sync.Pool`), arena bump allocation for keys, slab node allocation.
- **Group Commit:** Single writer goroutine batching writes with coordinated `fsync`.
- **Dual Transports:** High-performance gRPC API and idiomatic REST API.
- **Crash Consistency:** Full crash recovery replay with torn-tail detection and repair.

## Quick Start with Docker

```bash
# 1. Build and start the container
docker compose up -d --build

# 2. Store and retrieve a key via REST
curl -i -X PUT --data-binary 'hello' localhost:8080/v1/kv/greeting
curl -i localhost:8080/v1/kv/greeting

# 3. Stop the container
docker compose stop
```

## API Examples

### REST API
```bash
# Put key
curl -X PUT --data-binary 'Alice' localhost:8080/v1/kv/user:1

# Get key
curl localhost:8080/v1/kv/user:1

# Delete key
curl -X DELETE localhost:8080/v1/kv/user:1

# Range scan
curl 'localhost:8080/v1/scan?start=user:0&end=user:9&limit=10'
```

### gRPC API via vegactl
```bash
# Put key
vegactl put user:1 Alice

# Get key
vegactl get user:1

# Delete key
vegactl del user:1
```

## Configuration

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--data-dir` | `VEGA_DATA_DIR` | `/data` | Root directory for vlog, sst, and MANIFEST |
| `--grpc-addr` | `VEGA_GRPC_ADDR` | `:50051` | gRPC listen address |
| `--http-addr` | `VEGA_HTTP_ADDR` | `:8080` | REST listen address |
| `--pprof-addr` | `VEGA_PPROF_ADDR` | `""` | pprof/expvar address (e.g. `127.0.0.1:6060`) |
| `--sync` | `VEGA_SYNC` | `always` | fsync policy: `always`, `interval`, `never` |
| `--memtable-mb` | `VEGA_MEMTABLE_MB` | `64` | Memtable freeze threshold in MiB |
| `--vlog-segment-mb` | `VEGA_VLOG_SEGMENT_MB` | `1024` | vLog segment size in MiB |
| `--max-inflight` | `VEGA_MAX_INFLIGHT` | `2048` | In-flight request concurrency limit |
| `--max-key-bytes` | `VEGA_MAX_KEY_BYTES` | `8192` | Maximum key size in bytes |
| `--max-value-mb` | `VEGA_MAX_VALUE_MB` | `16` | Maximum value size in MiB |
| `--scan-prefetch` | `VEGA_SCAN_PREFETCH` | `8` | Scan prefetch worker pool size |
| `--block-cache-mb` | `VEGA_BLOCK_CACHE_MB` | `64` | SSTable block cache size in MiB |
| `--log-level` | `VEGA_LOG_LEVEL` | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`) |

## Building and Testing

```bash
# Build binaries
make build

# Run unit and race tests
make check

# Run full verification
make verify
```

## Project Layout

- `cmd/vegaserver`: Server daemon binary.
- `cmd/vegactl`: Command line client.
- `cmd/vegabench`: Benchmark and load generation tool.
- `internal/bufpool`: Size-class sync.Pool buffer management.
- `internal/arena`: Bump memory allocator for keys.
- `internal/memtable`: Arena-backed skip list.
- `internal/record`: 17-byte record structure.
- `internal/vlog`: Segmented value log with recovery.
- `internal/sstable`: SSTable builder and reader.
- `internal/bloom`: Bloom filter implementation.
- `internal/manifest`: Atomic JSON manifest manager.
- `internal/lsm`: LSM Version, flusher, and compactor.
- `internal/engine`: Core DB engine coordinating writer loop and recovery.
- `internal/service`: Business logic, request limits, metrics.
- `internal/grpcserver`: gRPC transport service.
- `internal/restserver`: REST HTTP transport.
- `pkg/client`: Client connection pool and helpers.

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md)
- [Storage Format Specification](docs/STORAGE_FORMAT.md)
- [API Reference](docs/API.md)
- [Operations & Deployment](docs/OPERATIONS.md)
- [Testing Strategy](docs/TESTING.md)
- [Architectural Decisions](docs/DECISIONS.md)
- [Benchmark Results](docs/RESULTS.md)

## Stated Limitations

- **No Garbage Collection:** vLog is append-only; obsolete values remain on disk until a GC system is added.
- **Two LSM Levels:** Fixed L0 and L1 levels suitable for single-node datasets.
- **Single Node:** No Raft or Paxos distributed consensus.

## License

[MIT](LICENSE) © 2026 Vega-OSS contributors
