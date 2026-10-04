# VegaDB Operations Guide

This guide describes how to run, configure, monitor, and maintain VegaDB in production environments.

---

## 1. Running VegaDB

### 1.1 Local Binary
```bash
# Build server and CLI
make build

# Run server with real engine on port 50051 (gRPC) and 8080 (REST)
./bin/vegaserver --data-dir /tmp/vegadata --grpc-addr :50051 --http-addr :8080 --store engine
```

### 1.2 Docker & Docker Compose
```bash
# Launch persistent VegaDB container
docker compose up -d

# Check health status
docker inspect --format '{{.State.Health.Status}}' vegadb

# Inspect logs
docker logs -f vegadb
```

---

## 2. Configuration Options

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--data-dir` | `VEGA_DATA_DIR` | `/data` | Path to database data directory |
| `--grpc-addr` | `VEGA_GRPC_ADDR` | `:50051` | gRPC server listening address |
| `--http-addr` | `VEGA_HTTP_ADDR` | `:8080` | REST API server listening address |
| `--pprof-addr` | `VEGA_PPROF_ADDR` | `""` | Optional pprof/expvar listening address |
| `--sync` | `VEGA_SYNC` | `always` | Durability policy: `always`, `interval`, `never` |
| `--memtable-mb` | `VEGA_MEMTABLE_MB` | `64` | Memtable freeze threshold in MiB |
| `--vlog-segment-mb`| `VEGA_VLOG_SEGMENT_MB` | `1024` | Maximum segment file size in MiB |
| `--max-inflight` | `VEGA_MAX_INFLIGHT` | `2048` | Maximum concurrent in-flight requests |
| `--max-key-bytes` | `VEGA_MAX_KEY_BYTES` | `8192` | Maximum key size in bytes |
| `--max-value-mb` | `VEGA_MAX_VALUE_MB` | `16` | Maximum value size in MiB |
| `--scan-prefetch` | `VEGA_SCAN_PREFETCH` | `8` | Parallel workers for range scan value prefetching |
| `--log-level` | `VEGA_LOG_LEVEL` | `info` | Logging verbosity: `debug`, `info`, `warn`, `error` |
| `--store` | `VEGA_STORE` | `engine` | Storage engine backend: `engine` or `memory` |

---

## 3. Observability & Metrics

Metrics are published in two formats:
1. **gRPC Stats Endpoint:** `vegactl stats` or `Vega.Stats` gRPC RPC.
2. **REST Stats Endpoint:** `GET /v1/stats`.
3. **Expvar Endpoint:** If `--pprof-addr=:6060` is set, metrics are published at `GET http://localhost:6060/debug/vars`.

### Key Metrics Monitored
- `puts`, `gets`, `deletes`, `scans`: Throughput counters per operation.
- `bytes_in`, `bytes_out`: Network and payload volume.
- `flushes`, `compactions`: Background LSM activity.
- `write_stalls`: Count of writer stalls caused by unflushed memtables.
- `group_commit_batches`, `group_commit_requests`: Group commit efficiency ratio.
- `memtable_bytes`, `l0_tables`, `l1_tables`: Live storage hierarchy footprint.

---

## 4. Backup & Disaster Recovery

VegaDB data directories contain:
- `MANIFEST`: Atomic JSON metadata state.
- `sst/`: Immutable SSTable files.
- `*.vlog`: Segmented append-only log files.

### Online Backup Procedure
1. Create a filesystem snapshot (e.g. ZFS, LVM, or AWS EBS snapshot) of the data directory volume.
2. Alternatively, copy all immutable SSTables and inactive `.vlog` files, followed by the active `.vlog` and `MANIFEST`.
3. On restore, VegaDB will verify all CRC32 checksums, replay the vLog from `VlogHead`, and resume serving with zero data corruption.
