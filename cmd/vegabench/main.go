package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"vegadb/pkg/client"
)

// LatencyTracker records latency measurements and computes summary percentiles.
type LatencyTracker struct {
	mu      sync.Mutex
	samples []time.Duration
}

// Record appends a latency measurement up to a capped buffer size.
func (lt *LatencyTracker) Record(d time.Duration) {
	lt.mu.Lock()
	if len(lt.samples) < 500000 {
		lt.samples = append(lt.samples, d)
	}
	lt.mu.Unlock()
}

// Percentiles calculates p50, p90, p95, p99, p99.9, and mean latency.
func (lt *LatencyTracker) Percentiles() (p50, p90, p95, p99, p999, avg time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	n := len(lt.samples)
	if n == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	sorted := make([]time.Duration, n)
	copy(sorted, lt.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	avg = total / time.Duration(n)

	p50 = sorted[int(float64(n)*0.50)]
	p90 = sorted[int(float64(n)*0.90)]
	p95 = sorted[int(float64(n)*0.95)]
	p99 = sorted[int(float64(n)*0.99)]
	p999Idx := int(float64(n) * 0.999)
	if p999Idx >= n {
		p999Idx = n - 1
	}
	p999 = sorted[p999Idx]

	return p50, p90, p95, p99, p999, avg
}

func main() {
	var (
		addr        string
		proto       string
		workload    string
		conns       int
		workers     int
		duration    time.Duration
		keyCount    int
		valSize     int
		seed        int64
		csvFilePath string
	)

	flag.StringVar(&addr, "addr", "127.0.0.1:50051", "Server target address")
	flag.StringVar(&proto, "proto", "grpc", "Benchmark protocol (grpc|rest)")
	flag.StringVar(&proto, "protocol", "grpc", "Benchmark protocol alias")
	flag.StringVar(&workload, "workload", "fillrandom", "Workload type (fillrandom|readrandom|scan|mixed|balanced|write-heavy|read-heavy)")
	flag.IntVar(&conns, "conns", 8, "Number of gRPC connections or connection pool size")
	flag.IntVar(&workers, "workers", 64, "Number of concurrent worker goroutines")
	flag.IntVar(&workers, "concurrency", 64, "Concurrency alias for workers")
	flag.DurationVar(&duration, "duration", 10*time.Second, "Benchmark duration")
	flag.IntVar(&keyCount, "keys", 100000, "Distinct keys in keyspace")
	flag.IntVar(&valSize, "value-size", 1024, "Value size in bytes")
	flag.IntVar(&valSize, "val-size", 1024, "Value size alias in bytes")
	flag.Int64Var(&seed, "seed", 1, "RNG seed for reproducible benchmarks")
	flag.StringVar(&csvFilePath, "csv", "", "Path to write CSV results summary")
	flag.Parse()

	fmt.Printf("=== VegaDB Benchmark (vegabench) ===\n")
	fmt.Printf("Target:      %s (%s)\n", addr, proto)
	fmt.Printf("Workload:    %s\n", workload)
	fmt.Printf("Conns:       %d connections\n", conns)
	fmt.Printf("Workers:     %d goroutines\n", workers)
	fmt.Printf("Duration:    %v\n", duration)
	fmt.Printf("Key Space:   %d keys\n", keyCount)
	fmt.Printf("Value Size:  %d bytes\n", valSize)
	fmt.Printf("Seed:        %d\n\n", seed)

	var ops atomic.Uint64
	var errCount atomic.Uint64
	var bytesTransferred atomic.Uint64

	readLat := &LatencyTracker{}
	writeLat := &LatencyTracker{}

	// Pre-generate pseudo-random payload using specified seed
	payload := make([]byte, valSize)
	valRng := rand.New(rand.NewSource(seed))
	_, _ = valRng.Read(payload)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	if proto == "grpc" {
		cl, err := client.New(client.Options{
			Addr:     addr,
			PoolSize: conns,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
			os.Exit(1)
		}
		defer cl.Close()

		for workerID := 0; workerID < workers; workerID++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed + int64(id)*1000))

				for ctx.Err() == nil {
					keyNum := rng.Intn(keyCount)
					// 16-byte fixed width format: user000000012345
					key := []byte(fmt.Sprintf("user%012d", keyNum))

					isWrite := false
					isScan := false
					switch workload {
					case "fillrandom":
						isWrite = true
					case "readrandom":
						isWrite = false
					case "scan":
						isScan = true
					case "mixed":
						isWrite = rng.Float32() < 0.20 // 80% read, 20% write
					case "write-heavy":
						isWrite = rng.Float32() < 0.90
					case "read-heavy":
						isWrite = rng.Float32() < 0.10
					case "balanced":
						isWrite = rng.Float32() < 0.50
					default:
						isWrite = rng.Float32() < 0.50
					}

					opStart := time.Now()
					if isScan {
						startKey := key
						endKey := []byte(fmt.Sprintf("user%012d", keyNum+100))
						entries, err := cl.Scan(ctx, startKey, endKey, 100)
						elapsed := time.Since(opStart)
						if err != nil && ctx.Err() == nil {
							errCount.Add(1)
						} else if err == nil {
							ops.Add(1)
							readLat.Record(elapsed)
							for _, e := range entries {
								bytesTransferred.Add(uint64(len(e.Key) + len(e.Value)))
							}
						}
						continue
					}

					if isWrite {
						err := cl.Put(ctx, key, payload)
						elapsed := time.Since(opStart)
						if err != nil && ctx.Err() == nil {
							errCount.Add(1)
						} else if err == nil {
							ops.Add(1)
							bytesTransferred.Add(uint64(len(key) + len(payload)))
							writeLat.Record(elapsed)
						}
					} else {
						val, err := cl.Get(ctx, key)
						elapsed := time.Since(opStart)
						if err != nil && !errors.Is(err, client.ErrNotFound) && ctx.Err() == nil {
							errCount.Add(1)
						} else {
							ops.Add(1)
							if err == nil {
								bytesTransferred.Add(uint64(len(key) + len(val)))
							}
							readLat.Record(elapsed)
						}
					}
				}
			}(workerID)
		}
	} else {
		// REST HTTP benchmark
		baseURL := addr
		if !bytes.HasPrefix([]byte(baseURL), []byte("http://")) && !bytes.HasPrefix([]byte(baseURL), []byte("https://")) {
			baseURL = "http://" + baseURL
		}

		httpClient := &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        workers * 2,
				MaxIdleConnsPerHost: workers * 2,
				IdleConnTimeout:     30 * time.Second,
			},
			Timeout: 5 * time.Second,
		}

		for workerID := 0; workerID < workers; workerID++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed + int64(id)*1000))

				for ctx.Err() == nil {
					keyNum := rng.Intn(keyCount)
					keyStr := fmt.Sprintf("user%012d", keyNum)

					isWrite := false
					switch workload {
					case "fillrandom":
						isWrite = true
					case "readrandom":
						isWrite = false
					case "mixed":
						isWrite = rng.Float32() < 0.20
					case "write-heavy":
						isWrite = rng.Float32() < 0.90
					case "read-heavy":
						isWrite = rng.Float32() < 0.10
					default:
						isWrite = rng.Float32() < 0.50
					}

					opStart := time.Now()
					if isWrite {
						req, err := http.NewRequestWithContext(ctx, http.MethodPut, baseURL+"/v1/kv/"+keyStr, bytes.NewReader(payload))
						if err == nil {
							resp, err := httpClient.Do(req)
							elapsed := time.Since(opStart)
							if err != nil && ctx.Err() == nil {
								errCount.Add(1)
							} else if err == nil {
								_, _ = io.Copy(io.Discard, resp.Body)
								_ = resp.Body.Close()
								if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
									ops.Add(1)
									bytesTransferred.Add(uint64(len(keyStr) + len(payload)))
									writeLat.Record(elapsed)
								} else {
									errCount.Add(1)
								}
							}
						}
					} else {
						req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/kv/"+keyStr, nil)
						if err == nil {
							resp, err := httpClient.Do(req)
							elapsed := time.Since(opStart)
							if err != nil && ctx.Err() == nil {
								errCount.Add(1)
							} else if err == nil {
								n, _ := io.Copy(io.Discard, resp.Body)
								_ = resp.Body.Close()
								if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
									ops.Add(1)
									bytesTransferred.Add(uint64(n))
									readLat.Record(elapsed)
								} else {
									errCount.Add(1)
								}
							}
						}
					}
				}
			}(workerID)
		}
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	totalOps := ops.Load()
	totalErrs := errCount.Load()
	throughput := float64(totalOps) / totalDuration.Seconds()
	mbTransferred := float64(bytesTransferred.Load()) / (1024 * 1024)
	bandwidth := mbTransferred / totalDuration.Seconds()

	fmt.Printf("=== Results ===\n")
	fmt.Printf("Total Completed Ops: %d\n", totalOps)
	fmt.Printf("Total Errors:        %d\n", totalErrs)
	fmt.Printf("Elapsed Time:        %.2fs\n", totalDuration.Seconds())
	fmt.Printf("Throughput:          %.1f ops/sec\n", throughput)
	fmt.Printf("Bandwidth:           %.2f MB/sec\n\n", bandwidth)

	rp50, rp90, rp95, rp99, rp999, ravg := readLat.Percentiles()
	wp50, wp90, wp95, wp99, wp999, wavg := writeLat.Percentiles()

	fmt.Printf("Latency (Read):\n")
	fmt.Printf("  p50:   %v\n", rp50)
	fmt.Printf("  p90:   %v\n", rp90)
	fmt.Printf("  p95:   %v\n", rp95)
	fmt.Printf("  p99:   %v\n", rp99)
	fmt.Printf("  p99.9: %v\n", rp999)
	fmt.Printf("  avg:   %v\n\n", ravg)

	fmt.Printf("Latency (Write):\n")
	fmt.Printf("  p50:   %v\n", wp50)
	fmt.Printf("  p90:   %v\n", wp90)
	fmt.Printf("  p95:   %v\n", wp95)
	fmt.Printf("  p99:   %v\n", wp99)
	fmt.Printf("  p99.9: %v\n", wp999)
	fmt.Printf("  avg:   %v\n", wavg)

	if csvFilePath != "" {
		writeCSVResult(csvFilePath, workload, throughput, bandwidth, rp50, rp95, rp99, rp999, wp50, wp95, wp99, wp999, totalErrs)
	}
}

func writeCSVResult(path, workload string, throughput, bandwidth float64, rp50, rp95, rp99, rp999, wp50, wp95, wp99, wp999 time.Duration, errs uint64) {
	fileExists := false
	if _, err := os.Stat(path); err == nil {
		fileExists = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open csv file: %v\n", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if !fileExists {
		_ = w.Write([]string{
			"workload", "throughput_ops_sec", "bandwidth_mb_sec",
			"read_p50_us", "read_p95_us", "read_p99_us", "read_p999_us",
			"write_p50_us", "write_p95_us", "write_p99_us", "write_p999_us",
			"errors",
		})
	}

	_ = w.Write([]string{
		workload,
		fmt.Sprintf("%.1f", throughput),
		fmt.Sprintf("%.2f", bandwidth),
		fmt.Sprintf("%d", rp50.Microseconds()),
		fmt.Sprintf("%d", rp95.Microseconds()),
		fmt.Sprintf("%d", rp99.Microseconds()),
		fmt.Sprintf("%d", rp999.Microseconds()),
		fmt.Sprintf("%d", wp50.Microseconds()),
		fmt.Sprintf("%d", wp95.Microseconds()),
		fmt.Sprintf("%d", wp99.Microseconds()),
		fmt.Sprintf("%d", wp999.Microseconds()),
		fmt.Sprintf("%d", errs),
	})
}
