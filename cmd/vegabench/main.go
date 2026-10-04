package main

import (
	"bytes"
	"context"
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

type LatencyTracker struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (lt *LatencyTracker) Record(d time.Duration) {
	lt.mu.Lock()
	if len(lt.samples) < 500000 {
		lt.samples = append(lt.samples, d)
	}
	lt.mu.Unlock()
}

func (lt *LatencyTracker) Percentiles() (p50, p90, p99, p999, avg time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	n := len(lt.samples)
	if n == 0 {
		return 0, 0, 0, 0, 0
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
	p99 = sorted[int(float64(n)*0.99)]
	p999Idx := int(float64(n) * 0.999)
	if p999Idx >= n {
		p999Idx = n - 1
	}
	p999 = sorted[p999Idx]

	return p50, p90, p99, p999, avg
}

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "Server target address")
	protocol := flag.String("protocol", "grpc", "Benchmark protocol (grpc|rest)")
	workload := flag.String("workload", "balanced", "Workload type (write-heavy|read-heavy|balanced|scan)")
	concurrency := flag.Int("concurrency", 16, "Number of concurrent worker goroutines")
	duration := flag.Duration("duration", 10*time.Second, "Benchmark duration")
	keyCount := flag.Int("keys", 100000, "Distinct keys in keyspace")
	valSize := flag.Int("val-size", 1024, "Value size in bytes")
	flag.Parse()

	fmt.Printf("=== VegaDB Benchmark (vegabench) ===\n")
	fmt.Printf("Target:      %s (%s)\n", *addr, *protocol)
	fmt.Printf("Workload:    %s\n", *workload)
	fmt.Printf("Concurrency: %d workers\n", *concurrency)
	fmt.Printf("Duration:    %v\n", *duration)
	fmt.Printf("Key Space:   %d keys\n", *keyCount)
	fmt.Printf("Value Size:  %d bytes\n\n", *valSize)

	var ops atomic.Uint64
	var errCount atomic.Uint64
	var bytesTransferred atomic.Uint64

	readLat := &LatencyTracker{}
	writeLat := &LatencyTracker{}

	// Pre-generate random payload
	payload := make([]byte, *valSize)
	rand.New(rand.NewSource(42)).Read(payload)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	if *protocol == "grpc" {
		cl, err := client.New(client.Options{
			Addr:     *addr,
			PoolSize: *concurrency,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
			os.Exit(1)
		}
		defer cl.Close()

		for workerID := 0; workerID < *concurrency; workerID++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1000))

				for ctx.Err() == nil {
					keyNum := rng.Intn(*keyCount)
					key := []byte(fmt.Sprintf("key_%08d", keyNum))

					isWrite := false
					switch *workload {
					case "write-heavy":
						isWrite = rng.Float32() < 0.90
					case "read-heavy":
						isWrite = rng.Float32() < 0.10
					case "balanced":
						isWrite = rng.Float32() < 0.50
					case "scan":
						isWrite = false
					default:
						isWrite = rng.Float32() < 0.50
					}

					opStart := time.Now()
					if *workload == "scan" {
						startKey := key
						endKey := []byte(fmt.Sprintf("key_%08d", keyNum+64))
						entries, err := cl.Scan(ctx, startKey, endKey, 64)
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
		baseURL := *addr
		if !bytes.HasPrefix([]byte(baseURL), []byte("http://")) && !bytes.HasPrefix([]byte(baseURL), []byte("https://")) {
			baseURL = "http://" + baseURL
		}

		httpClient := &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        *concurrency * 2,
				MaxIdleConnsPerHost: *concurrency * 2,
				IdleConnTimeout:     30 * time.Second,
			},
			Timeout: 5 * time.Second,
		}

		for workerID := 0; workerID < *concurrency; workerID++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1000))

				for ctx.Err() == nil {
					keyNum := rng.Intn(*keyCount)
					keyStr := fmt.Sprintf("key_%08d", keyNum)

					isWrite := false
					switch *workload {
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

	rp50, rp90, rp99, rp999, ravg := readLat.Percentiles()
	wp50, wp90, wp99, wp999, wavg := writeLat.Percentiles()

	fmt.Printf("Latency (Read):\n")
	fmt.Printf("  p50:   %v\n", rp50)
	fmt.Printf("  p90:   %v\n", rp90)
	fmt.Printf("  p99:   %v\n", rp99)
	fmt.Printf("  p99.9: %v\n", rp999)
	fmt.Printf("  avg:   %v\n\n", ravg)

	fmt.Printf("Latency (Write):\n")
	fmt.Printf("  p50:   %v\n", wp50)
	fmt.Printf("  p90:   %v\n", wp90)
	fmt.Printf("  p99:   %v\n", wp99)
	fmt.Printf("  p99.9: %v\n", wp999)
	fmt.Printf("  avg:   %v\n", wavg)
}
