package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"vegadb/internal/engine"
	"vegadb/internal/grpcserver"
	"vegadb/internal/metrics"
	"vegadb/internal/restserver"
	"vegadb/internal/service"
)

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if val, ok := os.LookupEnv(key); ok {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return defaultVal
}

func runHealthcheck(args []string) {
	fs := flag.NewFlagSet("healthcheck", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:50051", "gRPC address for health check")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: "vega.v1.Vega"})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		fmt.Fprintf(os.Stderr, "health check failed: %v, status: %v\n", err, resp.GetStatus())
		os.Exit(1)
	}
	os.Exit(0)
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		runHealthcheck(os.Args[2:])
		return
	}

	dataDir := flag.String("data-dir", getEnv("VEGA_DATA_DIR", "/data"), "Root data directory")
	grpcAddr := flag.String("grpc-addr", getEnv("VEGA_GRPC_ADDR", ":50051"), "gRPC listen address")
	httpAddr := flag.String("http-addr", getEnv("VEGA_HTTP_ADDR", ":8080"), "REST listen address")
	pprofAddr := flag.String("pprof-addr", getEnv("VEGA_PPROF_ADDR", ""), "pprof/expvar listen address")
	syncStr := flag.String("sync", getEnv("VEGA_SYNC", "always"), "fsync policy (always|interval|never)")
	memtableMB := flag.Int("memtable-mb", getEnvInt("VEGA_MEMTABLE_MB", 64), "Memtable freeze size in MiB")
	vlogSegmentMB := flag.Int64("vlog-segment-mb", getEnvInt64("VEGA_VLOG_SEGMENT_MB", 1024), "vLog segment size in MiB")
	maxInflight := flag.Int("max-inflight", getEnvInt("VEGA_MAX_INFLIGHT", 2048), "Max concurrent requests")
	maxKeyBytes := flag.Int("max-key-bytes", getEnvInt("VEGA_MAX_KEY_BYTES", 8192), "Max key size in bytes")
	maxValueMB := flag.Int("max-value-mb", getEnvInt("VEGA_MAX_VALUE_MB", 16), "Max value size in MiB")
	scanPrefetch := flag.Int("scan-prefetch", getEnvInt("VEGA_SCAN_PREFETCH", 8), "Goroutines fetching scan values")
	blockCacheMB := flag.Int("block-cache-mb", getEnvInt("VEGA_BLOCK_CACHE_MB", 64), "Block cache size in MiB")
	logLevelStr := flag.String("log-level", getEnv("VEGA_LOG_LEVEL", "info"), "Log level (debug|info|warn|error)")
	storeType := flag.String("store", getEnv("VEGA_STORE", "engine"), "Storage engine type (memory|engine)")
	reflection := flag.Bool("reflection", getEnvBool("VEGA_REFLECTION", false), "Enable gRPC reflection")
	flag.Parse()

	var logLevel slog.Level
	switch strings.ToLower(*logLevelStr) {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	logger.Info("starting vegaserver",
		"data_dir", *dataDir,
		"grpc_addr", *grpcAddr,
		"http_addr", *httpAddr,
		"store", *storeType,
	)

	var store service.Store
	if *storeType == "memory" {
		logger.Warn("WARNING: in-memory store, data is NOT persistent")
		store = service.NewMemStore()
	} else {
		syncPolicy := engine.SyncAlways
		switch strings.ToLower(*syncStr) {
		case "interval":
			syncPolicy = engine.SyncInterval
		case "never":
			syncPolicy = engine.SyncNever
		}

		opts := engine.Options{
			Dir:             *dataDir,
			MemtableSize:    *memtableMB * 1024 * 1024,
			VlogSegmentSize: *vlogSegmentMB * 1024 * 1024,
			MaxKeySize:      *maxKeyBytes,
			MaxValueSize:    *maxValueMB * 1024 * 1024,
			ScanPrefetch:    *scanPrefetch,
			BlockCacheSize:  *blockCacheMB * 1024 * 1024,
			Sync:            syncPolicy,
		}

		eng, err := engine.Open(opts)
		if err != nil {
			logger.Error("failed to open engine", "err", err)
			os.Exit(1)
		}
		store = eng
	}

	m := metrics.New()
	cfg := service.Config{
		MaxInflight:  *maxInflight,
		MaxKeySize:   *maxKeyBytes,
		MaxValueSize: *maxValueMB * 1024 * 1024,
	}

	svc, err := service.New(store, m, cfg)
	if err != nil {
		logger.Error("failed to initialize service", "err", err)
		os.Exit(1)
	}

	// Start pprof/expvar if requested
	if *pprofAddr != "" {
		go func() {
			logger.Info("pprof and expvar server starting", "addr", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil && err != http.ErrServerClosed {
				logger.Error("pprof server error", "err", err)
			}
		}()
	}

	// Start gRPC server
	grpcListener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		logger.Error("failed to listen on gRPC address", "addr", *grpcAddr, "err", err)
		os.Exit(1)
	}

	enableReflect := *reflection || logLevel == slog.LevelDebug
	grpcSrv := grpcserver.New(svc, grpcserver.ServerOptions{
		EnableReflection: enableReflect,
		Logger:           logger,
	})

	go func() {
		logger.Info("gRPC server listening", "addr", *grpcAddr)
		if err := grpcSrv.GRPCServer().Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("gRPC server failed", "err", err)
		}
	}()

	// Start REST server
	restSrv := restserver.New(svc, restserver.Options{
		Addr:         *httpAddr,
		Logger:       logger,
		MaxValueSize: int64(cfg.MaxValueSize),
	})

	httpListener, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		logger.Error("failed to listen on HTTP address", "addr", *httpAddr, "err", err)
		os.Exit(1)
	}

	go func() {
		logger.Info("REST server listening", "addr", *httpAddr)
		if err := restSrv.HTTPServer().Serve(httpListener); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed", "err", err)
		}
	}()

	// Graceful shutdown handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	logger.Info("shutdown signal received, initiating graceful shutdown")

	// 1. Mark not serving
	grpcSrv.MarkNotServing()
	restSrv.SetReady(false)

	// 2. Stop gRPC server with 20s timeout
	shutdownDone := make(chan struct{})
	go func() {
		grpcSrv.GRPCServer().GracefulStop()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		logger.Info("gRPC server gracefully stopped")
	case <-time.After(20 * time.Second):
		logger.Warn("gRPC server graceful stop timed out, forcing stop")
		grpcSrv.GRPCServer().Stop()
	}

	// 3. Stop HTTP server with 20s timeout
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer httpCancel()
	if err := restSrv.HTTPServer().Shutdown(httpCtx); err != nil {
		logger.Warn("HTTP server shutdown error", "err", err)
	} else {
		logger.Info("HTTP server gracefully stopped")
	}

	// 4. Close database store
	if err := svc.Close(); err != nil {
		logger.Error("error closing store", "err", err)
	} else {
		logger.Info("store closed cleanly")
	}

	logger.Info("shutdown complete")
	os.Exit(0)
}
