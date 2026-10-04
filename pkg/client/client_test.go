package client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"vegadb/internal/grpcserver"
	"vegadb/internal/metrics"
	"vegadb/internal/service"
	"vegadb/pkg/client"
)

const bufSize = 1024 * 1024

func setupServerAndClient(t *testing.T, poolSize int) (*client.Client, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	m := metrics.New()
	store := service.NewMemStore()
	svc, err := service.New(store, m, service.DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	srv := grpcserver.New(svc, grpcserver.ServerOptions{EnableReflection: true})
	go func() {
		_ = srv.GRPCServer().Serve(lis)
	}()

	cl, err := client.New(client.Options{
		Addr:     "passthrough://bufnet",
		PoolSize: poolSize,
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	cleanup := func() {
		_ = cl.Close()
		srv.GRPCServer().Stop()
		_ = lis.Close()
	}
	return cl, cleanup
}

func TestClientBasicCRUD(t *testing.T) {
	cl, cleanup := setupServerAndClient(t, 4)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Put
	key := []byte("alpha")
	val := []byte("omega")
	if err := cl.Put(ctx, key, val); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Get
	got, err := cl.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("got %s, want %s", got, val)
	}

	// Delete
	if err := cl.Delete(ctx, key); err != nil {
		t.Fatalf("del: %v", err)
	}

	// Get after delete -> ErrNotFound
	_, err = cl.Get(ctx, key)
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestClientScanAndStats(t *testing.T) {
	cl, cleanup := setupServerAndClient(t, 2)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := range 20 {
		k := fmt.Sprintf("k%02d", i)
		v := fmt.Sprintf("v%02d", i)
		if err := cl.Put(ctx, []byte(k), []byte(v)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Scan
	entries, err := cl.Scan(ctx, []byte("k05"), []byte("k15"), 5)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
	if string(entries[0].Key) != "k05" {
		t.Fatalf("expected first key k05, got %s", string(entries[0].Key))
	}

	// Stats
	counters, err := cl.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if counters["puts"] < 20 {
		t.Fatalf("expected at least 20 puts in stats, got %d", counters["puts"])
	}
}
