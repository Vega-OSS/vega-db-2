package grpcserver_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"vegadb/gen/vegapb"
	"vegadb/internal/grpcserver"
	"vegadb/internal/metrics"
	"vegadb/internal/service"
)

const bufSize = 1024 * 1024

func setupTestServer(t *testing.T) (vegapb.VegaClient, healthpb.HealthClient, func()) {
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

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}

	client := vegapb.NewVegaClient(conn)
	healthClient := healthpb.NewHealthClient(conn)

	cleanup := func() {
		_ = conn.Close()
		srv.GRPCServer().Stop()
		_ = lis.Close()
	}
	return client, healthClient, cleanup
}

func TestGRPCServerOperations(t *testing.T) {
	client, healthClient, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Health check
	hResp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{Service: "vega.v1.Vega"})
	if err != nil || hResp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health check failed: %v, status: %v", err, hResp.GetStatus())
	}

	// Put
	_, err = client.Put(ctx, &vegapb.PutRequest{Key: []byte("k1"), Value: []byte("v1")})
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Get
	gResp, err := client.Get(ctx, &vegapb.GetRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(gResp.GetValue()) != "v1" {
		t.Fatalf("unexpected value: got %s, want v1", string(gResp.GetValue()))
	}

	// Get NotFound
	_, err = client.Get(ctx, &vegapb.GetRequest{Key: []byte("k2")})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound code, got %v", status.Code(err))
	}

	// Delete
	_, err = client.Delete(ctx, &vegapb.DeleteRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Get after Delete
	_, err = client.Get(ctx, &vegapb.GetRequest{Key: []byte("k1")})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound code after delete, got %v", status.Code(err))
	}
}

func TestGRPCServerScanStream(t *testing.T) {
	client, _, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Insert 100 entries
	for i := range 100 {
		k := []byte{byte(i)}
		v := []byte{byte(i * 2)}
		_, err := client.Put(ctx, &vegapb.PutRequest{Key: k, Value: v})
		if err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	stream, err := client.Scan(ctx, &vegapb.ScanRequest{Start: []byte{10}, End: []byte{30}, Limit: 100})
	if err != nil {
		t.Fatalf("scan call failed: %v", err)
	}

	count := 0
	for {
		kv, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream recv error: %v", err)
		}
		count++
		if len(kv.GetKey()) == 0 {
			t.Fatal("empty key returned")
		}
	}
	if count != 20 {
		t.Fatalf("expected 20 items in scan [10, 30), got %d", count)
	}
}

func TestGRPCServerConcurrentClients(t *testing.T) {
	client, _, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	var wg sync.WaitGroup
	workers := 50

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := []byte{byte(id)}
			v := []byte("val")
			_, _ = client.Put(ctx, &vegapb.PutRequest{Key: k, Value: v})
			_, _ = client.Get(ctx, &vegapb.GetRequest{Key: k})
		}(i)
	}
	wg.Wait()
}
