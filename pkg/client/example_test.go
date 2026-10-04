package client_test

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"vegadb/internal/grpcserver"
	"vegadb/internal/metrics"
	"vegadb/internal/service"
	"vegadb/pkg/client"
)

func Example() {
	lis := bufconn.Listen(1024 * 1024)
	m := metrics.New()
	store := service.NewMemStore()
	svc, _ := service.New(store, m, service.DefaultConfig())
	srv := grpcserver.New(svc, grpcserver.ServerOptions{})
	go func() {
		_ = srv.GRPCServer().Serve(lis)
	}()
	defer srv.GRPCServer().Stop()
	defer lis.Close()

	cl, err := client.New(client.Options{
		Addr:     "passthrough://bufnet",
		PoolSize: 1,
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
		},
	})
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := cl.Put(ctx, []byte("user"), []byte("alice")); err != nil {
		panic(err)
	}

	val, err := cl.Get(ctx, []byte("user"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	// Output:
	// alice
}
