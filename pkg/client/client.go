package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"vegadb/gen/vegapb"
)

// Sentinel client errors.
var (
	ErrNotFound = errors.New("key not found")
	ErrClosed   = errors.New("client closed")
)

// Options configures the pooled gRPC client.
type Options struct {
	Addr        string
	PoolSize    int
	DialTimeout time.Duration
	DialOptions []grpc.DialOption
}

// DefaultOptions returns standard client settings.
func DefaultOptions() Options {
	return Options{
		Addr:        "127.0.0.1:50051",
		PoolSize:    4,
		DialTimeout: 5 * time.Second,
	}
}

// Client represents a thread-safe connection-pooled VegaDB client.
type Client struct {
	opts    Options
	conns   []*grpc.ClientConn
	clients []vegapb.VegaClient
	next    atomic.Uint64
	closed  atomic.Bool
}

// New creates a new Client with a connection pool to the specified server address.
func New(opts Options) (*Client, error) {
	if opts.Addr == "" {
		opts.Addr = "127.0.0.1:50051"
	}
	if opts.PoolSize <= 0 {
		opts.PoolSize = 4
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 5 * time.Second
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if len(opts.DialOptions) > 0 {
		dialOpts = append(dialOpts, opts.DialOptions...)
	}

	conns := make([]*grpc.ClientConn, opts.PoolSize)
	clients := make([]vegapb.VegaClient, opts.PoolSize)

	for i := 0; i < opts.PoolSize; i++ {
		conn, err := grpc.NewClient(opts.Addr, dialOpts...)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = conns[j].Close()
			}
			return nil, fmt.Errorf("client: dial %s: %w", opts.Addr, err)
		}
		conns[i] = conn
		clients[i] = vegapb.NewVegaClient(conn)
	}

	return &Client{
		opts:    opts,
		conns:   conns,
		clients: clients,
	}, nil
}

func (c *Client) client() (vegapb.VegaClient, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	idx := c.next.Add(1) % uint64(len(c.clients))
	return c.clients[idx], nil
}

// Put inserts or updates key with value.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	cl, err := c.client()
	if err != nil {
		return err
	}
	_, err = cl.Put(ctx, &vegapb.PutRequest{Key: key, Value: value})
	return mapGRPCError(err)
}

// Get retrieves value for key. Returns ErrNotFound if missing.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	cl, err := c.client()
	if err != nil {
		return nil, err
	}
	resp, err := cl.Get(ctx, &vegapb.GetRequest{Key: key})
	if err != nil {
		return nil, mapGRPCError(err)
	}
	return resp.Value, nil
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	cl, err := c.client()
	if err != nil {
		return err
	}
	_, err = cl.Delete(ctx, &vegapb.DeleteRequest{Key: key})
	return mapGRPCError(err)
}

// ScanEntry represents a single key-value entry returned from Scan.
type ScanEntry struct {
	Key   []byte
	Value []byte
}

// Scan streams keys in [start, end) up to limit entries.
func (c *Client) Scan(ctx context.Context, start, end []byte, limit uint32) ([]ScanEntry, error) {
	cl, err := c.client()
	if err != nil {
		return nil, err
	}
	stream, err := cl.Scan(ctx, &vegapb.ScanRequest{Start: start, End: end, Limit: limit})
	if err != nil {
		return nil, mapGRPCError(err)
	}

	var results []ScanEntry
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, mapGRPCError(err)
		}
		results = append(results, ScanEntry{
			Key:   resp.Key,
			Value: resp.Value,
		})
	}
	return results, nil
}

// Stats returns internal server metrics.
func (c *Client) Stats(ctx context.Context) (map[string]int64, error) {
	cl, err := c.client()
	if err != nil {
		return nil, err
	}
	resp, err := cl.Stats(ctx, &vegapb.StatsRequest{})
	if err != nil {
		return nil, mapGRPCError(err)
	}
	return resp.Counters, nil
}

// Close gracefully closes all pooled client connections.
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	var firstErr error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func mapGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.NotFound {
			return ErrNotFound
		}
	}
	return err
}
