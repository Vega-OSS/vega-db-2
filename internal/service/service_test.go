package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vegadb/internal/metrics"
	"vegadb/internal/service"
)

func TestServiceBasicOps(t *testing.T) {
	m := metrics.New()
	store := service.NewMemStore()
	svc, err := service.New(store, m, service.DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	ctx := context.Background()

	// Put
	if err := svc.Put(ctx, []byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}

	// Get
	val, err := svc.Get(ctx, []byte("key1"), nil)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if string(val) != "val1" {
		t.Fatalf("expected val1, got %s", string(val))
	}

	// Delete
	if err := svc.Delete(ctx, []byte("key1")); err != nil {
		t.Fatalf("unexpected del error: %v", err)
	}

	// Get after Delete
	_, err = svc.Get(ctx, []byte("key1"), nil)
	if !errors.Is(err, service.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestServiceInflightLimiter(t *testing.T) {
	m := metrics.New()
	store := service.NewMemStore()
	cfg := service.Config{
		MaxInflight:  2,
		MaxKeySize:   1024,
		MaxValueSize: 1024,
	}
	svc, err := service.New(store, m, cfg)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	_ = store.Put([]byte("k1"), []byte("v1"))

	// Fill the semaphore
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel1()
	defer cancel2()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = svc.Scan(ctx1, nil, nil, 0, func(_, _ []byte) bool {
			time.Sleep(50 * time.Millisecond)
			return true
		})
	}()

	go func() {
		defer wg.Done()
		_ = svc.Scan(ctx2, nil, nil, 0, func(_, _ []byte) bool {
			time.Sleep(50 * time.Millisecond)
			return true
		})
	}()

	time.Sleep(10 * time.Millisecond)

	// Third request with canceled or tight context should fail with ErrOverloaded
	ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancelTimeout()

	err = svc.Put(ctxTimeout, []byte("k"), []byte("v"))
	if !errors.Is(err, service.ErrOverloaded) {
		t.Fatalf("expected ErrOverloaded, got %v", err)
	}

	wg.Wait()
}
