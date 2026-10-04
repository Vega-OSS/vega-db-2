package service

import (
	"context"
	"errors"

	"vegadb/internal/metrics"
)

// Service errors mapped across transports.
var (
	// ErrInvalid indicates parameter validation failed (e.g. empty key, key/value too large).
	ErrInvalid = errors.New("service: invalid argument")

	// ErrOverloaded indicates max in-flight requests was exceeded or context expired waiting.
	ErrOverloaded = errors.New("service: server overloaded")

	// ErrInternal indicates an unhandled internal error.
	ErrInternal = errors.New("service: internal error")
)

// Config holds service constraints and limits.
type Config struct {
	MaxInflight  int
	MaxKeySize   int
	MaxValueSize int
}

// DefaultConfig provides sensible defaults matching specification.
func DefaultConfig() Config {
	return Config{
		MaxInflight:  2048,
		MaxKeySize:   8192,
		MaxValueSize: 16 * 1024 * 1024, // 16 MiB
	}
}

// Service provides a concurrency-bounded, metrics-tracked layer over Store.
type Service struct {
	store Store
	sem   chan struct{}
	m     *metrics.Metrics
	cfg   Config
}

// New constructs a Service wrapping the given Store and Metrics registry.
func New(store Store, m *metrics.Metrics, cfg Config) (*Service, error) {
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 2048
	}
	if cfg.MaxKeySize <= 0 {
		cfg.MaxKeySize = 8192
	}
	if cfg.MaxValueSize <= 0 {
		cfg.MaxValueSize = 16 * 1024 * 1024
	}
	return &Service{
		store: store,
		sem:   make(chan struct{}, cfg.MaxInflight),
		m:     m,
		cfg:   cfg,
	}, nil
}

func (s *Service) acquire(ctx context.Context) error {
	select {
	case s.sem <- struct{}{}:
		s.m.Inflight.Add(1)
		return nil
	default:
		// Queue wait bounded by context
		select {
		case s.sem <- struct{}{}:
			s.m.Inflight.Add(1)
			return nil
		case <-ctx.Done():
			s.m.RejectedOverloaded.Add(1)
			return ErrOverloaded
		}
	}
}

func (s *Service) release() {
	s.m.Inflight.Add(-1)
	<-s.sem
}

// Put validates request limits, acquires concurrency budget, and writes key-value pair.
func (s *Service) Put(ctx context.Context, key, value []byte) error {
	if len(key) == 0 || len(key) > s.cfg.MaxKeySize || len(value) > s.cfg.MaxValueSize {
		return ErrInvalid
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()

	s.m.Puts.Add(1)
	s.m.BytesIn.Add(int64(len(key) + len(value)))

	err := s.store.Put(key, value)
	if err != nil {
		if errors.Is(err, ErrClosed) {
			return ErrClosed
		}
		return ErrInternal
	}
	return nil
}

// Get validates key, acquires concurrency budget, and retrieves key value.
func (s *Service) Get(ctx context.Context, key, dst []byte) ([]byte, error) {
	if len(key) == 0 || len(key) > s.cfg.MaxKeySize {
		return nil, ErrInvalid
	}
	if err := s.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.release()

	s.m.Gets.Add(1)
	val, err := s.store.Get(key, dst)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.m.GetMisses.Add(1)
			return nil, ErrNotFound
		}
		if errors.Is(err, ErrClosed) {
			return nil, ErrClosed
		}
		return nil, ErrInternal
	}
	s.m.BytesOut.Add(int64(len(val)))
	return val, nil
}

// Delete validates key, acquires concurrency budget, and deletes key.
func (s *Service) Delete(ctx context.Context, key []byte) error {
	if len(key) == 0 || len(key) > s.cfg.MaxKeySize {
		return ErrInvalid
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()

	s.m.Deletes.Add(1)
	err := s.store.Delete(key)
	if err != nil {
		if errors.Is(err, ErrClosed) {
			return ErrClosed
		}
		return ErrInternal
	}
	return nil
}

// Scan scans keys in [start, end) respecting context cancellation.
func (s *Service) Scan(ctx context.Context, start, end []byte, limit int, fn func(k, v []byte) bool) error {
	if len(start) > s.cfg.MaxKeySize || len(end) > s.cfg.MaxKeySize {
		return ErrInvalid
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()

	s.m.Scans.Add(1)

	err := s.store.Scan(start, end, limit, func(k, v []byte) bool {
		if ctx.Err() != nil {
			return false
		}
		s.m.ScanEntries.Add(1)
		s.m.BytesOut.Add(int64(len(k) + len(v)))
		return fn(k, v)
	})
	if err != nil {
		if errors.Is(err, ErrClosed) {
			return ErrClosed
		}
		return ErrInternal
	}
	return ctx.Err()
}

// Stats returns a snapshot of metrics.
func (s *Service) Stats() map[string]int64 {
	return s.m.Snapshot()
}

// Close closes the underlying store.
func (s *Service) Close() error {
	return s.store.Close()
}
