package restserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Vega-OSS/vega-db/internal/service"
)

// Server encapsulates the HTTP REST API server for VegaDB.
type Server struct {
	svc        *service.Service
	httpServer *http.Server
	ready      atomic.Bool
	logger     *slog.Logger
	maxValSize int64
}

// Options configures the REST server.
type Options struct {
	Addr         string
	Logger       *slog.Logger
	MaxValueSize int64
}

// New creates and initializes a new REST Server using Go 1.22 ServeMux method patterns.
func New(svc *service.Service, opts Options) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	maxVal := opts.MaxValueSize
	if maxVal <= 0 {
		maxVal = 16 * 1024 * 1024 // 16 MiB default
	}

	s := &Server{
		svc:        svc,
		logger:     logger,
		maxValSize: maxVal,
	}
	s.ready.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/kv/{key}", s.handlePut)
	mux.HandleFunc("GET /v1/kv/{key}", s.handleGet)
	mux.HandleFunc("DELETE /v1/kv/{key}", s.handleDelete)
	mux.HandleFunc("GET /v1/scan", s.handleScan)
	mux.HandleFunc("POST /v1/batch", s.handleBatch)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /v1/stats", s.handleStats)

	s.httpServer = &http.Server{
		Addr:              opts.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 * 1024 * 1024,
	}

	return s
}

// HTTPServer returns the underlying *http.Server.
func (s *Server) HTTPServer() *http.Server {
	return s.httpServer
}

// SetReady updates the readiness status of the server.
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) parseKey(r *http.Request) ([]byte, error) {
	rawKey := r.PathValue("key")
	if r.Header.Get("X-Key-Encoding") == "base64url" {
		decoded, err := base64.RawURLEncoding.DecodeString(rawKey)
		if err != nil {
			return nil, err
		}
		return decoded, nil
	}
	return []byte(rawKey), nil
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, service.ErrInvalid):
		http.Error(w, "invalid argument", http.StatusBadRequest)
	case errors.Is(err, service.ErrOverloaded):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "too many requests", http.StatusTooManyRequests)
	case errors.Is(err, service.ErrClosed):
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key, err := s.parseKey(r)
	if err != nil || len(key) == 0 {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	if r.ContentLength < 0 {
		http.Error(w, "Length Required", http.StatusLengthRequired)
		return
	}
	if r.ContentLength > s.maxValSize {
		http.Error(w, "Payload Too Large", http.StatusRequestEntityTooLarge)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.maxValSize)
	val, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "Payload Too Large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	if err := s.svc.Put(r.Context(), key, val); err != nil {
		s.writeError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key, err := s.parseKey(r)
	if err != nil || len(key) == 0 {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	val, err := s.svc.Get(r.Context(), key, nil)
	if err != nil {
		s.writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(val)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(val)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key, err := s.parseKey(r)
	if err != nil || len(key) == 0 {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}

	if err := s.svc.Delete(r.Context(), key); err != nil {
		s.writeError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type scanItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	start := []byte(query.Get("start"))
	end := []byte(query.Get("end"))
	limit := 0
	if limitStr := query.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)

	bw := bufio.NewWriter(w)
	defer func() { _ = bw.Flush() }()

	enc := json.NewEncoder(bw)
	err := s.svc.Scan(r.Context(), start, end, limit, func(k, v []byte) bool {
		item := scanItem{
			Key:   string(k),
			Value: base64.StdEncoding.EncodeToString(v),
		}
		if err := enc.Encode(item); err != nil {
			return false
		}
		_ = bw.Flush()
		return true
	})
	if err != nil && !errors.Is(err, io.EOF) {
		s.logger.Debug("scan terminated", "error", err)
	}
}

type batchOp struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"` // base64
}

type batchRequest struct {
	Ops []batchOp `json:"ops"`
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxValSize)
	var req batchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json payload", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	for _, op := range req.Ops {
		k := []byte(op.Key)
		switch op.Op {
		case "put":
			v, err := base64.StdEncoding.DecodeString(op.Value)
			if err != nil {
				http.Error(w, "invalid base64 value in batch", http.StatusBadRequest)
				return
			}
			if err := s.svc.Put(ctx, k, v); err != nil {
				s.writeError(w, err)
				return
			}
		case "delete":
			if err := s.svc.Delete(ctx, k); err != nil {
				s.writeError(w, err)
				return
			}
		default:
			http.Error(w, "unknown batch operation: "+op.Op, http.StatusBadRequest)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	http.Error(w, "service unavailable", http.StatusServiceUnavailable)
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	stats := s.svc.Stats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}
