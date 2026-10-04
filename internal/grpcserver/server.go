package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/Vega-OSS/vega-db/gen/vegapb"
	"github.com/Vega-OSS/vega-db/internal/service"
)

// ServerOptions configures the gRPC server.
type ServerOptions struct {
	EnableReflection bool
	Logger           *slog.Logger
}

// Server encapsulates the gRPC server and health manager.
type Server struct {
	vegapb.UnimplementedVegaServer
	svc          *service.Service
	grpcServer   *grpc.Server
	healthServer *health.Server
	logger       *slog.Logger
}

// MapError converts internal service errors to corresponding gRPC status codes.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, service.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, service.ErrOverloaded):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, service.ErrClosed):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func unaryRecoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered in unary RPC", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

func streamRecoveryInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered in stream RPC", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(srv, ss)
	}
}

// New creates and initializes a new gRPC Server with standard interceptors, options, and health service.
func New(svc *service.Service, opts ServerOptions) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(32 * 1024 * 1024),
		grpc.MaxSendMsgSize(32 * 1024 * 1024),
		grpc.MaxConcurrentStreams(1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ReadBufferSize(64 * 1024),
		grpc.WriteBufferSize(64 * 1024),
		grpc.ChainUnaryInterceptor(unaryRecoveryInterceptor(logger)),
		grpc.ChainStreamInterceptor(streamRecoveryInterceptor(logger)),
	}

	s := grpc.NewServer(grpcOpts...)
	h := health.NewServer()
	srv := &Server{
		svc:          svc,
		grpcServer:   s,
		healthServer: h,
		logger:       logger,
	}

	vegapb.RegisterVegaServer(s, srv)
	healthpb.RegisterHealthServer(s, h)
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	h.SetServingStatus("vega.v1.Vega", healthpb.HealthCheckResponse_SERVING)

	if opts.EnableReflection {
		reflection.Register(s)
	}

	return srv
}

// GRPCServer returns the underlying *grpc.Server.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// HealthServer returns the health server instance.
func (s *Server) HealthServer() *health.Server {
	return s.healthServer
}

// MarkNotServing marks the server as NOT_SERVING in the health registry.
func (s *Server) MarkNotServing() {
	s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	s.healthServer.SetServingStatus("vega.v1.Vega", healthpb.HealthCheckResponse_NOT_SERVING)
}

// Put implements VegaServer.Put.
func (s *Server) Put(ctx context.Context, req *vegapb.PutRequest) (*vegapb.PutResponse, error) {
	if err := s.svc.Put(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, MapError(err)
	}
	return &vegapb.PutResponse{}, nil
}

// Get implements VegaServer.Get.
func (s *Server) Get(ctx context.Context, req *vegapb.GetRequest) (*vegapb.GetResponse, error) {
	val, err := s.svc.Get(ctx, req.GetKey(), nil)
	if err != nil {
		return nil, MapError(err)
	}
	respVal := make([]byte, len(val))
	copy(respVal, val)
	return &vegapb.GetResponse{Value: respVal}, nil
}

// Delete implements VegaServer.Delete.
func (s *Server) Delete(ctx context.Context, req *vegapb.DeleteRequest) (*vegapb.DeleteResponse, error) {
	if err := s.svc.Delete(ctx, req.GetKey()); err != nil {
		return nil, MapError(err)
	}
	return &vegapb.DeleteResponse{}, nil
}

// Scan implements VegaServer.Scan.
func (s *Server) Scan(req *vegapb.ScanRequest, stream vegapb.Vega_ScanServer) error {
	ctx := stream.Context()
	return s.svc.Scan(ctx, req.GetStart(), req.GetEnd(), int(req.GetLimit()), func(k, v []byte) bool {
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		valCopy := make([]byte, len(v))
		copy(valCopy, v)
		if err := stream.Send(&vegapb.KV{Key: keyCopy, Value: valCopy}); err != nil {
			return false
		}
		return true
	})
}

// Stats implements VegaServer.Stats.
func (s *Server) Stats(_ context.Context, _ *vegapb.StatsRequest) (*vegapb.StatsResponse, error) {
	counters := s.svc.Stats()
	return &vegapb.StatsResponse{Counters: counters}, nil
}
