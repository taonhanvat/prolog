package server

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/taonhanvat/prolog/api"
	"github.com/taonhanvat/prolog/internal/auth"
	"google.golang.org/grpc"
)

type Logger interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}
type grpcServer struct {
	Config *Config
	*api.UnimplementedLogServer
}
type Config struct {
	Logger      Logger
	Authorizer  *auth.Authorizer
	GetServerer GetServerer
}
type GetServerer interface {
	GetServers() ([]*api.Server, error)
}

func NewGrpcServer(config Config, opts ...grpc.ServerOption) *grpc.Server {
	opts = append(opts,
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			unaryAuthInterceptor,
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(),
			streamAuthInterceptor,
		)),
	)
	gsrv := grpc.NewServer(opts...)
	api.RegisterLogServer(gsrv, &grpcServer{&config, &api.UnimplementedLogServer{}})
	return gsrv
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.Config.Logger.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.Config.Logger.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	record.Offset = req.Offset
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err := stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) GetServers(ctx context.Context, req *api.GetServersRequest) (*api.GetServersResponse, error) {
	servers, err := s.Config.GetServerer.GetServers()
	if err != nil {
		return nil, err
	}
	return &api.GetServersResponse{Servers: servers}, nil
}
