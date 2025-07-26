package server

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func unaryAuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	grpcSrv := info.Server.(*grpcServer)
	if ok, err := authValidate(grpcSrv, info.FullMethod, ctx); !ok {
		return nil, err
	} else {
		return handler(ctx, req)
	}
}

func streamAuthInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	grpcSrv := srv.(*grpcServer)
	if ok, err := authValidate(grpcSrv, info.FullMethod, ss.Context()); !ok {
		return err
	} else {
		return handler(srv, ss)
	}
}

func authValidate(grpcSrv *grpcServer, requestingAction string, ctx context.Context) (bool, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return false, status.Error(codes.Unknown, "cannot find peer info")
	}
	if p.AuthInfo == nil {
		return false, status.Error(codes.Unauthenticated, "missing transport security")
	}
	tlsInfo := p.AuthInfo.(credentials.TLSInfo)
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return false, status.Error(codes.Unauthenticated, "missing cert")
	}
	subject := tlsInfo.State.PeerCertificates[0].Subject.CommonName

	if err := grpcSrv.Config.Authorizer.Authorize(subject, requestingAction); err != nil {
		return false, err
	}
	return true, nil
}
