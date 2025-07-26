package loadbalance

import (
	"context"
	"fmt"

	"github.com/taonhanvat/prolog/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

/*
* Resolver builds a scheme(prolog)'s resolver so api caller can detect
* which one is the leader, which one is the follower to navigate
* their requests appropriately.
 */
type Resolver struct {
	clientConn         resolver.ClientConn
	resolverConnection *grpc.ClientConn
}

func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.clientConn = cc
	grpcOpts := []grpc.DialOption{grpc.WithTransportCredentials(opts.DialCreds)}
	conn, err := grpc.NewClient(target.Endpoint(), grpcOpts...)
	if err != nil {
		return nil, err
	}
	r.resolverConnection = conn
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

const (
	Scheme string = "prolog"
)

func (r *Resolver) Scheme() string {
	return Scheme
}

func (r *Resolver) ResolveNow(opts resolver.ResolveNowOptions) {
	if r.resolverConnection == nil {
		return
	}

	resolverClient := api.NewLogClient(r.resolverConnection)
	ctx := context.Background()
	ctx.Done()
	resp, err := resolverClient.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		fmt.Printf("GetServers error: %v\n", err)
		return
	}
	var addrs []resolver.Address
	for _, server := range resp.Servers {
		addrs = append(addrs, resolver.Address{
			Addr:       server.RpcAddr,
			Attributes: attributes.New("isLeader", server.IsLeader),
		})
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses: addrs,
	})
}

func (r *Resolver) Close() {
	r.resolverConnection.Close()
}

func init() {
	resolver.Register(&Resolver{})
}
