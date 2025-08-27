package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/taonhanvat/prolog/api"
	"github.com/taonhanvat/prolog/internal/auth"
	"github.com/taonhanvat/prolog/internal/membership"
	"github.com/taonhanvat/prolog/internal/mlog"
	"github.com/taonhanvat/prolog/internal/server"
	"github.com/taonhanvat/prolog/internal/tool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	MembershipBindingAddr string
	MembershipBindingPort int
	StartJoinAddrs        []string
	NodeName              string

	RaftBindingAddr     string
	RaftBindingPort     int
	IsRaftBootstrapper  bool
	RaftClientTLSConfig *tls.Config
	RaftServerTLSConfig *tls.Config

	ACLModelFile  string
	ACLPolicyFile string

	RPCAddr string
	RPCPort int
	DataDir string
}
type Agent struct {
	Config         Config
	DistributedLog *mlog.DistributedLog
	membership     *membership.MemberShip
	grpcServer     *grpc.Server
	RPCServerError chan error
}

func New(c Config) (agent *Agent, err error) {
	agent = &Agent{
		Config:         c,
		RPCServerError: make(chan error),
	}
	err = tool.TryAll(
		agent.setupLog,
		agent.setupServer,
		agent.setupMemberShip,
	)
	if err != nil {
		return nil, err
	}
	return agent, nil
}

func (a *Agent) GracefulStop() error {
	a.grpcServer.GracefulStop()
	return tool.TryAll(
		a.membership.GracefulStop,
		a.DistributedLog.GracefulStop,
	)
}

func (a *Agent) setupLog() (err error) {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", a.Config.RaftBindingAddr, a.Config.RaftBindingPort))
	if err != nil {
		return err
	}
	config := mlog.Config{}
	config.Raft.StreamLayer = mlog.NewStreamLayer(
		l,
		a.Config.RaftClientTLSConfig,
		a.Config.RaftServerTLSConfig,
	)
	config.Raft.IsBootstrapper = a.Config.IsRaftBootstrapper
	config.Raft.LocalID = raft.ServerID(a.Config.NodeName)

	a.DistributedLog, err = mlog.NewDistributedLog(a.Config.DataDir, config)
	if err != nil {
		return err
	}
	if a.Config.IsRaftBootstrapper {
		return a.DistributedLog.WaitForLeader(10 * time.Second)
	}
	return nil
}

func (a *Agent) setupServer() (err error) {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", a.Config.RPCAddr, a.Config.RPCPort))
	if err != nil {
		return err
	}
	auth, err := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	if err != nil {
		return err
	}
	creds := credentials.NewTLS(a.Config.RaftServerTLSConfig)
	grpcOpts := grpc.Creds(creds)
	server := server.NewGrpcServer(
		server.Config{Logger: a.DistributedLog, Authorizer: auth, GetServerer: a},
		grpcOpts,
	)
	a.grpcServer = server
	go func() {
		err := server.Serve(l)
		a.RPCServerError <- err
	}()
	return nil
}

func (a *Agent) setupMemberShip() (err error) {
	membership, err := membership.New(
		a.DistributedLog,
		membership.Config{
			BindAddr: a.Config.MembershipBindingAddr,
			BindPort: a.Config.MembershipBindingPort,
			NodeName: a.Config.NodeName,
			Tags: map[string]string{
				"Addr":     fmt.Sprintf("%s:%d", a.Config.RaftBindingAddr, a.Config.RaftBindingPort),
				"Rpc_Addr": fmt.Sprintf("%s:%d", a.Config.RPCAddr, a.Config.RPCPort),
			},
			StartJoinAddrs: a.Config.StartJoinAddrs,
		},
	)
	if err != nil {
		return err
	}
	a.membership = membership
	return nil
}

func (a *Agent) GetServers() ([]*api.Server, error) {
	apiServers, err := a.DistributedLog.GetServers()
	if err != nil {
		return nil, err
	}
	for _, apiServer := range apiServers {
		a.membership.FulfillServer(apiServer)
	}
	return apiServers, nil
}
