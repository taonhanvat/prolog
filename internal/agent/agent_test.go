package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/taonhanvat/prolog/api"
	"github.com/taonhanvat/prolog/internal/agent"
	"github.com/taonhanvat/prolog/internal/config"
	"github.com/taonhanvat/prolog/internal/loadbalance"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestMain(t *testing.T) {
	agents := make([]*agent.Agent, 3)
	raftServerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)
	raftClientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        false,
	})
	require.NoError(t, err)
	for i := 0; i < len(agents); i++ {
		dataDir, err := os.MkdirTemp("", fmt.Sprintf("test_agent_%d", i))
		require.NoError(t, err)
		rpcPort, _ := strconv.Atoi(fmt.Sprintf("400%d", i))
		membershipBindingPort, _ := strconv.Atoi(fmt.Sprintf("401%d", i))
		raftBindingPort, _ := strconv.Atoi(fmt.Sprintf("402%d", i))
		var startJoinAddrs []string
		if agents[0] != nil {
			startJoinAddrs = append(startJoinAddrs, fmt.Sprintf("127.0.0.1:%d", agents[0].Config.MembershipBindingPort))
		}
		config := agent.Config{
			MembershipBindingAddr: "127.0.0.1",
			MembershipBindingPort: membershipBindingPort,
			StartJoinAddrs:        startJoinAddrs,
			NodeName:              fmt.Sprintf("node_%d", i),
			RaftBindingAddr:       "127.0.0.1",
			RaftBindingPort:       raftBindingPort,
			RaftClientTLSConfig:   raftClientTLSConfig,
			RaftServerTLSConfig:   raftServerTLSConfig,
			IsRaftBootstrapper:    i == 0,
			ACLModelFile:          config.ACLModelFile,
			ACLPolicyFile:         config.ACLPolicyFile,
			RPCAddr:               "127.0.0.1",
			RPCPort:               rpcPort,
			DataDir:               dataDir,
		}
		agent, err := agent.New(config)
		require.NoError(t, err)
		agents[i] = agent
	}

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	defer func() {
		for i := 0; i < len(agents); i++ {
			require.NoError(t, os.RemoveAll(agents[i].Config.DataDir))
		}
	}()

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        false,
	})
	require.NoError(t, err)

	leaderClient := client(t, tlsConfig, agents[0])
	message := []byte("message_0")
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{Record: &api.Record{
			Value: message,
		}},
	)
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(1 * time.Second)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, message)

	followerClient := client(t, tlsConfig, agents[1])
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, message)
}

func client(
	t *testing.T,
	tlsConfig *tls.Config,
	agent *agent.Agent,
) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}
	cc, err := grpc.NewClient(
		fmt.Sprintf("%s:///%s:%d", loadbalance.Scheme, agent.Config.RPCAddr, agent.Config.RPCPort),
		opts...,
	)
	require.NoError(t, err)
	return api.NewLogClient(cc)
}
