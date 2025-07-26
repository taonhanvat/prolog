package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/viper"
	"github.com/taonhanvat/prolog/internal/agent"
	"github.com/taonhanvat/prolog/internal/config"
)

func main() {
	agentConfig, err := buildAgentConfig()
	if err != nil {
		log.Fatal(err)
	}
	a, err := agent.New(agentConfig)
	if err != nil {
		log.Fatal(err)
	}

	signalStop := make(chan os.Signal, 1)
	signal.Notify(signalStop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-a.RPCServerError:
	case <-signalStop:
	}
	err = a.GracefulStop()
	if err != nil {
		fmt.Printf("error while stopping server: %v\n", err)
	}
}

func buildAgentConfig() (agent.Config, error) {
	v := viper.New()
	v.AutomaticEnv()
	agentConfig := agent.Config{
		MembershipBindingAddr: v.GetString("MEMBERSHIP_BINDING_ADDR"),
		MembershipBindingPort: v.GetInt("MEMBERSHIP_BINDING_PORT"),
		NodeName:              v.GetString("NODE_NAME"),
		RaftBindingAddr:       v.GetString("RAFT_BINDING_ADDR"),
		RaftBindingPort:       v.GetInt("RAFT_BINDING_PORT"),
		IsRaftBootstrapper:    v.GetBool("IS_RAFT_BOOTSTRAPPER"),
		StartJoinAddrs:        v.GetStringSlice("START_JOIN_ADDRS"),
		ACLModelFile:          v.GetString("ACL_MODEL_FILE"),
		ACLPolicyFile:         v.GetString("ACL_POLICY_FILE"),
		RPCAddr:               v.GetString("RPC_ADDR"),
		RPCPort:               v.GetInt("RPC_PORT"),
		DataDir:               v.GetString("DATA_DIR"),
	}

	serverTLS, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	if err != nil {
		return agentConfig, err
	}
	clientTlS, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.RootClientCertFile,
		KeyFile:  config.RootClientKeyFile,
		CAFile:   config.CAFile,
		Server:   false,
	})
	if err != nil {
		return agentConfig, err
	}
	agentConfig.RaftServerTLSConfig = serverTLS
	agentConfig.RaftClientTLSConfig = clientTlS

	return agentConfig, nil
}
