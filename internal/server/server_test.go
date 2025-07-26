package server_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/taonhanvat/prolog/api"
	"github.com/taonhanvat/prolog/internal/auth"
	"github.com/taonhanvat/prolog/internal/config"
	"github.com/taonhanvat/prolog/internal/mlog"
	"github.com/taonhanvat/prolog/internal/server"
)

type clientWrapper struct {
	api.LogClient
	testData string
}

func TestServer(t *testing.T) {
	var testScenarios = map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		logger server.Logger,
	){
		"unauthorized":              testUnAuthorized,
		"produce consume a message": testProduceConsume,
		"produce consume stream":    testProduceStream,
		"produce past boundary":     testProducePastBoundary,
	}

	for scenario, fn := range testScenarios {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, log, teardown := setupTest(t)
			defer teardown()
			fn(t, rootClient, nobodyClient, log)
		})
	}
}

func setupTest(t *testing.T) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	logger server.Logger,
	teardown func(),
) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// setup clients
	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
	) {
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(clientTLSConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		cc, err := grpc.NewClient(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(cc)
		return cc, client
	}

	rootConn, rootClient := newClient(config.RootClientCertFile, config.RootClientKeyFile)
	nobodyConn, nobodyClient := newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	// setup server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: l.Addr().String(),
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)
	dir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	logger, err = mlog.NewLog(dir, mlog.Config{})
	require.NoError(t, err)
	auth, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	require.NoError(t, err)
	server := server.NewGrpcServer(server.Config{Logger: logger, Authorizer: auth}, grpc.Creds(serverCreds))
	go func() {
		server.Serve(l)
	}()

	// setup cleaner
	teardown = func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
	}

	return
}

func testProducePastBoundary(t *testing.T, rootClient, _ api.LogClient, logger server.Logger) {
	log, ok := logger.(*mlog.Log)
	if ok {
		for i := range log.Config.Internal.Index.MaxSize {
			testProduceConsume(t, clientWrapper{LogClient: rootClient, testData: fmt.Sprintf("Test Data %d", i)}, nil, log)
		}
	}
}

func testProduceConsume(t *testing.T, rootClient api.LogClient, _ api.LogClient, _ server.Logger) {
	ctx := context.Background()

	test := func(value []byte) {
		send := &api.Record{Value: value}
		produceResponse, err := rootClient.Produce(ctx, &api.ProduceRequest{Record: send})
		require.NoError(t, err)
		consumeResponse, err := rootClient.Consume(ctx, &api.ConsumeRequest{Offset: produceResponse.Offset})
		require.NoError(t, err)
		require.Equal(t, produceResponse.Offset, consumeResponse.Record.Offset)
		require.Equal(t, send.Value, consumeResponse.Record.Value)
	}
	clientWrapper, ok := rootClient.(clientWrapper)
	if ok {
		test([]byte(clientWrapper.testData))
	} else {
		test([]byte("Test Data"))
	}
}

func testProduceStream(t *testing.T, rootClient, _ api.LogClient, _ server.Logger) {
	ctx := context.Background()
	records := []*api.Record{
		{
			Value: []byte("First message"),
		},
		{
			Value: []byte("Second message"),
		},
		{
			Value: []byte("Third message"),
		},
		{
			Value: []byte("Fourth message"),
		},
	}

	stream, err := rootClient.ProduceStream(ctx)
	require.NoError(t, err)
	for offset, record := range records {
		err = stream.Send(&api.ProduceRequest{Record: record})
		require.NoError(t, err)
		res, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, res.Offset, uint64(offset))
	}
}

func testUnAuthorized(
	t *testing.T,
	_,
	nobodyClient api.LogClient,
	_ server.Logger,
) {
	ctx := context.Background()
	// produce
	produceResponse, err := nobodyClient.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	require.Nil(t, produceResponse)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// consume
	consumeResponse, err := nobodyClient.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	require.Nil(t, consumeResponse)
	require.Equal(t, codes.Unknown, status.Code(err))
}
