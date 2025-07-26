package mlog

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/taonhanvat/prolog/api"
	"github.com/taonhanvat/prolog/internal/tool"
	"google.golang.org/protobuf/proto"
)

const (
	RaftDetectingByte byte = 1
)

type DistributedLog struct {
	log      *Log
	Raft     *raft.Raft
	logStore *logStore
	config   Config
}
type logStore struct {
	log  *Log
	node raft.ServerID
}

func NewDistributedLog(dirName string, config Config) (l *DistributedLog, err error) {
	l = &DistributedLog{config: config}
	if err = l.newLog(dirName, config); err != nil {
		return nil, err
	}
	if err = l.newRaft(dirName); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *DistributedLog) newLog(dirName string, config Config) (err error) {
	logPath := filepath.Join(dirName, "log")
	if err = os.MkdirAll(logPath, 0755); err != nil {
		return err
	}
	l.log, err = NewLog(logPath, config)
	return err
}

func (l *DistributedLog) newRaft(dirName string) error {
	raftPath := filepath.Join(dirName, "raft")
	if err := os.MkdirAll(raftPath, 0755); err != nil {
		return err
	}
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}
	fsm := &fsm{log: l.log}

	stableStorePath := filepath.Join(raftPath, "stable_store")
	_, err := os.Stat(stableStorePath)
	isNewCluster := os.IsNotExist(err)
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		return err
	}

	logStorePath := filepath.Join(raftPath, "log_store")
	if err = os.MkdirAll(logStorePath, 0755); err != nil {
		return err
	}
	logStoreConfig := l.config
	logStoreConfig.Internal.Store.InitOffset = 1
	logStore, err := newLogStore(logStorePath, logStoreConfig, l.config.Raft.LocalID)
	if err != nil {
		return err
	}
	l.logStore = logStore

	fileSnapshotStore, err := raft.NewFileSnapshotStore(
		raftPath,
		1,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	l.Raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		fileSnapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	if l.config.Raft.IsBootstrapper {
		// Check if this is a fresh cluster by looking at stable store
		if isNewCluster {
			config := raft.Configuration{
				Servers: []raft.Server{{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				}},
			}
			err = l.Raft.BootstrapCluster(config).Error()
			if err != nil {
				return err
			}
			fmt.Println("Cluster bootstrapped successfully")
		} else {
			fmt.Println("Raft state already exists, skipping bootstrap.")
		}
	}
	return err
}

func (l *DistributedLog) GracefulStop() error {
	l.logStore.log.Close()
	return tool.TryAll(
		l.Raft.Shutdown().Error,
		l.log.Close,
	)
}

type requestType byte

const (
	appendType requestType = 1
)

func (l *DistributedLog) Append(r *api.Record) (offset uint64, err error) {
	produceResponse, err := l.apply(appendType, r)
	if err != nil {
		return 0, err
	}
	return produceResponse.(*api.ProduceResponse).Offset, nil
}

func (l *DistributedLog) apply(rt requestType, m proto.Message) (interface{}, error) {
	var buffer bytes.Buffer
	if _, err := buffer.Write([]byte{byte(rt)}); err != nil {
		return nil, err
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	if _, err = buffer.Write(b); err != nil {
		return nil, err
	}
	future := l.Raft.Apply(buffer.Bytes(), time.Second*10)
	if err = future.Error(); err != nil {
		return nil, err
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

func (l *DistributedLog) Join(name, addr string) error {
	configFuture := l.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(name)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// this server has already joined
				return nil
			}
			// remove the existing server
			removeFuture := l.Raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := l.Raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

func (l *DistributedLog) Leave(name string) error {
	return l.Raft.RemoveServer(raft.ServerID(name), 0, 0).Error()
}

func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("time out")
		case <-ticker.C:
			if l := l.Raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	futureConfig := l.Raft.GetConfiguration()
	if err := futureConfig.Error(); err != nil {
		return nil, err
	}
	var servers []*api.Server
	_, leaderID := l.Raft.LeaderWithID()
	for _, raftServer := range futureConfig.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(raftServer.ID),
			IsLeader: raftServer.ID == leaderID,
		})
	}
	return servers, nil
}

type fsm struct {
	log *Log
}

func (f *fsm) Apply(log *raft.Log) interface{} {
	switch requestType(log.Data[0]) {
	case appendType:
		r := &api.Record{}
		if err := proto.Unmarshal(log.Data[1:], r); err != nil {
			return err
		}
		offset, err := f.log.Append(r)
		if err != nil {
			return err
		}
		return &api.ProduceResponse{Offset: offset}
	}
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	reader := f.log.reader()
	return &fsmSnapshot{sourceReader: reader}, nil
}

func (f *fsm) Restore(snapshot io.ReadCloser) (err error) {
	headerByte := make([]byte, headerLength)
	var buffer *bytes.Buffer
	for i := 0; ; i++ {
		if _, err = io.ReadFull(snapshot, headerByte); err != nil {
			return err
		}
		header := byteOrder.Uint64(headerByte)
		if _, err = io.CopyN(buffer, snapshot, int64(header)); err != nil {
			return err
		}
		var record *api.Record
		if err = proto.Unmarshal(buffer.Bytes(), record); err != nil {
			return err
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buffer.Reset()
	}
}

type fsmSnapshot struct {
	sourceReader io.Reader
}

func (fs *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, fs.sourceReader); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (fs *fsmSnapshot) Release() {}

func newLogStore(dirName string, c Config, node raft.ServerID) (*logStore, error) {
	log, err := NewLog(dirName, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log: log, node: node}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	f, err := l.log.LowestOffset()
	return f, err
}

func (l *logStore) LastIndex() (uint64, error) {
	f, err := l.log.HighestOffset()
	return f, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	record, err := l.log.Read(index)
	if err != nil {
		return err
	}
	out.Data = record.Value
	out.Term = record.Term
	out.Index = record.Offset
	out.Type = raft.LogType(record.Type)
	return nil
}

func (l *logStore) StoreLog(log *raft.Log) error {
	return l.StoreLogs([]*raft.Log{log})
}

func (l *logStore) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		_, err := l.log.Append(&api.Record{
			Value: log.Data,
			Term:  log.Term,
			Type:  uint32(log.Type),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(_, max uint64) error {
	result := l.log.Truncate(max)
	return result
}

type StreamLayer struct {
	net.Listener
	clientTLSConfig *tls.Config
	serverTLSConfig *tls.Config
}

func NewStreamLayer(
	l net.Listener,
	clientTLSConfig *tls.Config,
	serverTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		Listener:        l,
		clientTLSConfig: clientTLSConfig,
		serverTLSConfig: serverTLSConfig,
	}
}

func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (c net.Conn, err error) {
	dialer := &net.Dialer{Timeout: timeout}
	dial, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	if s.clientTLSConfig != nil {
		dial = tls.Client(dial, s.clientTLSConfig)
	}
	return dial, nil
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if s.serverTLSConfig != nil {
		conn = tls.Server(conn, s.serverTLSConfig)
	}
	return conn, nil
}
