package membership

import (
	"fmt"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/taonhanvat/prolog/api"
	"github.com/taonhanvat/prolog/internal/tool"
)

type Handler interface {
	Join(name, addr string) error
	Leave(addr string) error
}
type MemberShip struct {
	config  Config
	serf    *serf.Serf
	handler Handler
	events  chan serf.Event
}
type Config struct {
	BindAddr       string
	BindPort       int
	NodeName       string
	Tags           map[string]string
	StartJoinAddrs []string
}

func New(handler Handler, config Config) (*MemberShip, error) {
	m := &MemberShip{
		handler: handler,
		config:  config,
	}
	if err := m.setupSerf(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *MemberShip) setupSerf() error {
	config := serf.DefaultConfig()
	config.Init()
	config.NodeName = m.config.NodeName
	config.MemberlistConfig.BindPort = m.config.BindPort
	config.MemberlistConfig.BindAddr = m.config.BindAddr
	config.Tags = m.config.Tags
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	serf, err := serf.Create(config)
	if err != nil {
		return err
	}
	m.serf = serf
	go m.handleEvent()
	if len(m.config.StartJoinAddrs) > 0 &&
		m.config.StartJoinAddrs[0] != fmt.Sprintf("%s:%d", m.config.BindAddr, m.config.BindPort) {
		joinFunc := func() error {
			_, err := m.serf.Join(m.config.StartJoinAddrs, true)
			return err
		}
		err := retry(joinFunc, 50)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MemberShip) GracefulStop() (err error) {
	return tool.TryAll(
		m.serf.Leave,
		m.serf.Shutdown,
	)
}

func (m *MemberShip) handleEvent() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				m.handleJoin(member)
			}
		case serf.EventMemberFailed, serf.EventMemberLeave:
			for _, member := range e.(serf.MemberEvent).Members {
				m.handleLeave(member)
			}
		}
	}
}

func (m *MemberShip) handleJoin(member serf.Member) (err error) {
	return m.handler.Join(member.Name, member.Tags["Addr"])
}

func (m *MemberShip) handleLeave(member serf.Member) error {
	return m.handler.Leave(member.Name)
}

func (m *MemberShip) FulfillServer(apiServer *api.Server) {
	members := m.serf.Members()
	for _, member := range members {
		if member.Name == apiServer.Id {
			apiServer.RpcAddr = member.Tags["Rpc_Addr"]
		}
	}
}

func retry(callback func() error, attempt int) (err error) {
	for i := 0; i < attempt; i++ {
		err = callback()
		if err == nil {
			return nil
		}
		time.Sleep(5 * time.Second)
	}
	return err
}
