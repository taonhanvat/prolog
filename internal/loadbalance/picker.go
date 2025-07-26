package loadbalance

import (
	"strings"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

type Picker struct {
	leader          balancer.SubConn
	followers       []balancer.SubConn
	currentRotation int
}

func (p Picker) Build(info base.PickerBuildInfo) balancer.Picker {
	for sc, sci := range info.ReadySCs {
		if sci.Address.Attributes.Value("isLeader").(bool) {
			p.leader = sc
		} else {
			p.followers = append(p.followers, sc)
		}
	}
	return p
}

func (p Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var pr balancer.PickResult
	if strings.Contains(info.FullMethodName, "Produce") {
		pr.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Consume") {
		pr.SubConn = p.nextFollower()
	}
	if pr.SubConn == nil {
		return pr, balancer.ErrNoSubConnAvailable
	}
	return pr, nil
}

func (p Picker) nextFollower() balancer.SubConn {
	if len(p.followers) == 0 {
		return nil
	} else if p.currentRotation+1 >= len(p.followers) {
		p.currentRotation = 0
	} else {
		p.currentRotation++
	}
	return p.followers[p.currentRotation]
}

func init() {
	balancer.Register(base.NewBalancerBuilder(Scheme, Picker{}, base.Config{}))
}
