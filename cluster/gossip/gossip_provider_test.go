package gossip

import (
	"log"
	"testing"
	"time"

	"github.com/AsynkronIT/protoactor-go/cluster"
	"github.com/AsynkronIT/protoactor-go/eventstream"
)

func TestRegisterMember(t *testing.T) {
	if testing.Short() {
		return
	}

	p := New(200*time.Millisecond, nil, nil)
	defer p.Shutdown()
	err := p.RegisterMember("mycluster", "127.0.0.1", 8000, []string{"a", "b"})
	if err != nil {
		log.Fatal(err)
	}
}

func TestMonitoryMemberStatusChanges(t *testing.T) {
	if testing.Short() {
		return
	}

	p := New(200*time.Millisecond, nil, nil)
	defer p.Shutdown()
	err := p.RegisterMember("mycluster", "127.0.0.1", 8000, []string{"a", "b"})
	if err != nil {
		log.Fatal(err)
	}

	p.MonitorMemberStatusChanges()
	eventstream.Subscribe(func(m interface{}) {
		log.Printf("Event %+v", m.(cluster.ClusterTopologyEvent)[0])
	})
	time.Sleep(1 * time.Second)
}
