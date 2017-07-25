package gossip

import (
	"encoding/json"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/AsynkronIT/protoactor-go/cluster"
	"github.com/AsynkronIT/protoactor-go/eventstream"
	"github.com/hashicorp/memberlist"
	"github.com/satori/go.uuid"
)

// metadata is user-defined data used to store node level information
type metadata struct {
	ClusterName string   `json:"cluster_name"`
	MemberID    string   `json:"member_id"`
	Address     string   `json:"address"`
	Port        int      `json:"port"`
	Kinds       []string `json:"kinds"`
}

func Decode(data []byte) (*metadata, error) {
	meta := new(metadata)
	if err := json.Unmarshal(data, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *metadata) Encode() ([]byte, error) {
	return json.Marshal(m)
}

type delegate struct {
	metadata
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message.
func (d *delegate) NodeMeta(limit int) []byte {
	b, err := d.Encode()
	if err != nil {
		panic(nil)
	}
	return b
}

func (d *delegate) NotifyMsg([]byte) {}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

func (d *delegate) LocalState(join bool) []byte { return nil }

func (d *delegate) MergeRemoteState(buf []byte, join bool) {}

const (
	alive int32 = iota
	dead
)

// GossipConfig is used to configure gossip behavior for internal memberlist
type GossipConfig struct {
	ProbeInterval    time.Duration
	PushPullInterval time.Duration
	GossipInterval   time.Duration
	GossipNodes      int
}

// GossipProvider is a cluster provider implemented by using gossip based protocol
type GossipProvider struct {
	nodeState       int32
	monitorInterval time.Duration // interval to monitor member status changes
	existingNodes   []string

	config     *GossipConfig
	membership *memberlist.Memberlist
}

func New(monitorInterval time.Duration, existingNodes []string, config *GossipConfig) *GossipProvider {
	return &GossipProvider{
		monitorInterval: monitorInterval,
		existingNodes:   existingNodes,
		config:          config,
	}
}

func (p *GossipProvider) init(clusterName string, address string, port int, knownKinds []string) error {
	config := memberlist.DefaultLANConfig()
	if p.config != nil {
		config.ProbeInterval = p.config.ProbeInterval
		config.PushPullInterval = p.config.PushPullInterval
		config.GossipInterval = p.config.GossipInterval
		config.GossipNodes = p.config.GossipNodes
	}

	hostname, _ := os.Hostname()
	memberID := uuid.NewV4().String()
	config.Name = hostname + "-" + memberID
	config.BindPort = 0 // use dynamic bind port
	config.Delegate = &delegate{
		metadata: metadata{
			ClusterName: clusterName,
			MemberID:    memberID,
			Address:     address,
			Port:        port,
			Kinds:       knownKinds,
		},
	}

	var err error
	p.membership, err = memberlist.Create(config)
	if err != nil {
		return err
	}

	node := p.membership.LocalNode()
	log.Printf("[CLUSTER] [GOSSIP] Local node state: %v, %v", node.Addr, node.Port)

	if len(p.existingNodes) > 0 {
		if _, err := p.membership.Join(p.existingNodes); err != nil {
			return err
		}
	}

	return nil
}

func (p *GossipProvider) RegisterMember(clusterName string, address string, port int, knownKinds []string) error {
	err := p.init(clusterName, address, port, knownKinds)

	// force our own existence to be part of the first status update
	p.blockingStatusChange()

	return err
}

func (p *GossipProvider) blockingStatusChange() {
	p.notifyStatuses()
}

func (p *GossipProvider) notifyStatuses() {
	nodes := p.membership.Members()

	events := make(cluster.ClusterTopologyEvent, len(nodes))
	for i, n := range nodes {
		meta, err := Decode(n.Meta)
		if err != nil {
			log.Printf("[CLUSTER] [GOSSIP] Error %v", err)
			return
		}
		events[i] = &cluster.MemberStatus{
			MemberID: meta.MemberID,
			Host:     meta.Address,
			Port:     meta.Port,
			Kinds:    meta.Kinds,
			Alive:    true,
		}
	}

	// publish the current cluster topology onto the event stream
	eventstream.Publish(events)
}

func (p *GossipProvider) MonitorMemberStatusChanges() {
	go func() {
		for atomic.LoadInt32(&p.nodeState) == alive {
			p.notifyStatuses()
			time.Sleep(p.monitorInterval)
		}
	}()
}

func (p *GossipProvider) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&p.nodeState, alive, dead) {
		return nil
	}

	go func() {
		if err := p.membership.Leave(5 * time.Second); err != nil {
			log.Printf("[CLUSTER] [GOSSIP] Error %v", err)
		}
	}()
	return nil
}
