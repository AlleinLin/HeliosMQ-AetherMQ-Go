package controller

import (
	"context"
	"sync"
	"time"

	"github.com/aethermq/aethermq/internal/config"
	"github.com/aethermq/aethermq/internal/raft"
	"go.uber.org/zap"
)

type Controller struct {
	cfg       *config.ControllerConfig
	raftNode  *raft.RaftNode
	logger    *zap.Logger

	topics     map[string]*TopicInfo
	brokers    map[string]*BrokerInfo
	groups     map[string]*GroupInfo

	mu         sync.RWMutex
	leader     bool
}

type TopicInfo struct {
	Name              string
	Partitions        int32
	ReplicationFactor int32
	PartitionMap      map[int32]*PartitionInfo
	CreatedAt         time.Time
}

type PartitionInfo struct {
	Partition  int32
	Leader     string
	Replicas   []string
	ISR        []string
	HW         int64
	LEO        int64
}

type BrokerInfo struct {
	BrokerID  string
	Address   string
	Zone      string
	State     string
	LastHB    time.Time
}

type GroupInfo struct {
	GroupID    string
	Members    map[string]*MemberInfo
	Generation int64
	State      string
}

type MemberInfo struct {
	MemberID   string
	ClientID   string
	Topics     []string
	Assignment map[string][]int32
	LastHB     time.Time
}

func NewController(cfg *config.ControllerConfig, raftNode *raft.RaftNode, logger *zap.Logger) *Controller {
	c := &Controller{
		cfg:      cfg,
		raftNode: raftNode,
		logger:   logger,
		topics:   make(map[string]*TopicInfo),
		brokers:  make(map[string]*BrokerInfo),
		groups:   make(map[string]*GroupInfo),
	}
	go c.run()
	return c
}

func (c *Controller) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if c.raftNode.IsLeader() {
			c.leader = true
			c.reconcile()
		} else {
			c.leader = false
		}
	}
}

func (c *Controller) reconcile() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for brokerID, broker := range c.brokers {
		if now.Sub(broker.LastHB) > 30*time.Second {
			broker.State = "OFFLINE"
			c.reassignPartitions(brokerID)
		}
	}
}

func (c *Controller) reassignPartitions(failedBroker string) {
	for _, topic := range c.topics {
		for _, partition := range topic.PartitionMap {
			if partition.Leader == failedBroker {
				c.electNewLeader(topic.Name, partition.Partition)
			}
		}
	}
}

func (c *Controller) electNewLeader(topic string, partition int32) {
	topicInfo := c.topics[topic]
	if topicInfo == nil {
		return
	}

	partInfo := topicInfo.PartitionMap[partition]
	if partInfo == nil {
		return
	}

	for _, replica := range partInfo.Replicas {
		if broker, ok := c.brokers[replica]; ok && broker.State == "ONLINE" {
			if replica != partInfo.Leader {
				partInfo.Leader = replica
				c.logger.Info("elected new leader",
					zap.String("topic", topic),
					zap.Int32("partition", partition),
					zap.String("new_leader", replica))
				break
			}
		}
	}
}

func (c *Controller) CreateTopic(ctx context.Context, name string, partitions, replicationFactor int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.topics[name]; ok {
		return ErrTopicExists
	}

	if replicationFactor > int32(len(c.brokers)) {
		replicationFactor = int32(len(c.brokers))
	}

	topicInfo := &TopicInfo{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		PartitionMap:      make(map[int32]*PartitionInfo),
		CreatedAt:         time.Now(),
	}

	brokerList := make([]*BrokerInfo, 0, len(c.brokers))
	for _, b := range c.brokers {
		if b.State == "ONLINE" {
			brokerList = append(brokerList, b)
		}
	}

	for i := int32(0); i < partitions; i++ {
		replicas := c.selectReplicas(brokerList, int(replicationFactor), int(i))
		partInfo := &PartitionInfo{
			Partition: i,
			Leader:    replicas[0],
			Replicas:  replicas,
			ISR:       replicas,
		}
		topicInfo.PartitionMap[i] = partInfo
	}

	c.topics[name] = topicInfo

	cmd := &raft.Command{
		Op:    raft.CreateTopic,
		Key:   name,
		Value: topicInfo,
	}
	return c.raftNode.Propose(ctx, cmd)
}

func (c *Controller) DeleteTopic(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.topics[name]; !ok {
		return ErrTopicNotFound
	}

	delete(c.topics, name)

	cmd := &raft.Command{
		Op:  raft.DeleteTopic,
		Key: name,
	}
	return c.raftNode.Propose(ctx, cmd)
}

func (c *Controller) RegisterBroker(ctx context.Context, brokerID, address, zone string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.brokers[brokerID] = &BrokerInfo{
		BrokerID: brokerID,
		Address:  address,
		Zone:     zone,
		State:    "ONLINE",
		LastHB:   time.Now(),
	}

	cmd := &raft.Command{
		Op:  raft.RegisterBroker,
		Key: brokerID,
		Value: &BrokerInfo{
			BrokerID: brokerID,
			Address:  address,
			Zone:     zone,
		},
	}
	return c.raftNode.Propose(ctx, cmd)
}

func (c *Controller) UnregisterBroker(ctx context.Context, brokerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if broker, ok := c.brokers[brokerID]; ok {
		broker.State = "OFFLINE"
	}

	cmd := &raft.Command{
		Op:  raft.UnregisterBroker,
		Key: brokerID,
	}
	return c.raftNode.Propose(ctx, cmd)
}

func (c *Controller) BrokerHeartbeat(ctx context.Context, brokerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if broker, ok := c.brokers[brokerID]; ok {
		broker.LastHB = time.Now()
		broker.State = "ONLINE"
	}
	return nil
}

func (c *Controller) GetTopic(name string) (*TopicInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	topic, ok := c.topics[name]
	if !ok {
		return nil, ErrTopicNotFound
	}
	return topic, nil
}

func (c *Controller) ListTopics() []*TopicInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	topics := make([]*TopicInfo, 0, len(c.topics))
	for _, t := range c.topics {
		topics = append(topics, t)
	}
	return topics
}

func (c *Controller) selectReplicas(brokers []*BrokerInfo, count, partition int) []string {
	if len(brokers) == 0 {
		return nil
	}

	replicas := make([]string, 0, count)
	for i := 0; i < count && i < len(brokers); i++ {
		idx := (partition + i) % len(brokers)
		replicas = append(replicas, brokers[idx].BrokerID)
	}
	return replicas
}

func (c *Controller) RegisterGRPC(server interface{}) {
}

var (
	ErrTopicExists   = &ControllerError{Code: 409, Message: "topic already exists"}
	ErrTopicNotFound = &ControllerError{Code: 404, Message: "topic not found"}
)

type ControllerError struct {
	Code    int
	Message string
}

func (e *ControllerError) Error() string {
	return e.Message
}
