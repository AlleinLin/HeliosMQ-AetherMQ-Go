package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aethermq/aethermq/internal/common"
	"github.com/aethermq/aethermq/internal/config"
	"github.com/aethermq/aethermq/internal/metadata"
	"github.com/aethermq/aethermq/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var (
	produceTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aethermq_produce_total",
		Help: "Total number of messages produced",
	}, []string{"topic"})

	consumeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "aethermq_consume_total",
		Help: "Total number of messages consumed",
	}, []string{"topic", "group"})

	produceLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "aethermq_produce_latency_ms",
		Help:    "Produce latency in milliseconds",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 20, 50, 100},
	}, []string{"topic"})

	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aethermq_consumer_lag",
		Help: "Consumer lag in messages",
	}, []string{"topic", "group", "partition"})
)

type Broker struct {
	cfg         *config.BrokerConfig
	metaStore   *metadata.BoltStore
	segmentStore *storage.SegmentStore
	logger      *zap.Logger

	partitions  map[string]*Partition
	groups      map[string]*ConsumerGroup
	delayQueue  *DelayQueue
	dlqManager  *DLQManager

	mu          sync.RWMutex
	running     atomic.Bool
}

type Partition struct {
	topic      string
	partition  int32
	store      *storage.PartitionLog
	leader     bool
	replicas   []string
	isr        []string
	hw         int64
	leo        int64
}

type ConsumerGroup struct {
	groupID    string
	members    map[string]*Member
	assignment map[int32]string
	generation int64
	state      string
	mu         sync.RWMutex
}

type Member struct {
	memberID   string
	clientID   string
	topics     []string
	lastHB     time.Time
}

func NewBroker(cfg *config.BrokerConfig, metaStore *metadata.BoltStore, segmentStore *storage.SegmentStore, logger *zap.Logger) *Broker {
	b := &Broker{
		cfg:          cfg,
		metaStore:    metaStore,
		segmentStore: segmentStore,
		logger:       logger,
		partitions:   make(map[string]*Partition),
		groups:       make(map[string]*ConsumerGroup),
		delayQueue:   NewDelayQueue(),
		dlqManager:   NewDLQManager(),
	}
	b.running.Store(true)
	go b.delayQueue.Run()
	return b
}

func (b *Broker) Produce(ctx context.Context, topic string, partitionKey string, messages []*common.Message) ([]int64, error) {
	start := time.Now()
	defer func() {
		produceLatency.WithLabelValues(topic).Observe(float64(time.Since(start).Milliseconds()))
	}()

	partition, err := b.selectPartition(topic, partitionKey)
	if err != nil {
		return nil, err
	}

	partKey := partitionKeyFromParts(topic, partition)
	b.mu.RLock()
	p, ok := b.partitions[partKey]
	b.mu.RUnlock()

	if !ok {
		p = &Partition{
			topic:     topic,
			partition: partition,
			leader:    true,
		}
		b.mu.Lock()
		b.partitions[partKey] = p
		b.mu.Unlock()
	}

	var offsets []int64
	for _, msg := range messages {
		msg.Topic = topic
		msg.Partition = partition
		if msg.MessageID == "" {
			msg.MessageID = generateMessageID()
		}
		if msg.Timestamp == 0 {
			msg.Timestamp = time.Now().UnixMilli()
		}

		if msg.DeliverAt > 0 {
			b.delayQueue.Schedule(msg, time.UnixMilli(msg.DeliverAt))
			offsets = append(offsets, 0)
			continue
		}

		offset, err := b.segmentStore.Append(msg)
		if err != nil {
			return nil, err
		}
		offsets = append(offsets, offset)
		produceTotal.WithLabelValues(topic).Inc()
	}

	return offsets, nil
}

func (b *Broker) Fetch(ctx context.Context, topic string, partition int32, groupID string, offset int64, maxBytes int32) ([]*common.Message, int64, error) {
	messages, leo, err := b.segmentStore.Fetch(topic, partition, offset, maxBytes)
	if err != nil {
		return nil, -1, err
	}

	if groupID != "" {
		consumeTotal.WithLabelValues(topic, groupID).Add(float64(len(messages)))
		consumerLag.WithLabelValues(topic, groupID, string(partition)).Set(float64(leo - offset))
	}

	return messages, leo, nil
}

func (b *Broker) CommitOffset(ctx context.Context, groupID, topic string, partition int32, offset int64) error {
	return b.metaStore.CommitOffset(groupID, topic, partition, offset)
}

func (b *Broker) GetOffset(ctx context.Context, groupID, topic string, partition int32) (int64, error) {
	return b.metaStore.GetOffset(groupID, topic, partition)
}

func (b *Broker) JoinGroup(ctx context.Context, groupID, memberID, clientID string, topics []string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	group, ok := b.groups[groupID]
	if !ok {
		group = &ConsumerGroup{
			groupID:    groupID,
			members:    make(map[string]*Member),
			assignment: make(map[int32]string),
			generation: 0,
			state:      "Empty",
		}
		b.groups[groupID] = group
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	group.members[memberID] = &Member{
		memberID: memberID,
		clientID: clientID,
		topics:   topics,
		lastHB:   time.Now(),
	}

	if len(group.members) > 0 && group.state == "Empty" {
		group.state = "PreparingRebalance"
	}

	group.generation++
	group.state = "Stable"

	b.rebalance(groupID)

	return group.generation, nil
}

func (b *Broker) LeaveGroup(ctx context.Context, groupID, memberID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	group, ok := b.groups[groupID]
	if !ok {
		return nil
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	delete(group.members, memberID)
	if len(group.members) == 0 {
		group.state = "Empty"
	} else {
		b.rebalance(groupID)
	}

	return nil
}

func (b *Broker) Heartbeat(ctx context.Context, groupID, memberID string) error {
	b.mu.RLock()
	group, ok := b.groups[groupID]
	b.mu.RUnlock()

	if !ok {
		return nil
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if member, ok := group.members[memberID]; ok {
		member.lastHB = time.Now()
	}

	return nil
}

func (b *Broker) rebalance(groupID string) {
	group := b.groups[groupID]
	if len(group.members) == 0 {
		return
	}

	group.assignment = make(map[int32]string)
	members := make([]*Member, 0, len(group.members))
	for _, m := range group.members {
		members = append(members, m)
	}

	for _, topic := range members[0].topics {
		topicMeta, err := b.metaStore.GetTopic(topic)
		if err != nil {
			continue
		}
		for i := int32(0); i < topicMeta.Partitions; i++ {
			memberIdx := int(i) % len(members)
			group.assignment[i] = members[memberIdx].memberID
		}
	}
}

func (b *Broker) selectPartition(topic string, partitionKey string) (int32, error) {
	topicMeta, err := b.metaStore.GetTopic(topic)
	if err != nil {
		return 0, err
	}

	if partitionKey != "" {
		hash := hashString(partitionKey)
		return int32(hash % uint32(topicMeta.Partitions)), nil
	}

	return int32(time.Now().UnixNano() % int64(topicMeta.Partitions)), nil
}

func (b *Broker) Close() {
	b.running.Store(false)
	b.delayQueue.Close()
}

func (b *Broker) RegisterGRPC(server interface{}) {
}

func partitionKeyFromParts(topic string, partition int32) string {
	return topic + "-" + string(partition)
}

func generateMessageID() string {
	return time.Now().Format("20060102150405") + "-" + randomString(8)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().Nanosecond()%len(letters)]
	}
	return string(b)
}

func hashString(s string) uint32 {
	h := uint32(0)
	for _, c := range s {
		h = h*31 + uint32(c)
	}
	return h
}
