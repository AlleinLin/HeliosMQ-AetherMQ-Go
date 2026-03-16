package common

import (
	"time"

	"github.com/google/uuid"
)

type Message struct {
	MessageID      string            `json:"message_id"`
	Topic          string            `json:"topic"`
	Partition      int32             `json:"partition"`
	Offset         int64             `json:"offset"`
	Key            []byte            `json:"key"`
	Value          []byte            `json:"value"`
	Headers        map[string]string `json:"headers"`
	Timestamp      int64             `json:"timestamp"`
	PartitionKey   string            `json:"partition_key"`
	OrderingKey    string            `json:"ordering_key"`
	DeliverAt      int64             `json:"deliver_at"`
	TTL            int32             `json:"ttl"`
	MaxRetry       int32             `json:"max_retry"`
	RetryCount     int32             `json:"retry_count"`
	TraceID        string            `json:"trace_id"`
	ProducerID     string            `json:"producer_id"`
	SequenceNumber int64             `json:"sequence_number"`
	TransactionID  string            `json:"transaction_id"`
}

type MessageBatch struct {
	Messages    []*Message `json:"messages"`
	BaseOffset  int64      `json:"base_offset"`
	Count       int        `json:"count"`
	Size        int64      `json:"size"`
	Compression string     `json:"compression"`
}

func NewMessage(topic string, value []byte) *Message {
	return &Message{
		MessageID: uuid.NewString(),
		Topic:     topic,
		Value:     value,
		Timestamp: time.Now().UnixMilli(),
		Headers:   make(map[string]string),
	}
}

func (m *Message) WithKey(key []byte) *Message {
	m.Key = key
	return m
}

func (m *Message) WithPartitionKey(key string) *Message {
	m.PartitionKey = key
	return m
}

func (m *Message) WithHeader(key, value string) *Message {
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[key] = value
	return m
}

func (m *Message) WithDelay(delay time.Duration) *Message {
	m.DeliverAt = time.Now().Add(delay).UnixMilli()
	return m
}

func (m *Message) WithTTL(ttl time.Duration) *Message {
	m.TTL = int32(ttl.Seconds())
	return m
}

func (m *Message) WithMaxRetry(max int) *Message {
	m.MaxRetry = int32(max)
	return m
}

type TopicMetadata struct {
	Name              string `json:"name"`
	Partitions        int32  `json:"partitions"`
	ReplicationFactor int32  `json:"replication_factor"`
	RetentionMs       int64  `json:"retention_ms"`
	RetentionBytes    int64  `json:"retention_bytes"`
	SegmentMs         int64  `json:"segment_ms"`
	SegmentBytes      int64  `json:"segment_bytes"`
	Compression       string `json:"compression"`
	CleanupPolicy     string `json:"cleanup_policy"`
	MinInSyncReplicas int32  `json:"min_in_sync_replicas"`
}

type PartitionMetadata struct {
	Topic       string   `json:"topic"`
	Partition   int32    `json:"partition"`
	Leader      string   `json:"leader"`
	Replicas    []string `json:"replicas"`
	ISR         []string `json:"isr"`
	LEO         int64    `json:"leo"`
	HW          int64    `json:"hw"`
	LeaderEpoch int64    `json:"leader_epoch"`
}

type ConsumerGroupMetadata struct {
	GroupID      string            `json:"group_id"`
	State        string            `json:"state"`
	Members      map[string]Member `json:"members"`
	Generation   int64             `json:"generation"`
	ProtocolType string            `json:"protocol_type"`
}

type Member struct {
	MemberID   string   `json:"member_id"`
	ClientID   string   `json:"client_id"`
	Topics     []string `json:"topics"`
	Assignment []Assignment
}

type Assignment struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

type BrokerInfo struct {
	BrokerID  string `json:"broker_id"`
	Address   string `json:"address"`
	Zone      string `json:"zone"`
	State     string `json:"state"`
	Timestamp int64  `json:"timestamp"`
}
