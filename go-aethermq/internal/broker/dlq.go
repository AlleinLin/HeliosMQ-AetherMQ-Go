package broker

import (
	"sync"
	"time"

	"github.com/aethermq/aethermq/internal/common"
)

type DLQManager struct {
	mu      sync.RWMutex
	queues  map[string]*DLQueue
}

type DLQueue struct {
	topic      string
	group      string
	messages   []*common.Message
	maxSize    int
	retryCount map[string]int
}

func NewDLQManager() *DLQManager {
	return &DLQManager{
		queues: make(map[string]*DLQueue),
	}
}

func (m *DLQManager) AddToDLQ(msg *common.Message, group string, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := dlqKey(msg.Topic, group)
	queue, ok := m.queues[key]
	if !ok {
		queue = &DLQueue{
			topic:      msg.Topic,
			group:      group,
			messages:   make([]*common.Message, 0),
			maxSize:    10000,
			retryCount: make(map[string]int),
		}
		m.queues[key] = queue
	}

	if len(queue.messages) >= queue.maxSize {
		queue.messages = queue.messages[1:]
	}

	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}
	msg.Headers["x-dlq-reason"] = reason
	msg.Headers["x-dlq-original-topic"] = msg.Topic
	msg.Headers["x-dlq-timestamp"] = time.Now().Format(time.RFC3339)

	queue.messages = append(queue.messages, msg)
	return nil
}

func (m *DLQManager) GetDLQMessages(topic, group string, offset, limit int) ([]*common.Message, int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := dlqKey(topic, group)
	queue, ok := m.queues[key]
	if !ok {
		return nil, 0, nil
	}

	total := len(queue.messages)
	if offset >= total {
		return nil, total, nil
	}

	end := offset + limit
	if end > total {
		end = total
	}

	return queue.messages[offset:end], total, nil
}

func (m *DLQManager) RetryMessage(topic, group string, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := dlqKey(topic, group)
	queue, ok := m.queues[key]
	if !ok {
		return nil
	}

	for i, msg := range queue.messages {
		if msg.MessageID == messageID {
			queue.messages = append(queue.messages[:i], queue.messages[i+1:]...)
			msg.DeliverAt = 0
			msg.RetryCount++
			return nil
		}
	}

	return nil
}

func (m *DLQManager) ListDLQs() []DLQInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var infos []DLQInfo
	for key, queue := range m.queues {
		infos = append(infos, DLQInfo{
			Key:       key,
			Topic:     queue.topic,
			Group:     queue.group,
			Count:     len(queue.messages),
			MaxSize:   queue.maxSize,
		})
	}
	return infos
}

type DLQInfo struct {
	Key     string `json:"key"`
	Topic   string `json:"topic"`
	Group   string `json:"group"`
	Count   int    `json:"count"`
	MaxSize int    `json:"max_size"`
}

func dlqKey(topic, group string) string {
	return topic + ":" + group
}
