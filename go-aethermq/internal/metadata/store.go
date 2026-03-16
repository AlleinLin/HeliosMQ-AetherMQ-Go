package metadata

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketTopics       = []byte("topics")
	bucketPartitions   = []byte("partitions")
	bucketGroups       = []byte("groups")
	bucketOffsets      = []byte("offsets")
	bucketBrokers      = []byte("brokers")
	bucketTransactions = []byte("transactions")
)

type BoltStore struct {
	db *bolt.DB
	mu sync.RWMutex
}

func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		buckets := [][]byte{bucketTopics, bucketPartitions, bucketGroups, bucketOffsets, bucketBrokers, bucketTransactions}
		for _, b := range buckets {
			if _, e := tx.CreateBucketIfNotExists(b); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &BoltStore{db: db}, nil
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) CreateTopic(topic *TopicMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		if b.Get([]byte(topic.Name)) != nil {
			return errors.New("topic already exists")
		}
		data, err := json.Marshal(topic)
		if err != nil {
			return err
		}
		return b.Put([]byte(topic.Name), data)
	})
}

func (s *BoltStore) GetTopic(name string) (*TopicMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var meta *TopicMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		v := b.Get([]byte(name))
		if v == nil {
			return errors.New("topic not found")
		}
		var t TopicMeta
		if err := json.Unmarshal(v, &t); err != nil {
			return err
		}
		meta = &t
		return nil
	})
	return meta, err
}

func (s *BoltStore) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		return b.Delete([]byte(name))
	})
}

func (s *BoltStore) ListTopics() ([]*TopicMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var topics []*TopicMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTopics)
		return b.ForEach(func(k, v []byte) error {
			var t TopicMeta
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			topics = append(topics, &t)
			return nil
		})
	})
	return topics, err
}

func (s *BoltStore) UpdatePartition(meta *PartitionMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		key := partitionKey(meta.Topic, meta.Partition)
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), data)
	})
}

func (s *BoltStore) GetPartition(topic string, partition int32) (*PartitionMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var meta *PartitionMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		key := partitionKey(topic, partition)
		v := b.Get([]byte(key))
		if v == nil {
			return errors.New("partition not found")
		}
		var p PartitionMeta
		if err := json.Unmarshal(v, &p); err != nil {
			return err
		}
		meta = &p
		return nil
	})
	return meta, err
}

func (s *BoltStore) CommitOffset(group, topic string, partition int32, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOffsets)
		key := offsetKey(group, topic, partition)
		return b.Put([]byte(key), int64ToBytes(offset))
	})
}

func (s *BoltStore) GetOffset(group, topic string, partition int32) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var offset int64 = -1
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOffsets)
		key := offsetKey(group, topic, partition)
		v := b.Get([]byte(key))
		if v != nil {
			offset = bytesToInt64(v)
		}
		return nil
	})
	return offset, err
}

func (s *BoltStore) RegisterBroker(brokerID, address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBrokers)
		info := BrokerMeta{
			BrokerID:  brokerID,
			Address:   address,
			Timestamp: time.Now().UnixMilli(),
		}
		data, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return b.Put([]byte(brokerID), data)
	})
}

func (s *BoltStore) ListBrokers() ([]*BrokerMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var brokers []*BrokerMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBrokers)
		return b.ForEach(func(k, v []byte) error {
			var info BrokerMeta
			if err := json.Unmarshal(v, &info); err != nil {
				return err
			}
			brokers = append(brokers, &info)
			return nil
		})
	})
	return brokers, err
}

type TopicMeta struct {
	Name              string `json:"name"`
	Partitions        int32  `json:"partitions"`
	ReplicationFactor int32  `json:"replication_factor"`
	RetentionMs       int64  `json:"retention_ms"`
	RetentionBytes    int64  `json:"retention_bytes"`
	Compression       string `json:"compression"`
}

type PartitionMeta struct {
	Topic       string   `json:"topic"`
	Partition   int32    `json:"partition"`
	Leader      string   `json:"leader"`
	Replicas    []string `json:"replicas"`
	ISR         []string `json:"isr"`
	LEO         int64    `json:"leo"`
	HW          int64    `json:"hw"`
	LeaderEpoch int64    `json:"leader_epoch"`
}

type BrokerMeta struct {
	BrokerID  string `json:"broker_id"`
	Address   string `json:"address"`
	Zone      string `json:"zone"`
	Timestamp int64  `json:"timestamp"`
}

func partitionKey(topic string, partition int32) string {
	return topic + ":" + int32ToString(partition)
}

func offsetKey(group, topic string, partition int32) string {
	return group + ":" + topic + ":" + int32ToString(partition)
}

func int32ToString(n int32) string {
	return string(rune(n))
}

func int64ToBytes(n int64) []byte {
	return []byte{
		byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	}
}

func bytesToInt64(b []byte) int64 {
	return int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 |
		int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
}
