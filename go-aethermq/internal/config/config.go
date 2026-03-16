package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type BrokerConfig struct {
	NodeID         string        `yaml:"node_id"`
	ListenAddr     string        `yaml:"listen_addr"`
	DataDir        string        `yaml:"data_dir"`
	SegmentSize    int64         `yaml:"segment_size"`
	FlushInterval  time.Duration `yaml:"flush_interval"`
	Replication    int           `yaml:"replication"`
	ControllerAddr string        `yaml:"controller_addr"`
	MaxConnections int           `yaml:"max_connections"`
	MaxMessageSize int           `yaml:"max_message_size"`
	Compression    string        `yaml:"compression"`
	EnableTrace    bool          `yaml:"enable_trace"`
}

type ControllerConfig struct {
	NodeID           string        `yaml:"node_id"`
	ListenAddr       string        `yaml:"listen_addr"`
	RaftAddr         string        `yaml:"raft_addr"`
	DataDir          string        `yaml:"data_dir"`
	JoinAddr         string        `yaml:"join_addr"`
	ElectionTimeout  time.Duration `yaml:"election_timeout"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`
}

type GatewayConfig struct {
	NodeID         string        `yaml:"node_id"`
	ListenAddr     string        `yaml:"listen_addr"`
	ControllerAddr string        `yaml:"controller_addr"`
	MetricsAddr    string        `yaml:"metrics_addr"`
	MaxConnections int           `yaml:"max_connections"`
	ReadTimeout    time.Duration `yaml:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout"`
}

type TopicConfig struct {
	Name              string `yaml:"name"`
	Partitions        int    `yaml:"partitions"`
	ReplicationFactor int    `yaml:"replication_factor"`
	RetentionMs       int64  `yaml:"retention_ms"`
	RetentionBytes    int64  `yaml:"retention_bytes"`
	SegmentMs         int64  `yaml:"segment_ms"`
	SegmentBytes      int64  `yaml:"segment_bytes"`
	Compression       string `yaml:"compression"`
	CleanupPolicy     string `yaml:"cleanup_policy"`
	MinInSyncReplicas int    `yaml:"min_in_sync_replicas"`
	EnableDelay       bool   `yaml:"enable_delay"`
	MaxRetry          int    `yaml:"max_retry"`
	DLQEnabled        bool   `yaml:"dlq_enabled"`
}

func LoadBrokerConfig(path string) (*BrokerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg BrokerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	setDefaults(&cfg)
	return &cfg, nil
}

func LoadControllerConfig(path string) (*ControllerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg ControllerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func LoadGatewayConfig(path string) (*GatewayConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg GatewayConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func setDefaults(cfg *BrokerConfig) {
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = 1 << 30
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 10 * time.Millisecond
	}
	if cfg.Replication == 0 {
		cfg.Replication = 3
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 10000
	}
	if cfg.MaxMessageSize == 0 {
		cfg.MaxMessageSize = 4 * 1024 * 1024
	}
	if cfg.Compression == "" {
		cfg.Compression = "zstd"
	}
}
