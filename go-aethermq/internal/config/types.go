package config

type BrokerConfig struct {
	NodeID           string `yaml:"node_id"`
	ListenAddr       string `yaml:"listen_addr"`
	DataDir          string `yaml:"data_dir"`
	SegmentSize      int64  `yaml:"segment_size"`
	FlushInterval    int    `yaml:"flush_interval"`
	Replication      int    `yaml:"replication"`
	ControllerAddr   string `yaml:"controller_addr"`
	MaxConnections   int    `yaml:"max_connections"`
	MaxMessageSize   int    `yaml:"max_message_size"`
	Compression      string `yaml:"compression"`
	EnableTrace      bool   `yaml:"enable_trace"`
}

type ControllerConfig struct {
	NodeID           string `yaml:"node_id"`
	ListenAddr       string `yaml:"listen_addr"`
	RaftAddr         string `yaml:"raft_addr"`
	DataDir          string `yaml:"data_dir"`
	JoinAddr         string `yaml:"join_addr"`
	ElectionTimeout  int    `yaml:"election_timeout"`
	HeartbeatTimeout int    `yaml:"heartbeat_timeout"`
}

type GatewayConfig struct {
	NodeID         string `yaml:"node_id"`
	ListenAddr     string `yaml:"listen_addr"`
	ControllerAddr string `yaml:"controller_addr"`
	MetricsAddr    string `yaml:"metrics_addr"`
	MaxConnections int    `yaml:"max_connections"`
	ReadTimeout    int    `yaml:"read_timeout"`
	WriteTimeout   int    `yaml:"write_timeout"`
}
