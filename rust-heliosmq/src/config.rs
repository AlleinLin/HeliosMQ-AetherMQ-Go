use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    pub node_id: String,
    pub listen_addr: String,
    pub data_dir: String,
    pub controller_addr: String,
    pub segment_size: u64,
    pub flush_interval_ms: u64,
    pub replication_factor: u32,
    pub max_connections: u32,
    pub max_message_size: u32,
    pub compression: CompressionType,
    pub enable_trace: bool,
    pub io_threads: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            listen_addr: "0.0.0.0:9092".to_string(),
            data_dir: "./data".to_string(),
            controller_addr: "127.0.0.1:19091".to_string(),
            segment_size: 1024 * 1024 * 1024,
            flush_interval_ms: 10,
            replication_factor: 3,
            max_connections: 10000,
            max_message_size: 4 * 1024 * 1024,
            compression: CompressionType::Zstd,
            enable_trace: true,
            io_threads: num_cpus::get(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerConfig {
    pub node_id: String,
    pub listen_addr: String,
    pub raft_addr: String,
    pub data_dir: String,
    pub join_addr: Option<String>,
    pub election_timeout_ms: u64,
    pub heartbeat_timeout_ms: u64,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            listen_addr: "0.0.0.0:19091".to_string(),
            raft_addr: "0.0.0.0:19092".to_string(),
            data_dir: "./data/controller".to_string(),
            join_addr: None,
            election_timeout_ms: 1000,
            heartbeat_timeout_ms: 100,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    pub node_id: String,
    pub listen_addr: String,
    pub controller_addr: String,
    pub metrics_addr: String,
    pub max_connections: u32,
    pub read_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            listen_addr: "0.0.0.0:29092".to_string(),
            controller_addr: "127.0.0.1:19091".to_string(),
            metrics_addr: "0.0.0.0:9093".to_string(),
            max_connections: 50000,
            read_timeout_ms: 30000,
            write_timeout_ms: 30000,
            request_timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
    Snappy,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Zstd
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u32,
    pub retention_ms: i64,
    pub retention_bytes: i64,
    pub segment_ms: i64,
    pub segment_bytes: i64,
    pub compression: CompressionType,
    pub cleanup_policy: CleanupPolicy,
    pub min_in_sync_replicas: u32,
    pub enable_delay: bool,
    pub max_retry: u32,
    pub dlq_enabled: bool,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            partitions: 1,
            replication_factor: 3,
            retention_ms: 7 * 24 * 60 * 60 * 1000,
            retention_bytes: -1,
            segment_ms: 7 * 24 * 60 * 60 * 1000,
            segment_bytes: 1024 * 1024 * 1024,
            compression: CompressionType::Zstd,
            cleanup_policy: CleanupPolicy::Delete,
            min_in_sync_replicas: 2,
            enable_delay: true,
            max_retry: 16,
            dlq_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CleanupPolicy {
    Delete,
    Compact,
    CompactDelete,
}
