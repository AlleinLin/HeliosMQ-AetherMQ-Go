use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub message_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub timestamp: i64,
    pub partition_key: Option<String>,
    pub ordering_key: Option<String>,
    pub deliver_at: i64,
    pub ttl: i32,
    pub max_retry: i32,
    pub retry_count: i32,
    pub trace_id: Option<String>,
    pub producer_id: Option<String>,
    pub sequence_number: i64,
    pub transaction_id: Option<String>,
}

impl Message {
    pub fn new(topic: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            message_id: Uuid::now_v7().to_string(),
            topic: topic.into(),
            partition: 0,
            offset: -1,
            key: None,
            value,
            headers: HashMap::new(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            partition_key: None,
            ordering_key: None,
            deliver_at: 0,
            ttl: 0,
            max_retry: 16,
            retry_count: 0,
            trace_id: None,
            producer_id: None,
            sequence_number: 0,
            transaction_id: None,
        }
    }

    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn with_delay_ms(mut self, delay_ms: i64) -> Self {
        self.deliver_at = self.timestamp + delay_ms;
        self
    }

    pub fn with_ttl(mut self, ttl_seconds: i32) -> Self {
        self.ttl = ttl_seconds;
        self
    }

    pub fn with_max_retry(mut self, max: i32) -> Self {
        self.max_retry = max;
        self
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    pub fn size(&self) -> usize {
        let mut size = self.value.len();
        if let Some(ref key) = self.key {
            size += key.len();
        }
        size += self.headers.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
        size
    }

    pub fn is_delayed(&self) -> bool {
        self.deliver_at > 0 && self.deliver_at > self.timestamp
    }

    pub fn is_expired(&self) -> bool {
        if self.ttl <= 0 {
            return false;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        now > self.timestamp + (self.ttl as i64 * 1000)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBatch {
    pub messages: Vec<Message>,
    pub base_offset: i64,
    pub count: u32,
    pub size: u64,
    pub compression: String,
}

impl MessageBatch {
    pub fn new(messages: Vec<Message>) -> Self {
        let count = messages.len() as u32;
        let size = messages.iter().map(|m| m.size() as u64).sum();
        let base_offset = messages.first().map(|m| m.offset).unwrap_or(-1);
        Self {
            messages,
            base_offset,
            count,
            size,
            compression: "zstd".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub retention_ms: i64,
    pub retention_bytes: i64,
    pub segment_ms: i64,
    pub segment_bytes: i64,
    pub compression: String,
    pub cleanup_policy: String,
    pub min_in_sync_replicas: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub topic: String,
    pub partition: i32,
    pub leader: String,
    pub replicas: Vec<String>,
    pub isr: Vec<String>,
    pub leo: i64,
    pub hw: i64,
    pub leader_epoch: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMetadata {
    pub group_id: String,
    pub state: GroupState,
    pub members: HashMap<String, MemberInfo>,
    pub generation: i64,
    pub protocol_type: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub topics: Vec<String>,
    pub assignment: Vec<PartitionAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub broker_id: String,
    pub address: String,
    pub zone: String,
    pub state: BrokerState,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerState {
    Online,
    Offline,
    Starting,
    ShuttingDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscriptionType {
    Exclusive,
    Shared,
    Failover,
    KeyShared,
    Broadcast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckMode {
    Auto,
    Manual,
    Batch,
    Transaction,
}
