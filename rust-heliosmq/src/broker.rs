use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use tracing::{info, warn, error, instrument};

use crate::common::{Message, MessageBatch, TopicMetadata, PartitionMetadata, ConsumerGroupMetadata};
use crate::config::BrokerConfig;
use crate::storage::SegmentStore;
use crate::metadata::MetadataStore;
use crate::delay::DelayQueue;
use crate::dlq::DLQManager;
use crate::metrics::Metrics;

pub struct Broker {
    config: Arc<BrokerConfig>,
    metadata: Arc<MetadataStore>,
    storage: Arc<SegmentStore>,
    delay_queue: Arc<DelayQueue>,
    dlq_manager: Arc<DLQManager>,
    metrics: Arc<Metrics>,
    partitions: RwLock<HashMap<String, Arc<Partition>>>,
    groups: RwLock<HashMap<String, Arc<ConsumerGroup>>>,
    running: Mutex<bool>,
}

pub struct Partition {
    topic: String,
    partition: i32,
    leader: bool,
    replicas: Vec<String>,
    isr: Vec<String>,
    hw: Mutex<i64>,
    leo: Mutex<i64>,
}

pub struct ConsumerGroup {
    group_id: String,
    members: RwLock<HashMap<String, Member>>,
    assignment: RwLock<HashMap<i32, String>>,
    generation: Mutex<i64>,
    state: RwLock<GroupState>,
}

pub struct Member {
    member_id: String,
    client_id: String,
    topics: Vec<String>,
    last_heartbeat: Mutex<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

impl Broker {
    pub fn new(
        config: BrokerConfig,
        metadata: Arc<MetadataStore>,
        storage: Arc<SegmentStore>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            metadata,
            storage,
            delay_queue: Arc::new(DelayQueue::new()),
            dlq_manager: Arc::new(DLQManager::new()),
            metrics: Arc::new(Metrics::new()),
            partitions: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
            running: Mutex::new(true),
        }
    }

    pub fn start(&self) -> anyhow::Result<BrokerHandle> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        
        tokio::spawn({
            let delay_queue = self.delay_queue.clone();
            let storage = self.storage.clone();
            async move {
                delay_queue.run(storage).await;
            }
        });

        Ok(BrokerHandle {
            shutdown_tx: Some(shutdown_tx),
        })
    }

    #[instrument(skip(self, messages))]
    pub async fn produce(
        &self,
        topic: &str,
        partition_key: Option<&str>,
        messages: Vec<Message>,
    ) -> anyhow::Result<Vec<i64>> {
        let start = Instant::now();
        
        let topic_meta = self.metadata.get_topic(topic)
            .ok_or_else(|| anyhow::anyhow!("Topic not found: {}", topic))?;

        let mut offsets = Vec::with_capacity(messages.len());

        for mut msg in messages {
            let partition = self.select_partition(&topic_meta, partition_key);
            msg.topic = topic.to_string();
            msg.partition = partition;

            if msg.message_id.is_empty() {
                msg.message_id = uuid::Uuid::now_v7().to_string();
            }

            if msg.timestamp == 0 {
                msg.timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
            }

            if msg.is_delayed() {
                self.delay_queue.schedule(msg, Duration::from_millis(msg.deliver_at as u64));
                offsets.push(0);
                continue;
            }

            let offset = self.storage.append(&msg)?;
            offsets.push(offset);

            self.metrics.produce_total.inc_by(1);
        }

        self.metrics.produce_latency.observe(start.elapsed().as_secs_f64());

        Ok(offsets)
    }

    #[instrument(skip(self))]
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        group_id: Option<&str>,
        offset: i64,
        max_bytes: u32,
    ) -> anyhow::Result<(Vec<Message>, i64)> {
        let start = Instant::now();

        let (messages, leo) = self.storage.fetch(topic, partition, offset, max_bytes)?;

        if let Some(group) = group_id {
            self.metrics.consume_total.inc_by(messages.len() as f64);
            let lag = leo - offset;
            self.metrics.consumer_lag.set(lag as f64);
        }

        self.metrics.fetch_latency.observe(start.elapsed().as_secs_f64());

        Ok((messages, leo))
    }

    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> anyhow::Result<()> {
        self.metadata.commit_offset(group_id, topic, partition, offset)?;
        Ok(())
    }

    pub async fn get_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> i64 {
        self.metadata.get_offset(group_id, topic, partition)
    }

    pub async fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        client_id: &str,
        topics: Vec<String>,
    ) -> anyhow::Result<i64> {
        let mut groups = self.groups.write();
        
        let group = groups.entry(group_id.to_string())
            .or_insert_with(|| Arc::new(ConsumerGroup {
                group_id: group_id.to_string(),
                members: RwLock::new(HashMap::new()),
                assignment: RwLock::new(HashMap::new()),
                generation: Mutex::new(0),
                state: RwLock::new(GroupState::Empty),
            }));

        let mut members = group.members.write();
        members.insert(member_id.to_string(), Member {
            member_id: member_id.to_string(),
            client_id: client_id.to_string(),
            topics,
            last_heartbeat: Mutex::new(Instant::now()),
        });

        let mut generation = group.generation.lock();
        *generation += 1;

        self.rebalance(group_id);

        Ok(*generation)
    }

    pub async fn leave_group(&self, group_id: &str, member_id: &str) -> anyhow::Result<()> {
        let groups = self.groups.read();
        if let Some(group) = groups.get(group_id) {
            let mut members = group.members.write();
            members.remove(member_id);
            
            if members.is_empty() {
                *group.state.write() = GroupState::Empty;
            } else {
                drop(members);
                self.rebalance(group_id);
            }
        }
        Ok(())
    }

    pub async fn heartbeat(&self, group_id: &str, member_id: &str) -> anyhow::Result<()> {
        let groups = self.groups.read();
        if let Some(group) = groups.get(group_id) {
            let members = group.members.read();
            if let Some(member) = members.get(member_id) {
                *member.last_heartbeat.lock() = Instant::now();
            }
        }
        Ok(())
    }

    fn rebalance(&self, group_id: &str) {
        let groups = self.groups.read();
        if let Some(group) = groups.get(group_id) {
            let members: Vec<_> = group.members.read().values().collect();
            if members.is_empty() {
                return;
            }

            let mut assignment = group.assignment.write();
            assignment.clear();

            for member in &members {
                for topic in &member.topics {
                    if let Some(topic_meta) = self.metadata.get_topic(topic) {
                        for i in 0..topic_meta.partitions {
                            let member_idx = (i as usize) % members.len();
                            assignment.insert(i, members[member_idx].member_id.clone());
                        }
                    }
                }
            }

            *group.state.write() = GroupState::Stable;
        }
    }

    fn select_partition(&self, topic_meta: &TopicMetadata, partition_key: Option<&str>) -> i32 {
        match partition_key {
            Some(key) => {
                let hash = Self::hash_key(key);
                (hash % topic_meta.partitions as u32) as i32
            }
            None => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                (now % topic_meta.partitions as u128) as i32
            }
        }
    }

    fn hash_key(key: &str) -> u32 {
        let mut hash: u32 = 0;
        for byte in key.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
        }
        hash
    }
}

pub struct BrokerHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl BrokerHandle {
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}
