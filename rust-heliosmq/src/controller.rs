use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use tracing::{info, warn, error, instrument};

use crate::config::ControllerConfig;
use crate::raft::RaftNode;
use crate::common::{TopicMetadata, PartitionMetadata, BrokerInfo, BrokerState};

pub struct Controller {
    config: Arc<ControllerConfig>,
    raft: Arc<RaftNode>,
    topics: RwLock<HashMap<String, TopicInfo>>,
    brokers: RwLock<HashMap<String, BrokerInfo>>,
    groups: RwLock<HashMap<String, GroupInfo>>,
    leader: Mutex<bool>,
}

#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    pub partition_map: HashMap<i32, PartitionInfo>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition: i32,
    pub leader: String,
    pub replicas: Vec<String>,
    pub isr: Vec<String>,
    pub hw: i64,
    pub leo: i64,
}

#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub group_id: String,
    pub members: HashMap<String, MemberInfo>,
    pub generation: i64,
    pub state: GroupState,
}

#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub topics: Vec<String>,
    pub assignment: HashMap<String, Vec<i32>>,
    pub last_heartbeat: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

impl Controller {
    pub fn new(config: ControllerConfig, raft: Arc<RaftNode>) -> Self {
        Self {
            config: Arc::new(config),
            raft,
            topics: RwLock::new(HashMap::new()),
            brokers: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
            leader: Mutex::new(false),
        }
    }

    pub async fn start(&self) -> anyhow::Result<ControllerHandle> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn({
            let controller = self.clone_ref();
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if controller.raft.is_leader() {
                                *controller.leader.lock() = true;
                                controller.reconcile();
                            } else {
                                *controller.leader.lock() = false;
                            }
                        }
                        _ = &mut shutdown_rx => {
                            info!("Controller shutting down");
                            break;
                        }
                    }
                }
            }
        });

        Ok(ControllerHandle {
            shutdown_tx: Some(shutdown_tx),
        })
    }

    fn clone_ref(&self) -> Self {
        Self {
            config: self.config.clone(),
            raft: self.raft.clone(),
            topics: RwLock::new(self.topics.read().clone()),
            brokers: RwLock::new(self.brokers.read().clone()),
            groups: RwLock::new(self.groups.read().clone()),
            leader: Mutex::new(*self.leader.lock()),
        }
    }

    fn reconcile(&self) {
        let now = Instant::now();
        let mut brokers = self.brokers.write();
        
        for (broker_id, broker) in brokers.iter_mut() {
            if now.duration_since(Instant::now()) > Duration::from_secs(30) {
                if broker.state == BrokerState::Online {
                    broker.state = BrokerState::Offline;
                    warn!("Broker {} is offline", broker_id);
                    self.reassign_partitions(broker_id);
                }
            }
        }
    }

    fn reassign_partitions(&self, failed_broker: &str) {
        let mut topics = self.topics.write();
        
        for (topic_name, topic) in topics.iter_mut() {
            for (partition_id, partition) in topic.partition_map.iter_mut() {
                if partition.leader == failed_broker {
                    self.elect_new_leader(topic_name, *partition_id, partition);
                }
            }
        }
    }

    fn elect_new_leader(&self, topic: &str, partition: i32, partition_info: &mut PartitionInfo) {
        let brokers = self.brokers.read();
        
        for replica in &partition_info.replicas {
            if let Some(broker) = brokers.get(replica) {
                if broker.state == BrokerState::Online && replica != &partition_info.leader {
                    partition_info.leader = replica.clone();
                    info!(
                        topic = %topic,
                        partition = partition,
                        new_leader = %replica,
                        "Elected new leader"
                    );
                    break;
                }
            }
        }
    }

    pub async fn create_topic(&self, name: &str, partitions: i32, replication_factor: i32) -> anyhow::Result<()> {
        let mut topics = self.topics.write();
        
        if topics.contains_key(name) {
            return Err(anyhow::anyhow!("Topic already exists: {}", name));
        }

        let brokers: Vec<_> = self.brokers.read()
            .iter()
            .filter(|(_, b)| b.state == BrokerState::Online)
            .map(|(id, _)| id.clone())
            .collect();

        let replication_factor = replication_factor.min(brokers.len() as i32);

        let mut partition_map = HashMap::new();
        for i in 0..partitions {
            let replicas = self.select_replicas(&brokers, replication_factor as usize, i as usize);
            partition_map.insert(i, PartitionInfo {
                partition: i,
                leader: replicas.first().cloned().unwrap_or_default(),
                replicas,
                isr: Vec::new(),
                hw: 0,
                leo: 0,
            });
        }

        let topic_info = TopicInfo {
            name: name.to_string(),
            partitions,
            replication_factor,
            partition_map,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        };

        topics.insert(name.to_string(), topic_info);

        Ok(())
    }

    pub async fn delete_topic(&self, name: &str) -> anyhow::Result<()> {
        let mut topics = self.topics.write();
        
        if topics.remove(name).is_none() {
            return Err(anyhow::anyhow!("Topic not found: {}", name));
        }

        Ok(())
    }

    pub async fn register_broker(&self, broker_id: &str, address: &str, zone: &str) -> anyhow::Result<()> {
        let mut brokers = self.brokers.write();
        
        brokers.insert(broker_id.to_string(), BrokerInfo {
            broker_id: broker_id.to_string(),
            address: address.to_string(),
            zone: zone.to_string(),
            state: BrokerState::Online,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        });

        Ok(())
    }

    pub async fn unregister_broker(&self, broker_id: &str) -> anyhow::Result<()> {
        let mut brokers = self.brokers.write();
        
        if let Some(broker) = brokers.get_mut(broker_id) {
            broker.state = BrokerState::Offline;
        }

        Ok(())
    }

    pub fn get_topic(&self, name: &str) -> Option<TopicInfo> {
        self.topics.read().get(name).cloned()
    }

    pub fn list_topics(&self) -> Vec<TopicInfo> {
        self.topics.read().values().cloned().collect()
    }

    fn select_replicas(&self, brokers: &[String], count: usize, partition: usize) -> Vec<String> {
        if brokers.is_empty() || count == 0 {
            return Vec::new();
        }

        let mut replicas = Vec::with_capacity(count);
        for i in 0..count.min(brokers.len()) {
            let idx = (partition + i) % brokers.len();
            replicas.push(brokers[idx].clone());
        }
        replicas
    }
}

pub struct ControllerHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ControllerHandle {
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}
