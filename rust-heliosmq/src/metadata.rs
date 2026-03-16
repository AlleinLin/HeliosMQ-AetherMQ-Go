use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::RwLock;

use crate::common::{TopicMetadata, PartitionMetadata, BrokerInfo, BrokerState};

pub struct MetadataStore {
    db: rocksdb::DB,
    topics: RwLock<HashMap<String, TopicMetadata>>,
    partitions: RwLock<HashMap<String, PartitionMetadata>>,
    brokers: RwLock<HashMap<String, BrokerInfo>>,
}

impl MetadataStore {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let db = rocksdb::DB::open_default(path)?;
        
        let store = Self {
            db,
            topics: RwLock::new(HashMap::new()),
            partitions: RwLock::new(HashMap::new()),
            brokers: RwLock::new(HashMap::new()),
        };
        
        store.load_from_disk()?;
        Ok(store)
    }

    fn load_from_disk(&self) -> anyhow::Result<()> {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key);
            
            if key_str.starts_with("topic:") {
                if let Ok(meta) = serde_json::from_slice::<TopicMetadata>(&value) {
                    self.topics.write().insert(meta.name.clone(), meta);
                }
            } else if key_str.starts_with("partition:") {
                if let Ok(meta) = serde_json::from_slice::<PartitionMetadata>(&value) {
                    let key = format!("{}:{}", meta.topic, meta.partition);
                    self.partitions.write().insert(key, meta);
                }
            } else if key_str.starts_with("broker:") {
                if let Ok(info) = serde_json::from_slice::<BrokerInfo>(&value) {
                    self.brokers.write().insert(info.broker_id.clone(), info);
                }
            }
        }
        
        Ok(())
    }

    pub fn create_topic(&self, meta: TopicMetadata) -> anyhow::Result<()> {
        let key = format!("topic:{}", meta.name);
        let value = serde_json::to_vec(&meta)?;
        self.db.put(key, value)?;
        self.topics.write().insert(meta.name.clone(), meta);
        Ok(())
    }

    pub fn get_topic(&self, name: &str) -> Option<TopicMetadata> {
        self.topics.read().get(name).cloned()
    }

    pub fn delete_topic(&self, name: &str) -> anyhow::Result<()> {
        let key = format!("topic:{}", name);
        self.db.delete(key)?;
        self.topics.write().remove(name);
        Ok(())
    }

    pub fn list_topics(&self) -> Vec<TopicMetadata> {
        self.topics.read().values().cloned().collect()
    }

    pub fn update_partition(&self, meta: PartitionMetadata) -> anyhow::Result<()> {
        let key = format!("partition:{}:{}", meta.topic, meta.partition);
        let value = serde_json::to_vec(&meta)?;
        self.db.put(&key, value)?;
        
        let map_key = format!("{}:{}", meta.topic, meta.partition);
        self.partitions.write().insert(map_key, meta);
        Ok(())
    }

    pub fn get_partition(&self, topic: &str, partition: i32) -> Option<PartitionMetadata> {
        let key = format!("{}:{}", topic, partition);
        self.partitions.read().get(&key).cloned()
    }

    pub fn commit_offset(&self, group: &str, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        let key = format!("offset:{}:{}:{}", group, topic, partition);
        self.db.put(key, offset.to_be_bytes())?;
        Ok(())
    }

    pub fn get_offset(&self, group: &str, topic: &str, partition: i32) -> i64 {
        let key = format!("offset:{}:{}:{}", group, topic, partition);
        match self.db.get(key) {
            Ok(Some(value)) => {
                if value.len() == 8 {
                    i64::from_be_bytes(value.try_into().unwrap())
                } else {
                    -1
                }
            }
            _ => -1,
        }
    }

    pub fn register_broker(&self, broker_id: &str, address: &str, zone: &str) -> anyhow::Result<()> {
        let info = BrokerInfo {
            broker_id: broker_id.to_string(),
            address: address.to_string(),
            zone: zone.to_string(),
            state: BrokerState::Online,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        };
        
        let key = format!("broker:{}", broker_id);
        let value = serde_json::to_vec(&info)?;
        self.db.put(key, value)?;
        self.brokers.write().insert(broker_id.to_string(), info);
        Ok(())
    }

    pub fn unregister_broker(&self, broker_id: &str) -> anyhow::Result<()> {
        let key = format!("broker:{}", broker_id);
        self.db.delete(key)?;
        if let Some(info) = self.brokers.write().get_mut(broker_id) {
            info.state = BrokerState::Offline;
        }
        Ok(())
    }

    pub fn list_brokers(&self) -> Vec<BrokerInfo> {
        self.brokers.read().values().cloned().collect()
    }

    pub fn get_broker(&self, broker_id: &str) -> Option<BrokerInfo> {
        self.brokers.read().get(broker_id).cloned()
    }
}
