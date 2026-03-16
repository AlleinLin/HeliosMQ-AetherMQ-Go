use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn, error};

use crate::common::Message;
use crate::storage::SegmentStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionState::Empty => write!(f, "Empty"),
            TransactionState::Ongoing => write!(f, "Ongoing"),
            TransactionState::PrepareCommit => write!(f, "PrepareCommit"),
            TransactionState::PrepareAbort => write!(f, "PrepareAbort"),
            TransactionState::CompleteCommit => write!(f, "CompleteCommit"),
            TransactionState::CompleteAbort => write!(f, "CompleteAbort"),
            TransactionState::Dead => write!(f, "Dead"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnPartitionStatus {
    Pending,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct PartitionTxnRecord {
    pub topic: String,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub status: TxnPartitionStatus,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub txn_id: String,
    pub producer_id: String,
    pub state: TransactionState,
    pub start_time: i64,
    pub timeout: Duration,
    pub partitions: HashMap<String, PartitionTxnRecord>,
    pub pending_messages: Vec<Message>,
    pub commit_time: i64,
    pub abort_reason: Option<String>,
}

impl Transaction {
    pub fn new(txn_id: String, producer_id: String, timeout: Duration) -> Self {
        Self {
            txn_id,
            producer_id,
            state: TransactionState::Ongoing,
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            timeout,
            partitions: HashMap::new(),
            pending_messages: Vec::new(),
            commit_time: 0,
            abort_reason: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionConfig {
    pub default_timeout: Duration,
    pub max_timeout: Duration,
    pub max_partitions_per_txn: usize,
    pub abort_timed_out_txn: bool,
    pub remove_expired_txn: bool,
    pub checkpoint_interval: Duration,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(900),
            max_timeout: Duration::from_secs(86400),
            max_partitions_per_txn: 1000,
            abort_timed_out_txn: true,
            remove_expired_txn: true,
            checkpoint_interval: Duration::from_secs(60),
        }
    }
}

pub struct TransactionCoordinator {
    config: TransactionConfig,
    store: Arc<SegmentStore>,
    transactions: RwLock<HashMap<String, Transaction>>,
    producer_txns: RwLock<HashMap<String, String>>,
    time_index: Mutex<TransactionTimeIndex>,
    state_machine: TransactionStateMachine,
    running: Mutex<bool>,
}

impl TransactionCoordinator {
    pub fn new(config: TransactionConfig, store: Arc<SegmentStore>) -> Self {
        Self {
            config,
            store,
            transactions: RwLock::new(HashMap::new()),
            producer_txns: RwLock::new(HashMap::new()),
            time_index: Mutex::new(TransactionTimeIndex::new()),
            state_machine: TransactionStateMachine::new(),
            running: Mutex::new(true),
        }
    }

    pub fn begin_transaction(&self, producer_id: &str, timeout: Duration) -> anyhow::Result<String> {
        let timeout = if timeout.is_zero() {
            self.config.default_timeout
        } else {
            timeout.min(self.config.max_timeout)
        };

        let txn_id = generate_txn_id(producer_id);
        let mut txn = Transaction::new(txn_id.clone(), producer_id.to_string(), timeout);

        let expire_time = txn.start_time + timeout.as_millis() as i64;
        self.time_index.lock().add(&txn_id, expire_time);

        self.transactions.write().insert(txn_id.clone(), txn);
        self.producer_txns.write().insert(producer_id.to_string(), txn_id.clone());

        Ok(txn_id)
    }

    pub fn add_partitions(
        &self,
        txn_id: &str,
        partitions: Vec<(String, i32)>,
    ) -> anyhow::Result<()> {
        let mut txns = self.transactions.write();
        let txn = txns.get_mut(txn_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;

        if txn.state != TransactionState::Ongoing {
            return Err(anyhow::anyhow!("Transaction not active"));
        }

        if txn.partitions.len() + partitions.len() > self.config.max_partitions_per_txn {
            return Err(anyhow::anyhow!("Exceeds max partitions per transaction"));
        }

        for (topic, partition) in partitions {
            let key = partition_key(&topic, partition);
            if !txn.partitions.contains_key(&key) {
                txn.partitions.insert(key, PartitionTxnRecord {
                    topic,
                    partition,
                    first_offset: 0,
                    last_offset: 0,
                    status: TxnPartitionStatus::Pending,
                });
            }
        }

        Ok(())
    }

    pub fn add_messages(&self, txn_id: &str, messages: Vec<Message>) -> anyhow::Result<()> {
        let mut txns = self.transactions.write();
        let txn = txns.get_mut(txn_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;

        if txn.state != TransactionState::Ongoing {
            return Err(anyhow::anyhow!("Transaction not active"));
        }

        for mut msg in messages {
            msg.transaction_id = Some(txn_id.to_string());
            let key = partition_key(&msg.topic, msg.partition);
            if !txn.partitions.contains_key(&key) {
                txn.partitions.insert(key, PartitionTxnRecord {
                    topic: msg.topic.clone(),
                    partition: msg.partition,
                    first_offset: 0,
                    last_offset: 0,
                    status: TxnPartitionStatus::Pending,
                });
            }
            txn.pending_messages.push(msg);
        }

        Ok(())
    }

    pub fn prepare_commit(&self, txn_id: &str) -> anyhow::Result<()> {
        let mut txns = self.transactions.write();
        let txn = txns.get_mut(txn_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;

        if !self.state_machine.can_transition(txn.state, TransactionState::PrepareCommit) {
            return Err(anyhow::anyhow!("Cannot prepare commit from state {}", txn.state));
        }

        txn.state = TransactionState::PrepareCommit;
        Ok(())
    }

    pub fn commit(&self, txn_id: &str) -> anyhow::Result<()> {
        let mut txns = self.transactions.write();
        let txn = txns.get_mut(txn_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;

        if !self.state_machine.can_transition(txn.state, TransactionState::CompleteCommit) {
            if txn.state == TransactionState::CompleteAbort {
                return Err(anyhow::anyhow!("Transaction already aborted"));
            }
            return Err(anyhow::anyhow!("Cannot commit from state {}", txn.state));
        }

        for msg in &txn.pending_messages {
            if let Err(e) = self.store.append(msg) {
                txn.state = TransactionState::PrepareAbort;
                txn.abort_reason = Some(format!("Failed to append message: {}", e));
                return self.abort_internal(txn);
            }
        }

        for record in txn.partitions.values_mut() {
            record.status = TxnPartitionStatus::Committed;
        }

        txn.state = TransactionState::CompleteCommit;
        txn.commit_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        self.time_index.lock().remove(txn_id);
        self.producer_txns.write().remove(&txn.producer_id);

        Ok(())
    }

    pub fn abort(&self, txn_id: &str, reason: &str) -> anyhow::Result<()> {
        let mut txns = self.transactions.write();
        let txn = txns.get_mut(txn_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?;

        if txn.abort_reason.is_none() {
            txn.abort_reason = Some(reason.to_string());
        }

        self.abort_internal(txn)
    }

    fn abort_internal(&self, txn: &mut Transaction) -> anyhow::Result<()> {
        if !self.state_machine.can_transition(txn.state, TransactionState::CompleteAbort) {
            if txn.state == TransactionState::CompleteCommit {
                return Err(anyhow::anyhow!("Transaction already committed"));
            }
            return Err(anyhow::anyhow!("Cannot abort from state {}", txn.state));
        }

        txn.state = TransactionState::CompleteAbort;
        if txn.abort_reason.is_none() {
            txn.abort_reason = Some("User requested".to_string());
        }

        for record in txn.partitions.values_mut() {
            record.status = TxnPartitionStatus::Aborted;
        }

        txn.pending_messages.clear();

        self.time_index.lock().remove(&txn.txn_id);
        self.producer_txns.write().remove(&txn.producer_id);

        Ok(())
    }

    pub fn get_transaction_state(&self, txn_id: &str) -> Option<TransactionState> {
        self.transactions.read().get(txn_id).map(|t| t.state)
    }

    pub fn get_transaction(&self, txn_id: &str) -> Option<Transaction> {
        self.transactions.read().get(txn_id).cloned()
    }

    pub fn list_transactions(&self) -> Vec<Transaction> {
        self.transactions.read().values().cloned().collect()
    }

    pub fn check_expired_transactions(&self) {
        if !self.config.abort_timed_out_txn {
            return;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let expired = self.time_index.lock().get_expired(now);

        for txn_id in expired {
            if let Some(txn) = self.transactions.write().get_mut(&txn_id) {
                if txn.state == TransactionState::Ongoing {
                    txn.abort_reason = Some("Transaction timeout".to_string());
                    let _ = self.abort_internal(txn);
                }
            }
        }
    }

    pub fn close(&self) {
        *self.running.lock() = false;
    }
}

struct TransactionTimeIndex {
    by_time: HashMap<i64, Vec<String>>,
    by_txn_id: HashMap<String, i64>,
}

impl TransactionTimeIndex {
    fn new() -> Self {
        Self {
            by_time: HashMap::new(),
            by_txn_id: HashMap::new(),
        }
    }

    fn add(&mut self, txn_id: &str, expire_time: i64) {
        self.by_time.entry(expire_time).or_default().push(txn_id.to_string());
        self.by_txn_id.insert(txn_id.to_string(), expire_time);
    }

    fn remove(&mut self, txn_id: &str) {
        if let Some(expire_time) = self.by_txn_id.remove(txn_id) {
            if let Some(txns) = self.by_time.get_mut(&expire_time) {
                txns.retain(|id| id != txn_id);
                if txns.is_empty() {
                    self.by_time.remove(&expire_time);
                }
            }
        }
    }

    fn get_expired(&self, before: i64) -> Vec<String> {
        self.by_time
            .iter()
            .filter(|(t, _)| **t < before)
            .flat_map(|(_, txns)| txns.clone())
            .collect()
    }
}

struct TransactionStateMachine {
    transitions: HashMap<TransactionState, Vec<TransactionState>>,
}

impl TransactionStateMachine {
    fn new() -> Self {
        let mut transitions: HashMap<TransactionState, Vec<TransactionState>> = HashMap::new();
        
        transitions.insert(TransactionState::Empty, vec![TransactionState::Ongoing]);
        transitions.insert(TransactionState::Ongoing, vec![
            TransactionState::PrepareCommit,
            TransactionState::PrepareAbort,
            TransactionState::Dead,
        ]);
        transitions.insert(TransactionState::PrepareCommit, vec![
            TransactionState::CompleteCommit,
            TransactionState::PrepareAbort,
        ]);
        transitions.insert(TransactionState::PrepareAbort, vec![TransactionState::CompleteAbort]);
        transitions.insert(TransactionState::CompleteCommit, vec![]);
        transitions.insert(TransactionState::CompleteAbort, vec![]);
        transitions.insert(TransactionState::Dead, vec![]);

        Self { transitions }
    }

    fn can_transition(&self, from: TransactionState, to: TransactionState) -> bool {
        self.transitions.get(&from).map(|allowed| allowed.contains(&to)).unwrap_or(false)
    }
}

fn generate_txn_id(producer_id: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("txn-{}-{:x}", producer_id, timestamp)
}

fn partition_key(topic: &str, partition: i32) -> String {
    format!("{}-{}", topic, partition)
}

pub struct TransactionMarkerManager {
    store: Arc<SegmentStore>,
}

impl TransactionMarkerManager {
    pub fn new(store: Arc<SegmentStore>) -> Self {
        Self { store }
    }

    pub fn write_commit_marker(&self, txn_id: &str, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        let marker = Message {
            message_id: uuid::Uuid::new_v4().to_string(),
            topic: topic.to_string(),
            partition,
            offset,
            value: txn_id.as_bytes().to_vec(),
            headers: HashMap::from([
                ("__txn_marker".to_string(), "commit".to_string()),
                ("__txn_id".to_string(), txn_id.to_string()),
                ("__marker_offset".to_string(), offset.to_string()),
            ]),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            ..Default::default()
        };

        self.store.append(&marker)?;
        Ok(())
    }

    pub fn write_abort_marker(&self, txn_id: &str, topic: &str, partition: i32, offset: i64) -> anyhow::Result<()> {
        let marker = Message {
            message_id: uuid::Uuid::new_v4().to_string(),
            topic: topic.to_string(),
            partition,
            offset,
            value: txn_id.as_bytes().to_vec(),
            headers: HashMap::from([
                ("__txn_marker".to_string(), "abort".to_string()),
                ("__txn_id".to_string(), txn_id.to_string()),
                ("__marker_offset".to_string(), offset.to_string()),
            ]),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            ..Default::default()
        };

        self.store.append(&marker)?;
        Ok(())
    }

    pub fn is_transaction_marker(msg: &Message) -> bool {
        msg.headers.contains_key("__txn_marker")
    }

    pub fn parse_marker(msg: &Message) -> Option<(String, bool)> {
        let marker_type = msg.headers.get("__txn_marker")?;
        let txn_id = msg.headers.get("__txn_id")?.clone();
        let is_commit = marker_type == "commit";
        Some((txn_id, is_commit))
    }
}
