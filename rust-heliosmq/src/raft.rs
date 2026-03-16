use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use tracing::{info, warn, error};

use crate::config::ControllerConfig;

pub struct RaftNode {
    node_id: String,
    address: String,
    state: Mutex<RaftState>,
    leader_id: RwLock<Option<String>>,
    term: Mutex<u64>,
    commit_idx: Mutex<u64>,
    last_applied: Mutex<u64>,
    log: RwLock<Vec<LogEntry>>,
    applied_tx: mpsc::Sender<LogEntry>,
    running: Mutex<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub cmd: Command,
}

#[derive(Debug, Clone)]
pub struct Command {
    pub op: OpType,
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    CreateTopic,
    DeleteTopic,
    RegisterBroker,
    UnregisterBroker,
    UpdatePartition,
    CommitOffset,
}

impl RaftNode {
    pub async fn new(config: &ControllerConfig) -> anyhow::Result<Self> {
        let (applied_tx, mut applied_rx) = mpsc::channel(1000);

        let node = Self {
            node_id: config.node_id.clone(),
            address: config.raft_addr.clone(),
            state: Mutex::new(RaftState::Follower),
            leader_id: RwLock::new(None),
            term: Mutex::new(0),
            commit_idx: Mutex::new(0),
            last_applied: Mutex::new(0),
            log: RwLock::new(Vec::new()),
            applied_tx,
            running: Mutex::new(true),
        };

        Ok(node)
    }

    pub fn is_leader(&self) -> bool {
        *self.state.lock() == RaftState::Leader
    }

    pub fn get_leader_id(&self) -> Option<String> {
        self.leader_id.read().clone()
    }

    pub fn get_term(&self) -> u64 {
        *self.term.lock()
    }

    pub async fn propose(&self, cmd: Command) -> anyhow::Result<()> {
        let mut state = self.state.lock();
        
        if *state != RaftState::Leader {
            return Err(anyhow::anyhow!("Not the leader"));
        }

        let term = *self.term.lock();
        let mut log = self.log.write();
        
        let entry = LogEntry {
            index: log.len() as u64,
            term,
            cmd,
        };

        log.push(entry.clone());
        
        *self.commit_idx.lock() = entry.index;
        *self.last_applied.lock() = entry.index;

        let _ = self.applied_tx.send(entry).await;

        Ok(())
    }

    pub fn close(&self) {
        *self.running.lock() = false;
    }
}
