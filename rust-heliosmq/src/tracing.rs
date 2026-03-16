use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error};

use crate::common::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceEventType {
    Produce,
    Replicate,
    Consume,
    Ack,
    Retry,
    DLQ,
    Expire,
    DelaySchedule,
    DelayDeliver,
    TxnBegin,
    TxnCommit,
    TxnAbort,
}

impl std::fmt::Display for TraceEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraceEventType::Produce => write!(f, "PRODUCE"),
            TraceEventType::Replicate => write!(f, "REPLICATE"),
            TraceEventType::Consume => write!(f, "CONSUME"),
            TraceEventType::Ack => write!(f, "ACK"),
            TraceEventType::Retry => write!(f, "RETRY"),
            TraceEventType::DLQ => write!(f, "DLQ"),
            TraceEventType::Expire => write!(f, "EXPIRE"),
            TraceEventType::DelaySchedule => write!(f, "DELAY_SCHEDULE"),
            TraceEventType::DelayDeliver => write!(f, "DELAY_DELIVER"),
            TraceEventType::TxnBegin => write!(f, "TXN_BEGIN"),
            TraceEventType::TxnCommit => write!(f, "TXN_COMMIT"),
            TraceEventType::TxnAbort => write!(f, "TXN_ABORT"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub event_id: String,
    pub message_id: String,
    pub trace_id: String,
    pub event_type: TraceEventType,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub broker_id: String,
    pub client_id: String,
    pub producer_id: String,
    pub consumer_id: String,
    pub group_id: String,
    pub properties: HashMap<String, String>,
    pub metrics: Option<TraceMetrics>,
    pub parent_event: Option<String>,
    pub children: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceMetrics {
    pub latency_ms: i64,
    pub message_size: i64,
    pub processing_time: i64,
    pub queue_time: i64,
    pub retry_count: i32,
    pub replication_lag: i64,
}

#[derive(Debug, Clone)]
pub struct TraceConfig {
    pub enabled: bool,
    pub sample_rate: f64,
    pub retention_days: i32,
    pub max_events_per_trace: usize,
    pub batch_size: usize,
    pub flush_interval: Duration,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sample_rate: 1.0,
            retention_days: 7,
            max_events_per_trace: 1000,
            batch_size: 1000,
            flush_interval: Duration::from_millis(100),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_id: Option<String>,
    pub sampled: bool,
}

impl TraceContext {
    pub fn new() -> Self {
        Self {
            trace_id: uuid::Uuid::new_v4().to_string(),
            span_id: generate_span_id(),
            parent_id: None,
            sampled: true,
        }
    }

    pub fn child_span(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: generate_span_id(),
            parent_id: Some(self.span_id.clone()),
            sampled: self.sampled,
        }
    }

    pub fn to_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert("trace-id".to_string(), self.trace_id.clone());
        headers.insert("span-id".to_string(), self.span_id.clone());
        if let Some(ref parent_id) = self.parent_id {
            headers.insert("parent-id".to_string(), parent_id.clone());
        }
        headers.insert("sampled".to_string(), self.sampled.to_string());
        headers
    }

    pub fn from_headers(headers: &HashMap<String, String>) -> Self {
        Self {
            trace_id: headers.get("trace-id").cloned().unwrap_or_default(),
            span_id: headers.get("span-id").cloned().unwrap_or_default(),
            parent_id: headers.get("parent-id").cloned(),
            sampled: headers.get("sampled").map(|s| s == "true").unwrap_or(false),
        }
    }
}

pub struct MessageTracer {
    config: TraceConfig,
    store: TraceStore,
    indexer: TraceIndexer,
    sampler: TraceSampler,
    event_buffer: EventBuffer,
    running: Mutex<bool>,
}

impl MessageTracer {
    pub fn new(config: TraceConfig) -> Self {
        Self {
            config,
            store: TraceStore::new(),
            indexer: TraceIndexer::new(),
            sampler: TraceSampler::new(1.0),
            event_buffer: EventBuffer::new(1000),
            running: Mutex::new(true),
        }
    }

    pub fn trace_produce(&self, msg: &Message, broker_id: &str, client_id: &str) -> anyhow::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if !self.sampler.should_sample() {
            return Ok(());
        }

        let trace_ctx = TraceContext::new();
        let event = TraceEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            message_id: msg.message_id.clone(),
            trace_id: trace_ctx.trace_id,
            event_type: TraceEventType::Produce,
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            broker_id: broker_id.to_string(),
            client_id: client_id.to_string(),
            producer_id: msg.producer_id.clone().unwrap_or_default(),
            consumer_id: String::new(),
            group_id: String::new(),
            properties: msg.headers.clone(),
            metrics: Some(TraceMetrics {
                latency_ms: 0,
                message_size: msg.value.len() as i64,
                processing_time: 0,
                queue_time: 0,
                retry_count: 0,
                replication_lag: 0,
            }),
            parent_event: None,
            children: Vec::new(),
        };

        self.record_event(event)
    }

    pub fn trace_consume(&self, msg: &Message, broker_id: &str, client_id: &str, group_id: &str) -> anyhow::Result<()> {
        if !self.config.enabled || msg.trace_id.is_none() {
            return Ok(());
        }

        let event = TraceEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            message_id: msg.message_id.clone(),
            trace_id: msg.trace_id.clone().unwrap_or_default(),
            event_type: TraceEventType::Consume,
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            broker_id: broker_id.to_string(),
            client_id: client_id.to_string(),
            producer_id: String::new(),
            consumer_id: String::new(),
            group_id: group_id.to_string(),
            properties: msg.headers.clone(),
            metrics: None,
            parent_event: None,
            children: Vec::new(),
        };

        self.record_event(event)
    }

    pub fn trace_ack(&self, msg: &Message, client_id: &str, group_id: &str) -> anyhow::Result<()> {
        if !self.config.enabled || msg.trace_id.is_none() {
            return Ok(());
        }

        let event = TraceEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            message_id: msg.message_id.clone(),
            trace_id: msg.trace_id.clone().unwrap_or_default(),
            event_type: TraceEventType::Ack,
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            broker_id: String::new(),
            client_id: client_id.to_string(),
            producer_id: String::new(),
            consumer_id: String::new(),
            group_id: group_id.to_string(),
            properties: HashMap::new(),
            metrics: None,
            parent_event: None,
            children: Vec::new(),
        };

        self.record_event(event)
    }

    pub fn trace_retry(&self, msg: &Message, reason: &str) -> anyhow::Result<()> {
        if !self.config.enabled || msg.trace_id.is_none() {
            return Ok(());
        }

        let mut properties = HashMap::new();
        properties.insert("reason".to_string(), reason.to_string());
        properties.insert("retry_count".to_string(), msg.retry_count.to_string());

        let event = TraceEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            message_id: msg.message_id.clone(),
            trace_id: msg.trace_id.clone().unwrap_or_default(),
            event_type: TraceEventType::Retry,
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            broker_id: String::new(),
            client_id: String::new(),
            producer_id: String::new(),
            consumer_id: String::new(),
            group_id: String::new(),
            properties,
            metrics: Some(TraceMetrics {
                latency_ms: 0,
                message_size: 0,
                processing_time: 0,
                queue_time: 0,
                retry_count: msg.retry_count,
                replication_lag: 0,
            }),
            parent_event: None,
            children: Vec::new(),
        };

        self.record_event(event)
    }

    pub fn trace_dlq(&self, msg: &Message, group_id: &str, reason: &str) -> anyhow::Result<()> {
        if !self.config.enabled || msg.trace_id.is_none() {
            return Ok(());
        }

        let mut properties = HashMap::new();
        properties.insert("reason".to_string(), reason.to_string());

        let event = TraceEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            message_id: msg.message_id.clone(),
            trace_id: msg.trace_id.clone().unwrap_or_default(),
            event_type: TraceEventType::DLQ,
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
            broker_id: String::new(),
            client_id: String::new(),
            producer_id: String::new(),
            consumer_id: String::new(),
            group_id: group_id.to_string(),
            properties,
            metrics: None,
            parent_event: None,
            children: Vec::new(),
        };

        self.record_event(event)
    }

    pub fn trace_delay(&self, msg: &Message, schedule_time: i64, deliver_time: i64) -> anyhow::Result<()> {
        if !self.config.enabled || msg.trace_id.is_none() {
            return Ok(());
        }

        let schedule_event = TraceEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            message_id: msg.message_id.clone(),
            trace_id: msg.trace_id.clone().unwrap_or_default(),
            event_type: TraceEventType::DelaySchedule,
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: 0,
            timestamp: schedule_time,
            broker_id: String::new(),
            client_id: String::new(),
            producer_id: String::new(),
            consumer_id: String::new(),
            group_id: String::new(),
            properties: HashMap::from([("deliver_at".to_string(), deliver_time.to_string())]),
            metrics: None,
            parent_event: None,
            children: Vec::new(),
        };
        self.record_event(schedule_event)?;

        let deliver_event = TraceEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            message_id: msg.message_id.clone(),
            trace_id: msg.trace_id.clone().unwrap_or_default(),
            event_type: TraceEventType::DelayDeliver,
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: 0,
            timestamp: deliver_time,
            broker_id: String::new(),
            client_id: String::new(),
            producer_id: String::new(),
            consumer_id: String::new(),
            group_id: String::new(),
            properties: HashMap::new(),
            metrics: Some(TraceMetrics {
                latency_ms: 0,
                message_size: 0,
                processing_time: 0,
                queue_time: deliver_time - schedule_time,
                retry_count: 0,
                replication_lag: 0,
            }),
            parent_event: None,
            children: Vec::new(),
        };

        self.record_event(deliver_event)
    }

    pub fn get_trace(&self, trace_id: &str) -> Option<Trace> {
        let events = self.store.get_by_trace_id(trace_id);
        if events.is_empty() {
            return None;
        }

        let mut trace = Trace {
            trace_id: trace_id.to_string(),
            events,
            root_event: None,
            duration: 0,
        };

        trace.build_tree();
        Some(trace)
    }

    pub fn get_trace_by_message(&self, message_id: &str) -> Option<Trace> {
        let events = self.store.get_by_message_id(message_id);
        if events.is_empty() {
            return None;
        }

        let trace_id = events.first().map(|e| e.trace_id.clone()).unwrap_or_default();
        let mut trace = Trace {
            trace_id,
            events,
            root_event: None,
            duration: 0,
        };

        trace.build_tree();
        Some(trace)
    }

    fn record_event(&self, event: TraceEvent) -> anyhow::Result<()> {
        self.event_buffer.add(event.clone())?;
        self.indexer.index(&event);
        Ok(())
    }

    pub fn close(&self) {
        *self.running.lock() = false;
    }
}

#[derive(Debug, Clone)]
pub struct Trace {
    pub trace_id: String,
    pub events: Vec<TraceEvent>,
    pub root_event: Option<TraceEvent>,
    pub duration: i64,
}

impl Trace {
    fn build_tree(&mut self) {
        let mut event_map: HashMap<String, TraceEvent> = HashMap::new();
        for event in &self.events {
            event_map.insert(event.event_id.clone(), event.clone());
        }

        for event in &self.events {
            if let Some(ref parent_id) = event.parent_event {
                if let Some(parent) = event_map.get_mut(parent_id) {
                    let mut parent = parent.clone();
                    parent.children.push(event.event_id.clone());
                    event_map.insert(parent_id.clone(), parent);
                }
            } else {
                self.root_event = Some(event.clone());
            }
        }

        if let Some(first) = self.events.first() {
            if let Some(last) = self.events.last() {
                self.duration = last.timestamp - first.timestamp;
            }
        }
    }

    pub fn get_events_by_type(&self, event_type: TraceEventType) -> Vec<&TraceEvent> {
        self.events.iter().filter(|e| e.event_type == event_type).collect()
    }

    pub fn calculate_latency(&self) -> HashMap<TraceEventType, i64> {
        let mut latencies = HashMap::new();
        let mut produce_time: Option<i64> = None;

        for event in &self.events {
            if event.event_type == TraceEventType::Produce {
                produce_time = Some(event.timestamp);
            }
            if let Some(pt) = produce_time {
                latencies.insert(event.event_type, event.timestamp - pt);
            }
        }

        latencies
    }
}

struct TraceStore {
    events: RwLock<Vec<TraceEvent>>,
}

impl TraceStore {
    fn new() -> Self {
        Self {
            events: RwLock::new(Vec::new()),
        }
    }

    fn write(&self, event: &TraceEvent) {
        self.events.write().push(event.clone());
    }

    fn batch_write(&self, events: &[TraceEvent]) {
        self.events.write().extend(events.iter().cloned());
    }

    fn get_by_trace_id(&self, trace_id: &str) -> Vec<TraceEvent> {
        self.events
            .read()
            .iter()
            .filter(|e| e.trace_id == trace_id)
            .cloned()
            .collect()
    }

    fn get_by_message_id(&self, message_id: &str) -> Vec<TraceEvent> {
        self.events
            .read()
            .iter()
            .filter(|e| e.message_id == message_id)
            .cloned()
            .collect()
    }
}

struct TraceIndexer {
    indices: RwLock<HashMap<String, Vec<String>>>,
}

impl TraceIndexer {
    fn new() -> Self {
        Self {
            indices: RwLock::new(HashMap::new()),
        }
    }

    fn index(&self, event: &TraceEvent) {
        let mut indices = self.indices.write();

        let trace_key = format!("trace_id:{}", event.trace_id);
        indices.entry(trace_key).or_default().push(event.event_id.clone());

        let message_key = format!("message_id:{}", event.message_id);
        indices.entry(message_key).or_default().push(event.event_id.clone());

        let topic_key = format!("topic:{}:{}", event.topic, event.partition);
        indices.entry(topic_key).or_default().push(event.event_id.clone());
    }
}

struct TraceSampler {
    rate: f64,
    counter: Mutex<u64>,
}

impl TraceSampler {
    fn new(rate: f64) -> Self {
        Self {
            rate: rate.clamp(0.0, 1.0),
            counter: Mutex::new(0),
        }
    }

    fn should_sample(&self) -> bool {
        if self.rate >= 1.0 {
            return true;
        }
        if self.rate <= 0.0 {
            return false;
        }

        let mut counter = self.counter.lock();
        *counter += 1;
        (*counter % 100) as f64 / 100.0 < self.rate
    }
}

struct EventBuffer {
    events: Mutex<Vec<TraceEvent>>,
    batch_size: usize,
}

impl EventBuffer {
    fn new(batch_size: usize) -> Self {
        Self {
            events: Mutex::new(Vec::with_capacity(batch_size)),
            batch_size,
        }
    }

    fn add(&self, event: TraceEvent) -> anyhow::Result<()> {
        self.events.lock().push(event);
        Ok(())
    }

    fn flush(&self) -> Vec<TraceEvent> {
        let mut events = self.events.lock();
        let drained = events.drain(..).collect();
        events.reserve(self.batch_size);
        drained
    }
}

fn generate_span_id() -> String {
    uuid::Uuid::new_v4().to_string()[..16].to_string()
}
