use std::sync::Arc;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use crate::common::Message;

pub struct DLQManager {
    queues: RwLock<HashMap<String, DLQueue>>,
}

struct DLQueue {
    topic: String,
    group: String,
    messages: RwLock<Vec<Message>>,
    max_size: usize,
}

#[derive(Debug, Clone)]
pub struct DLQInfo {
    pub key: String,
    pub topic: String,
    pub group: String,
    pub count: usize,
    pub max_size: usize,
}

impl DLQManager {
    pub fn new() -> Self {
        Self {
            queues: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_to_dlq(&self, msg: Message, group: &str, reason: &str) -> anyhow::Result<()> {
        let key = dlq_key(&msg.topic, group);
        
        let mut queues = self.queues.write();
        let queue = queues.entry(key.clone())
            .or_insert_with(|| DLQueue {
                topic: msg.topic.clone(),
                group: group.to_string(),
                messages: RwLock::new(Vec::new()),
                max_size: 10000,
            });

        let mut messages = queue.messages.write();
        
        if messages.len() >= queue.max_size {
            messages.remove(0);
        }

        let mut msg = msg;
        if msg.headers.is_empty() {
            msg.headers = HashMap::new();
        }
        msg.headers.insert("x-dlq-reason".to_string(), reason.to_string());
        msg.headers.insert("x-dlq-original-topic".to_string(), msg.topic.clone());
        msg.headers.insert("x-dlq-timestamp".to_string(), 
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string()
        );

        messages.push(msg);

        Ok(())
    }

    pub fn get_dlq_messages(&self, topic: &str, group: &str, offset: usize, limit: usize) -> Option<(Vec<Message>, usize)> {
        let key = dlq_key(topic, group);
        let queues = self.queues.read();
        
        queues.get(&key).map(|queue| {
            let messages = queue.messages.read();
            let total = messages.len();
            
            if offset >= total {
                return (Vec::new(), total);
            }

            let end = (offset + limit).min(total);
            (messages[offset..end].to_vec(), total)
        })
    }

    pub fn retry_message(&self, topic: &str, group: &str, message_id: &str) -> anyhow::Result<Option<Message>> {
        let key = dlq_key(topic, group);
        let queues = self.queues.read();
        
        if let Some(queue) = queues.get(&key) {
            let mut messages = queue.messages.write();
            if let Some(pos) = messages.iter().position(|m| m.message_id == message_id) {
                let mut msg = messages.remove(pos);
                msg.deliver_at = 0;
                msg.retry_count += 1;
                return Ok(Some(msg));
            }
        }

        Ok(None)
    }

    pub fn list_dlqs(&self) -> Vec<DLQInfo> {
        let queues = self.queues.read();
        queues.iter().map(|(key, queue)| {
            DLQInfo {
                key: key.clone(),
                topic: queue.topic.clone(),
                group: queue.group.clone(),
                count: queue.messages.read().len(),
                max_size: queue.max_size,
            }
        }).collect()
    }
}

fn dlq_key(topic: &str, group: &str) -> String {
    format!("{}:{}", topic, group)
}
