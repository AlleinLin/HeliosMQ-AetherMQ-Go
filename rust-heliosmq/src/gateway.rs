use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use tracing::{info, warn, error, instrument};

use crate::config::GatewayConfig;
use crate::common::Message;

pub struct Gateway {
    config: Arc<GatewayConfig>,
    broker_pool: RwLock<BrokerPool>,
    router: RwLock<Router>,
    rate_limiter: Mutex<RateLimiter>,
}

struct BrokerPool {
    brokers: HashMap<String, BrokerConn>,
}

struct BrokerConn {
    broker_id: String,
    address: String,
    healthy: bool,
    last_heartbeat: Instant,
}

struct Router {
    topic_routes: HashMap<String, TopicRoute>,
}

struct TopicRoute {
    topic: String,
    partitions: HashMap<i32, PartitionRoute>,
    version: i64,
}

struct PartitionRoute {
    partition: i32,
    leader: String,
    replicas: Vec<String>,
}

struct RateLimiter {
    limits: HashMap<String, TokenBucket>,
}

struct TokenBucket {
    tokens: f64,
    max_tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config: Arc::new(config),
            broker_pool: RwLock::new(BrokerPool {
                brokers: HashMap::new(),
            }),
            router: RwLock::new(Router {
                topic_routes: HashMap::new(),
            }),
            rate_limiter: Mutex::new(RateLimiter {
                limits: HashMap::new(),
            }),
        }
    }

    pub async fn start(&self) -> anyhow::Result<GatewayHandle> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

        tokio::spawn({
            let gateway = self.clone_ref();
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            gateway.health_check();
                        }
                        _ = &mut shutdown_rx => {
                            info!("Gateway shutting down");
                            break;
                        }
                    }
                }
            }
        });

        Ok(GatewayHandle {
            shutdown_tx: Some(shutdown_tx),
        })
    }

    fn clone_ref(&self) -> Self {
        Self {
            config: self.config.clone(),
            broker_pool: RwLock::new(BrokerPool {
                brokers: self.broker_pool.read().brokers.clone(),
            }),
            router: RwLock::new(Router {
                topic_routes: self.router.read().topic_routes.clone(),
            }),
            rate_limiter: Mutex::new(RateLimiter {
                limits: self.rate_limiter.lock().limits.clone(),
            }),
        }
    }

    fn health_check(&self) {
        let mut pool = self.broker_pool.write();
        
        for (broker_id, conn) in pool.brokers.iter_mut() {
            let elapsed = Instant::now().duration_since(conn.last_heartbeat);
            if elapsed > Duration::from_secs(30) {
                if conn.healthy {
                    conn.healthy = false;
                    warn!("Broker {} is unhealthy", broker_id);
                }
            }
        }
    }

    #[instrument(skip(self, messages))]
    pub async fn produce(
        &self,
        topic: &str,
        partition_key: Option<&str>,
        messages: Vec<Message>,
    ) -> anyhow::Result<Vec<i64>> {
        if !self.check_rate_limit("produce", topic) {
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        let partition = self.select_partition(topic, partition_key);
        
        let leader = self.get_leader(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("No leader available"))?;

        let broker = self.get_broker(&leader)
            .ok_or_else(|| anyhow::anyhow!("Broker unavailable"))?;

        if !broker.healthy {
            return Err(anyhow::anyhow!("Broker unhealthy"));
        }

        self.forward_produce(&broker.address, topic, partition, messages).await
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
        if !self.check_rate_limit("fetch", topic) {
            return Err(anyhow::anyhow!("Rate limit exceeded"));
        }

        let leader = self.get_leader(topic, partition)
            .ok_or_else(|| anyhow::anyhow!("No leader available"))?;

        let broker = self.get_broker(&leader)
            .ok_or_else(|| anyhow::anyhow!("Broker unavailable"))?;

        if !broker.healthy {
            return Err(anyhow::anyhow!("Broker unhealthy"));
        }

        self.forward_fetch(&broker.address, topic, partition, group_id, offset, max_bytes).await
    }

    fn check_rate_limit(&self, operation: &str, topic: &str) -> bool {
        let key = format!("{}:{}", operation, topic);
        let mut limiter = self.rate_limiter.lock();
        
        limiter.allow(&key, 100000.0)
    }

    fn select_partition(&self, topic: &str, partition_key: Option<&str>) -> i32 {
        0
    }

    fn get_leader(&self, topic: &str, partition: i32) -> Option<String> {
        let router = self.router.read();
        router.topic_routes.get(topic)
            .and_then(|route| route.partitions.get(&partition))
            .map(|pr| pr.leader.clone())
    }

    fn get_broker(&self, broker_id: &str) -> Option<BrokerConn> {
        let pool = self.broker_pool.read();
        pool.brokers.get(broker_id).cloned()
    }

    async fn forward_produce(
        &self,
        broker_addr: &str,
        topic: &str,
        partition: i32,
        messages: Vec<Message>,
    ) -> anyhow::Result<Vec<i64>> {
        Ok(vec![0])
    }

    async fn forward_fetch(
        &self,
        broker_addr: &str,
        topic: &str,
        partition: i32,
        group_id: Option<&str>,
        offset: i64,
        max_bytes: u32,
    ) -> anyhow::Result<(Vec<Message>, i64)> {
        Ok((vec![], 0))
    }

    pub fn add_broker(&self, broker_id: &str, address: &str) {
        let mut pool = self.broker_pool.write();
        pool.brokers.insert(broker_id.to_string(), BrokerConn {
            broker_id: broker_id.to_string(),
            address: address.to_string(),
            healthy: true,
            last_heartbeat: Instant::now(),
        });
    }

    pub fn remove_broker(&self, broker_id: &str) {
        let mut pool = self.broker_pool.write();
        pool.brokers.remove(broker_id);
    }

    pub fn update_route(&self, topic: &str, partitions: HashMap<i32, PartitionRoute>) {
        let mut router = self.router.write();
        router.topic_routes.insert(topic.to_string(), TopicRoute {
            topic: topic.to_string(),
            partitions,
            version: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
        });
    }
}

impl RateLimiter {
    fn allow(&mut self, key: &str, rate: f64) -> bool {
        let bucket = self.limits.entry(key.to_string())
            .or_insert_with(|| TokenBucket {
                tokens: rate,
                max_tokens: rate * 10.0,
                refill_rate: rate,
                last_refill: Instant::now(),
            });

        let now = Instant::now();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens += elapsed * bucket.refill_rate;
        bucket.tokens = bucket.tokens.min(bucket.max_tokens);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct GatewayHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl GatewayHandle {
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}
