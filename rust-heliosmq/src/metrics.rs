use prometheus::{Counter, Gauge, Histogram, Registry};
use prometheus::core::{AtomicF64, AtomicU64};

lazy_static::lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}

pub struct Metrics {
    pub registry: Registry,
    
    pub produce_total: Counter,
    pub produce_bytes: Counter,
    pub produce_latency: Histogram,
    
    pub consume_total: Counter,
    pub consume_bytes: Counter,
    pub fetch_latency: Histogram,
    
    pub consumer_lag: Gauge,
    pub backlog_bytes: Gauge,
    
    pub connections_total: Gauge,
    pub requests_total: Counter,
    pub request_errors: Counter,
    
    pub storage_usage_bytes: Gauge,
    pub segment_count: Gauge,
    
    pub delay_scheduled: Counter,
    pub dlq_messages: Counter,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let produce_total = Counter::new("heliosmq_produce_total", "Total messages produced").unwrap();
        let produce_bytes = Counter::new("heliosmq_produce_bytes", "Total bytes produced").unwrap();
        let produce_latency = Histogram::with_opts(
            prometheus::HistogramOpts::new("heliosmq_produce_latency_ms", "Produce latency in milliseconds")
                .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0])
        ).unwrap();

        let consume_total = Counter::new("heliosmq_consume_total", "Total messages consumed").unwrap();
        let consume_bytes = Counter::new("heliosmq_consume_bytes", "Total bytes consumed").unwrap();
        let fetch_latency = Histogram::with_opts(
            prometheus::HistogramOpts::new("heliosmq_fetch_latency_ms", "Fetch latency in milliseconds")
                .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0])
        ).unwrap();

        let consumer_lag = Gauge::new("heliosmq_consumer_lag", "Consumer lag in messages").unwrap();
        let backlog_bytes = Gauge::new("heliosmq_backlog_bytes", "Backlog size in bytes").unwrap();

        let connections_total = Gauge::new("heliosmq_connections_total", "Total active connections").unwrap();
        let requests_total = Counter::new("heliosmq_requests_total", "Total requests processed").unwrap();
        let request_errors = Counter::new("heliosmq_request_errors", "Total request errors").unwrap();

        let storage_usage_bytes = Gauge::new("heliosmq_storage_usage_bytes", "Storage usage in bytes").unwrap();
        let segment_count = Gauge::new("heliosmq_segment_count", "Number of segments").unwrap();

        let delay_scheduled = Counter::new("heliosmq_delay_scheduled", "Total delayed messages scheduled").unwrap();
        let dlq_messages = Counter::new("heliosmq_dlq_messages", "Total messages in DLQ").unwrap();

        registry.register(Box::new(produce_total.clone())).unwrap();
        registry.register(Box::new(produce_bytes.clone())).unwrap();
        registry.register(Box::new(produce_latency.clone())).unwrap();
        registry.register(Box::new(consume_total.clone())).unwrap();
        registry.register(Box::new(consume_bytes.clone())).unwrap();
        registry.register(Box::new(fetch_latency.clone())).unwrap();
        registry.register(Box::new(consumer_lag.clone())).unwrap();
        registry.register(Box::new(backlog_bytes.clone())).unwrap();
        registry.register(Box::new(connections_total.clone())).unwrap();
        registry.register(Box::new(requests_total.clone())).unwrap();
        registry.register(Box::new(request_errors.clone())).unwrap();
        registry.register(Box::new(storage_usage_bytes.clone())).unwrap();
        registry.register(Box::new(segment_count.clone())).unwrap();
        registry.register(Box::new(delay_scheduled.clone())).unwrap();
        registry.register(Box::new(dlq_messages.clone())).unwrap();

        Self {
            registry,
            produce_total,
            produce_bytes,
            produce_latency,
            consume_total,
            consume_bytes,
            fetch_latency,
            consumer_lag,
            backlog_bytes,
            connections_total,
            requests_total,
            request_errors,
            storage_usage_bytes,
            segment_count,
            delay_scheduled,
            dlq_messages,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
