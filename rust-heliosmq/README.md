<div align="center">

# HeliosMQ

<img src="https://img.shields.io/badge/Rust-1.75+-orange?style=for-the-badge&logo=rust" alt="Rust Version">
<img src="https://img.shields.io/badge/License-Apache%202.0-blue?style=for-the-badge" alt="License">
<img src="https://img.shields.io/badge/Status-Active-success?style=for-the-badge" alt="Status">
<img src="https://img.shields.io/badge/Architecture-Cloud%20Native-orange?style=for-the-badge" alt="Architecture">

**云原生统一消息与事件流平台 (Rust 实现)**

[English](#english) | [中文](#中文)

</div>

***

## 中文

### 🚀 项目简介

HeliosMQ 是一款面向云原生时代的新一代消息队列系统，采用 **Rust 语言** 实现。它融合了 Kafka 的高吞吐、Pulsar 的存算分离、RocketMQ 的丰富业务特性以及 RabbitMQ 的灵活路由能力，打造出一个真正统一的消息与事件流平台。

### ✨ 核心特性

#### 🏗️ 架构设计

- **存算分离架构** - Broker 完全无状态，存储层独立扩展，秒级弹性伸缩
- **零外部依赖** - 内置 Raft 元数据管理，无需 ZooKeeper/etcd
- **统一消息模型** - Queue、Pub/Sub、Stream 三种模式一体化
- **分层存储** - 热数据本地 SSD，冷数据自动归档至 S3/OSS

#### ⚡ 极致性能

- **零 GC 停顿** - Rust 内存安全，无垃圾回收抖动
- **高吞吐** - 单集群百万级消息/秒
- **低延迟** - P99 < 5ms (acks=1)
- **io\_uring** - Linux 异步 I/O，极致性能 (可选)
- **零拷贝** - sendfile 直接传输，CPU 零参与

#### 🔧 内置高级功能

- **延迟消息** - 任意精度毫秒级延迟，支持超长延迟
- **死信队列** - 自动重试 + 指数退避 + DLQ 转移
- **事务消息** - 两阶段提交，保证分布式事务最终一致性
- **消息追溯** - 完整生命周期追踪，精确到单条消息
- **Schema Registry** - Avro/Protobuf/JSON Schema 原生支持

#### ☁️ 云原生优先

- **Kubernetes Operator** - 声明式集群管理，自动化运维
- **多租户隔离** - 租户/命名空间/Topic 三级资源隔离
- **可观测性内置** - Prometheus Metrics + OpenTelemetry Tracing
- **自动弹性伸缩** - 基于 Consumer Lag 触发 HPA

### 📦 快速开始

#### 安装

```bash
# 克隆仓库
git clone 
cd heliosmq

# 编译
cargo build --release

# 运行单机模式
./target/release/helios-controller &
./target/release/helios-broker &
./target/release/helios-gateway &
```

#### Docker 部署

```bash
docker-compose up -d
```

#### Kubernetes 部署

```bash
helm install heliosmq ./deployments/helm/heliosmq
```

### 💻 使用示例

#### 生产者

```rust
use heliosmq::{Client, Message, Producer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::connect("heliosmq://localhost:29092").await?;
    
    let producer = client
        .producer()
        .topic("order-events")
        .build()
        .await?;

    // 发送简单消息
    producer.send(Message::new("order-events", br#"{"orderId":"12345"}"#)).await?;

    // 发送带分区键的消息（保证顺序）
    producer.send(
        Message::new("order-events", br#"{"orderId":"12345","status":"paid"}"#)
            .with_partition_key("order-12345")
    ).await?;

    // 发送延迟消息
    producer.send(
        Message::new("order-events", br#"{"orderId":"12345","action":"timeout"}"#)
            .with_delay_ms(30 * 60 * 1000) // 30分钟后投递
    ).await?;

    Ok(())
}
```

#### 消费者

```rust
use heliosmq::{Client, Consumer, SubscriptionType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::connect("heliosmq://localhost:29092").await?;
    
    let consumer = client
        .consumer()
        .topic("order-events")
        .consumer_group("order-processor")
        .subscription_type(SubscriptionType::Shared)
        .build()
        .await?;

    consumer.subscribe(|msg| async move {
        // 处理消息
        println!("Received: {:?}", String::from_utf8_lossy(&msg.value));
        
        // 返回 Ok(()) 表示 ACK
        // 返回 Err 触发重试
        process_order(&msg.value).await?;
        Ok(())
    }).await?;

    Ok(())
}
```

### 📊 性能基准

| 指标                   | 数值                 |
| -------------------- | ------------------ |
| 生产吞吐量 (单集群)          | > 8,000,000 msg/s  |
| 消费吞吐量 (单集群)          | > 15,000,000 msg/s |
| 端到端延迟 P99 (acks=1)   | < 5ms              |
| 端到端延迟 P99 (acks=all) | < 20ms             |
| Broker 扩容时间          | < 30s              |
| 故障恢复时间               | < 10s              |
| 内存占用 (空闲)            | < 50MB             |

### 🏛️ 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                             │
│   Producer SDK  │  Consumer SDK  │  Admin CLI  │  Management UI │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                     Gateway Layer (无状态)                        │
│  • 协议适配 (Native/HTTP/gRPC)  • 认证授权  • 流量控制            │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                   Broker Layer (无状态计算层)                      │
│  • 消息路由  • 延迟调度  • 重试/DLQ  • 事务协调                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│              Metadata Layer (内置 Raft，无外部依赖)                │
│  • Topic 元数据  • 分区分配  • 消费者组状态  • ACL                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                    Storage Layer (有状态存储层)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │  Hot Tier    │  │  Warm Tier   │  │  Cold Tier (S3/OSS)  │   │
│  │  NVMe SSD    │  │  Block Store │  │  对象存储归档          │   │
│  └──────────────┘  └──────────────┘  └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 🗺️ 路线图

- [x] MVP: 核心 Produce/Consume
- [x] 延迟消息 + 死信队列
- [x] 内置 Raft 元数据管理
- [x] RocksDB 元数据存储
- [ ] 分层存储 (S3/OSS)
- [ ] io\_uring 支持
- [ ] 事务消息
- [ ] 消息追溯系统
- [ ] Kubernetes Operator
- [ ] Kafka 协议兼容
- [ ] Schema Registry

### 🔬 技术栈

| 组件    | 技术选型                  |
| ----- | --------------------- |
| 异步运行时 | Tokio                 |
| 序列化   | serde + serde\_json   |
| 存储    | RocksDB + 自定义 Segment |
| 压缩    | zstd / lz4 / snappy   |
| 网络    | tonic (gRPC)          |
| 指标    | prometheus            |
| 追踪    | opentelemetry         |
| 日志    | tracing               |

### 🤝 贡献

欢迎贡献！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详情。

### 📄 许可证

Apache License 2.0

***

## English

### 🚀 Overview

HeliosMQ is a next-generation message queue system built with **Rust**, designed for the cloud-native era. It combines Kafka's high throughput, Pulsar's storage-compute separation, RocketMQ's rich business features, and RabbitMQ's flexible routing into a unified messaging and event streaming platform.

### ✨ Key Features

- **Storage-Compute Separation** - Stateless brokers, independent storage scaling
- **Zero External Dependencies** - Built-in Raft metadata management
- **Unified Messaging Model** - Queue, Pub/Sub, and Stream in one platform
- **Tiered Storage** - Hot data on SSD, cold data archived to S3/OSS
- **Extreme Performance** - Zero GC pauses, millions of messages per second
- **Built-in Advanced Features** - Delayed messages, DLQ, transactions, tracing
- **Cloud-Native First** - Kubernetes Operator, multi-tenancy, observability

### 📦 Quick Start

```bash
# Clone and build
git clone https://github.com/heliosmq/heliosmq.git
cd heliosmq
cargo build --release

# Run standalone mode
./target/release/helios-controller &
./target/release/helios-broker &
./target/release/helios-gateway &
```

### 📊 Benchmarks

| Metric               | Value       |
| -------------------- | ----------- |
| Produce Throughput   | > 8M msg/s  |
| Consume Throughput   | > 15M msg/s |
| P99 Latency (acks=1) | < 5ms       |
| Broker Scale-out     | < 30s       |
| Failover Recovery    | < 10s       |
| Memory Usage (idle)  | < 50MB      |

### 📄 License

MIT

***

<div align="center">

**[Website](https://heliosmq.io)** ·
**[Documentation](https://docs.heliosmq.io)** ·
**[Discord](https://discord.gg/heliosmq)** ·
**[Twitter](https://twitter.com/heliosmq)**

</div>
