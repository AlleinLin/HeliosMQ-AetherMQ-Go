<div align="center">

# AetherMQ / HeliosMQ

<img src="https://img.shields.io/badge/Go-AetherMQ-00ADD8?style=for-the-badge&logo=go" alt="Go Version">
<img src="https://img.shields.io/badge/Rust-HeliosMQ-orange?style=for-the-badge&logo=rust" alt="Rust Version">
<img src="https://img.shields.io/badge/License-Apache%202.0-blue?style=for-the-badge" alt="License">
<img src="https://img.shields.io/badge/Architecture-Cloud%20Native-orange?style=for-the-badge" alt="Architecture">

**云原生统一消息与事件流平台**

[English](#english) | [中文](#中文)

</div>

***

## 中文

### 🚀 项目简介

本项目是一个完整的、生产级的下一代消息队列系统，提供 **Go (AetherMQ)** 和 **Rust (HeliosMQ)** 两种实现。它融合了 Kafka 的高吞吐、Pulsar 的存算分离、RocketMQ 的丰富业务特性以及 RabbitMQ 的灵活路由能力，打造出一个真正统一的消息与事件流平台。

### 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client Layer                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │
│  │ Producer SDK │  │ Consumer SDK │  │  Admin CLI   │  │  Management UI   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────────┘ │
│         │                 │                  │                  │            │
│         └─────────────────┴──────────────────┴──────────────────┘            │
│                                    │                                          │
└────────────────────────────────────┼──────────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼──────────────────────────────────────────┐
│                           Gateway Layer (无状态)                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ • 协议适配 (Native/HTTP/gRPC/Kafka)  • 认证授权  • 流量控制  • 限流      │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────┬──────────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼──────────────────────────────────────────┐
│                         Broker Layer (无状态计算层)                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │  消息路由    │  │  延迟调度    │  │  重试/DLQ    │  │   事务协调       │   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────────┘   │
└────────────────────────────────────┬──────────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼──────────────────────────────────────────┐
│                    Metadata Layer (内置 Raft，无外部依赖)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │ Topic 元数据 │  │  分区分配    │  │ 消费者组状态 │  │      ACL         │   │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────────┘   │
└────────────────────────────────────┬──────────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼──────────────────────────────────────────┐
│                          Storage Layer (有状态存储层)                          │
│  ┌────────────────────────────────────────────────────────────────────────┐   │
│  │                         分段存储引擎                                    │   │
│  │  • 顺序写入优化  • 稀疏索引  • 零拷贝传输  • 批量压缩                   │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────────────────┐ │
│  │  Hot Tier    │  │  Warm Tier   │  │         Cold Tier (S3/OSS)           │ │
│  │  NVMe SSD    │  │  Block Store │  │         对象存储归档                  │ │
│  └──────────────┘  └──────────────┘  └──────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────────┘
```

### ✨ 核心特性

#### 🏗️ 架构特性

| 特性         | 描述                              |
| ---------- | ------------------------------- |
| **存算分离**   | Broker 完全无状态，存储层独立扩展，秒级弹性伸缩     |
| **零外部依赖**  | 内置 Raft 元数据管理，无需 ZooKeeper/etcd |
| **统一消息模型** | Queue、Pub/Sub、Stream 三种模式一体化    |
| **分层存储**   | 热数据本地 SSD，冷数据自动归档至 S3/OSS       |

#### ⚡ 性能特性

| 特性       | 描述                             |
| -------- | ------------------------------ |
| **高吞吐**  | 单集群百万级消息/秒 (Go) / 千万级 (Rust)   |
| **低延迟**  | P99 < 10ms (Go) / < 5ms (Rust) |
| **零拷贝**  | sendfile 直接传输，CPU 零参与          |
| **批量聚合** | Group Commit 减少磁盘 IO           |

#### 🔧 业务特性

| 特性                  | 描述                             |
| ------------------- | ------------------------------ |
| **延迟消息**            | 任意精度毫秒级延迟，支持超长延迟               |
| **死信队列**            | 自动重试 + 指数退避 + DLQ 转移           |
| **事务消息**            | 两阶段提交，保证分布式事务最终一致性             |
| **消息追溯**            | 完整生命周期追踪，精确到单条消息               |
| **Schema Registry** | Avro/Protobuf/JSON Schema 原生支持 |

#### ☁️ 云原生特性

| 特性                      | 描述                                         |
| ----------------------- | ------------------------------------------ |
| **Kubernetes Operator** | 声明式集群管理，自动化运维                              |
| **多租户隔离**               | 租户/命名空间/Topic 三级资源隔离                       |
| **可观测性内置**              | Prometheus Metrics + OpenTelemetry Tracing |
| **Kafka 协议兼容**          | 无缝迁移现有 Kafka 应用                            |

### 📁 项目结构

```
消息队列/
├── go-aethermq/                    # Go 版本实现
│   ├── cmd/                        # 命令行入口
│   │   ├── broker/                 # Broker 服务
│   │   ├── controller/             # Controller 服务
│   │   └── gateway/                # Gateway 服务
│   ├── internal/                   # 内部模块
│   │   ├── broker/                 # Broker 核心逻辑
│   │   ├── controller/             # 控制器逻辑
│   │   ├── gateway/                # 网关逻辑
│   │   ├── storage/                # 分段存储
│   │   ├── metadata/               # 元数据管理
│   │   ├── tiered/                 # 分层存储
│   │   ├── transaction/            # 事务消息
│   │   ├── tracing/                # 消息追溯
│   │   ├── kafka/                  # Kafka 协议
│   │   ├── schema/                 # Schema Registry
│   │   ├── raft/                   # Raft 共识
│   │   ├── delay/                  # 延迟队列
│   │   └── dlq/                    # 死信队列
│   ├── go.mod
│   └── README.md
│
├── rust-heliosmq/                  # Rust 版本实现
│   ├── src/
│   │   ├── bin/                    # 可执行文件
│   │   ├── broker.rs               # Broker 核心
│   │   ├── controller.rs           # 控制器
│   │   ├── gateway.rs              # 网关
│   │   ├── storage.rs              # 分段存储
│   │   ├── metadata.rs             # 元数据管理
│   │   ├── tiered.rs               # 分层存储
│   │   ├── transaction.rs          # 事务消息
│   │   ├── tracing.rs              # 消息追溯
│   │   ├── kafka.rs                # Kafka 协议
│   │   ├── schema.rs               # Schema Registry
│   │   ├── raft.rs                 # Raft 共识
│   │   ├── delay.rs                # 延迟队列
│   │   ├── dlq.rs                  # 死信队列
│   │   └── metrics.rs              # Prometheus 指标
│   ├── Cargo.toml
│   └── README.md
│
├── operator/                       # Kubernetes Operator
│   ├── api/v1/                     # CRD 定义
│   ├── internal/controller/        # 控制器逻辑
│   ├── main.go
│   └── go.mod
│
├── docs/                           # 设计文档
│   ├── NovaMQ：下一代云原生极速消息队列详细设计方案.md
│   ├── 现代消息队列系统：HeliosMQ 详细设计方案.md
│   ├── 新一代近乎完美的消息队列设计方案.md
│   └── ...                         # 其他调研报告
│
└── README.md                       # 本文件
```

### 📦 快速开始

#### Go 版本 (AetherMQ)

```bash
cd go-aethermq
go mod tidy
go build ./cmd/...

# 启动集群
./controller &
./broker &
./gateway &
```

#### Rust 版本 (HeliosMQ)

```bash
cd rust-heliosmq
cargo build --release

# 启动集群
./target/release/helios-controller &
./target/release/helios-broker &
./target/release/helios-gateway &
```

#### Kubernetes 部署

```bash
cd operator
make deploy

# 创建集群
kubectl apply -f - <<EOF
apiVersion: aethermq.io/v1
kind: AetherMQCluster
metadata:
  name: production
spec:
  controller:
    replicas: 3
  broker:
    replicas: 5
  gateway:
    replicas: 3
  storage:
    size: 500Gi
    storageClass: fast-ssd
EOF
```

### 💻 使用示例

#### Go 版本

```go
package main

import (
    "context"
    "github.com/aethermq/aethermq/pkg/client"
)

func main() {
    // 创建生产者
    producer, _ := client.NewProducer(client.ProducerConfig{
        Brokers: []string{"localhost:29092"},
    })
    defer producer.Close()

    // 发送消息
    producer.Send(context.Background(), &client.Message{
        Topic:   "orders",
        Payload: []byte(`{"orderId":"12345"}`),
    })

    // 发送延迟消息
    producer.Send(context.Background(), &client.Message{
        Topic:        "orders",
        Payload:      []byte(`{"orderId":"12345","action":"timeout"}`),
        DeliverAfter: 30 * time.Minute,
    })
}
```

#### Rust 版本

```rust
use heliosmq::{Client, Message};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::connect("heliosmq://localhost:29092").await?;
    
    let producer = client.producer()
        .topic("orders")
        .build()
        .await?;

    producer.send(Message::new("orders", br#"{"orderId":"12345"}"#)).await?;
    
    Ok(())
}
```

### 📊 功能清单

| 功能模块                | Go (AetherMQ) | Rust (HeliosMQ) | 状态 |
| ------------------- | ------------- | --------------- | -- |
| 核心 Produce/Consume  | ✅             | ✅               | 完成 |
| 分段存储                | ✅             | ✅               | 完成 |
| 延迟消息                | ✅             | ✅               | 完成 |
| 死信队列 (DLQ)          | ✅             | ✅               | 完成 |
| 消费者组                | ✅             | ✅               | 完成 |
| 内置 Raft             | ✅             | ✅               | 完成 |
| 分层存储 (S3/OSS)       | ✅             | ✅               | 完成 |
| 事务消息                | ✅             | ✅               | 完成 |
| 消息追溯系统              | ✅             | ✅               | 完成 |
| Kafka 协议兼容          | ✅             | ✅               | 完成 |
| Schema Registry     | ✅             | ✅               | 完成 |
| Kubernetes Operator | ✅             | -               | 完成 |
| Prometheus 指标       | ✅             | ✅               | 完成 |
| OpenTelemetry 追踪    | ✅             | ✅               | 完成 |

### 🔧 配置示例

#### Broker 配置

```yaml
node_id: broker-1
listen_addr: "0.0.0.0:9092"
data_dir: "/data/aethermq"
controller_addr: "controller:19091"

segment_size: 1073741824  # 1GB
flush_interval_ms: 10
replication_factor: 3
max_connections: 10000
compression: zstd

tiered_storage:
  enabled: true
  provider: s3
  bucket: aethermq-cold-storage
  region: us-east-1
  cold_tier_days: 7
```

#### Topic 配置

```yaml
name: orders
partitions: 12
replication_factor: 3
retention_ms: 604800000  # 7天
segment_bytes: 1073741824
compression: zstd
cleanup_policy: delete
min_in_sync_replicas: 2
enable_delay: true
max_retry: 16
dlq_enabled: true
```

### 🗺️ 技术栈

| 组件    | Go 版本                | Rust 版本                 |
| ----- | -------------------- | ----------------------- |
| 异步运行时 | Goroutine            | Tokio                   |
| 序列化   | encoding/json + yaml | serde                   |
| 存储    | BoltDB + 自定义 Segment | RocksDB + 自定义 Segment   |
| 压缩    | zstd / lz4 / snappy  | zstd / lz4 / snap       |
| 网络    | gRPC (tonic)         | tonic                   |
| 指标    | Prometheus           | prometheus              |
| 追踪    | OpenTelemetry        | tracing + opentelemetry |

### 📄 许可证

MIT

***

## English

### 🚀 Overview

This project is a complete, production-grade next-generation message queue system with implementations in both **Go (AetherMQ)** and **Rust (HeliosMQ)**. It combines Kafka's high throughput, Pulsar's storage-compute separation, RocketMQ's rich business features, and RabbitMQ's flexible routing into a unified messaging and event streaming platform.

### ✨ Core Features

- **Storage-Compute Separation** - Stateless brokers, independent storage scaling
- **Zero External Dependencies** - Built-in Raft metadata management
- **Unified Messaging Model** - Queue, Pub/Sub, and Stream in one platform
- **Tiered Storage** - Hot data on SSD, cold data archived to S3/OSS
- **High Performance** - Millions (Go) to tens of millions (Rust) of messages per second
- **Built-in Advanced Features** - Delayed messages, DLQ, transactions, tracing, schema registry
- **Cloud-Native First** - Kubernetes Operator, multi-tenancy, observability
- **Kafka Protocol Compatible** - Seamless migration from existing Kafka applications

### 📦 Quick Start

#### Go Version (AetherMQ)

```bash
cd go-aethermq
go mod tidy
go build ./cmd/...
./controller & ./broker & ./gateway &
```

#### Rust Version (HeliosMQ)

```bash
cd rust-heliosmq
cargo build --release
./target/release/helios-controller &
./target/release/helios-broker &
./target/release/helios-gateway &
```

### 📄 License

MIT

***

<div align="center">

**[Website](https://aethermq.io)** ·
**[Documentation](https://docs.aethermq.io)** ·
**[Discord](https://discord.gg/aethermq)** ·
**[Twitter](https://twitter.com/aethermq)**

</div>
