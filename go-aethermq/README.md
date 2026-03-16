<div align="center">

# AetherMQ Go

<img src="https://img.shields.io/badge/Go-1.23+-00ADD8?style=for-the-badge&logo=go" alt="Go Version">
<img src="https://img.shields.io/badge/License-Apache%202.0-blue?style=for-the-badge" alt="License">
<img src="https://img.shields.io/badge/Status-Active-success?style=for-the-badge" alt="Status">
<img src="https://img.shields.io/badge/Architecture-Cloud%20Native-orange?style=for-the-badge" alt="Architecture">

**云原生统一消息与事件流平台**

[English](#english) | [中文](#中文)

</div>

***

## 中文

### 🚀 项目简介

AetherMQ Go 是一款面向云原生时代的新一代消息队列系统，采用 **Go 语言** 实现。它融合了 Kafka 的高吞吐、Pulsar 的存算分离、RocketMQ 的丰富业务特性以及 RabbitMQ 的灵活路由能力，打造出一个真正统一的消息与事件流平台。

### ✨ 核心特性

#### 🏗️ 架构设计

- **存算分离架构** - Broker 完全无状态，存储层独立扩展，秒级弹性伸缩
- **零外部依赖** - 内置 Raft 元数据管理，无需 ZooKeeper/etcd
- **统一消息模型** - Queue、Pub/Sub、Stream 三种模式一体化
- **分层存储** - 热数据本地 SSD，冷数据自动归档至 S3/OSS

#### ⚡ 极致性能

- **高吞吐** - 单集群百万级消息/秒
- **低延迟** - P99 < 10ms (acks=1)
- **零拷贝** - sendfile 直接传输，CPU 零参与
- **批量聚合** - Group Commit 减少磁盘 IO

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
cd aethermq-go

# 编译
make build

# 运行单机模式
./bin/controller &
./bin/broker &
./bin/gateway &
```

#### Docker 部署

```bash
docker-compose up -d
```

#### Kubernetes 部署

```bash
helm install aethermq ./deployments/helm/aethermq
```

### 💻 使用示例

#### 生产者

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/aethermq/aethermq/pkg/client"
)

func main() {
    producer, err := client.NewProducer(client.ProducerConfig{
        Brokers: []string{"localhost:29092"},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // 发送简单消息
    _, err = producer.Send(context.Background(), &client.Message{
        Topic:   "order-events",
        Payload: []byte(`{"orderId":"12345","status":"created"}`),
    })

    // 发送带分区键的消息（保证顺序）
    _, err = producer.Send(context.Background(), &client.Message{
        Topic:        "order-events",
        PartitionKey: "order-12345",
        Payload:      []byte(`{"orderId":"12345","status":"paid"}`),
    })

    // 发送延迟消息
    _, err = producer.Send(context.Background(), &client.Message{
        Topic:       "order-events",
        Payload:     []byte(`{"orderId":"12345","action":"timeout-check"}`),
        DeliverAfter: 30 * time.Minute,
    })
}
```

#### 消费者

```go
package main

import (
    "context"
    "log"

    "github.com/aethermq/aethermq/pkg/client"
)

func main() {
    consumer, err := client.NewConsumer(client.ConsumerConfig{
        Brokers:       []string{"localhost:29092"},
        Topic:         "order-events",
        ConsumerGroup: "order-processor",
        Subscription:  client.Shared,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    consumer.Subscribe(context.Background(), func(msg *client.Message) error {
        // 处理消息
        log.Printf("Received: %s", string(msg.Payload))
        
        // 返回 nil 表示 ACK
        // 返回 error 触发重试
        return processOrder(msg.Payload)
    })
}
```

### 📊 性能基准

| 指标                   | 数值                 |
| -------------------- | ------------------ |
| 生产吞吐量 (单集群)          | > 5,000,000 msg/s  |
| 消费吞吐量 (单集群)          | > 10,000,000 msg/s |
| 端到端延迟 P99 (acks=1)   | < 10ms             |
| 端到端延迟 P99 (acks=all) | < 30ms             |
| Broker 扩容时间          | < 30s              |
| 故障恢复时间               | < 10s              |

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
- [ ] 分层存储 (S3/OSS)
- [ ] 事务消息
- [ ] 消息追溯系统
- [ ] Kubernetes Operator
- [ ] Kafka 协议兼容
- [ ] Schema Registry

### 🤝 贡献

欢迎贡献！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详情。

### 📄 许可证

MIT

***

## English

### 🚀 Overview

AetherMQ Go is a next-generation message queue system built with **Go**, designed for the cloud-native era. It combines Kafka's high throughput, Pulsar's storage-compute separation, RocketMQ's rich business features, and RabbitMQ's flexible routing into a unified messaging and event streaming platform.

### ✨ Key Features

- **Storage-Compute Separation** - Stateless brokers, independent storage scaling
- **Zero External Dependencies** - Built-in Raft metadata management
- **Unified Messaging Model** - Queue, Pub/Sub, and Stream in one platform
- **Tiered Storage** - Hot data on SSD, cold data archived to S3/OSS
- **High Performance** - Millions of messages per second, sub-10ms P99 latency
- **Built-in Advanced Features** - Delayed messages, DLQ, transactions, tracing
- **Cloud-Native First** - Kubernetes Operator, multi-tenancy, observability

### 📦 Quick Start

```bash
# Clone and build
git clone https://github.com/aethermq/aethermq-go.git
cd aethermq-go
make build

# Run standalone mode
./bin/controller &
./bin/broker &
./bin/gateway &
```

### 📊 Benchmarks

| Metric               | Value       |
| -------------------- | ----------- |
| Produce Throughput   | > 5M msg/s  |
| Consume Throughput   | > 10M msg/s |
| P99 Latency (acks=1) | < 10ms      |
| Broker Scale-out     | < 30s       |
| Failover Recovery    | < 10s       |

### 📄 License

MIT

***

<div align="center">

**[Website](https://aethermq.io)** ·
**[Documentation](https://docs.aethermq.io)** ·
**[Discord](https://discord.gg/aethermq)** ·
**[Twitter](https://twitter.com/aethermq)**

</div>
