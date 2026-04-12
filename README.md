# Raft KV Store

基于 SOFAJRaft 的分布式 KV 存储系统。

## 项目结构

```
raft-kv-store/
├── pom.xml                          # Maven 配置
├── config/                           # 节点配置文件
│   ├── node1.yml                    # 节点 1 (Leader)
│   ├── node2.yml                    # 节点 2 (Follower)
│   └── node3.yml                    # 节点 3 (Follower)
├── scripts/                          # 启动脚本
│   ├── start-node1.sh               # 启动节点 1
│   ├── start-node2.sh               # 启动节点 2
│   ├── start-node3.sh               # 启动节点 3
│   └── demo.sh                      # 故障转移演示脚本
└── src/
    └── main/
        ├── java/com/raftkv/
        │   ├── RaftKVApplication.java
        │   ├── config/RaftProperties.java
        │   ├── controller/KVController.java
        │   ├── entity/
        │   │   ├── KVRequest.java
        │   │   ├── KVResponse.java
        │   │   ├── KVTask.java
        │   │   └── ClusterStats.java
        │   └── service/
        │       ├── KVStoreStateMachine.java  # 核心：Raft 状态机
        │       └── RaftKVService.java        # Raft 节点管理
        └── resources/
            └── application.yml
```

## 快速开始

### 1. 编译项目

```bash
cd raft-kv-store
mvn clean package -DskipTests
```

### 2. 启动集群

需要启动 3 个终端窗口：

**终端 1 - 启动节点 1 (Leader):**
```bash
./scripts/start-node1.sh
```

**终端 2 - 启动节点 2 (Follower):**
```bash
./scripts/start-node2.sh
```

**终端 3 - 启动节点 3 (Follower):**
```bash
./scripts/start-node3.sh
```

### 3. 验证集群状态

查看节点状态：
```bash
curl http://localhost:8081/kv/stats
curl http://localhost:8082/kv/stats
curl http://localhost:8083/kv/stats
```

查看当前 Leader：
```bash
curl http://localhost:8081/kv/leader
```

### 4. KV 操作

**写入数据 (PUT):**
```bash
curl -X PUT http://localhost:8081/kv/hello \
  -H "Content-Type: application/json" \
  -d '{"value": "world"}'
```

**读取数据 (GET):**
```bash
curl http://localhost:8081/kv/hello
```

**删除数据 (DELETE):**
```bash
curl -X DELETE http://localhost:8081/kv/hello
```

**获取所有数据:**
```bash
curl http://localhost:8081/kv/all
```

### 5. 故障转移演示

```bash
./scripts/demo.sh
```

演示步骤：
1. 向集群写入数据
2. 手动 kill 掉 Leader 节点
3. 观察新 Leader 选举
4. 验证数据完整性和可用性

## API 文档

### KV 操作

| 方法 | 路径 | 说明 |
|------|------|------|
| PUT | `/kv/{key}` | 设置 key-value |
| GET | `/kv/{key}` | 获取 value |
| DELETE | `/kv/{key}` | 删除 key |
| GET | `/kv/all` | 获取所有 kv |
| GET | `/kv/stats` | 集群状态 |

### 响应格式

```json
{
  "success": true,
  "key": "hello",
  "value": "world",
  "requestId": "xxx",
  "servedBy": "127.0.0.1:8081"
}
```

### 错误响应 (非 Leader)

```json
{
  "success": false,
  "error": "NOT_LEADER",
  "leaderEndpoint": "127.0.0.1:8082",
  "key": "hello"
}
```

## 架构设计

### Raft 集群架构

```
                    ┌─────────────────────────────────┐
                    │         Raft Cluster            │
                    │                                 │
    Client ──▶ Node1│◀─── 日志复制 ───▶│Node2│◀─── 日志复制 ───▶│Node3│
         (Leader)   │              (Follower)        │              (Follower)   │
                    └─────────────────────────────────┘
```

### 数据流

1. 客户端请求发送到任意节点
2. 非 Leader 节点重定向到 Leader
3. Leader 将操作写入本地日志
4. Leader 通过 Raft 协议复制到 Followers
5. 半数节点确认后，操作被提交 (Commit)
6. Leader 回调 StateMachine 执行实际操作
7. 返回结果给客户端

### 核心组件

| 组件 | 说明 |
|------|------|
| KVStoreStateMachine | Raft 状态机，处理 onApply 回调 |
| RaftKVService | Raft 节点生命周期管理 |
| KVController | REST API 入口 |

## 技术栈

| 组件 | 版本 |
|------|------|
| JDK | 17 |
| Spring Boot | 3.2.0 |
| SOFAJRaft | 1.4.0 |
| Bolt RPC | 1.6.7 |
| Netty | 4.1.100 |

## 注意事项

1. **Leader 必须先启动**：确保节点 1 先启动并成为 Leader
2. **Follower 启动时机**：节点 2、3 可以在 Leader 选举完成后启动
3. **数据持久化**：KV 数据存储在 `./data/nodeX/kv-store-data.json`
4. **Raft 日志**：Raft 相关数据存储在 `./data/nodeX/` 目录

## 常见问题

### Q: 节点启动后不是 Leader？
A: 等待 10-15 秒让选举完成。如果一直是 Candidate，检查网络连接和端口是否可用。

### Q: PUT 操作失败？
A: 检查当前节点是否是 Leader。如果不是，会返回 301 重定向。

### Q: 如何清理数据？
A: 删除 `./data/` 目录，然后重新启动集群。
