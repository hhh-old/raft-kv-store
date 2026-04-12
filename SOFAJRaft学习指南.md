# SOFAJRaft 核心概念与接口详解

## 一、SOFAJRaft 简介

SOFAJRaft 是蚂蚁金服开源的基于 Raft 一致性协议的 Java 实现，用于构建高可用、强一致性的分布式系统。

### 核心特性
- **强一致性**：基于 Raft 协议，保证所有节点数据一致
- **高可用**：容忍 (N-1)/2 个节点故障（3节点容忍1个，5节点容忍2个）
- **线性一致性读**：支持多种读策略，平衡性能和一致性
- **快照机制**：加速节点恢复，避免日志回放过慢
- **动态配置变更**：运行时可以添加/移除节点

---

## 二、Raft 协议核心概念

### 2.1 节点角色

Raft 协议中的节点有三种角色：

1. **Leader（领导者）**
   - 负责接收所有客户端写请求
   - 将日志复制到其他 Follower 节点
   - 定期发送心跳（heartbeat）维持领导地位
   - 一个 Raft 集群同一时刻只有一个 Leader

2. **Follower（追随者）**
   - 被动接收 Leader 的日志复制和心跳
   - 如果选举超时没收到心跳，会转换为 Candidate 发起选举
   - 可以处理客户端读请求（但可能需要重定向到 Leader）

3. **Candidate（候选人）**
   - 临时角色，用于发起选举
   - 选举成功后变为 Leader
   - 选举失败或发现更高任期则变为 Follower

### 2.2 任期（Term）

- Term 是 Raft 协议中的逻辑时钟
- 每次选举都会产生一个新的 term
- term 单调递增，用于检测过时的信息
- 如果节点发现更高的 term，会立即更新自己的 term 并转为 Follower

### 2.3 日志复制流程

```
客户端              Leader               Follower1     Follower2
  |                   |                     |              |
  |--- PUT(key,val)-->|                     |              |
  |                   |--- 写入本地日志 ------>|              |
  |                   |--- 写入本地日志 -------------------->|
  |                   |<--- 确认 --------------|              |
  |                   |<--- 确认 ------------------------------|
  |                   | (大多数确认，日志 committed)          |
  |                   |--- 应用到状态机 ------>|              |
  |                   |--- 应用到状态机 -------------------->|
  |<-- 成功响应 ------|                     |              |
```

**关键点**：
1. Leader 收到写请求后，先将日志写入本地
2. 并行复制到其他 Follower 节点
3. 等待大多数节点（majority）确认
4. 标记日志为 committed（已提交）
5. 应用到状态机
6. 返回成功响应给客户端

---

## 三、SOFAJRaft 核心接口详解

### 3.1 PeerId - 节点标识

```java
PeerId peerId = new PeerId();
peerId.parse("127.0.0.1:8081");  // 格式：ip:port
```

**说明**：
- PeerId 是 Raft 集群中节点的惟一标识
- 格式：`ip:port:index`（index 默认为 0）
- 用于在集群中标识和定位节点

### 3.2 NodeOptions - 节点配置选项

```java
NodeOptions nodeOptions = new NodeOptions();

// 选举超时时间（毫秒）
// Follower 在这个时间内没收到 Leader 心跳就会发起选举
// 实际超时时间会在 [electionTimeout, 2*electionTimeout] 之间随机选择
// 随机化避免多个节点同时竞选
nodeOptions.setElectionTimeoutMs(10000);

// 日志存储路径
// 存储 Raft 日志（WAL - Write-Ahead Log）
nodeOptions.setLogUri("./data/log");

// 元数据存储路径
// 存储 currentTerm、votedFor 等元数据
nodeOptions.setRaftMetaUri("./data/raft_meta");

// 快照存储路径
// 存储状态机快照
nodeOptions.setSnapshotUri("./data/snapshot");

// 初始集群配置
Configuration conf = new Configuration();
conf.parse("127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
nodeOptions.setInitialConf(conf);

// 绑定状态机
nodeOptions.setFsm(stateMachine);
```

### 3.3 Configuration - 集群配置

```java
Configuration conf = new Configuration();
conf.parse("127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
```

**说明**：
- 定义 Raft 集群的所有成员节点
- 格式：逗号分隔的 endpoint 列表
- 只有初始化节点（initializer）需要设置 initialConf
- 其他节点通过 Raft 协议自动学习集群配置

### 3.4 RaftGroupService - Raft 组服务

```java
RaftGroupService raftGroupService = new RaftGroupService(
    "kv-store-group",  // groupId: Raft 组 ID
    serverId,          // 当前节点的 PeerId
    nodeOptions,       // 节点配置
    rpcServer          // RPC 服务器
);

// 启动 Raft 节点
Node node = raftGroupService.start();

// 关闭 Raft 节点
raftGroupService.shutdown();
```

**说明**：
- RaftGroupService 是 SOFAJRaft 提供的高级 API
- 封装了 Raft 节点的完整生命周期管理
- 一个进程可以运行多个 RaftGroupService（多个 Raft 组）

### 3.5 Node - Raft 节点对象

```java
// 提交日志到 Raft 集群
Task task = new Task(data, closure);
node.apply(task);

// 获取节点状态
State state = node.getNodeState();  // STATE_LEADER, STATE_FOLLOWER, STATE_CANDIDATE

// 检查是否是 Leader
boolean isLeader = node.isLeader();

// 获取 Leader 节点 ID
PeerId leaderId = node.getLeaderId();

// 获取最后提交的日志索引
long committedIndex = node.getLastCommittedIndex();

// 获取最后应用的日志索引
long appliedIndex = node.getLastAppliedLogIndex();
```

### 3.6 Task - 日志任务

```java
// 创建任务
ByteBuffer data = ...;  // 要提交的数据（序列化后的业务数据）
Closure closure = new Closure() {
    @Override
    public void run(Status status) {
        // 日志被 committed 后的回调
        if (status.isOk()) {
            // 成功
        } else {
            // 失败
        }
    }
};

Task task = new Task(data, closure);

// 提交任务
node.apply(task);
```

**说明**：
- Task 表示一个待提交的日志条目
- data：业务数据（需要序列化）
- closure：回调接口，日志 committed 后执行
- apply 是异步操作，通过 closure 获取结果

### 3.7 Closure - 回调接口

```java
Closure closure = new Closure() {
    @Override
    public void run(Status status) {
        if (status.isOk()) {
            // 日志成功提交并应用
        } else {
            // 失败，status.getErrorMsg() 获取错误信息
        }
    }
};
```

**说明**：
- 当日志被大多数节点确认（committed）后调用
- status.isOk() 判断操作是否成功
- 常见失败原因：Leader 变更、网络分区、节点宕机

### 3.8 Status - 操作状态

```java
Status status = ...;

// 检查是否成功
if (status.isOk()) {
    // 成功
} else {
    // 失败
    String errorMsg = status.getErrorMsg();
    int code = status.getCode();
}
```

---

## 四、StateMachine - 状态机详解

StateMachine 是 Raft 协议的核心组件，负责应用已提交的日志。

### 4.1 核心方法

#### onApply - 应用日志

```java
@Override
public void onApply(Iterator iterator) {
    while (iterator.hasNext()) {
        // 获取日志数据
        ByteBuffer data = iterator.getData();
        
        // 反序列化为业务对象
        MyTask task = deserialize(data);
        
        // 应用到状态机
        applyTask(task);
        
        // 必须调用 next() 移动到下一条
        iterator.next();
    }
}
```

**重要**：
- Iterator 可能包含多条日志（批量应用，提高性能）
- 必须按顺序应用（调用 iterator.next()）
- Raft 保证所有节点以相同顺序应用相同日志

#### onLeaderStart - 成为 Leader

```java
@Override
public void onLeaderStart(long term) {
    // 当前节点赢得了选举，成为新的 Leader
    // 可以在这里执行 Leader 专属的初始化
}
```

#### onLeaderStop - 失去 Leader 地位

```java
@Override
public void onLeaderStop(Status status) {
    // 当前节点不再是 Leader
    // 可能在选举中失败，或发现更高任期的 Leader
}
```

#### onStartFollowing - 开始跟随 Leader

```java
@Override
public void onStartFollowing(LeaderChangeContext ctx) {
    // 开始跟随新的 Leader
    // ctx.getLeaderId() 获取新 Leader 的 PeerId
    // ctx.getTerm() 获取当前任期
}
```

#### onStopFollowing - 停止跟随 Leader

```java
@Override
public void onStopFollowing(LeaderChangeContext ctx) {
    // 准备发起选举，不再跟随当前 Leader
}
```

#### onSnapshotSave - 保存快照

```java
@Override
public void onSnapshotSave(SnapshotWriter writer, Closure done) {
    try {
        // 将状态机数据写入快照文件
        Path snapshotFile = Paths.get(writer.getPath() + File.separator + "data");
        writeData(snapshotFile.toFile());
        
        // 注册快照文件
        if (writer.addFile("data")) {
            done.run(Status.OK());  // 成功
        } else {
            done.run(new Status(-1, "Failed to add file"));
        }
    } catch (Exception e) {
        done.run(new Status(-1, "Failed to save snapshot"));
    }
}
```

#### onSnapshotLoad - 加载快照

```java
@Override
public boolean onSnapshotLoad(SnapshotReader reader) {
    try {
        // 检查快照文件是否存在
        if (reader.getFileMeta("data") == null) {
            return false;
        }
        
        // 从快照文件加载数据
        Path snapshotFile = Paths.get(reader.getPath() + File.separator + "data");
        loadData(snapshotFile.toFile());
        
        return true;
    } catch (Exception e) {
        return false;
    }
}
```

#### onError - 错误处理

```java
@Override
public void onError(RaftException e) {
    // Raft 节点发生严重错误
    // 如磁盘满、数据损坏等
    LOG.error("Raft error", e);
}
```

---

## 五、数据流详解

### 5.1 写操作（PUT/DELETE）完整流程

```
1. 客户端发起 PUT 请求
   ↓
2. 检查当前节点是否是 Leader
   - 是：继续
   - 否：返回 Leader 信息，客户端重定向
   ↓
3. 创建 KVTask 对象
   ↓
4. 序列化为字节数组
   ↓
5. 创建 Task 和 Closure
   ↓
6. 调用 node.apply(task)
   ↓
7. Raft 将日志追加到本地
   ↓
8. 并行复制到其他 Follower
   ↓
9. 等待大多数节点确认
   ↓
10. 标记日志为 committed
    ↓
11. 调用 stateMachine.onApply(iterator)
    ↓
12. 反序列化为 KVTask
    ↓
13. 应用到状态机（更新 HashMap）
    ↓
14. 持久化到磁盘文件
    ↓
15. 执行 Closure 回调
    ↓
16. 返回成功响应给客户端
```

### 5.2 读操作（GET）流程

```
1. 客户端发起 GET 请求
   ↓
2. 检查当前节点是否是 Leader
   ↓
3. 直接从状态机读取数据（内存 HashMap）
   ↓
4. 返回结果给客户端
```

**注意**：本实现的读操作是简化版本，没有经过 Raft 日志。
如果需要严格的线性一致性读，应该使用 SOFAJRaft 提供的 `readIndex` 机制。

### 5.3 选举流程

```
1. Follower 在 electionTimeout 时间内没收到 Leader 心跳
   ↓
2. 递增 term，转换为 Candidate
   ↓
3. 给自己投票
   ↓
4. 发送 RequestVote RPC 给其他节点
   ↓
5. 等待投票响应
   ↓
6. 如果获得大多数票（majority）
   - 赢得选举，成为 Leader
   - 发送 AppendEntries RPC（心跳）宣布自己的领导地位
   ↓
7. 如果发现更高 term 的 Leader
   - 转换为 Follower
   ↓
8. 如果选举超时没人获胜
   - 递增 term，发起新一轮选举
```

### 5.4 快照流程

#### 保存快照

```
1. Raft 检测到日志大小超过阈值
   ↓
2. 调用 stateMachine.onSnapshotSave(writer, done)
   ↓
3. 状态机加锁，保证数据一致性
   ↓
4. 将当前数据写入快照文件
   ↓
5. 调用 writer.addFile() 注册文件
   ↓
6. 调用 done.run(Status.OK()) 通知完成
   ↓
7. Raft 清理已快照的日志
```

#### 加载快照

```
1. 节点启动或收到 InstallSnapshot RPC
   ↓
2. 调用 stateMachine.onSnapshotLoad(reader)
   ↓
3. 检查快照文件是否存在
   ↓
4. 清空当前状态机数据
   ↓
5. 从快照文件加载数据
   ↓
6. 返回 true（成功）或 false（失败）
```

---

## 六、关键术语对照表

| 英文术语 | 中文翻译 | 说明 |
|---------|---------|------|
| Leader | 领导者 | 负责接收写请求、复制日志 |
| Follower | 追随者 | 被动接收日志和心跳 |
| Candidate | 候选人 | 发起选举的临时角色 |
| Term | 任期 | 逻辑时钟，每次选举递增 |
| Log Entry | 日志条目 | 记录客户端操作 |
| Committed | 已提交 | 日志被大多数节点确认 |
| Applied | 已应用 | 日志被应用到状态机 |
| State Machine | 状态机 | 执行业务逻辑的组件 |
| Snapshot | 快照 | 状态机的数据快照 |
| Election Timeout | 选举超时 | Follower 发起选举的等待时间 |
| Heartbeat | 心跳 | Leader 定期发送的消息 |
| Majority | 大多数 | 超过半数的节点数 |
| WAL | 预写式日志 | Write-Ahead Log |
| Closure | 闭包/回调 | 异步操作的回调接口 |
| Iterator | 迭代器 | 遍历待应用日志的工具 |

---

## 七、常见问题

### Q1: 为什么写操作必须在 Leader 上执行？

**A**: Raft 协议规定只有 Leader 能处理写请求，这是为了保证日志复制的顺序一致性。如果多个节点同时接受写请求，可能会导致数据不一致。

### Q2: 什么是线性一致性？

**A**: 线性一致性（Linearizability）保证读操作总是能读到最新的已提交数据。简单来说，就像一个单线程系统一样，所有操作都是原子的、有序的。

### Q3: 快照什么时候触发？

**A**: 
1. 自动触发：当日志大小超过配置的阈值时
2. 手动触发：通过 CLI 命令 `snapshot` 手动触发

### Q4: 如果节点宕机，数据会丢失吗？

**A**: 不会。Raft 保证：
- 只要大多数节点还存活，数据就不会丢失
- 节点重启后，通过日志回放或快照加载恢复数据

### Q5: 3 节点集群能容忍几个节点故障？

**A**: 1 个。3 节点集群需要至少 2 个节点存活才能达成多数派（majority）。

### Q6: 什么是脑裂（Split-Brain）？

**A**: 脑裂是指网络分区导致集群分成多个部分，每个部分都选出自己的 Leader。Raft 通过以下机制避免脑裂：
- 选举需要大多数票（网络分区后，只有一方能获得大多数）
- 如果 Leader 发现更高 term 的 Leader，会立即退位

---

## 八、学习资源

1. **Raft 动画演示**：https://raft.github.io/raftscope/index.html
2. **Raft 论文**：In Search of an Understandable Consensus Algorithm
3. **SOFAJRaft GitHub**：https://github.com/sofastack/sofa-jraft
4. **SOFAJRaft 文档**：https://www.sofastack.tech/sofa-jraft/docs/overview

---

## 九、调试技巧

### 查看日志

```bash
# 查看应用日志
tail -f logs/app.log

# 查看 Raft 日志
tail -f logs/jraft.log
```

### 检查节点状态

```bash
# 健康检查
curl http://localhost:9081/kv/health

# 查看集群状态
curl http://localhost:9081/kv/stats

# 查看 Leader 信息
curl http://localhost:9081/kv/leader
```

### 使用 CLI 工具

SOFAJRaft 提供了命令行工具，可以动态管理集群：

```bash
# 添加节点
java -cp jraft-core.jar com.alipay.sofa.jraft.cli.CliMain \
  --server-addr=127.0.0.1:8081 \
  add_peer --group-id=kv-store-group \
  --peer=127.0.0.1:8084

# 移除节点
java -cp jraft-core.jar com.alipay.sofa.jraft.cli.CliMain \
  --server-addr=127.0.0.1:8081 \
  remove_peer --group-id=kv-store-group \
  --peer=127.0.0.1:8084
```

---

## 十、总结

SOFAJRaft 是一个生产级的 Raft 实现，核心流程如下：

1. **启动阶段**：创建 RaftGroupService，配置 NodeOptions，启动节点
2. **选举阶段**：节点自动选举出 Leader
3. **写操作**：客户端 → Leader → 日志复制 → 大多数确认 → 应用到状态机 → 返回
4. **读操作**：本地读取（或 readIndex 保证一致性）
5. **快照**：定期保存状态机快照，加速恢复

关键接口：
- `Node.apply()`: 提交日志
- `StateMachine.onApply()`: 应用日志
- `StateMachine.onSnapshotSave/Load()`: 快照管理
- `StateMachine.onLeaderStart/Stop()`: 领导权变更

理解这些核心概念和接口，就能熟练使用 SOFAJRaft 构建分布式系统！
