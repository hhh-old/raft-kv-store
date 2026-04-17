# Raft KV Store 与 etcd 功能差距分析

## 已实现功能

| 功能 | 状态 | 说明 |
|------|------|------|
| 基本 KV 操作 | ✅ | PUT/GET/DELETE |
| Raft 共识 | ✅ | SOFAJRaft 实现 |
| Watch 机制 | ✅ | 支持精确匹配、前缀匹配、历史回放 |
| 线性一致性读 | ✅ | ReadIndex 机制 |
| 幂等性 | ✅ | 请求去重缓存 |
| 快照 | ✅ | 自动快照和加载 |

---

## 未实现的核心功能

### 1. Lease（租约）机制 ⭐⭐⭐⭐⭐

**重要性**：极高 - etcd 最核心的功能之一

**功能描述**：
- 客户端可以创建一个带 TTL（生存时间）的租约
- 将 Key 绑定到租约上
- 租约过期后，所有绑定的 Key 自动删除
- 支持租约续期（keepalive）

**使用场景**：
```bash
# 1. 服务注册与发现
# 服务启动时创建租约并注册
etcdctl lease grant 10  # 10秒租约
etcdctl put /services/user-service/192.168.1.1:8080 "healthy" --lease=12345
# 服务通过 keepalive 续期，宕机后自动剔除

# 2. 分布式锁（带超时）
etcdctl lock mylock --ttl=10

# 3. 临时配置
# 临时配置项，过期自动清理
```

**实现复杂度**：高
- 需要租约管理器（定时检查过期）
- 需要与 Watch 机制集成（过期触发删除事件）
- 需要支持租约续期 RPC

**建议实现方案**：
```java
@Component
public class LeaseManager {
    // 租约 ID -> Lease 对象
    private final Map<Long, Lease> leases = new ConcurrentHashMap<>();
    
    // 定时检查过期租约
    @Scheduled(fixedRate = 1000)
    public void checkExpiredLeases() {
        long now = System.currentTimeMillis();
        for (Lease lease : leases.values()) {
            if (lease.getExpiryTime() < now) {
                revokeLease(lease.getId());  // 删除所有绑定的 key
            }
        }
    }
}
```

---

### 2. Transaction（事务）⭐⭐⭐⭐⭐

**重要性**：极高 - 实现分布式锁、CAS 操作的基础

**功能描述**：
- 支持原子性的多 key 操作
- 支持条件判断（If-Then-Else）
- 支持 compare-and-swap（CAS）

**使用场景**：
```bash
# 1. 分布式锁（安全实现）
# 只有 key 不存在时才创建（原子操作）
etcdctl txn <<<'
mod("/lock/mylock") = "0"

# 如果 key 不存在
put("/lock/mylock", "owner-123")

# 否则
get("/lock/mylock")
'

# 2. 配置原子更新
# 只有当当前值等于预期值时才更新
etcdctl txn <<<'
value("/config/version") = "v1"

put("/config/version", "v2")
put("/config/data", "new-data")

get("/config/version")
'
```

**实现复杂度**：高
- 需要扩展 Raft 日志条目类型
- 需要实现条件判断逻辑
- 需要保证整个事务的原子性

**建议实现方案**：
```java
public class TransactionRequest {
    private List<Compare> compares;      // 条件判断
    private List<Operation> successOps;  // 成功时执行
    private List<Operation> failureOps;  // 失败时执行
}

public enum CompareType {
    VERSION,      // 版本号比较
    CREATE,       // 创建版本比较
    MOD,          // 修改版本比较
    VALUE,        // 值比较
}

// 在 StateMachine 中处理
public void onApply(TransactionRequest txn) {
    boolean success = evaluateCompares(txn.getCompares());
    if (success) {
        txn.getSuccessOps().forEach(this::applyOperation);
    } else {
        txn.getFailureOps().forEach(this::applyOperation);
    }
}
```

---

### 3. Multi-Version（多版本）⭐⭐⭐⭐

**重要性**：高 - 支持历史版本查询、回滚

**功能描述**：
- 每个 Key 保留多个历史版本
- 支持查询指定版本的数据
- 支持按版本号范围查询

**使用场景**：
```bash
# 1. 查询历史版本
etcdctl get /config/app --rev=100  # 查询版本100时的值

# 2. 查看修改历史
etcdctl watch /config/app --rev=1  # 从版本1开始的所有变化

# 3. 数据回滚
etcdctl get /config/app --rev=100 | etcdctl put /config/app
```

**实现复杂度**：中
- 修改 KV 存储结构，支持版本链
- 需要版本清理机制（防止无限增长）

**建议实现方案**：
```java
public class VersionedValue {
    private String value;
    private long createRevision;
    private long modRevision;
    private int version;  // 当前 key 的版本号（每次修改+1）
    private long leaseId;
}

// 存储结构：key -> List<VersionedValue>
private final Map<String, List<VersionedValue>> versionedStore = new ConcurrentHashMap<>();

// 查询指定版本
public String get(String key, long revision) {
    List<VersionedValue> versions = versionedStore.get(key);
    // 二分查找 <= revision 的最新版本
    return binarySearch(versions, revision);
}
```

---

### 4. Compaction（压缩）⭐⭐⭐⭐

**重要性**：高 - 防止历史数据无限增长

**功能描述**：
- 手动或自动压缩历史数据
- 删除指定版本号之前的所有历史
- 释放存储空间

**使用场景**：
```bash
# 1. 手动压缩
etcdctl compaction 1000  # 删除版本1000之前的所有历史

# 2. 自动压缩（etcd 服务端配置）
--auto-compaction-mode=periodic
--auto-compaction-retention=1h
```

**实现复杂度**：中
- 需要定期扫描和删除过期数据
- 需要考虑压缩过程中的并发访问

**建议实现方案**：
```java
@Component
public class CompactionManager {
    
    // 定时自动压缩
    @Scheduled(fixedRate = 3600000)  // 每小时
    public void autoCompact() {
        long currentRev = revisionManager.getCurrentRevision();
        long compactRev = currentRev - retentionCount;  // 保留最近 N 个版本
        compact(compactRev);
    }
    
    public void compact(long compactRevision) {
        // 1. 删除 EventHistory 中 revision < compactRevision 的事件
        eventHistory.compact(compactRevision);
        
        // 2. 删除 KVStore 中 modRevision < compactRevision 的旧版本
        kvStore.compact(compactRevision);
        
        // 3. 记录压缩点（用于快照）
        metadata.setCompactRevision(compactRevision);
    }
}
```

---

### 5. Cluster Membership（集群成员管理）⭐⭐⭐⭐

**重要性**：高 - 生产环境必需

**功能描述**：
- 动态添加/删除节点
- 替换故障节点
- 查看集群成员列表

**使用场景**：
```bash
# 1. 查看集群成员
etcdctl member list

# 2. 添加新节点
etcdctl member add node4 --peer-urls=http://127.0.0.1:8084
# 然后启动新节点

# 3. 删除节点
etcdctl member remove <member-id>

# 4. 替换故障节点
etcdctl member update <member-id> --peer-urls=http://127.0.0.1:8085
```

**实现复杂度**：高
- 需要修改 SOFAJRaft 的集群配置
- 需要处理配置变更的共识（两阶段提交）
- 需要考虑数据迁移

**建议实现方案**：
```java
@RestController
@RequestMapping("/cluster")
public class ClusterController {
    
    @PostMapping("/members")
    public ResponseEntity<?> addMember(@RequestBody MemberRequest request) {
        // 1. 通过 Raft 共识添加新配置
        raftKVService.proposeConfigurationChange(
            ConfigurationChangeType.ADD_NODE,
            request.getPeerUrl()
        );
        return ResponseEntity.ok().build();
    }
    
    @DeleteMapping("/members/{id}")
    public ResponseEntity<?> removeMember(@PathVariable String id) {
        // 1. 通过 Raft 共识移除节点
        raftKVService.proposeConfigurationChange(
            ConfigurationChangeType.REMOVE_NODE,
            id
        );
        return ResponseEntity.ok().build();
    }
}
```

---

### 6. Authentication（认证）⭐⭐⭐

**重要性**：中 - 生产环境安全必需

**功能描述**：
- 用户管理（创建/删除用户）
- 角色管理（创建/删除角色）
- 权限控制（读/写/前缀权限）
- TLS 证书认证

**使用场景**：
```bash
# 1. 启用认证
etcdctl auth enable

# 2. 创建用户
etcdctl user add root
etcdctl user add app-user

# 3. 创建角色
etcdctl role add read-only
etcdctl role grant-permission read-only read /config/

# 4. 绑定角色
etcdctl user grant-role app-user read-only

# 5. 带认证访问
etcdctl get /config/app --user=app-user:password
```

**实现复杂度**：中
- 需要用户/角色存储
- 需要集成 Spring Security
- 需要处理密码安全（bcrypt）

---

### 7. gRPC API ⭐⭐⭐

**重要性**：中 - 高性能场景必需

**功能描述**：
- 基于 Protocol Buffers 的 API
- 流式 Watch（双向流）
- 更高的性能（二进制协议）

**与 REST 对比**：
| 特性 | REST | gRPC |
|------|------|------|
| 协议 | HTTP/1.1 | HTTP/2 |
| 序列化 | JSON | Protocol Buffers |
| 性能 | 一般 | 高（2-5倍）|
| 流支持 | SSE（单向） | 双向流 |
| 类型安全 | 无 | 有 |

**实现复杂度**：中
- 需要定义 .proto 文件
- 需要集成 gRPC 框架
- 需要处理流式通信

---

### 8. 其他功能

#### 8.1 告警管理（Alarm）⭐⭐
- 磁盘空间不足告警
- 数据库大小限制
- 自动切换到只读模式

#### 8.2 指标监控（Metrics）⭐⭐⭐
- Prometheus 格式指标导出
- 关键指标：QPS、延迟、存储大小、Raft 状态

#### 8.3 数据导入/导出 ⭐⭐
- `etcdctl snapshot save/load`
- 全量备份和恢复

#### 8.4 代理模式（Proxy）⭐⭐
- 无状态代理节点
- 负载均衡

---

## 功能实现优先级建议

### 第一阶段（核心功能）- 2-3 个月
1. **Lease 机制** - 服务发现的基础
2. **Transaction 事务** - 分布式锁的基础
3. **Multi-Version** - 数据完整性的基础

### 第二阶段（生产必需）- 1-2 个月
4. **Compaction** - 防止存储无限增长
5. **Cluster Membership** - 运维必需
6. **Authentication** - 安全必需

### 第三阶段（性能优化）- 1 个月
7. **gRPC API** - 高性能场景
8. **Metrics** - 可观测性

---

## 技术选型建议

### Lease + Transaction 实现
```java
// 核心组件
LeaseManager          // 租约生命周期管理
TransactionProcessor  // 事务执行引擎
VersionedKVStore      // 多版本存储
CompactionManager     // 压缩管理
```

### 存储引擎选择
当前使用 ConcurrentHashMap，建议考虑：
- **RocksDB**：支持快照、压缩、高性能
- **BoltDB**：纯 Go（etcd 使用），但 Java 绑定较少

### 协议选择
- **REST**：保持现有，用于简单场景
- **gRPC**：新增，用于高性能场景

---

## 总结

当前项目实现了 etcd 约 **40%** 的核心功能，主要差距在：

| 功能类别 | 完成度 | 关键差距 |
|----------|--------|----------|
| 基础 KV | 90% | 缺少多版本 |
| Watch | 80% | 缺少双向流 |
| 高级功能 | 10% | 缺少 Lease、Transaction |
| 运维管理 | 20% | 缺少动态扩缩容 |
| 安全 | 0% | 缺少认证授权 |

**建议下一步**：优先实现 **Lease 机制**，这是服务发现和分布式锁的基础，也是 etcd 最核心的特性之一。
