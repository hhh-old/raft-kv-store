# Raft KV Store 与 etcd 功能差距分析（2025-04-15 更新）

## 已实现功能

| 功能 | 状态 | 说明 | 与 etcd 对齐度 |
|------|------|------|----------------|
| 基本 KV 操作 | ✅ | PUT/GET/DELETE | 100% |
| Raft 共识 | ✅ | SOFAJRaft 实现 | 100% |
| Watch 机制 | ✅ | 支持精确匹配、前缀匹配、历史回放 | 90% |
| 线性一致性读 | ✅ | ReadIndex 机制 | 100% |
| 幂等性 | ✅ | 请求去重缓存 | 100% |
| 快照 | ✅ | 自动快照和加载 | 100% |
| **Transaction（事务）** | ✅ **新增** | 支持 compare-then-execute、多 key 原子操作 | 95% |
| **MVCC 多版本** | ✅ **新增** | 支持历史版本查询、Revision 追踪 | 90% |
| **乐观并发控制** | ✅ **新增** | 事务冲突检测、细粒度锁 | 90% |
| **高级 Compare 操作符** | ✅ **新增** | EQUAL/GREATER/LESS/NOT_EQUAL/GREATER_EQUAL/LESS_EQUAL | 100% |

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

### 2. Transaction（事务）⭐⭐⭐⭐⭐ ✅ 已实现

**重要性**：极高 - 实现分布式锁、CAS 操作的基础

**实现状态**：✅ **已完成（2025-04-15）**

**已实现功能**：
- ✅ 原子性多 key 操作
- ✅ 条件判断（If-Then-Else）
- ✅ compare-and-swap（CAS）
- ✅ 6 种 Compare 操作符（EQUAL/GREATER/LESS/NOT_EQUAL/GREATER_EQUAL/LESS_EQUAL）
- ✅ 事务内可见性（事务内 PUT 后 GET 能看到新值）
- ✅ 乐观并发控制（OCC）

**使用示例**：
```java
// 1. 分布式锁（安全实现）
TxnRequest txn = TxnRequest.builder()
    .compare(Compare.version("/lock/mylock", CompareOp.EQUAL, 0))
    .success(Operation.put("/lock/mylock", "owner-123"))
    .failure(Operation.get("/lock/mylock"))
    .build();

// 2. CAS 原子更新
TxnRequest txn = TxnRequest.builder()
    .compare(Compare.value("/config/version", CompareOp.EQUAL, "v1"))
    .success(Operation.put("/config/version", "v2"))
    .failure(Operation.get("/config/version"))
    .build();
```

**与 etcd 的差距**：
- ✅ 功能完整度：95%
- ⚠️ 缺少：OR 逻辑（当前只有 AND）
- ⚠️ 缺少：范围比较（如 key > "/a" && key < "/z"）

---

### 3. MVCC 多版本存储 ⭐⭐⭐⭐ ✅ 已实现

**重要性**：高 - 支持历史版本查询、回滚

**实现状态**：✅ **已完成（2025-04-15）**

**已实现功能**：
- ✅ 每个 Key 保留多个历史版本（TreeMap 存储）
- ✅ 支持查询指定版本的数据（`getAtRevision`）
- ✅ Revision 格式对齐 etcd（mainRev.subRev）
- ✅ 支持版本号、创建版本、修改版本追踪
- ✅ 乐观并发控制（OCC）冲突检测
- ✅ 细粒度锁优化（无锁读、per-key 写锁）

**使用示例**：
```java
// 存储多个版本
mvccStore.put("key", "value1");  // rev=1.0
mvccStore.put("key", "value2");  // rev=2.0
mvccStore.put("key", "value3");  // rev=3.0

// 查询历史版本
KeyValue kv = mvccStore.getAtRevision("key", 2);  // 返回 value2

// 获取版本信息
long version = mvccStore.getVersion("key");        // 返回 3
Revision modRev = mvccStore.getModRevision("key"); // 返回 3.0
```

**与 etcd 的差距**：
- ✅ 功能完整度：90%
- ⚠️ 缺少：自动压缩（Compaction）
- ⚠️ 缺少：历史版本 TTL

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

## 功能实现优先级建议（更新）

### 第一阶段（核心功能）✅ 已完成 2/3
1. ~~Transaction 事务~~ ✅ **已完成** - 分布式锁的基础
2. ~~MVCC 多版本~~ ✅ **已完成** - 数据完整性的基础
3. **Lease 机制** ⏳ **待实现** - 服务发现的基础

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

## 总结（2025-04-15 更新）

当前项目实现了 etcd 约 **65%** 的核心功能，主要进展：

| 功能类别 | 完成度 | 关键进展 | 关键差距 |
|----------|--------|----------|----------|
| 基础 KV | 100% | ✅ MVCC 实现 | 无 |
| Watch | 85% | ✅ 基础实现完善 | ⚠️ 缺少双向流（gRPC） |
| 事务 | 95% | ✅ Compare-then-execute、OCC | ⚠️ 缺少 OR 逻辑 |
| 并发控制 | 90% | ✅ 无锁读、细粒度锁、OCC | ⚠️ 缺少自动重试机制 |
| 高级功能 | 30% | ✅ MVCC、事务 | ⏳ 缺少 Lease、Compaction |
| 运维管理 | 20% | - | ⏳ 缺少动态扩缩容 |
| 安全 | 0% | - | ⏳ 缺少认证授权 |

### 已实现的核心特性

```
✅ 基础 KV 操作（PUT/GET/DELETE）
✅ Raft 共识（SOFAJRaft）
✅ Watch 机制（精确匹配、前缀匹配、历史回放）
✅ 线性一致性读（ReadIndex）
✅ 幂等性（请求去重）
✅ 快照（自动保存/加载）
✅ 事务（Transaction）- 2025-04-15 完成
   - Compare-then-execute 语义
   - 6 种 Compare 操作符
   - 事务内可见性
✅ MVCC 多版本存储 - 2025-04-15 完成
   - Revision 追踪（mainRev.subRev）
   - 历史版本查询
   - 乐观并发控制（OCC）
✅ 锁优化 - 2025-04-15 完成
   - 无锁读（MVCC）
   - 细粒度写锁（per-key）
   - 事务乐观并发控制
```

### 与 etcd 的核心差距

| 功能 | 状态 | 优先级 |
|------|------|--------|
| **Lease 机制** | ⏳ 未实现 | ⭐⭐⭐⭐⭐ |
| **Compaction** | ⏳ 未实现 | ⭐⭐⭐⭐ |
| **Cluster Membership** | ⏳ 未实现 | ⭐⭐⭐⭐ |
| **Authentication** | ⏳ 未实现 | ⭐⭐⭐ |
| **gRPC API** | ⏳ 未实现 | ⭐⭐⭐ |

**建议下一步**：优先实现 **Lease 机制**，这是服务发现和分布式锁的基础，也是 etcd 最核心的特性之一。
