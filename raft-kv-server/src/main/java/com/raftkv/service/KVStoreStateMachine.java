package com.raftkv.service;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.raftkv.entity.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Raft 状态机实现 - KV 存储
 * 
 * 状态机（StateMachine）是 Raft 协议的核心组件之一，负责：
 * 1. 应用已提交的日志条目（onApply）
 * 2. 保存和加载快照（onSnapshotSave/onSnapshotLoad）
 * 3. 处理领导权变更（onLeaderStart/onLeaderStop）
 * 4. 处理错误（onError）
 * 
 * Raft 协议保证：
 * - 所有节点以相同的顺序应用相同的日志条目
 * - 这保证了所有节点的状态机最终会达到相同状态
 * - 这就是所谓的"状态机安全性"（State Machine Safety）
 * 
 * 本实现的特点：
 * - 使用 MVCCStore 作为存储引擎，支持多版本并发控制
 * - 支持无锁读操作，提高并发性能
 * - 支持 etcd 风格的事务（条件判断 + 批量操作）
 * - 支持 Watch 机制，实时推送数据变更
 * - 支持快照功能，加速节点恢复
 * - 支持幂等性保证，防止重复请求
 */
public class KVStoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(KVStoreStateMachine.class);

    // ==================== MVCC 存储引擎 ====================
    // 使用 MVCC 替代简单的 HashMap，支持多版本和乐观并发控制
    // 所有读写操作都通过 MVCCStore 进行
    private final MVCCStore mvccStore = new MVCCStore();
    
    /**
     * 已处理请求的缓存（用于幂等去重）
     * Key: requestId
     * Value: 操作结果（KVResponse）
     * 
     * 使用 LinkedHashMap 实现 LRU 淘汰策略：
     * - 当缓存满时，自动淘汰最久未使用的请求
     * - 避免简单清空导致的有效缓存丢失
     * - 访问顺序（accessOrder=true）保证最近使用的在前面
     * 
     * 注意：虽然 LinkedHashMap 不是线程安全的，但 onApply 方法由 Raft 保证单线程调用，
     * 且幂等检查在 Leader 端完成，所以是安全的。
     */
    private final Map<String, KVResponse> processedRequests = new LinkedHashMap<String, KVResponse>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, KVResponse> eldest) {
            // 当缓存大小超过限制时，自动淘汰最老的条目
            return size() > MAX_CACHE_SIZE;
        }
    };
    
    /**
     * 请求缓存的最大大小（防止内存溢出）
     * 使用 LRU 策略：超过限制时自动淘汰最久未使用的请求
     */
    private static final int MAX_CACHE_SIZE = 10000;

    /**
     * 事务结果缓存（用于回调获取结果）
     * key: requestId, value: TxnResponse
     * 使用 ConcurrentHashMap 保证线程安全
     */
    private final Map<String, TxnResponse> txnResultCache = new ConcurrentHashMap<>();
    
    /**
     * 事务结果缓存大小限制
     */
    private static final int MAX_TXN_CACHE_SIZE = 10000;
    
    // 当前任期号（term）
    // term 是 Raft 协议中的逻辑时钟，用于检测过时的信息
    // 每次选举都会产生一个新的 term
    private final AtomicLong currentTerm = new AtomicLong(0);
    
    // Leader 节点的 endpoint
    // 用于在 Follower 节点收到客户端请求时，返回 Leader 信息让客户端重定向
    private volatile String leaderEndpoint;
    
    // Leader 的任期号
    // 如果 > 0，表示当前节点知道谁是 Leader
    // 如果 == -1，表示当前节点不知道 Leader（可能正在选举）
    private final AtomicLong leaderTerm = new AtomicLong(-1);
    
    // JSON 序列化工具
    private final ObjectMapper objectMapper = new ObjectMapper();

    // ==================== Watch 机制相关字段 ====================
    
    // 全局版本号（用于 Watch 机制）
    // 注意：版本号由 MVCCStore 统一管理，通过 mvccStore.generateRevision() 生成
    // 这里只提供便捷的访问方法
    
    // Watch 管理器（由外部注入）
    private volatile WatchManager watchManager;

    public KVStoreStateMachine() {
        // 数据目录由 Raft 管理，状态机不再需要自己管理文件
        // 所有持久化工作交给 Raft 的 Snapshot 机制
        LOG.info("KVStoreStateMachine initialized");
    }

    /**
     * 获取 key 的值 - MVCC 无锁读优化
     * 
     * 使用 MVCCStore 实现无锁读，提高并发性能
     * 读操作不再阻塞写操作，也不被写操作阻塞
     */
    public String get(String key) {
        // 从 MVCCStore 读取最新版本（无锁）
        MVCCStore.KeyValue mvccKv = mvccStore.getLatest(key);
        if (mvccKv != null) {
            return mvccKv.getValue();
        }
        // key 不存在
        return null;
    }

    /**
     * 获取事务执行结果（用于回调）
     * @param requestId 请求 ID
     * @return 事务响应，如果不存在返回 null
     */
    public TxnResponse getTxnResult(String requestId) {
        return txnResultCache.get(requestId);
    }

    /**
     * 移除事务结果（清理缓存）
     * @param requestId 请求 ID
     */
    public void removeTxnResult(String requestId) {
        txnResultCache.remove(requestId);
    }

    /**
     * 获取所有 key-value - MVCC 无锁读优化
     * 
     * 从 MVCCStore 读取最新版本，无需全局锁
     */
    public Map<String, String> getAll() {
        // 从 MVCCStore 读取最新版本（无锁）
        Map<String, MVCCStore.KeyValue> allLatest = mvccStore.getAllLatest();
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, MVCCStore.KeyValue> entry : allLatest.entrySet()) {
            if (entry.getValue().getValue() != null) {  // 排除 tombstone
                result.put(entry.getKey(), entry.getValue().getValue());
            }
        }
        return result;
    }

    public String getLeaderEndpoint() {
        return leaderEndpoint;
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * 获取 key 数量 - MVCC 无锁读优化
     */
    public int getKeyCount() {
        // 从 MVCCStore 获取（无锁）
        MVCCStore.StoreStats stats = mvccStore.getStats();
        return (int) stats.getTotalKeys();
    }

    /**
     * 检查是否是重复请求（幂等去重）
     * 
     * 使用 LRU 缓存检查请求是否已处理。
     * 由于使用了 LinkedHashMap，访问已存在的 key 会更新其访问顺序。
     * 
     * @param requestId 请求 ID
     * @return true 表示已处理过，false 表示新请求
     */
    public boolean isDuplicateRequest(String requestId) {
        if (requestId == null || requestId.isEmpty()) {
            return false;  // 没有 requestId，视为新请求
        }
        synchronized (processedRequests) {
            return processedRequests.containsKey(requestId);
        }
    }

    /**
     * 缓存操作结果（用于幂等返回）
     * 
     * 使用 LRU 策略：当缓存满时，自动淘汰最久未使用的请求。
     * 这样可以在保持内存限制的同时，最大化缓存命中率。
     * 
     * @param task 已处理的任务
     */
    private void cacheResult(KVTask task) {
        if (task.getRequestId() == null || task.getRequestId().isEmpty()) {
            return;  // 没有 requestId，不缓存
        }

        // LRU 策略：LinkedHashMap 会自动处理淘汰
        // 当 size() > MAX_CACHE_SIZE 时，removeEldestEntry 返回 true，自动移除最老的条目

        // 构建响应对象并缓存
        KVResponse response;
        if (KVTask.OP_PUT.equals(task.getOp())) {
            response = KVResponse.success(
                task.getKey(), 
                task.getValue(), 
                task.getRequestId(), 
                null  // servedBy 在返回时设置
            );
        } else if (KVTask.OP_DELETE.equals(task.getOp())) {
            response = KVResponse.success(
                task.getKey(), 
                null, 
                task.getRequestId(), 
                null
            );
        } else {
            return;  // 未知操作，不缓存
        }

        synchronized (processedRequests) {
            processedRequests.put(task.getRequestId(), response);
        }
        LOG.debug("Cached result for requestId: {}, cache size: {}", task.getRequestId(), processedRequests.size());
    }

    /**
     * 获取已缓存的请求结果（用于幂等返回）
     * 
     * 从 LRU 缓存中获取请求结果。
     * 访问缓存会更新该条目的访问顺序（LinkedHashMap accessOrder=true）。
     * 
     * @param requestId 请求 ID
     * @return 缓存的响应对象，如果不存在则返回 null
     */
    public KVResponse getCachedResult(String requestId) {
        if (requestId == null || requestId.isEmpty()) {
            return null;
        }
        synchronized (processedRequests) {
            return processedRequests.get(requestId);
        }
    }

    /**
     * 应用已提交的日志条目到状态机
     * 
     * 这是状态机最核心的方法！
     * 
     * 调用时机：
     * 当日志条目被 Raft 集群的大多数节点确认（committed）后，
     * Raft 会调用这个方法来应用日志到状态机。
     * 
     * 重要特性：
     * 1. Iterator 可能包含多个连续的日志条目（批量应用，提高性能）
     * 2. 必须按照日志顺序依次应用（iterator.next() 移动到下一条）
     * 3. 这个方法是线程安全的，Raft 保证同一时刻只有一个线程调用
     * 4. 应用成功后，数据就持久化到了状态机
     * 
     * 工作流程：
     * 1. 遍历 Iterator 中的所有待应用日志
     * 2. 反序列化日志数据为 KVTask 对象
     * 3. 检查 requestId 是否已处理（幂等去重）
     * 4. 如果是重复请求，跳过执行
     * 5. 如果是新请求，根据操作类型（PUT/DELETE）更新 MVCCStore
     * 6. 缓存操作结果（用于幂等返回）
     * 7. 所有日志应用完成后，持久化到磁盘文件
     * 
     * @param iterator 日志迭代器，包含所有待应用的日志条目
     */
    @Override
    public void onApply(Iterator iterator) {
        LOG.debug("onApply called, iter.hasNext={}", iterator.hasNext());

        // 遍历所有待应用的日志条目
        // Iterator 是 SOFAJRaft 提供的迭代器，用于高效地批量处理日志
        while (iterator.hasNext()) {
            // 获取当前日志的索引位置
            long index = iterator.getIndex();

            // 【修复关键 1】获取当前日志条目绑定的回调 Closure
            // 注意：对于 Leader，这个 done 就是你 RaftKVService 里 new 出来的那个 Closure
            // 对于 Follower（通过网络同步日志的节点），这个 done 是 null
            Closure done = iterator.done();


            try {
                // 获取日志数据（ByteBuffer 格式）
                ByteBuffer dataBuffer = iterator.getData();
                if (dataBuffer != null && dataBuffer.hasRemaining()) {
                    // 将 ByteBuffer 转换为字节数组
                    byte[] data = new byte[dataBuffer.remaining()];
                    dataBuffer.get(data);
                    
                    // 反序列化为 KVTask 对象
                    KVTask task = deserialize(data);
                    if (task != null) {
                        // 恢复事务请求（因为 txnRequest 是 transient，不会被序列化）
                        if (KVTask.OP_TXN.equals(task.getOp()) && task.getValue() != null) {
                            try {
                                TxnRequest txnRequest = objectMapper.readValue(task.getValue(), TxnRequest.class);
                                task.setTxnRequest(txnRequest);
                                LOG.debug("Restored TxnRequest from value field for task at index {}", index);
                            } catch (Exception e) {
                                LOG.error("Failed to restore TxnRequest at index {}: {}", index, e.getMessage(), e);
                            }
                        }
                        
                        // 幂等检查：如果 requestId 已处理，跳过执行
                        if (isDuplicateRequest(task.getRequestId())) {
                            LOG.debug("Duplicate request skipped: requestId={}, index={}", 
                                task.getRequestId(), index);
                        } else {
                            // 应用这个任务到状态机
                            applyTask(task);
                            
                            // 缓存操作结果（用于幂等返回）
                            cacheResult(task);
                            
                            LOG.debug("Applied task at index {}: {} key={}", index, task.getOp(), task.getKey());
                        }
                    }
                }
                // 【修复关键 2】执行成功后，必须调用 done.run()！
                // 这行代码会触发 RaftKVService 中 Closure 的 run 方法，进而执行 latch.countDown()
                if (done != null) {
                    done.run(Status.OK());
                }
            } catch (Exception e) {
                LOG.error("Failed to apply task at index {}: {}", index, e.getMessage(), e);
                // 【修复关键 3】即使发生异常，也要通过回调告诉调用方失败了，不要让它死等
                if (done != null) {
                    done.run(new Status(-1, "Failed to apply task: " + e.getMessage()));
                }

            } finally {
                // 必须调用 next() 移动到下一条日志
                // 如果不调用 next()，Raft 会认为这条日志没有被应用
                iterator.next();
            }
        }

        // 注意：不再在这里进行任何磁盘持久化！
        // 
        // 数据持久化由 Raft 的 WAL（预写式日志）保证：
        // - 所有写操作已经通过 Raft 日志持久化到磁盘
        // - 节点重启时，Raft 会自动重放未应用的日志
        // 
        // 状态机快照由 onSnapshotSave/onSnapshotLoad 处理：
        // - 定期（日志积攒到一定量）触发快照保存
        // - 避免每次 apply 都全量写磁盘的性能灾难
        //
        // 这样设计保证：
        // 1. onApply 是纯内存操作，TPS 成百上千倍提升
        // 2. 数据安全由 Raft 机制保证，不会丢失
        // 3. 节点重启后能正确恢复到宕机前状态
    }

    /**
     * 应用任务 - etcd 风格的无锁/细粒度锁实现
     * 
     * 优化点：
     * 1. PUT/DELETE：使用 MVCCStore 的细粒度锁（每个 key 独立锁）
     * 2. TXN：使用乐观并发控制（MVCC 事务上下文）
     * 3. 完全移除 synchronized(kvStore) 全局锁
     *
     * raft层保证了applyTask是单线程执行的，不会有写并发操作
     */
    private void applyTask(KVTask task) {
        switch (task.getOp()) {
            case KVTask.OP_PUT:
                // 使用 MVCCStore 的细粒度锁（per-key lock）
                applyPutMVCC(task.getKey(), task.getValue());
                break;
            case KVTask.OP_DELETE:
                // 使用 MVCCStore 的细粒度锁
                applyDeleteMVCC(task.getKey());
                break;
            case KVTask.OP_TXN:
                // 事务操作 - 使用乐观并发控制
                if (task.getTxnRequest() != null) {
                    TxnResponse txnResponse = executeTransaction(task.getTxnRequest());
                    // 将结果缓存，供回调获取
                    if (task.getRequestId() != null) {
                        cacheTxnResult(task.getRequestId(), txnResponse);
                    }
                } else {
                    LOG.warn("TXN operation without txnRequest");
                }
                break;
            default:
                LOG.warn("Unknown operation: {}", task.getOp());
        }
    }
    
    /**
     * 缓存事务结果（线程安全）
     */
    private void cacheTxnResult(String requestId, TxnResponse response) {
        // 如果缓存已满，先清理一半
        if (txnResultCache.size() >= MAX_TXN_CACHE_SIZE) {
            int toRemove = MAX_TXN_CACHE_SIZE / 2;
            int removed = 0;
            for (String key : txnResultCache.keySet()) {
                if (removed >= toRemove) break;
                txnResultCache.remove(key);
                removed++;
            }
            LOG.warn("Txn result cache reached limit, removed {} entries", removed);
        }
        txnResultCache.put(requestId, response);
    }
    
    /**
     * 应用 PUT 操作 - 使用 MVCCStore 细粒度锁
     */
    private void applyPutMVCC(String key, String value) {
        // MVCCStore.put() 内部会生成 revision 并更新版本信息
        mvccStore.put(key, value);
        
        // 从 MVCCStore 获取刚生成的 revision
        MVCCStore.Revision modRev = mvccStore.getModRevision(key);
        long modRevValue = (modRev != null) ? modRev.getMainRev() : 0;
        
        // 触发 Watch 通知（使用带 try-catch 的 publishWatchEvent，防止 Watch 异常影响 onApply）
        if (watchManager != null) {
            // 直接从 MVCCStore 获取 KeyVersion
            MVCCStore.KeyVersion kv = mvccStore.getKeyVersion(key);
            long createRev = (kv != null) ? kv.createRevision : modRevValue;
            long version = (kv != null) ? kv.version : 1;
            WatchEvent event = WatchEvent.put(key, value, modRevValue, createRev, version);
            publishWatchEvent(event);
        }
    }
    
    /**
     * 应用 DELETE 操作 - 使用 MVCCStore 细粒度锁
     */
    private void applyDeleteMVCC(String key) {
        // MVCCStore.delete() 内部会生成 revision，不需要在这里生成
        mvccStore.delete(key);
        
        // 从 MVCCStore 获取刚生成的 revision
        MVCCStore.Revision modRev = mvccStore.getModRevision(key);
        long modRevValue = (modRev != null) ? modRev.getMainRev() : 0;
        
        // 触发 Watch 通知（使用带 try-catch 的 publishWatchEvent，防止 Watch 异常影响 onApply）
        if (watchManager != null) {
            WatchEvent event = WatchEvent.delete(key, modRevValue);
            publishWatchEvent(event);
        }
    }

    /**
     * 发布 Watch 事件
     * 
     * @param event Watch 事件
     */
    private void publishWatchEvent(WatchEvent event) {
        if (watchManager != null) {
            try {
                watchManager.publishEvent(event);
            } catch (Exception e) {
                LOG.error("Failed to publish watch event: {}", event, e);
            }
        }
    }

    /**
     * 设置 WatchManager（由 RaftKVService 注入）
     * 
     * @param watchManager Watch 管理器
     */
    public void setWatchManager(WatchManager watchManager) {
        this.watchManager = watchManager;
        LOG.info("WatchManager injected into state machine");
    }

    /**
     * 获取当前全局版本号
     * 
     * @return 当前 revision
     */
    public long getCurrentRevision() {
        return mvccStore.getCurrentRevision();
    }

    private KVTask deserialize(byte[] data) {
        try {
            return objectMapper.readValue(data, KVTask.class);
        } catch (IOException e) {
            LOG.error("Failed to deserialize task", e);
            return null;
        }
    }

    /**
     * 当前节点成为 Leader 时的回调
     * 
     * 调用时机：
     * 当当前节点赢得选举，成为新的 Leader 时，Raft 会调用这个方法。
     * 
     * 在这个方法中，通常会：
     * 1. 记录当前任期号
     * 2. 更新 Leader 信息
     * 3. 可以执行一些 Leader 专属的初始化操作
     * 
     * @param term 新的任期号
     */
    @Override
    public void onLeaderStart(long term) {
        LOG.info("onLeaderStart: term={}", term);
        this.leaderTerm.set(term);
        currentTerm.set(term);
    }

    /**
     * 当前节点失去 Leader 地位时的回调
     * 
     * 调用时机：
     * 当 Leader 发现更高任期的 Leader，或者网络分区导致失去多数派支持时，
     * 会调用这个方法。
     * 
     * 在这个方法中，通常会：
     * 1. 清除 Leader 标志
     * 2. 清理 Leader 专属的资源
     * 
     * @param status 失去领导权的原因
     */
    @Override
    public void onLeaderStop(Status status) {
        LOG.info("onLeaderStop: status={}", status);
        this.leaderTerm.set(-1);
    }

    /**
     * 开始跟随新 Leader 时的回调
     * 
     * 调用时机：
     * 当 Follower 收到新 Leader 的心跳或日志复制请求时，会调用这个方法。
     * 
     * 在这个方法中，可以：
     * 1. 记录新 Leader 的信息
     * 2. 更新当前任期号
     * 3. 用于后续客户端请求的重定向
     * 
     * @param ctx Leader 切换上下文，包含新 Leader 的 ID 和任期号
     */
    @Override
    public void onStartFollowing(com.alipay.sofa.jraft.entity.LeaderChangeContext ctx) {
        LOG.info("onStartFollowing: leaderId={}, term={}", ctx.getLeaderId(), ctx.getTerm());
        currentTerm.set(ctx.getTerm());
        if (ctx.getLeaderId() != null) {
            this.leaderEndpoint = ctx.getLeaderId().toString();
        }
    }

    /**
     * 停止跟随 Leader 时的回调
     * 
     * 调用时机：
     * 当节点准备发起选举（选举超时）时，会调用这个方法。
     * 
     * @param ctx Leader 切换上下文
     */
    @Override
    public void onStopFollowing(com.alipay.sofa.jraft.entity.LeaderChangeContext ctx) {
        LOG.info("onStopFollowing: leaderId={}, term={}", ctx.getLeaderId(), ctx.getTerm());
        this.leaderEndpoint = null;
    }

    /**
     * Raft 错误处理回调
     * 
     * 调用时机：
     * 当 Raft 节点发生严重错误时（如磁盘满、数据损坏等），会调用这个方法。
     * 
     * 常见错误：
     * 1. 磁盘空间不足，无法写入日志
     * 2. 日志文件损坏
     * 3. 状态机应用日志时发生异常
     * 
     * @param e Raft 异常信息
     */
    @Override
    public void onError(RaftException e) {
        LOG.error("Raft error: {}", e.getMessage(), e);
    }

    /**
     * 保存状态机快照（由 JRaft 定期自动触发）
     *
     * 
     * 快照触发时机：
     * - 当日志积攒到一定量（默认 10 万条）时，Raft 自动触发
     * - 也可以通过 CLI 命令手动触发

     * 
     * @param writer 快照写入器
     * @param done 完成回调，必须调用 done.run() 通知 Raft 快照结果
     */
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        LOG.info("onSnapshotSave starting...");
        try {
            // 将 MVCCStore、processedRequests、revision 打包成一个整体保存
            Map<String, Object> snapshotData = new HashMap<>();
            
            // 保存完整的 MVCC 数据（包括所有历史版本和元数据）
            snapshotData.put("mvccSnapshot", mvccStore.snapshot());
            
            synchronized (processedRequests) {
                // 将 LRU Cache 转换为普通 Map 保存
                snapshotData.put("processedRequests", new HashMap<>(processedRequests));
            }

            // 写入快照文件
            Path snapshotFile = Paths.get(writer.getPath() + File.separator + "statemachine-data.json");
            objectMapper.writeValue(snapshotFile.toFile(), snapshotData);

            // 告诉 Raft 引擎这个文件是快照的一部分
            if (writer.addFile("statemachine-data.json")) {
                done.run(Status.OK());
                LOG.info("Snapshot saved successfully: {} keys, {} cached requests, revision={}", 
                        mvccStore.getKeyCount(), processedRequests.size(), mvccStore.getCurrentRevision());
            } else {
                done.run(new Status(-1, "Failed to add file to snapshot writer"));
            }
        } catch (Exception e) {
            LOG.error("Failed to save snapshot", e);
            done.run(new Status(-1, "Failed to save snapshot: " + e.getMessage()));
        }
    }

    /**
     * 加载快照（节点启动或落后太多时由 JRaft 自动触发）
     * 
     * 恢复流程：
     * 1. JRaft 先调用 onSnapshotLoad 加载最近一次快照
     * 2. 然后自动重放快照之后的 WAL 日志
     * 3. 最终状态恢复到宕机前的一瞬间
     * 
     * 与 onSnapshotSave 的区别：
     * - onSnapshotSave 是异步的（通过 Closure 回调）
     * - onSnapshotLoad 是同步的（直接返回 boolean）
     * 
     * @param reader 快照读取器
     * @return true 表示加载成功，false 表示失败
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean onSnapshotLoad(SnapshotReader reader) {
        LOG.info("onSnapshotLoad starting: path={}", reader.getPath());
        try {
            // 检查快照中是否有 statemachine-data.json 文件
            if (reader.getFileMeta("statemachine-data.json") == null) {
                LOG.warn("No statemachine-data.json file found in snapshot");
                return false;
            }

            Path snapshotFile = Paths.get(reader.getPath() + File.separator + "statemachine-data.json");
            if (Files.exists(snapshotFile)) {
                // 反序列化包含所有状态的综合数据
                Map<String, Object> snapshotData = objectMapper.readValue(snapshotFile.toFile(), Map.class);

                // 1. 恢复完整的 MVCC 数据（包括所有历史版本和元数据）
                Map<String, Object> mvccSnapshot = (Map<String, Object>) snapshotData.get("mvccSnapshot");
                if (mvccSnapshot != null) {
                    mvccStore.restore(mvccSnapshot);
                    LOG.info("Restored MVCCStore from snapshot: {} keys, revision={}", 
                            mvccStore.getKeyCount(), mvccStore.getCurrentRevision());
                }

                // 2. 恢复幂等请求缓存
                Map<String, Object> loadedRequests = (Map<String, Object>) snapshotData.get("processedRequests");
                if (loadedRequests != null) {
                    synchronized (processedRequests) {
                        processedRequests.clear();
                        for (Map.Entry<String, Object> entry : loadedRequests.entrySet()) {
                            Map<String, Object> respMap = (Map<String, Object>) entry.getValue();
                            KVResponse response = new KVResponse();
                            response.setSuccess((Boolean) respMap.getOrDefault("success", false));
                            response.setKey((String) respMap.get("key"));
                            response.setValue((String) respMap.get("value"));
                            response.setRequestId((String) respMap.get("requestId"));
                            response.setError((String) respMap.get("error"));
                            processedRequests.put(entry.getKey(), response);
                        }
                    }
                }

                LOG.info("Snapshot loaded successfully: {} keys, {} cached requests, revision={}",
                        mvccStore.getKeyCount(), processedRequests.size(), mvccStore.getCurrentRevision());
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.error("Failed to load snapshot", e);
            return false;
        }
    }

    // ==================== 事务支持 ====================

    /**
     * 执行事务
     *
     * 优化说明：
     * - Raft 保证 onApply 是单线程串行执行的
     * - 不可能有并发事务冲突
     * - 保留事务的业务逻辑（条件判断 + 原子执行）
     *
     * @param txnRequest 事务请求
     * @return 事务响应
     */
    public TxnResponse executeTransaction(TxnRequest txnRequest) {
        try {
            // 1. 评估所有 compare 条件
            boolean allConditionsMet = evaluateCompares(txnRequest.getCompares());

            // 2. 根据条件结果选择执行的操作列表
            java.util.List<Operation> opsToExecute = allConditionsMet
                    ? txnRequest.getSuccess()
                    : txnRequest.getFailure();

            // 3. 判断是否是只读事务（没有 PUT/DELETE 操作）
            boolean isReadOnly = opsToExecute.stream()
                    .allMatch(op -> op.getType() == Operation.OpType.GET || op.getType() == Operation.OpType.TXN);

            // 4. 生成事务共享的 mainRev（etcd 语义：只读事务不消耗 revision）
            long txnMainRev;
            if (isReadOnly) {
                // 只读事务：使用当前 revision，不生成新的
                txnMainRev = mvccStore.getCurrentRevision();
                LOG.debug("Read-only transaction, using current revision: {}", txnMainRev);
            } else {
                // 读写事务：生成新的 revision
                txnMainRev = mvccStore.generateRevision().getMainRev();
                LOG.debug("Read-write transaction, generated new revision: {}", txnMainRev);
            }

            // 5. 执行操作列表（支持事务内可见性，共享 mainRev）
            java.util.Map<Integer, TxnResponse.OpResult> results = new java.util.HashMap<>();
            java.util.Map<String, String> txnContext = new java.util.HashMap<>();
            java.util.Map<String, MVCCStore.KeyVersion> txnVersionContext = new java.util.HashMap<>();

            for (int i = 0; i < opsToExecute.size(); i++) {
                Operation op = opsToExecute.get(i);
                // 每个操作使用相同的 mainRev，但不同的 subRev（从 0 开始递增）
                TxnResponse.OpResult result = executeOperationWithContext(op, txnContext, txnVersionContext, txnMainRev, i);
                results.put(i, result);
            }

            // 6. 构建响应
            if (allConditionsMet) {
                return TxnResponse.success(opsToExecute, results);
            } else {
                return TxnResponse.failure(opsToExecute, results);
            }

        } catch (Exception e) {
            LOG.error("Failed to execute transaction", e);
            return TxnResponse.error("Transaction failed: " + e.getMessage());
        }
    }

    /**
     * 评估所有 compare 条件（从 MVCCStore 无锁读取）
     */
    private boolean evaluateCompares(java.util.List<Compare> compares) {
        if (compares == null || compares.isEmpty()) {
            return true;
        }

        for (Compare compare : compares) {
            if (!evaluateCompare(compare)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 评估单个 compare 条件（从 MVCCStore 无锁读取）
     */
    private boolean evaluateCompare(Compare compare) {
        String key = compare.getKey();
        Compare.CompareType target = compare.getTarget();
        Compare.CompareOp op = compare.getOp();
        Object expectedValue = compare.getValue();

        // 从 MVCCStore 读取（无锁）
        switch (target) {
            case VERSION:
                long version = mvccStore.getVersion(key);
                return compareLong(version, op, ((Number) expectedValue).longValue());
            case CREATE:
                MVCCStore.Revision createRev = mvccStore.getCreateRevision(key);
                long createRevValue = (createRev != null) ? createRev.getMainRev() : 0;
                return compareLong(createRevValue, op, ((Number) expectedValue).longValue());
            case MOD:
                MVCCStore.Revision modRev = mvccStore.getModRevision(key);
                long modRevValue = (modRev != null) ? modRev.getMainRev() : 0;
                return compareLong(modRevValue, op, ((Number) expectedValue).longValue());
            case VALUE:
                MVCCStore.KeyValue kv = mvccStore.getLatest(key);
                String actualValue = (kv != null) ? kv.getValue() : null;
                return compareValue(actualValue, op, (String) expectedValue);
            default:
                LOG.warn("Unknown compare target: {}", target);
                return false;
        }
    }

    /**
     * 比较 key 的值 - 支持 GREATER_EQUAL / LESS_EQUAL
     * 
     * @param actualValue 实际值
     * @param op 比较操作符
     * @param expectedValue 期望值
     * @return true 如果比较结果为真
     */
    private boolean compareValue(String actualValue, Compare.CompareOp op, String expectedValue) {
        switch (op) {
            case EQUAL:
                return (actualValue == null && expectedValue == null) ||
                        (actualValue != null && actualValue.equals(expectedValue));
            case NOT_EQUAL:
                return (actualValue == null && expectedValue != null) ||
                        (actualValue != null && !actualValue.equals(expectedValue));
            case GREATER:
                return actualValue != null && actualValue.compareTo(expectedValue) > 0;
            case LESS:
                return actualValue != null && actualValue.compareTo(expectedValue) < 0;
            case GREATER_EQUAL:
                return actualValue != null && actualValue.compareTo(expectedValue) >= 0;
            case LESS_EQUAL:
                return actualValue != null && actualValue.compareTo(expectedValue) <= 0;
            default:
                return false;
        }
    }

    /**
     * 比较两个 long 值 - 支持 GREATER_EQUAL / LESS_EQUAL
     */
    private boolean compareLong(long actual, Compare.CompareOp op, long expected) {
        switch (op) {
            case EQUAL:
                return actual == expected;
            case NOT_EQUAL:
                return actual != expected;
            case GREATER:
                return actual > expected;
            case LESS:
                return actual < expected;
            case GREATER_EQUAL:
                return actual >= expected;
            case LESS_EQUAL:
                return actual <= expected;
            default:
                return false;
        }
    }

    /**
     * 在事务中执行 PUT 操作（支持事务上下文）
     */
    private TxnResponse.OpResult executePutInTxn(String key, String value,
                                                  java.util.Map<String, String> txnContext,
                                                  java.util.Map<String, MVCCStore.KeyVersion> txnVersionContext,
                                                  long mainRev,
                                                  int subRev) {
        // etcd 事务语义：所有操作共享同一个 mainRev，通过 subRev 区分顺序
        MVCCStore.Revision txnRev = new MVCCStore.Revision(mainRev, subRev);
        
        // 使用指定的 revision 写入（不生成新的 revision）
        mvccStore.putWithRevision(key, value, txnRev);

        // 获取 Key 的版本信息（从 MVCCStore 获取）
        MVCCStore.KeyVersion keyVersion = mvccStore.getKeyVersion(key);

        long createRevision;
        long version;

        if (keyVersion == null) {
            // 新 Key
            createRevision = mainRev;
            version = 1;
        } else {
            // 已存在的 Key
            createRevision = keyVersion.createRevision;
            version = keyVersion.version;
        }

        // 更新事务上下文（让后续操作可见）
        txnContext.put(key, value);
        txnVersionContext.put(key, new MVCCStore.KeyVersion(createRevision, mainRev, version));

        // 生成 Watch 事件
        publishWatchEvent(WatchEvent.put(key, value, mainRev, createRevision, version));

        return TxnResponse.OpResult.success(Operation.OpType.PUT, key, version, mainRev);
    }

    /**
     * 在事务中执行 DELETE 操作（支持事务上下文）
     */
    private TxnResponse.OpResult executeDeleteInTxn(String key,
                                                     java.util.Map<String, String> txnContext,
                                                     java.util.Map<String, MVCCStore.KeyVersion> txnVersionContext,
                                                     long mainRev,
                                                     int subRev) {
        // etcd 事务语义：所有操作共享同一个 mainRev，通过 subRev 区分顺序
        MVCCStore.Revision txnRev = new MVCCStore.Revision(mainRev, subRev);
        
        // 使用指定的 revision 删除（不生成新的 revision）
        mvccStore.deleteWithRevision(key, txnRev);

        // 更新事务上下文（标记为已删除）
        txnContext.put(key, null);  // null 表示已删除
        txnVersionContext.remove(key);

        // 生成 Watch 事件
        publishWatchEvent(WatchEvent.delete(key, mainRev));

        return TxnResponse.OpResult.success(Operation.OpType.DELETE, key, 0, mainRev);
    }

    /**
     * 在事务中执行 GET 操作（支持事务上下文）
     */
    private TxnResponse.OpResult executeGetInTxn(String key,
                                                  java.util.Map<String, String> txnContext,
                                                  java.util.Map<String, MVCCStore.KeyVersion> txnVersionContext) {
        // 优先从事务上下文获取（事务内可见性）
        if (txnContext.containsKey(key)) {
            String value = txnContext.get(key);
            MVCCStore.KeyVersion keyVersion = txnVersionContext.get(key);

            if (value == null) {
                // 在事务中被删除了
                return TxnResponse.OpResult.getSuccess(key, null, 0, getCurrentRevision());
            }

            long version = (keyVersion != null) ? keyVersion.version : 0;
            long revision = (keyVersion != null) ? keyVersion.modRevision : getCurrentRevision();
            return TxnResponse.OpResult.getSuccess(key, value, version, revision);
        }

        // 从 MVCCStore 获取
        MVCCStore.KeyValue mvccKv = mvccStore.getLatest(key);
        String value = (mvccKv != null) ? mvccKv.getValue() : null;
        MVCCStore.KeyVersion keyVersion = mvccStore.getKeyVersion(key);

        long version = (keyVersion != null) ? keyVersion.version : 0;
        long revision = (keyVersion != null) ? keyVersion.modRevision : getCurrentRevision();

        return TxnResponse.OpResult.getSuccess(key, value, version, revision);
    }

    /**
     * 执行单个操作（支持事务上下文）
     *
     * @param op 操作
     * @param txnContext 事务上下文（存储事务内的临时修改）
     * @param txnVersionContext 事务版本上下文
     * @return 操作结果
     */
    private TxnResponse.OpResult executeOperationWithContext(Operation op,
                                                              java.util.Map<String, String> txnContext,
                                                              java.util.Map<String, MVCCStore.KeyVersion> txnVersionContext,
                                                              long mainRev,
                                                              int subRev) {
        if (op == null) {
            return TxnResponse.OpResult.failure(null, null, "Null operation");
        }

        String key = op.getKey();

        switch (op.getType()) {
            case PUT:
                return executePutInTxn(key, op.getValue(), txnContext, txnVersionContext, mainRev, subRev);
            case DELETE:
                return executeDeleteInTxn(key, txnContext, txnVersionContext, mainRev, subRev);
            case GET:
                return executeGetInTxn(key, txnContext, txnVersionContext);
            case TXN:
                // 嵌套事务 - 传递上下文和 revision
                TxnResponse nestedResponse = executeTransactionWithContext(op.getNestedTxn(), txnContext, txnVersionContext, mainRev, subRev);
                return TxnResponse.OpResult.builder()
                        .success(nestedResponse.isSucceeded())
                        .type(Operation.OpType.TXN)
                        .build();
            default:
                return TxnResponse.OpResult.failure(op.getType(), key, "Unknown operation type");
        }
    }

    /**
     * 执行事务（支持外部上下文）- 用于嵌套事务
     * 
     * @param mainRev 事务共享的主版本号（etcd 语义：一个事务内所有操作共享同一个 mainRev）
     * @param subRevBase 子版本号起始值，用于区分同一事务内不同操作的顺序
     */
    private TxnResponse executeTransactionWithContext(TxnRequest txnRequest,
                                                       java.util.Map<String, String> txnContext,
                                                       java.util.Map<String, MVCCStore.KeyVersion> txnVersionContext,
                                                       long mainRev,
                                                       int subRevBase) {
        try {
            // 1. 评估所有 compare 条件
            boolean allConditionsMet = evaluateCompares(txnRequest.getCompares());

            // 2. 根据条件结果选择执行的操作列表
            java.util.List<Operation> opsToExecute = allConditionsMet
                    ? txnRequest.getSuccess()
                    : txnRequest.getFailure();

            // 3. 执行操作列表（使用传入的上下文和共享的 mainRev）
            java.util.Map<Integer, TxnResponse.OpResult> results = new java.util.HashMap<>();
            for (int i = 0; i < opsToExecute.size(); i++) {
                Operation op = opsToExecute.get(i);
                // 每个操作使用相同的 mainRev，但不同的 subRev（递增）
                TxnResponse.OpResult result = executeOperationWithContext(op, txnContext, txnVersionContext, mainRev, subRevBase + i);
                results.put(i, result);
            }

            // 4. 构建响应
            if (allConditionsMet) {
                return TxnResponse.success(opsToExecute, results);
            } else {
                return TxnResponse.failure(opsToExecute, results);
            }

        } catch (Exception e) {
            LOG.error("Failed to execute nested transaction", e);
            return TxnResponse.error("Nested transaction failed: " + e.getMessage());
        }
    }
}
