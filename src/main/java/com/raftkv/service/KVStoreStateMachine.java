package com.raftkv.service;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.raftkv.entity.KVResponse;
import com.raftkv.entity.KVTask;
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
 * - 这保证了所有节点的状态机最终会达到相同的状态
 * - 这就是所谓的"状态机安全性"（State Machine Safety）
 * 
 * 本实现的特点：
 * - 使用内存 HashMap 存储数据
 * - 每次 apply 后持久化到本地文件（kv-store-data.json）
 * - 支持快照功能，加速节点恢复
 */
public class KVStoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(KVStoreStateMachine.class);

    // KV 数据存储，使用 HashMap 实现
    // 所有对 kvStore 的访问都需要加锁，保证线程安全
    private final Map<String, String> kvStore = new HashMap<>();
    
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
    
    // 持久化文件路径
    // 每次 apply 操作后，会将数据保存到这个文件
    private final String dataFilePath;
    
    // 幂等请求缓存的持久化文件路径
    // 用于节点重启后恢复已处理请求的缓存，保证幂等性
    private final String requestCacheFilePath;
    
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
    
    // 读锁，用于保护 get 操作
    private final Object readLock = new Object();

    public KVStoreStateMachine(String dataDir) {
        this.dataFilePath = dataDir + File.separator + "kv-store-data.json";
        this.requestCacheFilePath = dataDir + File.separator + "request-cache.json";
        Path dataPath = Paths.get(dataDir);
        try {
            Files.createDirectories(dataPath);
        } catch (IOException e) {
            LOG.error("Failed to create data directory: {}", dataDir, e);
        }
        loadData();
        loadRequestCache();
    }

    private void loadData() {
        Path dataFile = Paths.get(dataFilePath);
        if (Files.exists(dataFile)) {
            try (BufferedReader reader = Files.newBufferedReader(dataFile, StandardCharsets.UTF_8)) {
                StringBuilder content = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line);
                }
                Map<String, String> data = objectMapper.readValue(content.toString(), Map.class);
                synchronized (kvStore) {
                    kvStore.putAll(data);
                }
                LOG.info("Loaded {} keys from data file", data.size());
            } catch (IOException e) {
                LOG.warn("Failed to load data file, starting fresh: {}", e.getMessage());
            }
        }
    }

    private void saveData() {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(dataFilePath), StandardCharsets.UTF_8)) {
            synchronized (kvStore) {
                objectMapper.writeValue(writer, kvStore);
            }
        } catch (IOException e) {
            LOG.error("Failed to save data file", e);
        }
    }

    /**
     * 加载幂等请求缓存
     * 
     * 节点重启时从磁盘恢复已处理请求的缓存，保证幂等性在重启后仍然有效。
     * 这是幂等性持久化的关键：即使节点重启，也能识别重复请求。
     */
    @SuppressWarnings("unchecked")
    private void loadRequestCache() {
        Path cacheFile = Paths.get(requestCacheFilePath);
        if (Files.exists(cacheFile)) {
            try (BufferedReader reader = Files.newBufferedReader(cacheFile, StandardCharsets.UTF_8)) {
                StringBuilder content = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line);
                }
                
                // 反序列化缓存数据
                Map<String, Object> cacheData = objectMapper.readValue(content.toString(), Map.class);
                
                // 恢复 processedRequests
                Map<String, Object> requests = (Map<String, Object>) cacheData.get("processedRequests");
                if (requests != null) {
                    for (Map.Entry<String, Object> entry : requests.entrySet()) {
                        // 将 Map 转换回 KVResponse
                        Object value = entry.getValue();
                        if (value instanceof Map) {
                            Map<String, Object> responseMap = (Map<String, Object>) value;
                            KVResponse response = new KVResponse();
                            response.setSuccess((Boolean) responseMap.getOrDefault("success", false));
                            response.setKey((String) responseMap.get("key"));
                            response.setValue((String) responseMap.get("value"));
                            response.setRequestId((String) responseMap.get("requestId"));
                            response.setError((String) responseMap.get("error"));
                            response.setLeaderEndpoint((String) responseMap.get("leaderEndpoint"));
                            response.setServedBy((String) responseMap.get("servedBy"));
                            processedRequests.put(entry.getKey(), response);
                        }
                    }
                    LOG.info("Loaded {} cached requests from disk", processedRequests.size());
                }
            } catch (IOException e) {
                LOG.warn("Failed to load request cache, starting fresh: {}", e.getMessage());
            }
        }
    }

    /**
     * 保存幂等请求缓存到磁盘
     * 
     * 每次 apply 操作后，将已处理请求的缓存持久化到磁盘。
     * 这样即使节点崩溃或重启，也能保证幂等性。
     */
    private void saveRequestCache() {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(requestCacheFilePath), StandardCharsets.UTF_8)) {
            Map<String, Object> cacheData = new HashMap<>();
            cacheData.put("processedRequests", processedRequests);
            cacheData.put("timestamp", System.currentTimeMillis());
            objectMapper.writeValue(writer, cacheData);
        } catch (IOException e) {
            LOG.error("Failed to save request cache", e);
        }
    }

    public String get(String key) {
        synchronized (readLock) {
            return kvStore.get(key);
        }
    }

    public Map<String, String> getAll() {
        synchronized (kvStore) {
            return new HashMap<>(kvStore);
        }
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

    public int getKeyCount() {
        synchronized (kvStore) {
            return kvStore.size();
        }
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
     * 获取当前节点的 endpoint（用于设置 servedBy）
     */
    private String getCurrentEndpoint() {
        // 这个方法需要从外部传入，暂时返回 null
        // 实际使用时在 RaftKVService 中设置 servedBy
        return null;
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
     * 5. 如果是新请求，根据操作类型（PUT/DELETE）更新内存中的 kvStore
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

        // 所有日志应用完成后，持久化到磁盘
        // 这样即使节点重启，数据也不会丢失
        saveData();
        
        // 持久化幂等请求缓存
        // 保证节点重启后仍能识别重复请求
        saveRequestCache();
    }

    private void applyTask(KVTask task) {
        synchronized (kvStore) {
            switch (task.getOp()) {
                case KVTask.OP_PUT:
                    kvStore.put(task.getKey(), task.getValue());
                    LOG.info("PUT: key={}, value={}", task.getKey(), task.getValue());
                    break;
                case KVTask.OP_DELETE:
                    kvStore.remove(task.getKey());
                    LOG.info("DELETE: key={}", task.getKey());
                    break;
                default:
                    LOG.warn("Unknown operation: {}", task.getOp());
            }
        }
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
     * 保存快照
     * 
     * 快照的作用：
     * Raft 日志会不断增长，如果节点重启后需要从头回放所有日志，恢复会很慢。
     * 快照是状态机在某个日志索引处的完整数据快照，可以大大加速节点恢复。
     * 
     * 调用时机：
     * 1. 当日志大小超过阈值时，Raft 会自动触发快照
     * 2. 也可以通过 CLI 命令手动触发
     * 
     * 快照流程：
     * 1. Raft 调用 onSnapshotSave，传入 SnapshotWriter
     * 2. 状态机将当前数据写入快照文件
     * 3. 调用 writer.addFile() 注册快照文件
     * 4. 完成后调用 done.run(Status.OK()) 通知 Raft
     * 
     * 注意事项：
     * - 快照保存是异步的，不能阻塞太久
     * - 需要保证快照的一致性（加锁）
     * - 快照失败会影响节点的恢复能力
     * 
     * @param writer 快照写入器
     * @param done 完成回调，必须调用 done.run() 通知 Raft 快照结果
     */
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        LOG.info("onSnapshotSave");
        try {
            // 加锁保证快照期间数据不被修改
            synchronized (kvStore) {
                // 将 kvStore 数据写入快照文件
                Path snapshotFile = Paths.get(writer.getPath() + File.separator + "kv-data");
                objectMapper.writeValue(snapshotFile.toFile(), kvStore);

                // 注册快照文件
                // addFile 告诉 Raft：这个文件是快照的一部分
                if (writer.addFile("kv-data")) {
                    // 快照成功，通知 Raft
                    done.run(Status.OK());
                    LOG.info("Snapshot saved successfully");
                } else {
                    // 注册文件失败
                    done.run(new Status(-1, "Failed to add file to snapshot writer"));
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to save snapshot", e);
            done.run(new Status(-1, "Failed to save snapshot: %s", e.getMessage()));
        }
    }

    /**
     * 加载快照
     * 
     * 调用时机：
     * 1. 节点启动时，如果有快照文件，会调用这个方法加载
     * 2. Leader 发送 InstallSnapshot RPC 时（新节点加入或落后太多的 Follower）
     * 
     * 快照加载流程：
     * 1. Raft 调用 onSnapshotLoad，传入 SnapshotReader
     * 2. 状态机从快照文件读取数据
     * 3. 清空当前状态机数据
     * 4. 加载快照数据到状态机
     * 5. 返回 true 表示成功，false 表示失败
     * 
     * 与 onSnapshotSave 的区别：
     * - onSnapshotSave 是异步的（通过 Closure 回调）
     * - onSnapshotLoad 是同步的（直接返回 boolean）
     * 
     * @param reader 快照读取器
     * @return true 表示加载成功，false 表示失败
     */
    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        LOG.info("onSnapshotLoad: path={}", reader.getPath());
        try {
            // 检查快照中是否有 kv-data 文件
            if (reader.getFileMeta("kv-data") == null) {
                LOG.warn("No kv-data file found in snapshot");
                return false;
            }

            // 读取快照文件
            Path snapshotFile = Paths.get(reader.getPath() + File.separator + "kv-data");
            if (Files.exists(snapshotFile)) {
                // 反序列化快照数据
                Map<String, String> data = objectMapper.readValue(snapshotFile.toFile(), Map.class);
                
                // 清空当前数据，加载快照数据
                synchronized (kvStore) {
                    kvStore.clear();
                    kvStore.putAll(data);
                }
                LOG.info("Snapshot loaded, {} keys", data.size());
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.error("Failed to load snapshot", e);
            return false;
        }
    }
}
