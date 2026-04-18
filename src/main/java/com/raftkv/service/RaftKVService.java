package com.raftkv.service;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.raftkv.config.RaftProperties;
import com.raftkv.entity.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Raft KV Service - Manages the Raft node and provides KV operations.
 */
@Service
public class RaftKVService {

    private static final Logger LOG = LoggerFactory.getLogger(RaftKVService.class);

    @Autowired
    private RaftProperties raftProperties;
    
    @Autowired
    private WatchManager watchManager;

    private RaftGroupService raftGroupService;
    private Node node;
    private KVStoreStateMachine stateMachine;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean initialized = false;
    
    // 存储所有节点的 raft endpoint -> http endpoint 映射
    // 用于 Leader 重定向时查找正确的 HTTP URL
    // Key: raft endpoint (ip:port), Value: http endpoint (ip:httpPort)
    private final Map<String, String> raftToHttpEndpointMap = new HashMap<>();

    @PostConstruct
    public void init() {
        try {
            LOG.info("Initializing Raft node: {}", raftProperties);
            startRaftNode();
            initialized = true;
            LOG.info("Raft node started successfully");
        } catch (Exception e) {
            LOG.error("Failed to initialize Raft node", e);
            throw new RuntimeException("Failed to initialize Raft node", e);
        }
    }

    /**
     * 启动 Raft 节点的核心方法
     * 
     * 该方法完成以下工作：
     * 1. 解析当前节点的 endpoint（格式：ip:port）
     * 2. 创建数据存储目录（用于存放 Raft 日志、元数据、快照）
     * 3. 创建 RPC 服务器（用于节点间通信）
     * 4. 配置 Raft 节点选项（选举超时、存储路径、初始集群配置等）
     * 5. 创建并绑定状态机（KVStoreStateMachine）
     * 6. 启动 RaftGroupService，正式开始参与 Raft 协议
     * 
     * SOFAJRaft 关键概念：
     * - PeerId: 节点标识，格式为 ip:port:index
     * - NodeOptions: Raft 节点的配置选项
     * - Configuration: 集群配置，包含所有 peer 节点
     * - StateMachine: 状态机，负责应用已提交的日志条目
     * - RaftGroupService: Raft 组服务，管理节点的生命周期
     */
    private void startRaftNode() throws Exception {
        // 解析当前节点的 endpoint（例如：127.0.0.1:8081）
        // PeerId 是 SOFAJRaft 中节点的惟一标识，格式为 ip:port:index
        PeerId serverId = new PeerId();
        if (!serverId.parse(raftProperties.getEndpoint())) {
            throw new IllegalArgumentException("Failed to parse server endpoint: " + raftProperties.getEndpoint());
        }

        // 创建数据目录，用于存储 Raft 的三类核心数据：
        // 1. log: Raft 日志（WAL，Write-Ahead Log）
        // 2. raft_meta: Raft 元数据（currentTerm、votedFor 等）
        // 3. snapshot: 状态机快照（用于加速节点恢复）
        File dataDir = new File(raftProperties.getDataDir());
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        // 创建 RPC 服务器，用于节点间的 Raft 协议通信
        // 包括：选举请求/响应、AppendEntries（日志复制）、InstallSnapshot（快照传输）等
        // RaftRpcServerFactory 是 SOFAJRaft 提供的工厂类，底层使用 Bolt RPC 框架（基于 Netty）
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());

        // 创建并配置 Raft 节点选项
        NodeOptions nodeOptions = new NodeOptions();
        
        // 设置选举超时时间（毫秒）
        // Raft 协议中，Follower 在选举超时时间内没收到 Leader 的心跳就会发起选举
        // 
        // 超时层级设计（本地开发环境）：
        // - electionTimeout: 1000ms（Leader 选举，可配置）
        // - heartbeat: ~100ms（由 SOFAJRaft 自动计算 = electionTimeout / 10）
        // - writeTimeout: 5000ms（客户端写超时）
        // - clientTimeout: 8000ms（客户端 HTTP 超时）
        //
        // 这样设计保证：heartbeat << electionTimeout << writeTimeout << clientTimeout
        // 避免：1）不必要的 Leader 切换 2）客户端超时先于服务器完成
        //
        // SOFAJRaft 会自动在 [electionTimeout, 2*electionTimeout] 范围内随机选择，避免多节点同时竞选
        nodeOptions.setElectionTimeoutMs(raftProperties.getElectionTimeoutMs());
        
        // 是否禁用 CLI 服务（CLI 用于运行时动态修改集群配置，如添加/移除节点）
        // false 表示启用 CLI 服务，允许通过命令行工具动态管理集群
        nodeOptions.setDisableCli(false);

        // 设置 Raft 日志存储路径
        // Raft 日志是预写式日志（WAL），所有写操作必须先写入日志，才能应用到状态机
        // 这是保证数据不丢失和一致性的核心机制
        nodeOptions.setLogUri(raftProperties.getDataDir() + File.separator + "log");
        
        // 设置 Raft 元数据存储路径
        // 元数据包括：currentTerm（当前任期）、votedFor（当前任期投票给的候选人）
        // 这些元数据需要持久化，以保证节点重启后能正确恢复状态
        nodeOptions.setRaftMetaUri(raftProperties.getDataDir() + File.separator + "raft_meta");
        
        // 设置快照存储路径
        // 快照是状态机在某个日志索引处的完整数据快照
        // 作用：避免节点重启后需要从头回放所有日志，加速恢复过程
        nodeOptions.setSnapshotUri(raftProperties.getDataDir() + File.separator + "snapshot");
        
        // 配置快照触发参数，防止 Raft 日志无限增长
        // 
        // snapshotLogIndexMargin: 每积累多少条日志触发一次快照
        // - 默认 0（不基于日志数量触发）
        // - 建议值：10000（每 1 万条日志触发一次快照）
        // 
        // snapshotIntervalSecs: 每隔多少秒触发一次快照
        // - 默认 3600（1小时）
        // - 建议值：3600（1小时）或 7200（2小时）
        //
        // 触发条件：满足任一条件即触发
        // 快照生成后，Raft 会自动清理已被快照的日志，释放磁盘空间
        nodeOptions.setSnapshotLogIndexMargin(raftProperties.getSnapshotLogIndexMargin());
        nodeOptions.setSnapshotIntervalSecs(raftProperties.getSnapshotIntervalSecs());

        // 所有节点都需要设置初始集群配置
        // initializer 节点会基于这个配置发起第一次选举
        // non-initializer 节点会尝试连接到这些节点并加入集群
        Configuration initConf = new Configuration();
        
        // 解析 peers 字符串（格式：ip1:port1,ip2:port2,ip3:port3）
        // 这个配置定义了集群中有哪些节点
        if (!initConf.parse(raftProperties.getPeers())) {
            throw new IllegalArgumentException("Failed to parse peers: " + raftProperties.getPeers());
        }
        
        // 设置初始集群配置
        // 这个配置告诉 Raft 节点：集群初始成员有哪些
        nodeOptions.setInitialConf(initConf);
        LOG.info("Node configured with initial conf: {}", raftProperties.getPeers());
        
        // 初始化 raft endpoint -> http endpoint 映射
        // 支持每个节点有不同的 httpPort，不依赖固定 offset
        initRaftToHttpEndpointMap();
        
        if (raftProperties.isInitializer()) {
            LOG.info("This node is the initializer - will initiate the cluster");
        }

        // 创建状态机实例
        // 状态机是 Raft 协议的核心组件之一，负责：
        // 1. onApply: 应用已提交的日志条目到状态机（数据写入）
        // 2. onSnapshotSave/onSnapshotLoad: 快照的保存和加载
        // 3. onLeaderStart/onLeaderStop: 领导权变更通知
        // 状态机保证了所有节点以相同的顺序应用相同的操作，从而实现数据一致性
        stateMachine = new KVStoreStateMachine(raftProperties.getDataDir());
        
        // 注入 WatchManager，用于生成 Watch 事件
        stateMachine.setWatchManager(watchManager);
        
        // 将状态机绑定到 Raft 节点
        // 当 Raft 日志被大多数节点确认后（committed），就会调用 stateMachine.onApply() 应用该日志
        nodeOptions.setFsm(stateMachine);

        // 创建并启动 RaftGroupService
        // RaftGroupService 是 SOFAJRaft 提供的高级 API，封装了 Raft 节点的完整生命周期管理：
        // - 初始化内部组件（日志存储、状态机调用器、选举定时器等）
        // - 启动 RPC 服务器
        // - 启动 Raft 协议引擎
        // 
        // 参数说明：
        // - groupId: Raft 组 ID，用于区分不同的 Raft 集群（一个进程可以运行多个 Raft 组）
        // - serverId: 当前节点的 PeerId
        // - nodeOptions: 节点配置选项
        // - rpcServer: RPC 服务器实例
        raftGroupService = new RaftGroupService(
                raftProperties.getGroupId(),
                serverId,
                nodeOptions,
                rpcServer
        );

        // 正式启动 Raft 节点
        // 这个方法会：
        // 1. 初始化所有内部组件
        // 2. 启动 RPC 服务器监听其他节点的连接
        // 3. 启动选举定时器，开始参与选举
        // 4. 返回 Node 对象，用于后续的 Raft 操作（如 apply、转移领导权等）
        node = raftGroupService.start();

        // 等待节点初始化完成
        // Raft 节点启动后需要一些时间来加载元数据、恢复状态、参与选举
        // STATE_UNINITIALIZED 表示节点还未完成初始化
        int waitCount = 0;
        while (node.getNodeState() == State.STATE_UNINITIALIZED && waitCount < 30) {
            LOG.info("Waiting for node to initialize... (count={})", waitCount);
            Thread.sleep(1000);
            waitCount++;
        }

        // 打印节点角色信息
        // Raft 协议中的三种角色：
        // - LEADER: 领导者，负责接收客户端请求、复制日志
        // - FOLLOWER: 追随者，被动接收 Leader 的日志和心跳
        // - CANDIDATE: 候选人，临时角色，用于发起选举
        if (node.isLeader()) {
            LOG.info("This node is the LEADER");
        } else if (node.getNodeState() == State.STATE_FOLLOWER) {
            LOG.info("This node is a FOLLOWER");
        } else {
            LOG.warn("Node state after {} seconds: {}", waitCount, node.getNodeState());
        }

        LOG.info("Raft cluster peers: {}", raftProperties.getPeers());
    }

    public String getLeaderEndpoint() {
        if (node != null && node.getLeaderId() != null) {
            return node.getLeaderId().toString();
        }
        return stateMachine != null ? stateMachine.getLeaderEndpoint() : null;
    }

    public boolean isLeader() {
        return node != null && node.isLeader();
    }

    /**
     * 检查节点是否健康
     * 
     * 健康条件：
     * 1. 节点已初始化
     * 2. 节点状态正常（Leader 或 Follower）
     * 
     * 注意：
     * - 对于 3 节点集群，如果只剩 1 个节点，虽然它可能是 Leader，
     *   但无法形成多数派，无法提交新日志，从业务角度应该认为不健康
     * - 但 SOFAJRaft 的 API 限制，我们无法直接检测其他节点是否真的宕机
     * - 实际生产环境应该配合外部监控来判断集群健康状态
     * 
     * @return true 表示健康，false 表示不健康
     */
    public boolean isReady() {
        if (!initialized || node == null) {
            return false;
        }
        
        State state = node.getNodeState();
        
        // 检查节点状态
        // 只有 Leader 或 Follower 状态才算健康
        // CANDIDATE、TRANSFERRING 等状态都算不健康
        return state == State.STATE_LEADER || state == State.STATE_FOLLOWER;
    }
    
    /**
     * 检查集群是否有多数派可用
     * 
     * 这个方法用于更严格的集群健康检查
     * 如果返回 false，说明集群无法处理写请求
     * 
     * @return true 表示有多数派，false 表示没有
     */
    public boolean hasQuorum() {
        if (!initialized || node == null) {
            return false;
        }
        
        // 获取配置中的节点数
        int totalPeers = node.listPeers().size();
        int quorum = (totalPeers / 2) + 1;
        
        // 简化判断：如果当前是 Leader，假设自己是健康的
        // 实际上 SOFAJRaft 会阻止没有多数派的 Leader 提交日志
        if (node.isLeader()) {
            // Leader 如果能正常工作，说明至少自己是健康的
            // 但无法确定其他节点状态
            return true;
        }
        
        // Follower 只要能连接到 Leader 就算有多数派
        return node.getNodeState() == State.STATE_FOLLOWER;
    }
    
    /**
     * 初始化 raft endpoint -> http endpoint 映射
     * 
     * 通过 peer-http-endpoints 配置建立映射
     * 要求 raft.peers 和 raft.peer-http-endpoints 一一对应
     */
    private void initRaftToHttpEndpointMap() {
        String peerHttpEndpoints = raftProperties.getPeerHttpEndpoints();
        String peers = raftProperties.getPeers();
        
        if (peerHttpEndpoints == null || peerHttpEndpoints.isEmpty()) {
            LOG.error("raft.peer-http-endpoints is not configured! " +
                      "Please add peer-http-endpoints to your configuration.");
            return;
        }
        
        String[] raftPeers = peers.split(",");
        String[] httpPeers = peerHttpEndpoints.split(",");
        
        if (raftPeers.length != httpPeers.length) {
            LOG.error("Raft peers count ({}) != HTTP peers count ({}). " +
                      "Please ensure raft.peers and raft.peer-http-endpoints have the same number of entries.",
                    raftPeers.length, httpPeers.length);
            return;
        }
        
        for (int i = 0; i < raftPeers.length; i++) {
            String raftEndpoint = raftPeers[i].trim();
            String httpEndpoint = httpPeers[i].trim();
            raftToHttpEndpointMap.put(raftEndpoint, httpEndpoint);
            LOG.debug("Mapped raft endpoint {} to http endpoint {}", raftEndpoint, httpEndpoint);
        }
        LOG.info("Raft to HTTP endpoint mapping: {}", raftToHttpEndpointMap);
    }
    
    /**
     * 根据 raft endpoint 获取对应的 http endpoint
     * 
     * 用于 Leader 重定向时构造正确的 HTTP URL
     * 
     * @param raftEndpoint Raft endpoint (ip:port)
     * @return HTTP endpoint (ip:httpPort)，如果找不到返回 null
     */
    public String getHttpEndpointByRaftEndpoint(String raftEndpoint) {
        if (raftEndpoint == null) {
            return null;
        }
        
        // 移除可能的 index 后缀 (ip:port:index -> ip:port)
        String normalizedEndpoint = normalizeEndpoint(raftEndpoint);
        
        String httpEndpoint = raftToHttpEndpointMap.get(normalizedEndpoint);
        if (httpEndpoint == null) {
            LOG.warn("Cannot find HTTP endpoint for raft endpoint: {} (normalized: {})", 
                    raftEndpoint, normalizedEndpoint);
        }
        return httpEndpoint;
    }
    
    /**
     * 标准化 endpoint 格式
     * 移除 index 后缀：ip:port:index -> ip:port
     */
    private String normalizeEndpoint(String endpoint) {
        if (endpoint == null) {
            return null;
        }
        
        // 处理 ip:port:index 格式
        int lastColon = endpoint.lastIndexOf(':');
        if (lastColon > 0) {
            String afterLastColon = endpoint.substring(lastColon + 1);
            try {
                // 如果能解析为数字，检查是否是 index（通常是 0, 1, 2）
                Integer.parseInt(afterLastColon);
                // 再往前找一个冒号
                String beforeLastColon = endpoint.substring(0, lastColon);
                int secondLastColon = beforeLastColon.lastIndexOf(':');
                if (secondLastColon > 0) {
                    // 是 ip:port:index 格式，返回 ip:port
                    return beforeLastColon;
                }
            } catch (NumberFormatException e) {
                // 不是数字，说明是 ip:port 格式，直接返回
            }
        }
        return endpoint;
    }

    public String getCurrentEndpoint() {
        if (node != null && node.getNodeId() != null) {
            return node.getNodeId().getPeerId().toString();
        }
        return null;
    }

    /**
     * 向 Raft 集群提交一个 PUT 操作（支持幂等）
     * 
     * Raft 写流程：
     * 1. 检查当前节点是否是 Leader（只有 Leader 能处理写请求）
     * 2. 检查是否是重复请求（幂等去重）
     * 3. 如果是 Leader，将操作封装为 Task 并提交到 Raft 日志
     * 4. 等待日志被大多数节点确认（committed）
     * 5. 日志被应用到状态机后，返回成功响应
     * 
     * @param key 键
     * @param value 值
     * @param requestId 客户端生成的请求 ID（重试时必须相同，用于幂等去重）
     * @return 操作结果
     */
    public KVResponse put(String key, String value, String requestId) {
        // 检查当前节点是否是 Leader
        // Raft 协议规定：只有 Leader 能处理客户端的写请求
        // 这是为了保证日志复制的顺序一致性
        if (!isLeader()) {
            return redirectToLeader(key, value);
        }

        // 如果没有提供 requestId，生成一个（向后兼容）
        if (requestId == null || requestId.isEmpty()) {
            requestId = UUID.randomUUID().toString();
        }

        // 幂等检查：如果是重复请求，直接返回缓存的结果
        KVResponse cachedResult = stateMachine.getCachedResult(requestId);
        if (cachedResult != null) {
            LOG.info("Request already processed, returning cached result: requestId={}, key={}", 
                requestId, key);
            // 更新 servedBy 信息
            cachedResult.setServedBy(getCurrentEndpoint());
            return cachedResult;
        }

        try {
            // 创建 KVTask 对象，封装 PUT 操作
            // KVTask 包含了操作类型、key、value、时间戳等信息
            // 这个对象会被序列化后写入 Raft 日志
            KVTask task = KVTask.put(key, value, requestId);

            // 将 Task 序列化为字节数组
            byte[] data = objectMapper.writeValueAsBytes(task);
            ByteBuffer dataBuffer = ByteBuffer.wrap(data);
            
            // 创建 CountDownLatch 用于同步等待
            // Raft 的 apply 操作是异步的，需要等待日志被 committed 并应用到状态机
            CountDownLatch latch = new CountDownLatch(1);
            
            // 使用 AtomicReference 存储 apply 操作的状态
            AtomicReference<Status> statusRef = new AtomicReference<>();

            // 创建 Raft Task 对象
            // Task 是 SOFAJRaft 中表示一个待提交日志条目的核心类
            // 参数：
            // - dataBuffer: 日志数据（序列化的 KVTask）
            // - Closure: 回调接口，当日志被 committed 后调用
            Task raftTask = new Task(dataBuffer, new com.alipay.sofa.jraft.Closure() {
                @Override
                public void run(Status status) {
                    // 这个回调会在日志被大多数节点确认后执行
                    // status 表示操作结果：
                    // - status.isOk() == true: 日志成功提交并应用
                    // - status.isOk() == false: 操作失败（如 Leader 变更、网络分区等）
                    statusRef.set(status);
                    latch.countDown(); // 唤醒等待的线程
                }
            });

            // 将 Task 提交到 Raft 节点
            // 这个方法会：
            // 1. 将数据追加到本地 Raft 日志
            // 2. 并行复制到其他 Follower 节点
            // 3. 等待大多数节点确认
            // 4. 标记日志为 committed
            // 5. 应用到状态机（调用 stateMachine.onApply()）
            // 6. 执行 Closure 回调
            node.apply(raftTask);

            // 同步等待操作完成
            // 等待时长由 writeTimeout 配置（默认 3000ms）
            // 如果超时，说明集群可能出现问题（网络分区、节点宕机等）,结果未知，可能已经将日志提交到多数节点，也可能失败，所以需要状态机的幂等性。
            boolean success = latch.await(raftProperties.getWriteTimeout(), TimeUnit.MILLISECONDS);
            if (!success) {
                return KVResponse.failure("Write timeout", requestId);
            }

            // 检查操作状态
            Status status = statusRef.get();
            if (status.isOk()) {
                LOG.info("PUT successful: key={}, value={}, requestId={}", key, value, requestId);
                return KVResponse.success(key, value, requestId, getCurrentEndpoint());
            } else {
                // 操作失败，返回错误信息
                // 常见失败原因：
                // - 当前节点不再是 Leader（发生了 Leader 切换）
                // - 集群无法达成多数派（节点宕机过多）
                // - 网络分区
                return KVResponse.failure(status.getErrorMsg(), requestId);
            }
        } catch (Exception e) {
            LOG.error("PUT failed: key={}", key, e);
            return KVResponse.failure(e.getMessage(), requestId);
        }
    }

    /**
     * 向 Raft 集群提交一个 PUT 操作（向后兼容版本）
     * 
     * @deprecated 使用 {@link #put(String, String, String)} 代替，支持幂等
     * @param key 键
     * @param value 值
     * @return 操作结果
     */
    @Deprecated
    public KVResponse put(String key, String value) {
        return put(key, value, null);  // 自动生成 requestId
    }

    /**
     * 向 Raft 集群提交一个 DELETE 操作（支持幂等）
     * 
     * @param key 要删除的键
     * @param requestId 客户端生成的请求 ID（重试时必须相同，用于幂等去重）
     * @return 操作结果
     */
    public KVResponse delete(String key, String requestId) {
        if (!isLeader()) {
            return redirectToLeaderForDelete(key);
        }

        // 如果没有提供 requestId，生成一个（向后兼容）
        if (requestId == null || requestId.isEmpty()) {
            requestId = UUID.randomUUID().toString();
        }

        // 幂等检查
        KVResponse cachedResult = stateMachine.getCachedResult(requestId);
        if (cachedResult != null) {
            LOG.info("Delete request already processed, returning cached result: requestId={}, key={}", 
                requestId, key);
            cachedResult.setServedBy(getCurrentEndpoint());
            return cachedResult;
        }

        try {
            KVTask task = KVTask.delete(key, requestId);

            byte[] data = objectMapper.writeValueAsBytes(task);
            ByteBuffer dataBuffer = ByteBuffer.wrap(data);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Status> statusRef = new AtomicReference<>();

            Task raftTask = new Task(dataBuffer, new com.alipay.sofa.jraft.Closure() {
                @Override
                public void run(Status status) {
                    statusRef.set(status);
                    latch.countDown();
                }
            });

            node.apply(raftTask);

            boolean success = latch.await(raftProperties.getWriteTimeout(), TimeUnit.MILLISECONDS);
            if (!success) {
                return KVResponse.failure("Delete timeout", requestId);
            }

            Status status = statusRef.get();
            if (status.isOk()) {
                LOG.info("DELETE successful: key={}, requestId={}", key, requestId);
                return KVResponse.success(key, null, requestId, getCurrentEndpoint());
            } else {
                return KVResponse.failure(status.getErrorMsg(), requestId);
            }
        } catch (Exception e) {
            LOG.error("DELETE failed: key={}", key, e);
            return KVResponse.failure(e.getMessage(), requestId);
        }
    }

    /**
     * 向 Raft 集群提交一个 DELETE 操作（向后兼容版本）
     *
     * @deprecated 使用 {@link #delete(String, String)} 代替，支持幂等
     * @param key 要删除的键
     * @return 操作结果
     */
    @Deprecated
    public KVResponse delete(String key) {
        return delete(key, null);
    }

    /**
     * 从状态机中读取一个值（线性一致性读）
     * 
     * Raft 读流程（使用 ReadIndex 机制）：
     * 1. 检查当前节点是否是 Leader
     * 2. 调用 node.readIndex()，记录当前的 committed index
     * 3. Leader 向 Follower 发送心跳，确认自己仍是 Leader
     * 4. 收到大多数 Follower 确认后，等待状态机应用到 ReadIndex
     * 5. 安全地读取状态机数据，保证读到的是最新的 committed 数据
     * 6. 返回结果
     * 
     * ReadIndex 机制保证：
     * - 读取的数据一定是已提交的（committed）
     * - 即使发生 Leader 切换，也能读到最新数据
     * - 满足线性一致性（Linearizability）要求
     * 
     * 性能特点：
     * - 需要一次 RTT（与 Follower 通信）
     * - 延迟约 10-50ms（取决于网络）
     * - 吞吐量约 10,000 ops/sec（单线程）
     * 
     * @param key 键
     * @return 读取结果
     */
    public KVResponse get(String key) {
        String requestId = UUID.randomUUID().toString();

        // 1. 检查当前节点是否是 Leader
        // 如果不是 Leader，返回 Leader 信息让客户端重定向
        if (!isLeader()) {
            String leader = getLeaderHttpUrl();
            if (leader != null) {
                return KVResponse.builder()
                        .success(false)
                        .error("NOT_LEADER")
                        .leaderEndpoint(leader)
                        .requestId(requestId)
                        .build();
            }
            return KVResponse.failure("No leader available", requestId);
        }

        // 2. 使用 ReadIndex 进行线性一致性读
        try {
            CompletableFuture<KVResponse> future = new CompletableFuture<>();
            
            // 将 key 作为 requestContext 传递到回调中
            // 这样可以在回调中知道要读取哪个 key
            byte[] requestContext = key.getBytes(StandardCharsets.UTF_8);
            
            // 调用 ReadIndex
            // SOFAJRaft 会：
            // 1. 记录当前的 committed index（ReadIndex）
            // 2. 向 Follower 发送心跳，确认 Leader 身份
            // 3. 等待大多数 Follower 确认
            // 4. 等待状态机应用到 ReadIndex
            // 5. 调用回调函数
            node.readIndex(requestContext, new ReadIndexClosure() {
                @Override
                public void run(Status status, long index, byte[] reqCtx) {
                    if (status.isOk()) {
                        // ReadIndex 成功，可以安全读取
                        // 此时状态机已经应用到 index，保证读到的是最新的 committed 数据
                        try {
                            String readKey = new String(reqCtx, StandardCharsets.UTF_8);
                            String value = stateMachine.get(readKey);
                            LOG.debug("ReadIndex GET successful: key={}, value={}, index={}", 
                                readKey, value, index);
                            
                            future.complete(KVResponse.success(
                                readKey, value, requestId, getCurrentEndpoint()));
                        } catch (Exception e) {
                            LOG.error("ReadIndex callback error", e);
                            future.complete(KVResponse.failure(e.getMessage(), requestId));
                        }
                    } else {
                        // ReadIndex 失败（可能 Leader 切换、网络分区等）
                        LOG.warn("ReadIndex failed: {}", status.getErrorMsg());
                        future.complete(KVResponse.failure(
                            "Read failed: " + status.getErrorMsg(), requestId));
                    }
                }
            });
            
            // 3. 等待 ReadIndex 完成（设置超时）
            // 超时时间由 readTimeout 配置（默认 3000ms）
            KVResponse response = future.get(raftProperties.getReadTimeout(), TimeUnit.MILLISECONDS);
            return response;
            
        } catch (TimeoutException e) {
            LOG.error("ReadIndex timeout: key={}", key);
            return KVResponse.failure("Read timeout", requestId);
        } catch (Exception e) {
            LOG.error("ReadIndex error: key={}", key, e);
            return KVResponse.failure(e.getMessage(), requestId);
        }
    }

    /**
     * 获取所有键值对（线性一致性读）
     * 
     * 使用 ReadIndex 机制保证线性一致性：
     * 1. 记录当前的 committed index（ReadIndex）
     * 2. Leader 向 Follower 发送心跳，确认自己仍是 Leader
     * 3. 收到大多数 Follower 确认后，等待状态机应用到 ReadIndex
     * 4. 安全地读取状态机数据
     * 
     * 注意：与单个 key 的 get 操作不同，getAll 需要读取整个状态机快照
     * 
     * @return 所有键值对的副本，如果当前不是 Leader 则返回 null（需要重定向）
     */
    public Map<String, String> getAll() {
        // 非 Leader 节点返回 null，由 Controller 重定向到 Leader
        // 这是为了保证线性一致性：只有 Leader 能通过 ReadIndex 确认自己仍是 Leader
        if (!isLeader()) {
            LOG.warn("GET_ALL request received on non-leader node, redirecting to leader");
            return null;
        }

        // Leader 使用 ReadIndex 保证线性一致性
        try {
            CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
            
            // 使用特殊标记表示这是 getAll 操作
            byte[] requestContext = "__GET_ALL__".getBytes(StandardCharsets.UTF_8);
            
            node.readIndex(requestContext, new ReadIndexClosure() {
                @Override
                public void run(Status status, long index, byte[] reqCtx) {
                    if (status.isOk()) {
                        // ReadIndex 成功，状态机已应用到 index，可以安全读取
                        try {
                            Map<String, String> allData = stateMachine.getAll();
                            LOG.debug("ReadIndex GET_ALL successful: {} keys, index={}", 
                                allData.size(), index);
                            future.complete(allData);
                        } catch (Exception e) {
                            LOG.error("ReadIndex GET_ALL callback error", e);
                            future.completeExceptionally(e);
                        }
                    } else {
                        // ReadIndex 失败（Leader 可能已经变更）
                        LOG.warn("ReadIndex GET_ALL failed: {}", status.getErrorMsg());
                        future.completeExceptionally(
                            new RuntimeException("Read failed: " + status.getErrorMsg()));
                    }
                }
            });
            
            // 等待 ReadIndex 完成
            return future.get(raftProperties.getReadTimeout(), TimeUnit.MILLISECONDS);
            
        } catch (TimeoutException e) {
            LOG.error("ReadIndex GET_ALL timeout");
            throw new RuntimeException("GET_ALL timeout", e);
        } catch (Exception e) {
            LOG.error("ReadIndex GET_ALL error", e);
            throw new RuntimeException("GET_ALL failed", e);
        }
    }

    public ClusterStats getClusterStats() {
        String role = "UNKNOWN";
        if (node != null) {
            State state = node.getNodeState();
            switch (state) {
                case STATE_LEADER:
                    role = "LEADER";
                    break;
                case STATE_FOLLOWER:
                    role = "FOLLOWER";
                    break;
                case STATE_CANDIDATE:
                    role = "CANDIDATE";
                    break;
                default:
                    role = state.name();
            }
        }

        return ClusterStats.builder()
                .role(role)
                .endpoint(getCurrentEndpoint())
                .currentTerm(stateMachine.getCurrentTerm())
                .commitIndex(node != null ? node.getLastCommittedIndex() : 0)
                .lastApplied(node != null ? node.getLastAppliedLogIndex() : 0)
                .leaderEndpoint(getLeaderEndpoint())
                .peers(Arrays.asList(raftProperties.getPeers().split(",")))
                .keyCount(stateMachine.getKeyCount())
                .groupId(raftProperties.getGroupId())
                .nodeId(node != null ? node.getNodeId().toString() : null)
                .build();
    }

    /**
     * 获取当前全局版本号（用于 Watch 机制）
     * 
     * @return 当前 revision，如果状态机未初始化返回 0
     */
    public long getCurrentRevision() {
        return stateMachine != null ? stateMachine.getCurrentRevision() : 0;
    }

    private String getLeaderHttpUrl() {
        String leader = getLeaderEndpoint();
        if (leader == null) {
            return null;
        }

        // Calculate offset: all nodes use the same http-port = raft-port + offset
        int offset = raftProperties.getHttpPort() - raftProperties.getPort();

        // Parse leader's raft port (format: ip:port or ip:port:index)
        int lastColon = leader.lastIndexOf(':');
        if (lastColon > 0) {
            String leaderIp = leader.substring(0, lastColon);
            try {
                int leaderRaftPort = Integer.parseInt(leader.substring(lastColon + 1));
                return leaderIp + ":" + (leaderRaftPort + offset);
            } catch (NumberFormatException e) {
                LOG.warn("Failed to parse raft port from leader endpoint: {}", leader);
            }
        }
        return leader;
    }

    private KVResponse redirectToLeader(String key, String value) {
        String leader = getLeaderHttpUrl();
        return KVResponse.builder()
                .success(false)
                .error("NOT_LEADER")
                .leaderEndpoint(leader)
                .key(key)
                .value(value)
                .build();
    }

    private KVResponse redirectToLeaderForDelete(String key) {
        String leader = getLeaderHttpUrl();
        return KVResponse.builder()
                .success(false)
                .error("NOT_LEADER")
                .leaderEndpoint(leader)
                .key(key)
                .build();
    }

    // ==================== 事务支持 ====================

    /**
     * 执行事务操作
     *
     * 事务流程：
     * 1. 检查当前节点是否是 Leader
     * 2. 幂等检查（如果提供了 requestId）
     * 3. 将事务请求提交到 Raft 日志
     * 4. 等待日志被 committed 并应用到状态机
     * 5. 返回事务执行结果
     *
     * @param txnRequest 事务请求
     * @return 事务响应
     */
    public TxnResponse executeTransaction(TxnRequest txnRequest) {
        // 检查当前节点是否是 Leader
        if (!isLeader()) {
            return TxnResponse.notLeader(getLeaderHttpUrl());
        }

        // 如果没有提供 requestId，生成一个
        final String requestId;
        if (txnRequest.getRequestId() == null || txnRequest.getRequestId().isEmpty()) {
            requestId = UUID.randomUUID().toString();
            txnRequest.setRequestId(requestId);
        } else {
            requestId = txnRequest.getRequestId();
        }

        // 幂等检查：如果是重复请求，直接返回缓存的结果
        // 注意：这里简化处理，实际应该缓存 TxnResponse
        // 为了简化，暂时不缓存事务结果（事务通常不重复执行）

        try {
            // 创建 KVTask 对象，封装事务操作
            KVTask task = KVTask.txn(txnRequest, requestId);

            // 将 Task 序列化为字节数组
            // 注意：需要将 TxnRequest 序列化为 JSON 存储在 value 字段
            String txnJson = objectMapper.writeValueAsString(txnRequest);
            task.setValue(txnJson);

            byte[] data = objectMapper.writeValueAsBytes(task);
            ByteBuffer dataBuffer = ByteBuffer.wrap(data);

            // 创建 CountDownLatch 用于同步等待
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Status> statusRef = new AtomicReference<>();
            AtomicReference<TxnResponse> txnResponseRef = new AtomicReference<>();

            // 创建 Raft Task 对象
            Task raftTask = new Task(dataBuffer, new com.alipay.sofa.jraft.Closure() {
                @Override
                public void run(Status status) {
                    statusRef.set(status);
                    // 事务已经在 onApply 中执行，结果存储在 StateMachine 的缓存中
                    if (status.isOk()) {
                        TxnResponse txnResponse = stateMachine.getTxnResult(requestId);
                        if (txnResponse != null) {
                            txnResponseRef.set(txnResponse);
                            // 清理缓存
                            stateMachine.removeTxnResult(requestId);
                        }
                    }
                    latch.countDown();
                }
            });

            // 将 Task 提交到 Raft 节点
            node.apply(raftTask);

            // 等待操作完成（带超时）
            boolean success = latch.await(raftProperties.getWriteTimeout(), TimeUnit.MILLISECONDS);

            if (!success) {
                LOG.error("Transaction timeout: requestId={}", requestId);
                return TxnResponse.error("Transaction timeout");
            }

            Status status = statusRef.get();
            // 返回事务执行结果
            Status raftStatus = statusRef.get();
            if (raftStatus != null && !raftStatus.isOk()) {
                LOG.error("Transaction failed: requestId={}, error={}", requestId, raftStatus.getErrorMsg());
                return TxnResponse.error("Transaction failed: " + raftStatus.getErrorMsg());
            }

            // 返回事务执行结果（从 onApply 中获取）
            TxnResponse txnResponse = txnResponseRef.get();
            if (txnResponse != null) {
                return txnResponse;
            } else {
                return TxnResponse.error("No transaction response from state machine");
            }

        } catch (Exception e) {
            LOG.error("Failed to execute transaction: requestId={}", requestId, e);
            return TxnResponse.error("Failed to execute transaction: " + e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down Raft node...");
        if (raftGroupService != null) {
            raftGroupService.shutdown();
        }
        LOG.info("Raft node shutdown complete");
    }
}
