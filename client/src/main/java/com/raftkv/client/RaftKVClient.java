package com.raftkv.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.raftkv.entity.*;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Raft KV Store 客户端
 * 
 * 特性：
 * 1. 幂等性支持：自动生成或使用客户端提供的 requestId
 * 2. 自动重试：超时或失败时自动重试
 * 3. Leader 自动重定向：自动跟随 Leader 切换
 * 4. 指数退避：避免雪崩效应
 * 5. 故障转移：节点宕机时自动切换到其他健康节点
 * 6. 端点健康状态管理：自动检测不健康节点并跳过
 * 
 * 使用示例：
 * <pre>
 * RaftKVClient client = RaftKVClient.builder()
 *     .serverUrls(Arrays.asList(
 *         "http://localhost:9081",
 *         "http://localhost:9082",
 *         "http://localhost:9083"
 *     ))
 *     .maxRetries(3)
 *     .timeoutSeconds(3)
 *     .build();
 * 
 * // PUT 操作（节点宕机时自动切换）
 * KVResponse response = client.put("name", "Alice");
 * 
 * // GET 操作
 * KVResponse getResponse = client.get("name");
 * 
 * // DELETE 操作
 * KVResponse deleteResponse = client.delete("name");
 * </pre>
 */
@Slf4j
public class RaftKVClient {

    // ==================== 端点健康状态管理 ====================
    
    /**
     * 端点状态
     */
    private static class EndpointState {
        private final String url;
        private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
        private volatile long lastFailureTime = 0;
        
        EndpointState(String url) {
            this.url = url;
        }
        
        String getUrl() { return url; }
        
        int getConsecutiveFailures() {
            return consecutiveFailures.get();
        }
        
        void recordSuccess() {
            consecutiveFailures.set(0);
        }
        
        void recordFailure() {
            consecutiveFailures.incrementAndGet();
            lastFailureTime = System.currentTimeMillis();
        }
        
        boolean isHealthy() {
            return consecutiveFailures.get() < MAX_CONSECUTIVE_FAILURES;
        }
    }
    
    /** 端点最大连续失败次数，超过后标记为不健康 */
    private static final int MAX_CONSECUTIVE_FAILURES = 3;
    
    // ==================== 核心字段 ====================

    private final List<String> serverUrls;
    private final ConcurrentHashMap<String, EndpointState> endpointStates;
    private final int maxRetries;
    private final int timeoutSeconds;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    
    // 当前已知的 Leader 地址
    private final AtomicReference<String> currentLeader = new AtomicReference<>();
    
    // 活跃的 Watch 监听器
    private final CopyOnWriteArrayList<WatchListener> activeWatches = new CopyOnWriteArrayList<>();

    private RaftKVClient(Builder builder) {
        this.serverUrls = builder.serverUrls;
        this.maxRetries = builder.maxRetries;
        this.timeoutSeconds = builder.timeoutSeconds;
        this.objectMapper = createObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                .build();
        
        // 初始化端点状态映射（使用 ConcurrentHashMap 提升并发安全性）
        this.endpointStates = new ConcurrentHashMap<>();
        for (String url : serverUrls) {
            endpointStates.put(normalizeUrl(url), new EndpointState(normalizeUrl(url)));
        }
        
        // 初始化 Leader 为第一个节点
        if (!serverUrls.isEmpty()) {
            currentLeader.set(normalizeUrl(serverUrls.get(0)));
        }
    }

    /**
     * 便捷构造函数（使用默认配置）
     *
     * @param serverUrl 服务器地址，如 "http://localhost:9081"
     */
    public RaftKVClient(String serverUrl) {
        this.serverUrls = List.of(serverUrl);
        this.maxRetries = 3;
        this.timeoutSeconds = 8;
        this.objectMapper = createObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                .build();
        
        // 初始化端点状态映射（单节点模式）
        this.endpointStates = new ConcurrentHashMap<>();
        this.endpointStates.put(normalizeUrl(serverUrl), new EndpointState(normalizeUrl(serverUrl)));
        
        this.currentLeader.set(normalizeUrl(serverUrl));
    }

    /**
     * 创建 Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * PUT 操作（自动生成 requestId）
     * 
     * @param key 键
     * @param value 值
     * @return 响应
     */
    public KVResponse put(String key, String value) {
        String requestId = generateRequestId(key, value);
        return put(key, value, requestId);
    }

    /**
     * PUT 操作（指定 requestId，支持幂等）
     *
     * @param key 键
     * @param value 值
     * @param requestId 请求 ID（重试时必须相同）
     * @return 响应
     */
    public KVResponse put(String key, String value, String requestId) {
        return put(key, value, requestId, null);
    }

    /**
     * PUT 操作（带 Lease 绑定）
     *
     * @param key 键
     * @param value 值
     * @param requestId 请求 ID
     * @param leaseId 租约 ID
     * @return 响应
     */
    public KVResponse put(String key, String value, String requestId, Long leaseId) {
        log.debug("PUT request: key={}, value={}, requestId={}, leaseId={}", key, value, requestId, leaseId);

        Map<String, Object> body = new HashMap<>();
        body.put("key", key);
        body.put("value", value);
        body.put("requestId", requestId);
        if (leaseId != null) {
            body.put("leaseId", leaseId);
        }

        return executeApi("PUT",
                endpoint -> createJsonRequest(endpoint + "/kv", "PUT", body),
                httpResponse -> {
                    if (httpResponse.statusCode() == 200 || httpResponse.statusCode() == 301) {
                        return objectMapper.readValue(httpResponse.body(), KVResponse.class);
                    }
                    throw new RuntimeException("HTTP " + httpResponse.statusCode());
                });
    }

    /**
     * GET 操作
     *
     * @param key 键
     * @return 响应
     */
    public KVResponse get(String key) {
        log.debug("GET request: key={}", key);

        String encodedKey = java.net.URLEncoder.encode(key, java.nio.charset.StandardCharsets.UTF_8);

        return executeApi("GET",
                endpoint -> createJsonRequest(endpoint + "/kv?keyParam=" + encodedKey, "GET", null),
                httpResponse -> {
                    if (httpResponse.statusCode() == 200 || httpResponse.statusCode() == 301) {
                        return objectMapper.readValue(httpResponse.body(), KVResponse.class);
                    }
                    throw new RuntimeException("HTTP " + httpResponse.statusCode());
                });
    }

    /**
     * DELETE 操作（自动生成 requestId）
     *
     * @param key 键
     * @return 响应
     */
    public KVResponse delete(String key) {
        String requestId = generateRequestId("delete", key);
        return delete(key, requestId);
    }

    /**
     * DELETE 操作（指定 requestId，支持幂等）
     *
     * @param key 键
     * @param requestId 请求 ID（重试时必须相同）
     * @return 响应
     */
    public KVResponse delete(String key, String requestId) {
        log.debug("DELETE request: key={}, requestId={}", key, requestId);

        String encodedKey = java.net.URLEncoder.encode(key, java.nio.charset.StandardCharsets.UTF_8);

        return executeApi("DELETE",
                endpoint -> createJsonRequest(endpoint + "/kv?keyParam=" + encodedKey + "&requestId=" + requestId, "DELETE", null),
                httpResponse -> {
                    if (httpResponse.statusCode() == 200 || httpResponse.statusCode() == 301) {
                        return objectMapper.readValue(httpResponse.body(), KVResponse.class);
                    }
                    throw new RuntimeException("HTTP " + httpResponse.statusCode());
                });
    }

    /**
     * 健康检查
     *
     * 轮询所有配置的节点，只要有一个节点返回 HTTP 200 即认为健康。
     * 遇到 301 重定向时会自动跟随并更新 Leader。
     *
     * @return 是否健康
     */
    public boolean healthCheck() {
        for (String url : serverUrls) {
            try {
                String endpoint = normalizeUrl(url);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint + "/kv/health"))
                        .timeout(Duration.ofSeconds(timeoutSeconds))
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient.send(request,
                        HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    return true;
                }
                if (response.statusCode() == 301) {
                    String location = response.headers().firstValue("Location").orElse(null);
                    if (location != null) {
                        handleHttpRedirect(location);
                    }
                }
            } catch (Exception e) {
                log.debug("Health check failed on {}: {}", url, e.getMessage());
            }
        }
        return false;
    }

    /**
     * GET ALL 操作（获取所有键值对）
     *
     * 使用线性一致性读：只有 Leader 能处理此请求
     * 如果当前节点不是 Leader，会自动重定向到 Leader
     *
     * @return 所有键值对的 Map
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> getAll() {
        log.debug("GET_ALL request");

        Map<String, Object> result = executeMapApi("GET_ALL", "/kv/all", "GET", null);

        // 处理空响应
        if (result == null || result.isEmpty()) {
            return new HashMap<>();
        }

        return (Map<String, String>) (Map<?, ?>) result;
    }

    // ==================== Range 查询（对齐 etcd v3 API） ====================

    /**
     * Range 查询 - 对齐 etcd v3 Range API
     *
     * 支持的参数：
     * - key: 起始键
     * - rangeEnd: 结束键（可选，null 表示单一键查询）
     * - limit: 返回条数限制（0 表示无限制）
     * - revision: 读取指定版本（0 表示最新）
     * - sortOrder: 排序顺序
     * - sortTarget: 排序字段
     * - countOnly: 仅返回计数
     *
     * 示例：
     * // 单一键查询
     * RangeResponse resp = client.range("key1");
     *
     * // 范围查询 [key1, key3)
     * RangeResponse resp = client.range("key1", "key3");
     *
     * // 前缀查询（limit=10，排序）
     * RangeResponse resp = client.range(RangeRequest.builder()
     *     .key("/users/")
     *     .rangeEnd("/users0")
     *     .limit(10)
     *     .sortOrder(RangeRequest.SortOrder.ASC)
     *     .sortTarget(RangeRequest.SortTarget.KEY)
     *     .build());
     *
     * @param request Range 请求
     * @return Range 响应
     */
    public RangeResponse range(RangeRequest request) {
        log.debug("Range request: key={}, rangeEnd={}, limit={}, revision={}, sortOrder={}, sortTarget={}, countOnly={}",
                request.getKey(), request.getRangeEnd(), request.getLimit(), request.getRevision(),
                request.getSortOrder(), request.getSortTarget(), request.isCountOnly());

        try {
            final String jsonBody = objectMapper.writeValueAsString(request);

            return executeApi("RANGE",
                    endpoint -> {
                        String url = endpoint + "/kv/range";
                        HttpRequest httpRequest = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .timeout(Duration.ofSeconds(timeoutSeconds))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                                .build();
                        return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    },
                    httpResponse -> {
                        if (httpResponse.statusCode() != 200) {
                            throw new RuntimeException("Range failed: HTTP " + httpResponse.statusCode());
                        }
                        return objectMapper.readValue(httpResponse.body(), RangeResponse.class);
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize range request", e);
        }
    }

    /**
     * 便捷方法：单一键查询
     */
    public RangeResponse range(String key) {
        return range(RangeRequest.builder().key(key).build());
    }

    /**
     * 便捷方法：范围查询
     */
    public RangeResponse range(String key, String rangeEnd) {
        return range(RangeRequest.builder().key(key).rangeEnd(rangeEnd).build());
    }



    /**
     * 遍历所有配置的节点，找到当前 Leader 并更新内部 Leader 引用。
     *
     * @return Leader 的 HTTP 端点（如 http://127.0.0.1:9081），找不到返回 null
     */
    public String findLeader() {
        for (String url : serverUrls) {
            try {
                String endpoint = normalizeUrl(url);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint + "/kv/stats"))
                        .timeout(Duration.ofSeconds(timeoutSeconds))
                        .GET()
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    Map stats = objectMapper.readValue(response.body(), Map.class);
                    if ("LEADER".equals(stats.get("role"))) {
                        updateLeader(endpoint);
                        return endpoint;
                    }
                }
            } catch (Exception e) {
                log.debug("Find leader failed on {}: {}", url, e.getMessage());
            }
        }
        return null;
    }

    /**
     * 获取集群统计信息（查询当前 Leader）
     */
    public String getStats() {
        try {
            String url = getCurrentLeader() + "/kv/stats";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, 
                    HttpResponse.BodyHandlers.ofString());
            return response.body();
        } catch (Exception e) {
            log.error("Get stats failed", e);
            return null;
        }
    }

    /**
     * 标记端点为健康（成功响应）
     */
    private void markEndpointHealthy(String endpoint) {
        if (endpoint == null) return;
        String normalizedEndpoint = normalizeUrl(endpoint);
        EndpointState state = endpointStates.get(normalizedEndpoint);
        if (state != null) {
            state.recordSuccess();
        }
    }

    /**
     * 标记端点为不健康（连接失败）
     */
    private void markEndpointUnhealthy(String endpoint) {
        if (endpoint == null) return;
        String normalizedEndpoint = normalizeUrl(endpoint);
        EndpointState state = endpointStates.get(normalizedEndpoint);
        if (state != null) {
            state.recordFailure();
            log.info("Endpoint {} marked unhealthy, consecutive failures: {}", 
                    endpoint, state.getConsecutiveFailures());
        }
    }

    // 已合并到文件底部的统一版本 isConnectionFailure(Throwable)

    /**
     * 查找可用的端点（优先选择健康的端点）
     * 
     * @param triedEndpoints 已尝试过的端点集合
     * @return 可用的端点 URL，如果没有可用端点返回 null
     */
    private String findAvailableEndpoint(Set<String> triedEndpoints) {
        // 如果只有一个端点，直接返回（单节点模式）
        if (serverUrls.size() == 1) {
            return null;
        }

        // 1. 优先选择当前 Leader（如果还没尝试过）
        String leader = currentLeader.get();
        if (leader != null && !triedEndpoints.contains(leader)) {
            EndpointState leaderState = endpointStates.get(leader);
            if (leaderState == null || leaderState.isHealthy()) {
                return leader;
            }
        }

        // 2. 遍历所有端点，寻找健康的端点
        for (String url : serverUrls) {
            String normalizedUrl = normalizeUrl(url);
            if (!triedEndpoints.contains(normalizedUrl)) {
                EndpointState state = endpointStates.get(normalizedUrl);
                if (state != null && state.isHealthy()) {
                    return normalizedUrl;
                }
            }
        }

        // 3. 如果所有健康端点都尝试过，尝试不健康的端点（作为最后手段）
        for (String url : serverUrls) {
            String normalizedUrl = normalizeUrl(url);
            if (!triedEndpoints.contains(normalizedUrl)) {
                return normalizedUrl;
            }
        }

        return null;  // 所有端点都已尝试
    }

    /**
     * 计算指数退避时间（带随机抖动）
     * 
     * @param attempt 当前尝试次数
     * @return 退避时间（毫秒）
     */
    private long calculateBackoff(int attempt) {
        // 基础延迟 100ms，最大延迟 3s
        long baseDelay = 100;
        long maxDelay = 3000;
        long delay = Math.min(baseDelay * (long) Math.pow(2, attempt - 1), maxDelay);
        
        // 添加随机抖动（0-25%），避免惊群效应
        long jitter = (long) (delay * 0.25 * Math.random());
        return delay + jitter;
    }

    /**
     * 生成 requestId
     * 
     * 策略：基于操作类型、key 和时间戳
     * 保证相同业务操作生成相同 requestId（用于重试）
     */
    private String generateRequestId(String... parts) {
        StringBuilder sb = new StringBuilder("req-");
        for (String part : parts) {
            sb.append(part).append("-");
        }
        sb.append(System.currentTimeMillis());
        return sb.toString();
    }

    /**
     * 获取当前 Leader 地址（规范化后的 URL）
     */
    private String getCurrentLeader() {
        String leader = currentLeader.get();
        if (leader == null || leader.isEmpty()) {
            // 如果没有 Leader，使用第一个节点
            leader = normalizeUrl(serverUrls.get(0));
            currentLeader.set(leader);
        }
        return leader;
    }

    /**
     * 更新 Leader 地址
     * 自动处理 URL 格式（添加 http:// 前缀如果缺失）
     */
    public void updateLeader(String leaderUrl) {
        String normalizedUrl = normalizeUrl(leaderUrl);
        currentLeader.set(normalizedUrl);
        log.info("Updated leader to: {}", normalizedUrl);
    }

    /**
     * 规范化 URL，确保包含协议前缀
     */
    private String normalizeUrl(String url) {
        if (url == null || url.isEmpty()) {
            return url;
        }
        // 如果 URL 不以 http:// 或 https:// 开头，添加 http://
        if (!url.startsWith("http://") && !url.startsWith("https://")) {
            return "http://" + url;
        }
        return url;
    }

    /**
     * Builder 类
     */
    public static class Builder {
        private List<String> serverUrls;
        private int maxRetries = 3;
        // 客户端超时必须大于服务器 write-timeout (5000ms)
        // 设置为 8 秒，给网络延迟和重试留出足够时间
        private int timeoutSeconds = 8;

        public Builder serverUrls(List<String> serverUrls) {
            this.serverUrls = serverUrls;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder timeoutSeconds(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
            return this;
        }

        public RaftKVClient build() {
            if (serverUrls == null || serverUrls.isEmpty()) {
                throw new IllegalArgumentException("serverUrls cannot be null or empty");
            }
            return new RaftKVClient(this);
        }
    }

    // ==================== 统一重试引擎（重构核心） ====================

    /**
     * 重试上下文
     * 封装重试过程中的状态信息
     */
    private static class RetryContext {
        private final String operation;
        private final int maxRetries;
        private final Set<String> triedEndpoints = new HashSet<>();
        private Exception lastException;
        private int attempt;

        RetryContext(String operation, int maxRetries) {
            this.operation = operation;
            this.maxRetries = maxRetries;
        }

        String getOperation() { return operation; }
        int getMaxRetries() { return maxRetries; }
        int getAttempt() { return attempt; }
        Exception getLastException() { return lastException; }
        Set<String> getTriedEndpoints() { return triedEndpoints; }

        void incrementAttempt() { this.attempt++; }
        void setLastException(Exception e) { this.lastException = e; }
        void addTriedEndpoint(String endpoint) { this.triedEndpoints.add(endpoint); }

        boolean hasMoreAttempts() { return attempt < maxRetries; }
        boolean isFirstAttempt() { return attempt == 1; }
    }

    /**
     * API 执行器接口
     * 封装单个 HTTP 请求的执行逻辑
     *
     * @param <T> Response 类型
     */
    @FunctionalInterface
    private interface ApiExecutor<T> {
        HttpResponse<String> execute(String endpoint) throws Exception;
    }

    /**
     * Response 解析器接口
     * 将 HTTP 响应解析为具体的 Response 对象
     *
     * @param <T> Response 类型
     */
    @FunctionalInterface
    private interface ResponseParser<T> {
        T parse(HttpResponse<String> httpResponse) throws Exception;
    }

    /**
     * 统一的 API 执行引擎
     *
     * 整合了以下能力：
     * 1. 重试机制（指数退避 + 随机抖动）
     * 2. Leader 重定向处理
     * 3. 端点健康检测与故障转移
     * 4. 首次超时立即重试（避免雪球效应）
     *
     * @param operation   操作名称（用于日志）
     * @param executor    HTTP 请求执行器
     * @param parser      响应解析器
     * @param <T>         Response 类型
     * @return 解析后的 Response 对象
     */
    private <T extends RaftBaseResponse> T executeApi(
            String operation,
            ApiExecutor<T> executor,
            ResponseParser<T> parser) {

        RetryContext ctx = new RetryContext(operation, maxRetries);

        while (true) {
            ctx.incrementAttempt();

            try {
                String currentEndpoint = getCurrentLeader();
                log.debug("{} attempt {} using endpoint: {}", operation, ctx.getAttempt(), currentEndpoint);

                // 执行 HTTP 请求
                HttpResponse<String> httpResponse = executor.execute(currentEndpoint);

                // 处理 HTTP 301 重定向
                if (httpResponse.statusCode() == 301) {
                    String location = httpResponse.headers().firstValue("Location").orElse(null);
                    if (location != null) {
                        handleHttpRedirect(location);
                    }
                    continue;
                }

                // 检查 HTTP 状态码
                if (httpResponse.statusCode() != 200) {
                    // 尝试解析错误响应（允许返回业务错误，如 HTTP 400 的 BAD_REQUEST）
                    try {
                        T errorResponse = parser.parse(httpResponse);
                        markEndpointHealthy(currentEndpoint);
                        log.warn("{} failed: error={}", operation, errorResponse.getError());
                        return errorResponse;
                    } catch (Exception parseEx) {
                        throw new RuntimeException(operation + " failed: HTTP " + httpResponse.statusCode()
                                + ", body: " + httpResponse.body());
                    }
                }

                // 解析响应
                T response = parser.parse(httpResponse);

                // 标记端点健康
                markEndpointHealthy(currentEndpoint);

                // 处理 NOT_LEADER 重定向循环
                response = handleLeaderRedirect(operation, executor, parser, response);

                // 记录成功日志
                if (response.isSuccess()) {
                    log.debug("{} successful", operation);
                } else {
                    log.warn("{} failed: error={}", operation, response.getError());
                }

                return response;

            } catch (Exception e) {
                ctx.setLastException(e);
                log.warn("{} attempt {} failed: {}", operation, ctx.getAttempt(), e.getMessage());

                // 处理连接失败（故障转移）
                if (isConnectionFailure(e)) {
                    handleConnectionFailure(ctx);

                    if (ctx.hasMoreAttempts()) {
                        continue;
                    }
                }

                // 处理指数退避
                if (ctx.hasMoreAttempts()) {
                    if (ctx.isFirstAttempt() && e instanceof java.net.http.HttpTimeoutException) {
                        // 首次超时立即重试
                        log.debug("Timeout on first attempt, retrying immediately...");
                        continue;
                    }

                    long waitTime = calculateBackoff(ctx.getAttempt());
                    log.debug("Waiting {}ms before retry...", waitTime);
                    sleepQuietly(waitTime);
                    continue;
                }

                // 所有重试都失败
                break;
            }
        }

        throw new RuntimeException(operation + " failed after " + maxRetries + " attempts",
                ctx.getLastException());
    }

    /**
     * 处理 HTTP 301 重定向
     *
     * 从 Location 头安全提取 Leader 地址（scheme + host + port），
     * 忽略路径和查询参数，避免 URL 污染。
     */
    private void handleHttpRedirect(String location) {
        try {
            URI redirectUri = new URI(location);
            // 只保留协议、主机和端口
            String newLeader = redirectUri.getScheme() + "://" + redirectUri.getHost();
            if (redirectUri.getPort() > 0) {
                newLeader += ":" + redirectUri.getPort();
            }
            updateLeader(newLeader);
        } catch (Exception e) {
            // URI 解析失败时使用正则提取 scheme://host:port
            log.warn("Failed to parse redirect location: {}, using regex fallback", location);
            String newLeader = extractBaseUrl(location);
            if (newLeader != null) {
                updateLeader(newLeader);
            } else {
                log.error("Cannot extract leader from redirect location: {}", location);
            }
        }
    }

    /**
     * 从任意字符串中提取 base URL（scheme://host:port）
     *
     * 例如：
     * - "http://127.0.0.1:9081/kv/range?key=abc" → "http://127.0.0.1:9081"
     * - "https://example.com:8443" → "https://example.com:8443"
     */
    private String extractBaseUrl(String url) {
        if (url == null || url.isEmpty()) return null;
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(https?://[^/]+)");
        java.util.regex.Matcher matcher = pattern.matcher(url);
        return matcher.find() ? matcher.group(1) : null;
    }

    /**
     * 处理 Leader 重定向循环
     *
     * 安全限制：最多允许 3 次重定向，防止服务端返回错误的 Leader 信息导致无限循环。
     */
    private <T extends RaftBaseResponse> T handleLeaderRedirect(
            String operation,
            ApiExecutor<T> executor,
            ResponseParser<T> parser,
            T response) throws Exception {

        int redirectCount = 0;
        final int MAX_REDIRECTS = 3;

        while (response.isNotLeader() && response.getLeaderEndpoint() != null) {
            if (++redirectCount > MAX_REDIRECTS) {
                throw new RuntimeException(operation + " failed: too many leader redirects ("
                        + redirectCount + "), possible redirect loop");
            }

            log.info("Redirecting to leader ({}): {}", redirectCount, response.getLeaderEndpoint());
            updateLeader(response.getLeaderEndpoint());
            markEndpointHealthy(getCurrentLeader());

            // 在新 Leader 上重试
            HttpResponse<String> httpResponse = executor.execute(getCurrentLeader());

            if (httpResponse.statusCode() != 200) {
                throw new RuntimeException(operation + " failed after redirect: HTTP "
                        + httpResponse.statusCode());
            }

            response = parser.parse(httpResponse);
        }

        return response;
    }

    /**
     * 处理连接失败（故障转移）
     */
    private void handleConnectionFailure(RetryContext ctx) {
        String currentEndpoint = getCurrentLeader();
        markEndpointUnhealthy(currentEndpoint);
        ctx.addTriedEndpoint(currentEndpoint);

        String available = findAvailableEndpoint(ctx.getTriedEndpoints());
        if (available != null) {
            log.info("Connection failure, switching to: {}", available);
            updateLeader(available);
        } else {
            log.warn("No available endpoints, all endpoints have been tried");
        }
    }

    /**
     * 安全休眠（忽略中断）
     */
    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 判断是否为连接失败异常（需要故障转移）
     *
     * 统一版本：合并了两个历史版本的判断逻辑，覆盖所有已知的连接异常场景。
     *
     * @param e 异常对象
     * @return 是否为连接失败
     */
    private boolean isConnectionFailure(Throwable e) {
        if (e == null) return false;
        String msg = e.getMessage();
        return e instanceof java.net.ConnectException
            || e instanceof java.net.http.HttpTimeoutException
            || e instanceof java.io.IOException
            || (msg != null && (
                msg.contains("Connection refused")
                || msg.contains("Connection reset")
                || msg.contains("Connect timed out")
                || msg.contains("connection closed")
                || msg.contains("Unreachable")
                || msg.contains("Network is unreachable")
            ));
    }

    /**
     * 创建 ObjectMapper 实例
     * 配置忽略未知字段（兼容服务端返回的额外字段如 notLeader）
     */
    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    /**
     * 创建 JSON HTTP 请求
     *
     * @param url 请求 URL
     * @param method HTTP 方法 ("GET", "POST", "PUT", "DELETE")
     * @param body 请求体（GET 请求传 null）
     * @return HTTP 响应
     */
    private HttpResponse<String> createJsonRequest(String url, String method, Map<String, Object> body) throws Exception {
        log.debug("Sending {} request to: {}", method, url);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .header("Content-Type", "application/json");

        if ("PUT".equals(method)) {
            String json = objectMapper.writeValueAsString(body);
            requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(json));
        } else if ("POST".equals(method)) {
            String json = objectMapper.writeValueAsString(body);
            requestBuilder.POST(HttpRequest.BodyPublishers.ofString(json));
        } else if ("DELETE".equals(method)) {
            requestBuilder.DELETE();
        } else {
            requestBuilder.GET();
        }

        return httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 通用 Map 执行引擎（用于返回 Map 类型的请求，如 Lease 操作）
     * 不支持 Leader 重定向，因为 Map 类型无法判断是否为 NOT_LEADER
     */
    private Map<String, Object> executeMapApi(String operation, String path, String method, Map<String, Object> body) {
        RetryContext ctx = new RetryContext(operation, maxRetries);

        while (true) {
            ctx.incrementAttempt();

            try {
                String currentEndpoint = getCurrentLeader();
                log.debug("{} attempt {} using endpoint: {}", operation, ctx.getAttempt(), currentEndpoint);

                HttpResponse<String> httpResponse = createJsonRequest(currentEndpoint + path, method, body);

                // 处理 HTTP 301 重定向
                if (httpResponse.statusCode() == 301) {
                    String location = httpResponse.headers().firstValue("Location").orElse(null);
                    if (location != null) {
                        handleHttpRedirect(location);
                    }
                    continue;
                }

                // 检查 HTTP 状态码
                if (httpResponse.statusCode() != 200) {
                    throw new RuntimeException(operation + " failed: HTTP " + httpResponse.statusCode());
                }

                markEndpointHealthy(currentEndpoint);
                return objectMapper.readValue(httpResponse.body(), Map.class);

            } catch (Exception e) {
                ctx.setLastException(e);
                log.warn("{} attempt {} failed: {}", operation, ctx.getAttempt(), e.getMessage());

                // 处理连接失败
                if (isConnectionFailure(e)) {
                    handleConnectionFailure(ctx);

                    if (ctx.hasMoreAttempts()) {
                        continue;
                    }
                }

                // 处理指数退避
                if (ctx.hasMoreAttempts()) {
                    if (ctx.isFirstAttempt() && e instanceof java.net.http.HttpTimeoutException) {
                        continue;
                    }
                    sleepQuietly(calculateBackoff(ctx.getAttempt()));
                    continue;
                }

                break;
            }
        }

        throw new RuntimeException(operation + " failed after " + maxRetries + " attempts", ctx.getLastException());
    }

    // ==================== Watch 机制 ====================

    /**
     * 监听指定 Key 的变化
     * 
     * @param key 要监听的 Key
     * @param callback 事件回调函数
     * @return WatchListener 用于取消监听
     */
    public WatchListener watch(String key, Consumer<WatchEvent> callback) {
        return watch(key, false, 0, callback);
    }

    /**
     * 监听指定前缀的所有 Key 变化
     * 
     * @param prefix Key 前缀
     * @param callback 事件回调函数
     * @return WatchListener 用于取消监听
     */
    public WatchListener watchPrefix(String prefix, Consumer<WatchEvent> callback) {
        return watch(prefix, true, 0, callback);
    }
    
    /**
     * 从指定版本开始监听前缀的所有 Key 变化
     * 
     * @param prefix Key 前缀
     * @param startRevision 起始版本号
     * @param callback 事件回调函数
     * @return WatchListener 用于取消监听
     */
    public WatchListener watchPrefixFromRevision(String prefix, long startRevision, Consumer<WatchEvent> callback) {
        return watch(prefix, true, startRevision, callback);
    }

    /**
     * 从历史版本开始监听
     * 
     * @param key 要监听的 Key
     * @param startRevision 起始版本号
     * @param callback 事件回调函数
     * @return WatchListener 用于取消监听
     */
    public WatchListener watchFromRevision(String key, long startRevision, Consumer<WatchEvent> callback) {
        return watch(key, false, startRevision, callback);
    }

    /**
     * 创建 Watch 订阅（通用方法，etcd 风格一步式）
     * 非阻塞调用操作
     *
     * 客户端直接发送 POST /watch/stream，服务端立即返回 SSE 事件流，
     * 无需预先调用 POST /watch 获取 watchId。
     *
     * @param key Key 或前缀
     * @param isPrefix 是否为前缀匹配
     * @param startRevision 起始版本号（0 表示从当前开始）
     * @param callback 事件回调
     * @return WatchListener
     */
    private WatchListener watch(String key, boolean isPrefix, long startRevision, Consumer<WatchEvent> callback) {
        log.info("Creating watch: key={}, prefix={}, startRevision={}", key, isPrefix, startRevision);


        WatchListener listener = new WatchListener(null, key, isPrefix, startRevision, callback);
        activeWatches.add(listener);

        // 启动重连循环（异步）
        CompletableFuture.runAsync(() -> runWatchLoop(listener))
                .exceptionally(ex -> {
                    log.error("Watch loop ended unexpectedly: key={}", key, ex);
                    activeWatches.remove(listener);
                    return null;
                });

        return listener;
    }

    /**
     * Watch 重连循环：连接断开后指数退避重试
     *
     * etcd 风格：记录 lastReceivedRevision，断线后用 startRevision = last + 1 重新连接，
     * 服务端通过 Double-Check 回放断线期间的历史事件。
     */
    private void runWatchLoop(WatchListener listener) {
        while (!listener.isCancelled()) {
            try {
                startDirectWatchStream(listener);
                // 正常结束（服务端关闭连接且未抛异常）
                log.info("Watch stream ended normally: key={}", listener.getKey());
            } catch (Exception e) {
                if (listener.isCancelled()) {
                    break;
                }
                log.warn("Watch connection lost: key={}, error={}", listener.getKey(), e.getMessage());
            }

            if (listener.isCancelled()) {
                break;
            }

            // 指数退避等待
            listener.incrementReconnectAttempt();
            long delay = listener.getReconnectDelayMs();
            log.info("Watch reconnecting in {}ms: key={}, attempt={}",
                    delay, listener.getKey(), listener.getReconnectAttempt());

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.info("Watch loop ended: key={}", listener.getKey());
        activeWatches.remove(listener);
    }

    /**
     * 启动一步式 Watch 事件流监听（单次连接），支持跨节点故障转移
     */
    private void startDirectWatchStream(WatchListener listener) throws Exception {
        // 每次连接生成新的 watchId，旧连接由服务端在检测到断开后自动清理
        String newWatchId = UUID.randomUUID().toString();
        listener.updateWatchId(newWatchId);
        listener.resetReconnectState();

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("key", listener.getKey());
        requestBody.put("prefix", listener.isPrefix());
        requestBody.put("startRevision", listener.getNextStartRevision());
        requestBody.put("watchId", newWatchId);

        String jsonBody = objectMapper.writeValueAsString(requestBody);
        Set<String> triedEndpoints = new HashSet<>();
        Exception lastException = null;

        while (triedEndpoints.size() < serverUrls.size()) {
            String endpoint = getCurrentLeader();
            if (triedEndpoints.contains(endpoint)) {
                String available = findAvailableEndpoint(triedEndpoints);
                if (available == null) break;
                endpoint = available;
                updateLeader(endpoint);
            }
            triedEndpoints.add(endpoint);

            String url = endpoint + "/watch/stream";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .header("Accept", "text/event-stream")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .build();

            try {
                HttpResponse<java.io.InputStream> response = httpClient.send(request,
                        HttpResponse.BodyHandlers.ofInputStream());

                if (response.statusCode() != 200) {
                    // 服务端明确返回 503 且带 Leader-Url，切换到新 Leader 重试
                    if (response.statusCode() == 503) {
                        String leaderUrl = response.headers().firstValue("Leader-Url").orElse(null);
                        if (leaderUrl != null) {
                            updateLeader(leaderUrl);
                            continue;
                        }
                    }
                    throw new RuntimeException("HTTP " + response.statusCode());
                }

                readSseStream(listener, response.body());
                return; // 正常结束（服务端关闭连接或客户端取消）

            } catch (Exception e) {
                lastException = e;
                if (isConnectionFailure(e)) {
                    markEndpointUnhealthy(endpoint);
                }
                log.warn("Watch connection failed on {}: {}", endpoint, e.getMessage());
                // 继续尝试下一个可用节点
            }
        }

        throw lastException != null ? lastException : new RuntimeException("All endpoints failed for watch");
    }

    /**
     * 读取 SSE 流（复用的一步式和两步式通用逻辑）
     */
    private void readSseStream(WatchListener listener, java.io.InputStream inputStream) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            StringBuilder eventData = new StringBuilder();
            String eventType = "";

            while (!listener.isCancelled() && (line = reader.readLine()) != null) {
                if (line.startsWith("event:")) {
                    eventType = line.substring(6).trim();
                } else if (line.startsWith("data:")) {
                    // 多行 data 用换行符连接
                    if (eventData.length() > 0) {
                        eventData.append("\n");
                    }
                    eventData.append(line.substring(5).trim());
                } else if (line.isEmpty()) {
                    // 事件结束（空行），处理事件
                    if (eventData.length() > 0) {
                        processWatchEvent(listener, eventType, eventData.toString());
                        eventData.setLength(0);
                        eventType = "";
                    }
                }
            }
        }
    }

    /**
     * 处理 Watch 事件。
     * 控制类事件（init/heartbeat/error）若检测到连到非 Leader 节点，会抛异常触发外层重连。
     */
    private void processWatchEvent(WatchListener listener, String eventType, String data) throws Exception {
        if ("init".equals(eventType)) {
            log.debug("Watch init event: {}", data);
            Map<String, Object> init = objectMapper.readValue(data, Map.class);
            Boolean isLeader = (Boolean) init.get("isLeader");
            if (Boolean.FALSE.equals(isLeader)) {
                String leaderUrl = (String) init.get("leaderUrl");
                if (leaderUrl != null) updateLeader(leaderUrl);
                throw new IOException("Watch server is not leader (init)");
            }
            return;
        }
        if ("heartbeat".equals(eventType)) {
            log.debug("Watch heartbeat: {}", data);
            Map<String, Object> hb = objectMapper.readValue(data, Map.class);
            Boolean isLeader = (Boolean) hb.get("isLeader");
            if (Boolean.FALSE.equals(isLeader)) {
                String leaderUrl = (String) hb.get("leaderUrl");
                if (leaderUrl != null) updateLeader(leaderUrl);
                throw new IOException("Watch server is not leader (heartbeat)");
            }
            return;
        }
        if ("error".equals(eventType)) {
            log.warn("Watch error event: {}", data);
            Map<String, Object> err = objectMapper.readValue(data, Map.class);
            String code = (String) err.get("code");
            if ("NOT_LEADER".equals(code)) {
                String leaderUrl = (String) err.get("leaderUrl");
                // 忽略 null 或 "null" 字符串的无效 leaderUrl
                if (leaderUrl != null && !leaderUrl.isEmpty() && !"null".equals(leaderUrl)) {
                    updateLeader(leaderUrl);
                }
                throw new IOException("Watch server returned NOT_LEADER");
            }
            if ("COMPACT_REVISION".equals(code)) {
                Number oldestRevision = (Number) err.get("oldestRevision");
                log.warn("Watch revision compacted, oldest available: {}. Will reconnect from latest.", oldestRevision);
                listener.updateLastRevision(0); // 重置，下次从最新 revision 开始
                throw new IOException("Watch revision compacted");
            }
            return;
        }

        try {
            WatchEvent event = objectMapper.readValue(data, WatchEvent.class);
            log.debug("Received watch event: type={}, key={}, revision={}",
                    event.getType(), event.getKey(), event.getRevision());

            // 更新最后收到的 revision（用于断线后精确重连）
            listener.updateLastRevision(event.getRevision());
            // 收到有效事件后重置退避（连接健康）
            listener.resetReconnectState();

            listener.getCallback().accept(event);

        } catch (Exception e) {
            log.error("Failed to process watch event: {}", data, e);
        }
    }

    /**
     * 取消 Watch 监听。
     * 标记 listener 为已取消，runWatchLoop 检测后退出，并通知服务端关闭当前连接。
     */
    public void cancelWatch(WatchListener listener) {
        if (listener == null) {
            return;
        }
        listener.cancel();
        String currentWatchId = listener.getWatchId();
        if (currentWatchId != null) {
            try {
                // 使用 executeMapApi 获得 301 重定向、故障转移和重试能力
                executeMapApi("CANCEL_WATCH", "/watch/" + currentWatchId, "DELETE", null);
                log.info("Watch cancelled: key={}, watchId={}", listener.getKey(), currentWatchId);
            } catch (Exception e) {
                log.warn("Failed to cancel watch on server: watchId={}", currentWatchId, e);
            }
        }
    }

    /**
     * 获取当前全局版本号
     *
     * @return 当前 revision，失败返回 -1
     */
    public long getCurrentRevision() {
        try {
            Map<String, Object> result = executeMapApi("GET_REVISION", "/watch/revision", "GET", null);
            Number revision = (Number) result.get("revision");
            return revision != null ? revision.longValue() : -1;
        } catch (Exception e) {
            log.error("Failed to get current revision", e);
        }
        return -1;
    }

    /**
     * 关闭所有 Watch 监听
     */
    public void closeAllWatches() {
        for (WatchListener listener : activeWatches) {
            listener.cancel();
        }
        // 不立即 clear，由各个 runWatchLoop 检测到 cancelled 后自行移除
        log.info("All watches marked for closing");
    }

    // ==================== Lease 支持 ====================

    /**
     * 创建 Lease
     *
     * @param ttl 租约有效期（秒）
     * @return LeaseGrantResponse
     */
    public LeaseGrantResponse leaseGrant(int ttl) {
        log.debug("Lease grant request: ttl={}", ttl);
        Map<String, Object> body = new HashMap<>();
        body.put("ttl", ttl);

        return executeApi("LEASE_GRANT",
                endpoint -> createJsonRequest(endpoint + "/lease/grant", "POST", body),
                httpResponse -> {
                    if (httpResponse.statusCode() != 200) {
                        throw new RuntimeException("Lease grant failed: HTTP " + httpResponse.statusCode());
                    }
                    return objectMapper.readValue(httpResponse.body(), LeaseGrantResponse.class);
                });
    }

    /**
     * 撤销 Lease
     *
     * @param leaseId 租约 ID
     * @return true 如果成功
     */
    public boolean leaseRevoke(long leaseId) {
        log.debug("Lease revoke request: id={}", leaseId);
        Map<String, Object> body = new HashMap<>();
        body.put("id", leaseId);

        try {
            Map<String, Object> result = executeMapApi("LEASE_REVOKE", "/lease/revoke", "POST", body);
            return Boolean.TRUE.equals(result.get("success"));
        } catch (Exception e) {
            log.error("Lease revoke failed", e);
        }
        return false;
    }

    /**
     * Lease KeepAlive（续约）
     *
     * @param leaseId 租约 ID
     * @return true 如果成功
     */
    public boolean leaseKeepAlive(long leaseId) {
        log.debug("Lease keepalive request: id={}", leaseId);
        Map<String, Object> body = new HashMap<>();
        body.put("id", leaseId);

        try {
            Map<String, Object> result = executeMapApi("LEASE_KEEPALIVE", "/lease/keepalive", "POST", body);
            return Boolean.TRUE.equals(result.get("success"));
        } catch (Exception e) {
            log.error("Lease keepalive failed", e);
        }
        return false;
    }

    /**
     * 查询 Lease 剩余 TTL
     *
     * @param leaseId 租约 ID
     * @return 剩余秒数，-1 表示不存在
     */
    public long leaseTtl(long leaseId) {
        log.debug("Lease TTL request: id={}", leaseId);
        try {
            Map<String, Object> result = executeMapApi("LEASE_TTL", "/lease/ttl?id=" + leaseId, "GET", null);
            Number ttl = (Number) result.get("ttl");
            return ttl != null ? ttl.longValue() : -1;
        } catch (Exception e) {
            log.error("Lease TTL query failed", e);
        }
        return -1;
    }

    /**
     * 获取所有活跃 Lease ID
     *
     * @return Lease ID 列表
     */
    public List<Long> leaseLeases() {
        log.debug("Lease leases request");
        try {
            Map<String, Object> result = executeMapApi("LEASE_LIST", "/lease/leases", "GET", null);
            List<Number> leases = (List<Number>) result.get("leases");
            if (leases != null) {
                return leases.stream().map(Number::longValue).collect(Collectors.toList());
            }
        } catch (Exception e) {
            log.error("Lease leases query failed", e);
        }
        return new ArrayList<>();
    }

    // ==================== 事务支持 ====================

    /**
     * 执行事务
     */
    public TxnResponse transaction(TxnRequest txnRequest) {
        try {
            String jsonBody = objectMapper.writeValueAsString(txnRequest);

            return executeApi("TRANSACTION",
                    endpoint -> {
                        String url = endpoint + "/txn";
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .timeout(Duration.ofSeconds(timeoutSeconds))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                                .build();
                        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    },
                    httpResponse -> {
                        if (httpResponse.statusCode() != 200) {
                            throw new RuntimeException("Transaction failed: HTTP " + httpResponse.statusCode());
                        }
                        return objectMapper.readValue(httpResponse.body(), TxnResponse.class);
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize transaction request", e);
        }
    }

    /**
     * CAS（Compare-And-Swap）操作
     */
    public TxnResponse cas(String key, String expectedValue, String newValue) {
        Map<String, Object> request = new HashMap<>();
        request.put("key", key);
        request.put("expectedValue", expectedValue);
        request.put("newValue", newValue);

        try {
            String jsonBody = objectMapper.writeValueAsString(request);

            return executeApi("CAS",
                    endpoint -> {
                        String url = endpoint + "/txn/cas";
                        HttpRequest httpRequest = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .timeout(Duration.ofSeconds(timeoutSeconds))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                                .build();
                        return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    },
                    httpResponse -> {
                        if (httpResponse.statusCode() != 200) {
                            throw new RuntimeException("CAS failed: HTTP " + httpResponse.statusCode());
                        }
                        return objectMapper.readValue(httpResponse.body(), TxnResponse.class);
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize CAS request", e);
        }
    }
    
    /**
     * CAS 使用版本号
     */
    public TxnResponse casWithVersion(String key, long expectedVersion, String newValue) {
        TxnRequest txnRequest = TxnRequest.builder()
                .compares(List.of(Compare.version(key, Compare.CompareOp.EQUAL, expectedVersion)))
                .success(List.of(Operation.put(key, newValue)))
                .failure(List.of(Operation.get(key)))
                .build();
        return transaction(txnRequest);
    }

    // ==================== Compact 压缩（对齐 etcd v3 API） ====================

    /**
     * 压缩历史版本 - 对齐 etcd v3 Compaction API
     *
     * 压缩流程：
     * 1. 将压缩请求提交到 Leader
     * 2. Leader 将请求复制到集群
     * 3. 集群多数节点确认后，应用到状态机
     * 4. revision < compactRevision 的历史版本被删除
     *
     * etcd 语义：
     * - compaction 是幂等的，多次压缩同一 revision 都返回成功
     * - 压缩后无法再通过 Range 查询被压缩的历史版本
     * - Watch 的 startRevision 如果小于压缩版本会收到 COMPACT_REVISION 错误
     *
     * 示例：
     * // 压缩到当前 revision - 1（保留最新版本）
     * long currentRev = client.getCurrentRevision();
     * CompactResponse resp = client.compact(currentRev - 1);
     *
     * @param request 压缩请求
     * @return 压缩响应
     */
    public CompactResponse compact(CompactRequest request) {
        log.debug("Compact request: revision={}, requestId={}", request.getRevision(), request.getRequestId());

        final String jsonBody;
        try {
            jsonBody = objectMapper.writeValueAsString(request);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize compact request", e);
        }

        return executeApi("COMPACT",
                endpoint -> {
                    String url = endpoint + "/kv/compact";
                    HttpRequest httpRequest = HttpRequest.newBuilder()
                            .uri(URI.create(url))
                            .timeout(Duration.ofSeconds(timeoutSeconds))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                            .build();
                    return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                },
                httpResponse -> {
                    // 解析响应（包括错误响应）
                    return objectMapper.readValue(httpResponse.body(), CompactResponse.class);
                });
    }

    /**
     * 便捷方法：压缩到指定 revision
     */
    public CompactResponse compact(long revision) {
        return compact(CompactRequest.of(revision));
    }

    /**
     * 便捷方法：压缩到当前 revision - 1（保留最新版本）
     */
    public CompactResponse compactToPreviousRevision() {
        long currentRevision = getCurrentRevision();
        if (currentRevision <= 1) {
            throw new RuntimeException("Current revision is too small to compact");
        }
        return compact(currentRevision - 1);
    }

    /**
     * 获取分布式锁
     */
    public boolean acquireLock(String lockKey, String owner) {
        Map<String, Object> request = new HashMap<>();
        request.put("lockKey", lockKey);
        request.put("owner", owner);

        try {
            String jsonBody = objectMapper.writeValueAsString(request);

            TxnResponse response = executeApi("ACQUIRE_LOCK",
                    endpoint -> {
                        String url = endpoint + "/txn/lock";
                        HttpRequest httpRequest = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .timeout(Duration.ofSeconds(timeoutSeconds))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                                .build();
                        return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    },
                    httpResponse -> {
                        if (httpResponse.statusCode() != 200) {
                            throw new RuntimeException("Acquire lock failed: HTTP " + httpResponse.statusCode());
                        }
                        return objectMapper.readValue(httpResponse.body(), TxnResponse.class);
                    });
            return response.isSucceeded();
        } catch (Exception e) {
            log.error("Failed to acquire lock: {} for owner: {}", lockKey, owner, e);
            return false;
        }
    }

    /**
     * 释放分布式锁
     */
    public boolean releaseLock(String lockKey, String owner) {
        Map<String, Object> request = new HashMap<>();
        request.put("lockKey", lockKey);
        request.put("owner", owner);

        try {
            String jsonBody = objectMapper.writeValueAsString(request);

            TxnResponse response = executeApi("RELEASE_LOCK",
                    endpoint -> {
                        String url = endpoint + "/txn/unlock";
                        HttpRequest httpRequest = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .timeout(Duration.ofSeconds(timeoutSeconds))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                                .build();
                        return httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                    },
                    httpResponse -> {
                        if (httpResponse.statusCode() != 200) {
                            throw new RuntimeException("Release lock failed: HTTP " + httpResponse.statusCode());
                        }
                        return objectMapper.readValue(httpResponse.body(), TxnResponse.class);
                    });
            return response.isSucceeded();
        } catch (Exception e) {
            log.error("Failed to release lock: {} for owner: {}", lockKey, owner, e);
            return false;
        }
    }

    /**
     * Watch 监听器类（支持断线重连）
     */
    public static class WatchListener {
        private volatile String watchId;
        private final String key;
        private final boolean isPrefix;
        private final long initialStartRevision;
        private final Consumer<WatchEvent> callback;
        private volatile boolean cancelled = false;

        // 断线重连状态，客户端记录收到的事件的Revision，用于断线重连标记
        private volatile long lastReceivedRevision = 0;
        private volatile int reconnectAttempt = 0;
        private volatile long reconnectDelayMs = 1000;

        public WatchListener(String watchId, String key, boolean isPrefix,
                             long initialStartRevision, Consumer<WatchEvent> callback) {
            this.watchId = watchId;
            this.key = key;
            this.isPrefix = isPrefix;
            this.initialStartRevision = initialStartRevision;
            this.callback = callback;
        }

        public synchronized void updateWatchId(String watchId) {
            this.watchId = watchId;
        }

        public String getWatchId() {
            return watchId;
        }

        public String getKey() {
            return key;
        }

        public boolean isPrefix() {
            return isPrefix;
        }

        public Consumer<WatchEvent> getCallback() {
            return callback;
        }

        public void cancel() {
            this.cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void updateLastRevision(long revision) {
            if (revision > this.lastReceivedRevision) {
                this.lastReceivedRevision = revision;
            }
        }

        public long getNextStartRevision() {
            return lastReceivedRevision > 0 ? lastReceivedRevision + 1 : initialStartRevision;
        }

        public void resetReconnectState() {
            this.reconnectAttempt = 0;
            this.reconnectDelayMs = 1000;
        }

        public void incrementReconnectAttempt() {
            this.reconnectAttempt++;
            this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, 30000);
        }

        public long getReconnectDelayMs() {
            return reconnectDelayMs;
        }

        public int getReconnectAttempt() {
            return reconnectAttempt;
        }
    }
}
