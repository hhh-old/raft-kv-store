package com.raftkv.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raftkv.entity.*;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * // PUT 操作
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

    private final java.util.List<String> serverUrls;
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
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                .build();
        
        // 初始化 Leader 为第一个节点
        if (!serverUrls.isEmpty()) {
            currentLeader.set(serverUrls.get(0));
        }
    }

    /**
     * 便捷构造函数（使用默认配置）
     *
     * @param serverUrl 服务器地址，如 "http://localhost:9081"
     */
    public RaftKVClient(String serverUrl) {
        this.serverUrls = java.util.List.of(serverUrl);
        this.maxRetries = 3;
        this.timeoutSeconds = 8;
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                .build();
        this.currentLeader.set(serverUrl);
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
        log.debug("PUT request: key={}, value={}, requestId={}", key, value, requestId);

        Map<String, Object> body = new HashMap<>();
        body.put("key", key);  // 将 key 放入 body，支持带斜杠的 key
        body.put("value", value);
        body.put("requestId", requestId);

        return executeWithRetry("PUT", () -> {
            String url = getCurrentLeader() + "/kv";
            return executeRequest(url, "PUT", body);
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

        return executeWithRetry("GET", () -> {
            // 使用查询参数传递 key，支持带斜杠的 key
            String encodedKey = java.net.URLEncoder.encode(key, java.nio.charset.StandardCharsets.UTF_8);
            String url = getCurrentLeader() + "/kv?keyParam=" + encodedKey;
            return executeRequest(url, "GET", null);
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

        return executeWithRetry("DELETE", () -> {
            // 使用查询参数传递 key，支持带斜杠的 key
            String encodedKey = java.net.URLEncoder.encode(key, java.nio.charset.StandardCharsets.UTF_8);
            String url = getCurrentLeader() + "/kv?keyParam=" + encodedKey + "&requestId=" + requestId;
            return executeRequest(url, "DELETE", null);
        });
    }

    /**
     * 健康检查
     * 
     * @return 是否健康
     */
    public boolean healthCheck() {
        try {
            String url = getCurrentLeader() + "/kv/health";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, 
                    HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Health check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * GET ALL 操作（获取所有键值对）
     * 
     * 使用线性一致性读：只有 Leader 能处理此请求
     * 如果当前节点不是 Leader，会自动重定向到 Leader
     * 
     * @return 所有键值对的 Map
     */
    public Map<String, String> getAll() {
        log.debug("GET_ALL request");

        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("Attempt {} to GET_ALL using leader: {}", attempt, currentLeader.get());
                String url = getCurrentLeader() + "/kv/all";
                Map<String, String> result = executeGetAllRequest(url);
                log.debug("GET_ALL successful");
                return result;

            } catch (Exception e) {
                lastException = e;
                log.warn("GET_ALL attempt {} failed: {}", attempt, e.getMessage());

                // 处理重定向
                if (e.getMessage() != null && e.getMessage().contains("NOT_LEADER")) {
                    log.info("Retrying with new leader...");
                    continue;  // 立即重试，不等待
                }

                if (attempt < maxRetries) {
                    // 指数退避
                    long waitTime = (long) Math.pow(2, attempt - 1) * 100;
                    log.debug("Waiting {}ms before retry...", waitTime);
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }

        throw new RuntimeException("GET_ALL failed after " + maxRetries + " attempts", lastException);
    }

    /**
     * 执行 GET ALL 请求
     */
    @SuppressWarnings("unchecked")
    private Map<String, String> executeGetAllRequest(String url) throws Exception {
        log.debug("Sending GET_ALL request to: {}", url);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            // 解析 JSON 响应为 Map
            String body = response.body();
            if (body == null || body.isEmpty() || body.equals("{}")) {
                return new HashMap<>();
            }
            // 使用 ObjectMapper 解析 JSON
            return objectMapper.readValue(body, Map.class);
        } else if (response.statusCode() == 301) {
            // 处理重定向
            String location = response.headers().firstValue("Location")
                    .orElseThrow(() -> new RuntimeException("Redirect without Location header"));
            log.info("GET_ALL redirected to: {}", location);
            currentLeader.set(location.replace("/kv/all", ""));
            throw new RuntimeException("NOT_LEADER");
        } else {
            throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());
        }
    }

    /**
     * 获取集群统计信息
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
     * 执行请求（带重试和 Leader 重定向）
     */
    private KVResponse executeWithRetry(String operation, RequestExecutor executor) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("Attempt {} to {} using leader: {}", attempt, operation, currentLeader.get());
                KVResponse response = executor.execute();
                log.debug("Response: success={}, error={}, leaderUrl={}, isNotLeader={}",
                    response.isSuccess(), response.getError(), response.getLeaderUrl(), response.isNotLeader());

                // 处理重定向循环：直到不是重定向为止
                while (response.isNotLeader() && response.getLeaderUrl() != null) {
                    log.info("Redirecting to Leader: {}", response.getLeaderUrl());
                    updateLeader(response.getLeaderUrl());  // 使用 normalizeUrl 处理
                    log.debug("Updated leader, retrying with: {}", currentLeader.get());
                    response = executor.execute();
                    log.debug("After redirect - Response: success={}, error={}, isNotLeader={}",
                        response.isSuccess(), response.getError(), response.isNotLeader());
                }

                // 此时 response 不是重定向
                if (response.isSuccess()) {
                    log.debug("{} successful: requestId={}", operation, response.getRequestId());
                } else {
                    log.warn("{} failed: error={}", operation, response.getError());
                }
                return response;

            } catch (Exception e) {
                lastException = e;
                log.warn("{} attempt {} failed: {}", operation, attempt, e.getMessage());

                if (attempt < maxRetries) {
                    // 如果是超时且这是第一次尝试，不等待立即重试
                    // 原因：首次请求可能因为网络连接初始化而超时，实际服务端可能已处理成功
                    if (attempt == 1 && e instanceof java.net.http.HttpTimeoutException) {
                        log.debug("Timeout on first attempt, retrying immediately without waiting...");
                        continue;
                    }
                    // 指数退避
                    long waitTime = (long) Math.pow(2, attempt - 1) * 100;
                    log.debug("Waiting {}ms before retry...", waitTime);
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }

        throw new RuntimeException(operation + " failed after " + maxRetries + " attempts", lastException);
    }

    /**
     * 执行 HTTP 请求
     */
    private KVResponse executeRequest(String url, String method, Map<String, Object> body) throws Exception {
        log.debug("Sending {} request to: {}", method, url);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .header("Content-Type", "application/json");

        // 设置请求方法和 body
        if ("PUT".equals(method)) {
            String json = objectMapper.writeValueAsString(body);
            requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(json));
        } else if ("DELETE".equals(method)) {
            requestBuilder.DELETE();
        } else {
            requestBuilder.GET();
        }

        HttpRequest request = requestBuilder.build();

        // 发送请求
        HttpResponse<String> response = httpClient.send(request, 
                HttpResponse.BodyHandlers.ofString());

        // 解析响应
        if (response.statusCode() == 200 || response.statusCode() == 301) {
            return objectMapper.readValue(response.body(), KVResponse.class);
        } else {
            throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());
        }
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
     * 获取当前 Leader 地址
     */
    private String getCurrentLeader() {
        String leader = currentLeader.get();
        if (leader == null || leader.isEmpty()) {
            // 如果没有 Leader，使用第一个节点
            leader = serverUrls.get(0);
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
        private java.util.List<String> serverUrls;
        private int maxRetries = 3;
        // 客户端超时必须大于服务器 write-timeout (5000ms)
        // 设置为 8 秒，给网络延迟和重试留出足够时间
        private int timeoutSeconds = 8;

        public Builder serverUrls(java.util.List<String> serverUrls) {
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

    /**
     * 请求执行器接口
     */
    @FunctionalInterface
    private interface RequestExecutor {
        KVResponse execute() throws Exception;
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
     * 创建 Watch 订阅（通用方法）
     * 
     * @param key Key 或前缀
     * @param isPrefix 是否为前缀匹配
     * @param startRevision 起始版本号（0 表示从当前开始）
     * @param callback 事件回调
     * @return WatchListener
     */
    private WatchListener watch(String key, boolean isPrefix, long startRevision, Consumer<WatchEvent> callback) {
        log.info("Creating watch: key={}, prefix={}, startRevision={}", key, isPrefix, startRevision);

        try {
            // 1. 创建 Watch 订阅
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("key", key);
            requestBody.put("prefix", isPrefix);
            requestBody.put("startRevision", startRevision);

            String url = getCurrentLeader() + "/watch";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestBody)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to create watch: " + response.body());
            }

            Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
            String watchId = (String) result.get("watchId");
            long currentRevision = ((Number) result.get("currentRevision")).longValue();

            log.info("Watch created: id={}, currentRevision={}", watchId, currentRevision);

            // 2. 启动 SSE 监听线程
            WatchListener listener = new WatchListener(watchId, key, isPrefix, callback);
            activeWatches.add(listener);
            
            // 异步启动事件流监听，并处理异常
            CompletableFuture.runAsync(() -> startWatchStream(listener))
                    .exceptionally(ex -> {
                        log.error("Watch stream task failed: {}", watchId, ex);
                        listener.onError(ex instanceof Exception ? (Exception) ex : new RuntimeException(ex));
                        activeWatches.remove(listener);
                        return null;
                    });

            return listener;

        } catch (Exception e) {
            log.error("Failed to create watch", e);
            throw new RuntimeException("Failed to create watch", e);
        }
    }

    /**
     * 启动 Watch 事件流监听
     */
    private void startWatchStream(WatchListener listener) {
        try {
            String url = getCurrentLeader() + "/watch/" + listener.getWatchId();
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "text/event-stream")
                    .GET();
            // 长连接：不设置超时，让连接一直保持
            HttpRequest request = requestBuilder.build();

            HttpResponse<java.io.InputStream> response = httpClient.send(request, 
                    HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() != 200) {
                log.error("Watch stream failed: HTTP {}", response.statusCode());
                listener.onError(new RuntimeException("HTTP " + response.statusCode()));
                return;
            }

            // 读取 SSE 流
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(response.body()))) {
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

            log.info("Watch stream ended: {}", listener.getWatchId());
            listener.onComplete();

        } catch (Exception e) {
            log.error("Watch stream error: {}", listener.getWatchId(), e);
            listener.onError(e);
        } finally {
            activeWatches.remove(listener);
        }
    }

    /**
     * 处理 Watch 事件
     */
    private void processWatchEvent(WatchListener listener, String eventType, String data) {
        try {
            if ("init".equals(eventType)) {
                // 初始化事件
                log.debug("Watch init event: {}", data);
                return;
            }

            // 解析事件数据
            WatchEvent event = objectMapper.readValue(data, WatchEvent.class);
            log.debug("Received watch event: type={}, key={}, revision={}",
                    event.getType(), event.getKey(), event.getRevision());

            // 调用回调
            listener.getCallback().accept(event);

        } catch (Exception e) {
            log.error("Failed to process watch event: {}", data, e);
        }
    }

    /**
     * 取消 Watch 监听
     * 
     * @param watchId Watch ID
     */
    public void cancelWatch(String watchId) {
        activeWatches.stream()
                .filter(w -> w.getWatchId().equals(watchId))
                .findFirst()
                .ifPresent(WatchListener::cancel);

        try {
            String url = getCurrentLeader() + "/watch/" + watchId;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .DELETE()
                    .build();

            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            log.info("Watch cancelled: {}", watchId);

        } catch (Exception e) {
            log.warn("Failed to cancel watch on server: {}", watchId, e);
        }
    }

    /**
     * 获取当前全局版本号
     * 
     * @return 当前 revision，失败返回 -1
     */
    public long getCurrentRevision() {
        try {
            String url = getCurrentLeader() + "/watch/revision";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                Map<String, Object> result = objectMapper.readValue(response.body(), Map.class);
                return ((Number) result.get("revision")).longValue();
            }
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
        activeWatches.clear();
        log.info("All watches closed");
    }

    // ==================== 事务支持 ====================

    /**
     * 执行事务
     *
     * @param txnRequest 事务请求
     * @return 事务响应
     */
    public TxnResponse transaction(TxnRequest txnRequest) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("Attempt {} to execute transaction using leader: {}", attempt, currentLeader.get());

                String url = getCurrentLeader() + "/txn";
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(timeoutSeconds))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(txnRequest)))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                // 处理重定向
                if (response.statusCode() == 301) {
                    String newLeader = response.headers().firstValue("Location")
                            .map(loc -> loc.replace("http://", "").replace("/txn", ""))
                            .orElse(null);
                    if (newLeader != null) {
                        log.info("Redirecting to Leader: {}", newLeader);
                        updateLeader(newLeader);
                    }
                    continue;  // 重试
                }

                if (response.statusCode() != 200) {
                    throw new RuntimeException("Transaction failed: HTTP " + response.statusCode());
                }

                return objectMapper.readValue(response.body(), TxnResponse.class);

            } catch (Exception e) {
                lastException = e;
                log.warn("Transaction attempt {} failed: {}", attempt, e.getMessage());

                if (attempt < maxRetries) {
                    long waitTime = (long) Math.pow(2, attempt - 1) * 100;
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }

        throw new RuntimeException("Transaction failed after " + maxRetries + " attempts", lastException);
    }

    /**
     * CAS（Compare-And-Swap）操作
     *
     * @param key key
     * @param expectedValue 预期值
     * @param newValue 新值
     * @return 事务响应
     */
    public TxnResponse cas(String key, String expectedValue, String newValue) {
        Map<String, Object> request = new HashMap<>();
        request.put("key", key);
        request.put("expectedValue", expectedValue);
        request.put("newValue", newValue);

        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                String url = getCurrentLeader() + "/txn/cas";
                HttpRequest httpRequest = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(timeoutSeconds))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(request)))
                        .build();

                HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 301) {
                    String newLeader = response.headers().firstValue("Location")
                            .map(loc -> loc.replace("http://", "").replace("/txn/cas", ""))
                            .orElse(null);
                    if (newLeader != null) {
                        updateLeader(newLeader);
                    }
                    continue;
                }

                if (response.statusCode() != 200) {
                    throw new RuntimeException("CAS failed: HTTP " + response.statusCode());
                }

                return objectMapper.readValue(response.body(), TxnResponse.class);

            } catch (Exception e) {
                lastException = e;
                log.warn("CAS attempt {} failed: {}", attempt, e.getMessage());

                if (attempt < maxRetries) {
                    long waitTime = (long) Math.pow(2, attempt - 1) * 100;
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }

        throw new RuntimeException("CAS failed after " + maxRetries + " attempts", lastException);
    }

    /**
     * CAS 使用版本号
     *
     * @param key key
     * @param expectedVersion 预期版本号
     * @param newValue 新值
     * @return 事务响应
     */
    public TxnResponse casWithVersion(String key, long expectedVersion, String newValue) {
        TxnRequest txnRequest = TxnRequest.builder()
                .compares(java.util.List.of(Compare.version(key, Compare.CompareOp.EQUAL, expectedVersion)))
                .success(java.util.List.of(Operation.put(key, newValue)))
                .failure(java.util.List.of(Operation.get(key)))
                .build();

        return transaction(txnRequest);
    }

    /**
     * 获取分布式锁
     *
     * @param lockKey 锁的 key
     * @param owner 锁的持有者标识
     * @return true 如果获取锁成功
     */
    public boolean acquireLock(String lockKey, String owner) {
        Map<String, Object> request = new HashMap<>();
        request.put("lockKey", lockKey);
        request.put("owner", owner);

        try {
            String url = getCurrentLeader() + "/txn/lock";
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(request)))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 301) {
                String newLeader = response.headers().firstValue("Location")
                        .map(loc -> loc.replace("http://", "").replace("/txn/lock", ""))
                        .orElse(null);
                if (newLeader != null) {
                    updateLeader(newLeader);
                    return acquireLock(lockKey, owner);  // 重试
                }
                return false;
            }

            TxnResponse txnResponse = objectMapper.readValue(response.body(), TxnResponse.class);
            return txnResponse.isSucceeded();

        } catch (Exception e) {
            log.error("Failed to acquire lock: {} for owner: {}", lockKey, owner, e);
            return false;
        }
    }

    /**
     * 释放分布式锁
     *
     * @param lockKey 锁的 key
     * @param owner 锁的持有者标识
     * @return true 如果释放锁成功
     */
    public boolean releaseLock(String lockKey, String owner) {
        Map<String, Object> request = new HashMap<>();
        request.put("lockKey", lockKey);
        request.put("owner", owner);

        try {
            String url = getCurrentLeader() + "/txn/unlock";
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(request)))
                    .build();

            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 301) {
                String newLeader = response.headers().firstValue("Location")
                        .map(loc -> loc.replace("http://", "").replace("/txn/unlock", ""))
                        .orElse(null);
                if (newLeader != null) {
                    updateLeader(newLeader);
                    return releaseLock(lockKey, owner);  // 重试
                }
                return false;
            }

            TxnResponse txnResponse = objectMapper.readValue(response.body(), TxnResponse.class);
            return txnResponse.isSucceeded();

        } catch (Exception e) {
            log.error("Failed to release lock: {} for owner: {}", lockKey, owner, e);
            return false;
        }
    }

    /**
     * Watch 监听器类
     */
    public static class WatchListener {
        private final String watchId;
        private final String key;
        private final boolean isPrefix;
        private final Consumer<WatchEvent> callback;
        private volatile boolean cancelled = false;
        private volatile boolean completed = false;
        private Exception error = null;

        public WatchListener(String watchId, String key, boolean isPrefix, Consumer<WatchEvent> callback) {
            this.watchId = watchId;
            this.key = key;
            this.isPrefix = isPrefix;
            this.callback = callback;
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

        public void onComplete() {
            this.completed = true;
        }

        public void onError(Exception e) {
            this.error = e;
        }

        public boolean isCompleted() {
            return completed;
        }

        public Exception getError() {
            return error;
        }
    }
}
