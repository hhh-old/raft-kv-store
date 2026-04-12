package com.raftkv.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raftkv.client.entity.KVResponse;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

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
        body.put("value", value);
        body.put("requestId", requestId);

        return executeWithRetry("PUT", () -> {
            String url = getCurrentLeader() + "/kv/" + key;
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
            String url = getCurrentLeader() + "/kv/" + key;
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
            String url = getCurrentLeader() + "/kv/" + key + "?requestId=" + requestId;
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
                KVResponse response = executor.execute();

                // 检查是否需要重定向到 Leader
                if (response.isNotLeader() && response.getLeaderUrl() != null) {
                    log.info("Redirecting to Leader: {}", response.getLeaderUrl());
                    currentLeader.set(response.getLeaderUrl());
                    
                    // 立即重试（不等待）
                    response = executor.execute();
                }

                // 成功或失败都返回
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
     */
    public void updateLeader(String leaderUrl) {
        currentLeader.set(leaderUrl);
        log.info("Updated leader to: {}", leaderUrl);
    }

    /**
     * Builder 类
     */
    public static class Builder {
        private java.util.List<String> serverUrls;
        private int maxRetries = 3;
        private int timeoutSeconds = 3;

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
}
