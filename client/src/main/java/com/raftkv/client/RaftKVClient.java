package com.raftkv.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raftkv.client.entity.KVResponse;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
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
                    currentLeader.set(response.getLeaderUrl());
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
}
