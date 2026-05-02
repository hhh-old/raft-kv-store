package com.raftkv.client;

import com.raftkv.client.config.ClientConfig;
import com.raftkv.client.serialization.JsonCodec;
import com.raftkv.client.transport.EndpointManager;
import com.raftkv.client.transport.HttpTransport;
import com.raftkv.client.transport.RetryEngine;
import com.raftkv.client.watch.WatchListener;
import com.raftkv.client.watch.WatchManager;
import com.raftkv.entity.*;
import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Raft KV Store 客户端（门面类）
 *
 * 特性：
 * 1. 幂等性支持：自动生成或使用客户端提供的 requestId
 * 2. 自动重试：超时或失败时自动重试
 * 3. Leader 自动重定向：自动跟随 Leader 切换
 * 4. 指数退避：避免雪崩效应
 * 5. 故障转移：节点宕机时自动切换到其他健康节点
 * 6. 端点健康状态管理：自动检测不健康节点并跳过
 *
 * 架构分层：
 * - API 层：RaftKVClient（本类），面向用户的统一入口
 * - 传输层：HttpTransport，HTTP 连接管理
 * - 路由层：EndpointManager，端点健康 + Leader 发现
 * - 重试层：RetryEngine，统一重试 + 故障转移
 * - Watch 层：WatchManager，SSE 流式事件 + 断线重连
 * - 序列化层：JsonCodec，JSON 编解码
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
 *     .timeoutSeconds(8)
 *     .build();
 *
 * KVResponse response = client.put("name", "Alice");
 * KVResponse getResponse = client.get("name");
 * </pre>
 */
@Slf4j
public class RaftKVClient {

    private final ClientConfig config;
    private final EndpointManager endpointManager;
    private final HttpTransport httpTransport;
    private final JsonCodec jsonCodec;
    private final RetryEngine retryEngine;
    private final WatchManager watchManager;

    private RaftKVClient(ClientConfig config) {
        this.config = config;
        this.endpointManager = new EndpointManager(config);
        this.httpTransport = new HttpTransport(config);
        this.jsonCodec = new JsonCodec();
        this.retryEngine = new RetryEngine(endpointManager, config);
        this.watchManager = new WatchManager(endpointManager, httpTransport, jsonCodec, retryEngine);
    }

    /**
     * 便捷构造函数（使用默认配置）
     */
    public RaftKVClient(String serverUrl) {
        this(ClientConfig.builder()
                .serverUrls(List.of(serverUrl))
                .build());
    }

    public static Builder builder() {
        return new Builder();
    }

    // ==================== KV 操作 ====================

    public KVResponse put(String key, String value) {
        return put(key, value, generateRequestId(key, value));
    }

    public KVResponse put(String key, String value, String requestId) {
        return put(key, value, requestId, null);
    }

    public KVResponse put(String key, String value, String requestId, Long leaseId) {
        log.debug("PUT request: key={}, value={}, requestId={}, leaseId={}", key, value, requestId, leaseId);
        Map<String, Object> body = new HashMap<>();
        body.put("key", key);
        body.put("value", value);
        body.put("requestId", requestId);
        if (leaseId != null) {
            body.put("leaseId", leaseId);
        }

        String jsonBody = toJson(body);
        return retryEngine.executeApi("PUT",
                endpoint -> httpTransport.sendPut(endpoint + "/kv", jsonBody),
                this::parseKvResponse);
    }

    public KVResponse get(String key) {
        log.debug("GET request: key={}", key);
        String encodedKey = java.net.URLEncoder.encode(key, java.nio.charset.StandardCharsets.UTF_8);

        return retryEngine.executeApi("GET",
                endpoint -> httpTransport.sendGet(endpoint + "/kv?keyParam=" + encodedKey),
                this::parseKvResponse);
    }

    public KVResponse delete(String key) {
        return delete(key, generateRequestId("delete", key));
    }

    public KVResponse delete(String key, String requestId) {
        log.debug("DELETE request: key={}, requestId={}", key, requestId);
        String encodedKey = java.net.URLEncoder.encode(key, java.nio.charset.StandardCharsets.UTF_8);

        return retryEngine.executeApi("DELETE",
                endpoint -> httpTransport.sendDelete(endpoint + "/kv?keyParam=" + encodedKey + "&requestId=" + requestId),
                this::parseKvResponse);
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getAll() {
        log.debug("GET_ALL request");
        Map<String, Object> result = retryEngine.executeMapApi("GET_ALL",
                endpoint -> httpTransport.sendGet(endpoint + "/kv/all"));
        return result == null || result.isEmpty() ? new HashMap<>() : (Map<String, String>) (Map<?, ?>) result;
    }

    // ==================== Range 查询 ====================

    public RangeResponse range(RangeRequest request) {
        log.debug("Range request: key={}, rangeEnd={}, limit={}, revision={}, sortOrder={}, sortTarget={}, countOnly={}",
                request.getKey(), request.getRangeEnd(), request.getLimit(), request.getRevision(),
                request.getSortOrder(), request.getSortTarget(), request.isCountOnly());

        String jsonBody = toJson(request);
        return retryEngine.executeApi("RANGE",
                endpoint -> httpTransport.sendPost(endpoint + "/kv/range", jsonBody),
                httpResponse -> {
                    if (httpResponse.statusCode() != 200) {
                        throw new RuntimeException("Range failed: HTTP " + httpResponse.statusCode());
                    }
                    return jsonCodec.readValue(httpResponse.body(), RangeResponse.class);
                });
    }

    public RangeResponse range(String key) {
        return range(RangeRequest.builder().key(key).build());
    }

    public RangeResponse range(String key, String rangeEnd) {
        return range(RangeRequest.builder().key(key).rangeEnd(rangeEnd).build());
    }

    // ==================== 集群管理 ====================

    public boolean healthCheck() {
        for (String url : endpointManager.getServerUrls()) {
            try {
                String endpoint = endpointManager.normalizeUrl(url);
                HttpResponse<String> response = httpTransport.sendGet(endpoint + "/kv/health");
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

    @SuppressWarnings("unchecked")
    public String findLeader() {
        for (String url : endpointManager.getServerUrls()) {
            try {
                String endpoint = endpointManager.normalizeUrl(url);
                HttpResponse<String> response = httpTransport.sendGet(endpoint + "/kv/stats");
                if (response.statusCode() == 200) {
                    Map<String, Object> stats = jsonCodec.getObjectMapper().readValue(response.body(), Map.class);
                    if ("LEADER".equals(stats.get("role"))) {
                        endpointManager.updateLeader(endpoint);
                        return endpoint;
                    }
                }
            } catch (Exception e) {
                log.debug("Find leader failed on {}: {}", url, e.getMessage());
            }
        }
        return null;
    }

    public String getStats() {
        try {
            String url = endpointManager.getCurrentLeader() + "/kv/stats";
            HttpResponse<String> response = httpTransport.sendGet(url);
            return response.body();
        } catch (Exception e) {
            log.error("Get stats failed", e);
            return null;
        }
    }

    public void updateLeader(String leaderUrl) {
        endpointManager.updateLeader(leaderUrl);
    }

    // ==================== Watch 机制 ====================

    public WatchListener watch(String key, Consumer<WatchEvent> callback) {
        return watchManager.watch(key, false, 0, callback);
    }

    public WatchListener watchPrefix(String prefix, Consumer<WatchEvent> callback) {
        return watchManager.watch(prefix, true, 0, callback);
    }

    public WatchListener watchPrefixFromRevision(String prefix, long startRevision, Consumer<WatchEvent> callback) {
        return watchManager.watch(prefix, true, startRevision, callback);
    }

    public WatchListener watchFromRevision(String key, long startRevision, Consumer<WatchEvent> callback) {
        return watchManager.watch(key, false, startRevision, callback);
    }

    public void cancelWatch(WatchListener listener) {
        watchManager.cancelWatch(listener);
    }

    public long getCurrentRevision() {
        return watchManager.getCurrentRevision();
    }

    public void closeAllWatches() {
        watchManager.closeAllWatches();
    }

    // ==================== Lease 支持 ====================

    public LeaseGrantResponse leaseGrant(int ttl) {
        log.debug("Lease grant request: ttl={}", ttl);
        Map<String, Object> body = new HashMap<>();
        body.put("ttl", ttl);

        String jsonBody = toJson(body);
        return retryEngine.executeApi("LEASE_GRANT",
                endpoint -> httpTransport.sendPost(endpoint + "/lease/grant", jsonBody),
                httpResponse -> {
                    if (httpResponse.statusCode() != 200) {
                        throw new RuntimeException("Lease grant failed: HTTP " + httpResponse.statusCode());
                    }
                    return jsonCodec.readValue(httpResponse.body(), LeaseGrantResponse.class);
                });
    }

    public boolean leaseRevoke(long leaseId) {
        log.debug("Lease revoke request: id={}", leaseId);
        Map<String, Object> body = new HashMap<>();
        body.put("id", leaseId);

        String jsonBody = toJson(body);
        try {
            Map<String, Object> result = retryEngine.executeMapApi("LEASE_REVOKE",
                    endpoint -> httpTransport.sendPost(endpoint + "/lease/revoke", jsonBody));
            return Boolean.TRUE.equals(result.get("success"));
        } catch (Exception e) {
            log.error("Lease revoke failed", e);
        }
        return false;
    }

    public boolean leaseKeepAlive(long leaseId) {
        log.debug("Lease keepalive request: id={}", leaseId);
        Map<String, Object> body = new HashMap<>();
        body.put("id", leaseId);

        String jsonBody = toJson(body);
        try {
            Map<String, Object> result = retryEngine.executeMapApi("LEASE_KEEPALIVE",
                    endpoint -> httpTransport.sendPost(endpoint + "/lease/keepalive", jsonBody));
            return Boolean.TRUE.equals(result.get("success"));
        } catch (Exception e) {
            log.error("Lease keepalive failed", e);
        }
        return false;
    }

    public long leaseTtl(long leaseId) {
        log.debug("Lease TTL request: id={}", leaseId);
        try {
            Map<String, Object> result = retryEngine.executeMapApi("LEASE_TTL",
                    endpoint -> httpTransport.sendGet(endpoint + "/lease/ttl?id=" + leaseId));
            Number ttl = (Number) result.get("ttl");
            return ttl != null ? ttl.longValue() : -1;
        } catch (Exception e) {
            log.error("Lease TTL query failed", e);
        }
        return -1;
    }

    @SuppressWarnings("unchecked")
    public List<Long> leaseLeases() {
        log.debug("Lease leases request");
        try {
            Map<String, Object> result = retryEngine.executeMapApi("LEASE_LIST",
                    endpoint -> httpTransport.sendGet(endpoint + "/lease/leases"));
            List<Number> leases = (List<Number>) result.get("leases");
            if (leases != null) {
                return leases.stream().map(Number::longValue).toList();
            }
        } catch (Exception e) {
            log.error("Lease leases query failed", e);
        }
        return List.of();
    }

    // ==================== 事务支持 ====================

    public TxnResponse transaction(TxnRequest txnRequest) {
        String jsonBody = toJson(txnRequest);
        return retryEngine.executeApi("TRANSACTION",
                endpoint -> httpTransport.sendPost(endpoint + "/txn", jsonBody),
                httpResponse -> {
                    if (httpResponse.statusCode() != 200) {
                        throw new RuntimeException("Transaction failed: HTTP " + httpResponse.statusCode());
                    }
                    return jsonCodec.readValue(httpResponse.body(), TxnResponse.class);
                });
    }

    public TxnResponse cas(String key, String expectedValue, String newValue) {
        Map<String, Object> request = new HashMap<>();
        request.put("key", key);
        request.put("expectedValue", expectedValue);
        request.put("newValue", newValue);

        String jsonBody = toJson(request);
        return retryEngine.executeApi("CAS",
                endpoint -> httpTransport.sendPost(endpoint + "/txn/cas", jsonBody),
                httpResponse -> {
                    if (httpResponse.statusCode() != 200) {
                        throw new RuntimeException("CAS failed: HTTP " + httpResponse.statusCode());
                    }
                    return jsonCodec.readValue(httpResponse.body(), TxnResponse.class);
                });
    }

    public TxnResponse casWithVersion(String key, long expectedVersion, String newValue) {
        TxnRequest txnRequest = TxnRequest.builder()
                .compares(List.of(Compare.version(key, Compare.CompareOp.EQUAL, expectedVersion)))
                .success(List.of(Operation.put(key, newValue)))
                .failure(List.of(Operation.get(key)))
                .build();
        return transaction(txnRequest);
    }

    // ==================== Compact 压缩 ====================

    public CompactResponse compact(CompactRequest request) {
        log.debug("Compact request: revision={}, requestId={}", request.getRevision(), request.getRequestId());
        String jsonBody = toJson(request);

        return retryEngine.executeApi("COMPACT",
                endpoint -> httpTransport.sendPost(endpoint + "/kv/compact", jsonBody),
                httpResponse -> jsonCodec.readValue(httpResponse.body(), CompactResponse.class));
    }

    public CompactResponse compact(long revision) {
        return compact(CompactRequest.of(revision));
    }

    public CompactResponse compactToPreviousRevision() {
        long currentRevision = getCurrentRevision();
        if (currentRevision <= 1) {
            throw new RuntimeException("Current revision is too small to compact");
        }
        return compact(currentRevision - 1);
    }

    // ==================== 分布式锁 ====================

    public boolean acquireLock(String lockKey, String owner) {
        Map<String, Object> request = new HashMap<>();
        request.put("lockKey", lockKey);
        request.put("owner", owner);

        String jsonBody = toJson(request);
        try {
            TxnResponse response = retryEngine.executeApi("ACQUIRE_LOCK",
                    endpoint -> httpTransport.sendPost(endpoint + "/txn/lock", jsonBody),
                    httpResponse -> {
                        if (httpResponse.statusCode() != 200) {
                            throw new RuntimeException("Acquire lock failed: HTTP " + httpResponse.statusCode());
                        }
                        return jsonCodec.readValue(httpResponse.body(), TxnResponse.class);
                    });
            return response.isSucceeded();
        } catch (Exception e) {
            log.error("Failed to acquire lock: {} for owner: {}", lockKey, owner, e);
            return false;
        }
    }

    public boolean releaseLock(String lockKey, String owner) {
        Map<String, Object> request = new HashMap<>();
        request.put("lockKey", lockKey);
        request.put("owner", owner);

        String jsonBody = toJson(request);
        try {
            TxnResponse response = retryEngine.executeApi("RELEASE_LOCK",
                    endpoint -> httpTransport.sendPost(endpoint + "/txn/unlock", jsonBody),
                    httpResponse -> {
                        if (httpResponse.statusCode() != 200) {
                            throw new RuntimeException("Release lock failed: HTTP " + httpResponse.statusCode());
                        }
                        return jsonCodec.readValue(httpResponse.body(), TxnResponse.class);
                    });
            return response.isSucceeded();
        } catch (Exception e) {
            log.error("Failed to release lock: {} for owner: {}", lockKey, owner, e);
            return false;
        }
    }

    // ==================== 内部工具方法 ====================

    private KVResponse parseKvResponse(HttpResponse<String> httpResponse) throws Exception {
        if (httpResponse.statusCode() == 200 || httpResponse.statusCode() == 301) {
            return jsonCodec.readValue(httpResponse.body(), KVResponse.class);
        }
        throw new RuntimeException("HTTP " + httpResponse.statusCode());
    }

    private void handleHttpRedirect(String location) {
        try {
            java.net.URI redirectUri = new java.net.URI(location);
            String newLeader = redirectUri.getScheme() + "://" + redirectUri.getHost();
            if (redirectUri.getPort() > 0) {
                newLeader += ":" + redirectUri.getPort();
            }
            endpointManager.updateLeader(newLeader);
        } catch (Exception e) {
            log.warn("Failed to parse redirect location: {}", location);
        }
    }

    private String generateRequestId(String... parts) {
        StringBuilder sb = new StringBuilder("req-");
        for (String part : parts) {
            sb.append(part).append("-");
        }
        sb.append(System.currentTimeMillis());
        return sb.toString();
    }

    private String toJson(Object value) {
        try {
            return jsonCodec.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize request body", e);
        }
    }

    // ==================== Builder ====================

    public static class Builder {
        private final ClientConfig.Builder configBuilder = ClientConfig.builder();

        public Builder serverUrls(List<String> serverUrls) {
            configBuilder.serverUrls(serverUrls);
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            configBuilder.maxRetries(maxRetries);
            return this;
        }

        public Builder timeoutSeconds(int timeoutSeconds) {
            configBuilder.timeoutSeconds(timeoutSeconds);
            return this;
        }

        public RaftKVClient build() {
            return new RaftKVClient(configBuilder.build());
        }
    }
}
