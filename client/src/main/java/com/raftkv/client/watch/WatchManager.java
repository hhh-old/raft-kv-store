package com.raftkv.client.watch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raftkv.client.serialization.JsonCodec;
import com.raftkv.client.transport.EndpointManager;
import com.raftkv.client.transport.HttpTransport;
import com.raftkv.client.transport.RetryEngine;
import com.raftkv.entity.WatchEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Watch 管理器
 *
 * 职责：
 * 1. 管理活跃的 Watch 订阅列表
 * 2. 启动和维护 SSE 事件流连接
 * 3. 断线后指数退避重连
 * 4. 跨节点故障转移（当连接的节点不是 Leader 时）
 */
@Slf4j
public class WatchManager {

    private final EndpointManager endpointManager;
    private final HttpTransport httpTransport;
    private final JsonCodec jsonCodec;
    private final RetryEngine retryEngine;
    private final CopyOnWriteArrayList<WatchListener> activeWatches = new CopyOnWriteArrayList<>();

    public WatchManager(EndpointManager endpointManager, HttpTransport httpTransport,
                        JsonCodec jsonCodec, RetryEngine retryEngine) {
        this.endpointManager = endpointManager;
        this.httpTransport = httpTransport;
        this.jsonCodec = jsonCodec;
        this.retryEngine = retryEngine;
    }

    /**
     * 创建 Watch 订阅（通用方法）
     */
    public WatchListener watch(String key, boolean isPrefix, long startRevision, Consumer<WatchEvent> callback) {
        log.info("Creating watch: key={}, prefix={}, startRevision={}", key, isPrefix, startRevision);

        WatchListener listener = new WatchListener(null, key, isPrefix, startRevision, callback);
        activeWatches.add(listener);

        CompletableFuture.runAsync(() -> runWatchLoop(listener))
                .exceptionally(ex -> {
                    log.error("Watch loop ended unexpectedly: key={}", key, ex);
                    activeWatches.remove(listener);
                    return null;
                });

        return listener;
    }

    /**
     * 取消 Watch 监听
     */
    public void cancelWatch(WatchListener listener) {
        if (listener == null) {
            return;
        }
        listener.cancel();
        String currentWatchId = listener.getWatchId();
        if (currentWatchId != null) {
            try {
                retryEngine.executeMapApi("CANCEL_WATCH",
                        endpoint -> {
                            String url = endpoint + "/watch/" + currentWatchId;
                            return httpTransport.sendDelete(url);
                        });
                log.info("Watch cancelled: key={}, watchId={}", listener.getKey(), currentWatchId);
            } catch (Exception e) {
                log.warn("Failed to cancel watch on server: watchId={}", currentWatchId, e);
            }
        }
    }

    /**
     * 获取当前全局版本号
     */
    public long getCurrentRevision() {
        try {
            Map<String, Object> result = retryEngine.executeMapApi("GET_REVISION",
                    endpoint -> httpTransport.sendGet(endpoint + "/watch/revision"));
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
        log.info("All watches marked for closing");
    }

    // ==================== 内部方法 ====================

    /**
     * Watch 重连循环：连接断开后指数退避重试
     */
    private void runWatchLoop(WatchListener listener) {
        while (!listener.isCancelled()) {
            try {
                startDirectWatchStream(listener);
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
        String newWatchId = UUID.randomUUID().toString();
        listener.updateWatchId(newWatchId);
        listener.resetReconnectState();

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("key", listener.getKey());
        requestBody.put("prefix", listener.isPrefix());
        requestBody.put("startRevision", listener.getNextStartRevision());
        requestBody.put("watchId", newWatchId);

        String jsonBody = jsonCodec.writeValueAsString(requestBody);
        Set<String> triedEndpoints = new HashSet<>();
        Exception lastException = null;

        while (triedEndpoints.size() < endpointManager.getServerCount()) {
            String endpoint = endpointManager.getCurrentLeader();
            if (triedEndpoints.contains(endpoint)) {
                String available = endpointManager.findAvailableEndpoint(triedEndpoints);
                if (available == null) break;
                endpoint = available;
                endpointManager.updateLeader(endpoint);
            }
            triedEndpoints.add(endpoint);

            try {
                HttpResponse<java.io.InputStream> response = httpTransport.sendSsePost(
                        endpoint + "/watch/stream", jsonBody);

                if (response.statusCode() != 200) {
                    if (response.statusCode() == 503) {
                        String leaderUrl = response.headers().firstValue("Leader-Url").orElse(null);
                        if (leaderUrl != null) {
                            endpointManager.updateLeader(leaderUrl);
                            continue;
                        }
                    }
                    throw new RuntimeException("HTTP " + response.statusCode());
                }

                readSseStream(listener, response.body());
                return;

            } catch (Exception e) {
                lastException = e;
                if (isConnectionFailure(e)) {
                    endpointManager.markEndpointUnhealthy(endpoint);
                }
                log.warn("Watch connection failed on {}: {}", endpoint, e.getMessage());
            }
        }

        throw lastException != null ? lastException : new RuntimeException("All endpoints failed for watch");
    }

    /**
     * 读取 SSE 流
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
                    if (eventData.length() > 0) {
                        eventData.append("\n");
                    }
                    eventData.append(line.substring(5).trim());
                } else if (line.isEmpty()) {
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
     * 处理 Watch 事件
     */
    @SuppressWarnings("unchecked")
    private void processWatchEvent(WatchListener listener, String eventType, String data) throws Exception {
        ObjectMapper mapper = jsonCodec.getObjectMapper();

        if ("init".equals(eventType)) {
            log.debug("Watch init event: {}", data);
            Map<String, Object> init = mapper.readValue(data, Map.class);
            Boolean isLeader = (Boolean) init.get("isLeader");
            if (Boolean.FALSE.equals(isLeader)) {
                String leaderUrl = (String) init.get("leaderUrl");
                if (leaderUrl != null) endpointManager.updateLeader(leaderUrl);
                throw new IOException("Watch server is not leader (init)");
            }
            return;
        }
        if ("heartbeat".equals(eventType)) {
            log.debug("Watch heartbeat: {}", data);
            Map<String, Object> hb = mapper.readValue(data, Map.class);
            Boolean isLeader = (Boolean) hb.get("isLeader");
            if (Boolean.FALSE.equals(isLeader)) {
                String leaderUrl = (String) hb.get("leaderUrl");
                if (leaderUrl != null) endpointManager.updateLeader(leaderUrl);
                throw new IOException("Watch server is not leader (heartbeat)");
            }
            return;
        }
        if ("error".equals(eventType)) {
            log.warn("Watch error event: {}", data);
            Map<String, Object> err = mapper.readValue(data, Map.class);
            String code = (String) err.get("code");
            if ("NOT_LEADER".equals(code)) {
                String leaderUrl = (String) err.get("leaderUrl");
                if (leaderUrl != null && !leaderUrl.isEmpty() && !"null".equals(leaderUrl)) {
                    endpointManager.updateLeader(leaderUrl);
                }
                throw new IOException("Watch server returned NOT_LEADER");
            }
            if ("COMPACT_REVISION".equals(code)) {
                Number oldestRevision = (Number) err.get("oldestRevision");
                log.warn("Watch revision compacted, oldest available: {}. Will reconnect from latest.", oldestRevision);
                listener.updateLastRevision(0);
                throw new IOException("Watch revision compacted");
            }
            return;
        }

        try {
            WatchEvent event = mapper.readValue(data, WatchEvent.class);
            log.debug("Received watch event: type={}, key={}, revision={}",
                    event.getType(), event.getKey(), event.getRevision());

            listener.updateLastRevision(event.getRevision());
            listener.resetReconnectState();
            listener.getCallback().accept(event);

        } catch (Exception e) {
            log.error("Failed to process watch event: {}", data, e);
        }
    }

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
}
