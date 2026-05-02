package com.raftkv.client.transport;

import com.raftkv.client.config.ClientConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 端点管理器
 *
 * 职责：
 * 1. 管理所有服务端节点的健康状态
 * 2. 维护当前已知的 Leader 地址
 * 3. 提供故障转移时的可用端点选择策略
 * 4. URL 规范化
 */
@Slf4j
public class EndpointManager {

    private final List<String> serverUrls;
    private final ConcurrentHashMap<String, EndpointState> endpointStates;
    private final AtomicReference<String> currentLeader = new AtomicReference<>();
    private final int maxConsecutiveFailures;

    public EndpointManager(ClientConfig config) {
        this.serverUrls = config.getServerUrls();
        this.maxConsecutiveFailures = config.getMaxConsecutiveFailures();
        this.endpointStates = new ConcurrentHashMap<>();

        for (String url : serverUrls) {
            String normalized = normalizeUrl(url);
            endpointStates.put(normalized, new EndpointState(normalized));
        }

        if (!serverUrls.isEmpty()) {
            currentLeader.set(normalizeUrl(serverUrls.get(0)));
        }
    }

    /**
     * 获取当前 Leader 地址（规范化后的 URL）
     */
    public String getCurrentLeader() {
        String leader = currentLeader.get();
        if (leader == null || leader.isEmpty()) {
            leader = normalizeUrl(serverUrls.get(0));
            currentLeader.set(leader);
        }
        return leader;
    }

    /**
     * 更新 Leader 地址
     */
    public void updateLeader(String leaderUrl) {
        String normalizedUrl = normalizeUrl(leaderUrl);
        currentLeader.set(normalizedUrl);
        log.info("Updated leader to: {}", normalizedUrl);
    }

    /**
     * 获取所有服务端 URL
     */
    public List<String> getServerUrls() {
        return serverUrls;
    }

    /**
     * 获取服务端 URL 数量
     */
    public int getServerCount() {
        return serverUrls.size();
    }

    /**
     * 标记端点为健康（成功响应）
     */
    public void markEndpointHealthy(String endpoint) {
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
    public void markEndpointUnhealthy(String endpoint) {
        if (endpoint == null) return;
        String normalizedEndpoint = normalizeUrl(endpoint);
        EndpointState state = endpointStates.get(normalizedEndpoint);
        if (state != null) {
            state.recordFailure();
            log.info("Endpoint {} marked unhealthy, consecutive failures: {}",
                    endpoint, state.getConsecutiveFailures());
        }
    }

    /**
     * 查找可用的端点（优先选择健康的端点）
     *
     * @param triedEndpoints 已尝试过的端点集合
     * @return 可用的端点 URL，如果没有可用端点返回 null
     */
    public String findAvailableEndpoint(Set<String> triedEndpoints) {
        if (serverUrls.size() == 1) {
            return null;
        }

        // 1. 优先选择当前 Leader（如果还没尝试过且健康）
        String leader = currentLeader.get();
        if (leader != null && !triedEndpoints.contains(leader)) {
            EndpointState leaderState = endpointStates.get(leader);
            if (leaderState == null || leaderState.isHealthy(maxConsecutiveFailures)) {
                return leader;
            }
        }

        // 2. 遍历所有端点，寻找健康的端点
        for (String url : serverUrls) {
            String normalizedUrl = normalizeUrl(url);
            if (!triedEndpoints.contains(normalizedUrl)) {
                EndpointState state = endpointStates.get(normalizedUrl);
                if (state != null && state.isHealthy(maxConsecutiveFailures)) {
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

        return null;
    }

    /**
     * 规范化 URL，确保包含协议前缀
     */
    public String normalizeUrl(String url) {
        if (url == null || url.isEmpty()) {
            return url;
        }
        if (!url.startsWith("http://") && !url.startsWith("https://")) {
            return "http://" + url;
        }
        return url;
    }
}
