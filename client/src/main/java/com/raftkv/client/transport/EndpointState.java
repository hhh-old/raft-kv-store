package com.raftkv.client.transport;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 端点健康状态
 *
 * 记录每个服务端节点的连续失败次数和最后失败时间，用于故障转移决策。
 */
public class EndpointState {
    private final String url;
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private volatile long lastFailureTime = 0;

    public EndpointState(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    public void recordSuccess() {
        consecutiveFailures.set(0);
    }

    public void recordFailure() {
        consecutiveFailures.incrementAndGet();
        lastFailureTime = System.currentTimeMillis();
    }

    public boolean isHealthy(int maxConsecutiveFailures) {
        return consecutiveFailures.get() < maxConsecutiveFailures;
    }
}
