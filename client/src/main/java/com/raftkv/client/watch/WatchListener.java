package com.raftkv.client.watch;

import com.raftkv.entity.WatchEvent;

import java.util.function.Consumer;

/**
 * Watch 监听器（支持断线重连）
 *
 * 封装 Watch 订阅的元数据和重连状态。
 * 用户通过此对象取消监听。
 */
public class WatchListener {
    private volatile String watchId;
    private final String key;
    private final boolean isPrefix;
    private final long initialStartRevision;
    private final Consumer<WatchEvent> callback;
    private volatile boolean cancelled = false;

    // 断线重连状态，客户端记录收到的事件的 Revision，用于断线重连标记
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
