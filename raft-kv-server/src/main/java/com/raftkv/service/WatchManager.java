package com.raftkv.service;

import com.raftkv.entity.WatchEvent;
import com.raftkv.entity.WatchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Watch 管理器 - 管理所有的 Watch 订阅和事件分发
 * 
 * 核心职责：
 * 1. 管理活跃的 Watch 订阅（注册、取消、查询）
 * 2. 将事件路由到匹配的订阅者
 * 3. 支持精确匹配和前缀匹配
 * 4. 处理客户端连接断开
 * 
 * 设计要点：
 * 1. 线程安全：使用 ConcurrentHashMap 和 CopyOnWriteArrayList
 * 2. 高效匹配：精确匹配 O(1)，前缀匹配 O(n)（n=活跃 watch 数量）
 * 3. 异步发送：事件发送不阻塞事件生产者
 * 
 * 事件路由流程：
 * 1. KVStoreStateMachine.onApply 产生 WatchEvent
 * 2. WatchManager.publishEvent 接收事件
 * 3. 遍历所有活跃订阅，匹配 key（精确或前缀）
 * 4. 通过 SseEmitter 发送给客户端
 */
@Component
public class WatchManager {

    private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);

    /**
     * 默认的 SSE 超时时间（毫秒）
     * 
     * 0 表示不超时，保持长连接
     */
    private static final long DEFAULT_SSE_TIMEOUT = 0L;

    /**
     * 发送事件的超时时间（毫秒）
     * 
     * 如果超过此时间还发送失败，认为客户端已断开
     */
    private static final long SEND_TIMEOUT_MS = 5000;

    @Autowired
    private EventHistory eventHistory;

    @Autowired
    @Lazy
    private RaftKVService raftKVService;

    /**
     * 活跃的 Watch 订阅
     * 
     * Key: watchId
     * Value: WatchSubscription
     */
    private final Map<String, WatchSubscription> activeWatches = new ConcurrentHashMap<>();

    /**
     * 精确匹配的 Watch 索引
     * 
     * Key: 监听的 key
     * Value: 监听该 key 的 watchId 列表
     * 
     * 用于 O(1) 快速查找精确匹配的订阅
     */
    private final Map<String, List<String>> exactMatchIndex = new ConcurrentHashMap<>();

    /**
     * 前缀匹配的 Watch 列表
     * 
     * 存储所有前缀匹配的订阅
     * 事件到来时遍历此列表进行前缀匹配
     */
    private final List<WatchSubscription> prefixWatches = new CopyOnWriteArrayList<>();

    /**
     * 清理过期 Watch 的定时任务执行器
     */
    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "watch-cleanup");
        t.setDaemon(true);
        return t;
    });

    /**
     * Watch 订阅内部类
     */
    private static class WatchSubscription {
        final String watchId;
        final String key;
        final boolean prefix;
        final long startRevision;
        final SseEmitter emitter;
        volatile boolean closed = false;

        WatchSubscription(String watchId, String key, boolean prefix, 
                         long startRevision, SseEmitter emitter) {
            this.watchId = watchId;
            this.key = key;
            this.prefix = prefix;
            this.startRevision = startRevision;
            this.emitter = emitter;
        }
    }

    /**
     * 初始化 - 启动清理任务
     */
    @PostConstruct
    public void init() {
        // 每 30 秒清理一次已关闭的 Watch
        cleanupExecutor.scheduleWithFixedDelay(this::cleanupClosedWatches, 30, 30, TimeUnit.SECONDS);
        LOG.info("WatchManager initialized");
    }

    /**
     * 销毁 - 关闭所有 Watch
     */
    @PreDestroy
    public void destroy() {
        cleanupExecutor.shutdown();
        activeWatches.values().forEach(sub -> {
            sub.closed = true;
            sub.emitter.complete();
        });
        activeWatches.clear();
        exactMatchIndex.clear();
        prefixWatches.clear();
        LOG.info("WatchManager destroyed");
    }

    /**
     * 创建新的 Watch 订阅
     * 
     * @param request Watch 请求
     * @return Watch ID
     */
    public String createWatch(WatchRequest request) {
        // 生成 watchId
        String watchId = request.getWatchId() != null ? 
                request.getWatchId() : UUID.randomUUID().toString();

        // 创建 SSE Emitter
        SseEmitter emitter = new SseEmitter(DEFAULT_SSE_TIMEOUT);

        // 创建订阅对象
        WatchSubscription subscription = new WatchSubscription(
                watchId, request.getKey(), request.isPrefix(), 
                request.getStartRevision(), emitter);

        // 存储订阅
        activeWatches.put(watchId, subscription);

        // 添加到索引
        if (request.isPrefix()) {
            prefixWatches.add(subscription);
        } else {
            exactMatchIndex.computeIfAbsent(request.getKey(), k -> new CopyOnWriteArrayList<>())
                    .add(watchId);
        }

        // 设置关闭回调
        emitter.onCompletion(() -> {
            LOG.debug("Watch completed: {}", watchId);
            closeWatch(watchId);
        });
        emitter.onTimeout(() -> {
            LOG.warn("Watch timeout: {}", watchId);
            closeWatch(watchId);
        });
        emitter.onError(e -> {
            LOG.error("Watch error: {}", watchId, e);
            closeWatch(watchId);
        });

        // 如果有 startRevision，先发送历史事件
        if (request.getStartRevision() > 0) {
            sendHistoricalEvents(subscription);
        }

        // 发送初始事件（当前 revision）
        sendInitialEvent(subscription);

        LOG.info("Created watch: id={}, key={}, prefix={}, startRevision={}",
                watchId, request.getKey(), request.isPrefix(), request.getStartRevision());

        return watchId;
    }

    /**
     * 获取指定 Watch ID 的 SSE Emitter
     * 
     * @param watchId Watch ID
     * @return SseEmitter，如果不存在返回 null
     */
    public SseEmitter getEmitter(String watchId) {
        WatchSubscription subscription = activeWatches.get(watchId);
        return subscription != null ? subscription.emitter : null;
    }

    /**
     * 关闭指定的 Watch
     * 
     * @param watchId Watch ID
     */
    public void closeWatch(String watchId) {
        WatchSubscription subscription = activeWatches.remove(watchId);
        if (subscription == null) {
            return;
        }

        subscription.closed = true;

        // 从索引中移除
        if (subscription.prefix) {
            prefixWatches.remove(subscription);
        } else {
            List<String> watchIds = exactMatchIndex.get(subscription.key);
            if (watchIds != null) {
                watchIds.remove(watchId);
                if (watchIds.isEmpty()) {
                    exactMatchIndex.remove(subscription.key);
                }
            }
        }

        // 完成 emitter
        try {
            subscription.emitter.complete();
        } catch (Exception e) {
            // 忽略
        }

        LOG.info("Closed watch: {}", watchId);
    }

    /**
     * 发布事件到所有匹配的订阅
     * 
     * @param event Watch 事件
     */
    public void publishEvent(WatchEvent event) {
        if (event == null) {
            return;
        }

        String key = event.getKey();

        // 1. 精确匹配
        List<String> exactWatchIds = exactMatchIndex.get(key);
        if (exactWatchIds != null) {
            for (String watchId : exactWatchIds) {
                WatchSubscription sub = activeWatches.get(watchId);
                if (sub != null && !sub.closed) {
                    sendEvent(sub, event);
                }
            }
        }

        // 2. 前缀匹配
        for (WatchSubscription sub : prefixWatches) {
            if (!sub.closed && key.startsWith(sub.key)) {
                sendEvent(sub, event);
            }
        }

        // 3. 添加到历史记录
        eventHistory.addEvent(event);
    }

    /**
     * 发送事件到指定订阅
     */
    private void sendEvent(WatchSubscription sub, WatchEvent event) {
        try {
            // 只发送 revision >= startRevision 的事件
            if (event.getRevision() < sub.startRevision) {
                return;
            }

            SseEmitter.SseEventBuilder eventBuilder = SseEmitter.event()
                    .name(event.getType().name().toLowerCase())
                    .data(event);

            sub.emitter.send(eventBuilder);
            LOG.debug("Sent event to watch {}: revision={}, key={}",
                    sub.watchId, event.getRevision(), event.getKey());
        } catch (Exception e) {
            // 捕获所有异常，包括 IOException 和 IllegalStateException（emitter 已关闭）
            LOG.warn("Failed to send event to watch {}, closing: {}", sub.watchId, e.getMessage());
            closeWatch(sub.watchId);
        }
    }

    /**
     * 发送历史事件
     */
    private void sendHistoricalEvents(WatchSubscription sub) {
        List<WatchEvent> events = eventHistory.getEventsFrom(sub.startRevision);
        if (events.isEmpty()) {
            LOG.info("No historical events available for watch {} from revision {}", 
                    sub.watchId, sub.startRevision);
            return;
        }

        LOG.info("Sending {} historical events to watch {}", events.size(), sub.watchId);

        // 复制列表，避免在遍历时被修改
        List<WatchEvent> eventsCopy = new java.util.ArrayList<>(events);
        
        for (WatchEvent event : eventsCopy) {
            if (sub.closed) {
                break;
            }

            // 检查 key 是否匹配
            if (sub.prefix) {
                if (!event.getKey().startsWith(sub.key)) {
                    continue;
                }
            } else {
                if (!event.getKey().equals(sub.key)) {
                    continue;
                }
            }

            sendEvent(sub, event);
        }
    }

    /**
     * 发送初始事件（通知客户端当前 revision）
     */
    private void sendInitialEvent(WatchSubscription sub) {
        try {
            long currentRevision = raftKVService.getCurrentRevision();
            SseEmitter.SseEventBuilder initEvent = SseEmitter.event()
                    .name("init")
                    .data("{\"watchId\":\"" + sub.watchId + "\",\"currentRevision\":" + currentRevision + "}");
            sub.emitter.send(initEvent);
        } catch (Exception e) {
            LOG.warn("Failed to send init event to watch {}: {}", sub.watchId, e.getMessage());
            closeWatch(sub.watchId);
        }
    }

    /**
     * 清理已关闭的 Watch
     */
    private void cleanupClosedWatches() {
        int count = 0;
        // 复制 key 列表，避免在遍历时修改 Map
        List<String> watchIds = new java.util.ArrayList<>(activeWatches.keySet());
        for (String watchId : watchIds) {
            WatchSubscription sub = activeWatches.get(watchId);
            if (sub != null && sub.closed) {
                closeWatch(watchId);
                count++;
            }
        }
        if (count > 0) {
            LOG.debug("Cleaned up {} closed watches", count);
        }
    }

    /**
     * 获取活跃的 Watch 数量
     */
    public int getActiveWatchCount() {
        return activeWatches.size();
    }

    /**
     * 检查 Watch 是否存在
     */
    public boolean hasWatch(String watchId) {
        return activeWatches.containsKey(watchId);
    }
}
