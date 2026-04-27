package com.raftkv.service;

import com.raftkv.entity.WatchEvent;
import com.raftkv.entity.WatchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Watch 管理器 - 管理所有的 Watch 订阅和事件分发
 *
 * 核心职责：
 * 1. 管理活跃的 Watch 订阅（注册、关闭、查询）
 * 2. 将事件路由到匹配的订阅者
 * 3. 支持精确匹配和前缀匹配
 * 4. 处理客户端连接断开
 *
 * 架构设计（一步式 etcd 风格）：
 * - 每个 POST /watch/stream 请求创建一个 WatchSubscription（一对一）
 * - exactMatchIndex：按 key 索引活跃订阅，O(1) 精确匹配
 * - prefixWatches：按前缀索引活跃订阅，遍历匹配
 *
 * 事件路由流程：
 * 1. 先写入历史记录（保证不丢失）
 * 2. 再推送给活跃订阅者
 */
@Component
public class WatchManager {

    private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);

    @Autowired
    private EventHistory eventHistory;

    @Autowired
    @Lazy
    private RaftKVService raftKVService;

    @Autowired
    private MVCCStore mvccStore;

    /**
     * 活跃的 Watch 订阅（一对一：一个 watchId 对应一个 SSE 连接）
     */
    private final Map<String, WatchSubscription> activeWatches = new ConcurrentHashMap<>();

    /**
     * 精确匹配的 Watch 订阅列表
     *
     * Key: 监听的 key
     * Value: 监听该 key 的 WatchSubscription 列表
     *
     * 用于 O(1) 快速查找精确匹配的订阅
     */
    private final Map<String, List<WatchSubscription>> exactMatchIndex = new ConcurrentHashMap<>();

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
     * 注册新的 SSE 连接（一对一）
     */
    public void registerConnection(WatchSubscription subscription) {
        // 直接覆盖：同一个 watchId 的新连接会替换旧连接，
        // 旧连接的 onCompletion 回调会在之后触发 closeWatch，安全清理
        activeWatches.put(subscription.watchId, subscription);

        if (subscription.prefix) {
            prefixWatches.add(subscription);
        } else {
            exactMatchIndex.computeIfAbsent(subscription.key, k -> new CopyOnWriteArrayList<>())
                    .add(subscription);
        }

        LOG.info("Registered SSE connection for watch: {}, key={}",
                subscription.watchId, subscription.key);
    }

    /**
     * 关闭指定的 Watch 连接（线程安全）
     */
    public void closeWatch(String watchId, WatchSubscription subscription) {
        if (subscription == null) {
            return;
        }
        subscription.closed = true;

        // 从活跃列表中移除（仅当当前 subscription 与存储的匹配时才移除，防止误删新连接）
        activeWatches.computeIfPresent(watchId, (k, sub) -> sub == subscription ? null : sub);

        // 从索引中移除
        if (subscription.prefix) {
            prefixWatches.remove(subscription);
        } else {
            exactMatchIndex.computeIfPresent(subscription.key, (k, subs) -> {
                subs.remove(subscription);
                return subs.isEmpty() ? null : subs;
            });
        }

        // 完成 emitter（防止重复 complete）
        if (!subscription.completed) {
            subscription.completed = true;
            try {
                subscription.emitter.complete();
            } catch (Exception e) {
                // 忽略
            }
        }

        LOG.debug("Closed SSE connection for watch: {}", watchId);
    }

    /**
     * 获取订阅
     */
    public WatchSubscription getSubscription(String watchId) {
        return activeWatches.get(watchId);
    }

    /**
     * Watch 订阅（SSE 连接）
     */
    public static class WatchSubscription {
        public final String watchId;
        public final String key;
        public final boolean prefix;
        public final long startRevision;
        public final SseEmitter emitter;
        public volatile boolean closed = false;
        // 标记 emitter 是否已完成，防止重复调用 complete()
        public volatile boolean completed = false;
        // 记录最后一次发送的 revision，用于保证版本号单调递增，防止重复发送
        public final AtomicLong lastSentRevision = new AtomicLong(-1);

        public WatchSubscription(String watchId, String key, boolean prefix,
                         long startRevision, SseEmitter emitter) {
            this.watchId = watchId;
            this.key = key;
            this.prefix = prefix;
            this.startRevision = startRevision;
            this.emitter = emitter;
        }
    }

    /**
     * 初始化 - 启动清理和心跳任务
     */
    @PostConstruct
    public void init() {
        // 每 30 秒清理一次已关闭的 Watch
        cleanupExecutor.scheduleWithFixedDelay(this::cleanupExpiredData, 30, 30, TimeUnit.SECONDS);
        // 每 30 秒发送一次心跳，防止中间代理因 idle 超时而断开 SSE
        cleanupExecutor.scheduleWithFixedDelay(this::sendHeartbeats, 30, 30, TimeUnit.SECONDS);
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
            try { sub.emitter.complete(); } catch (Exception e) {}
        });
        activeWatches.clear();
        exactMatchIndex.clear();
        prefixWatches.clear();
        LOG.info("WatchManager destroyed");
    }

    /**
     * 清理已关闭的 Watch 连接（兜底任务）
     */
    private void cleanupExpiredData() {
        activeWatches.entrySet().removeIf(entry -> {
            WatchSubscription sub = entry.getValue();
            if (sub.closed) {
                if (!sub.completed) {
                    sub.completed = true;
                    try { sub.emitter.complete(); } catch (Exception e) {}
                }
                return true;
            }
            return false;
        });
    }

    /**
     * 发送心跳到所有活跃连接
     *
     * 防止 Nginx/ALB 等中间代理因 idle 超时而断开 SSE。
     * 心跳发送失败说明连接已死，立即触发清理。
     */
    private void sendHeartbeats() {
        long now = System.currentTimeMillis();
        boolean isLeader = raftKVService != null && raftKVService.isLeader();
        // 拷贝避免遍历期间 closeWatch 修改 Map
        for (WatchSubscription sub : new ArrayList<>(activeWatches.values())) {
            if (sub.closed || sub.completed) {
                continue;
            }
            try {
                SseEmitter.SseEventBuilder heartbeat = SseEmitter.event()
                        .name("heartbeat")
                        .data("{\"time\":" + now + ",\"watchId\":\"" + sub.watchId + "\",\"isLeader\":" + isLeader + "}");
                sub.emitter.send(heartbeat);
                LOG.debug("Sent heartbeat to watch: {}", sub.watchId);
            } catch (Exception e) {
                LOG.warn("Heartbeat failed for watch {}, closing: {}", sub.watchId, e.getMessage());
                closeWatch(sub.watchId, sub);
            }
        }
    }

    /**
     * 发送初始化事件（当前 revision）
     */
    public void sendInitialEvent(WatchSubscription subscription) throws Exception {
        long currentRevision = raftKVService.getCurrentRevision();
        boolean isLeader = raftKVService.isLeader();
        SseEmitter.SseEventBuilder initEvent = SseEmitter.event()
                .name("init")
                .data("{\"watchId\":\"" + subscription.watchId + "\",\"currentRevision\":" + currentRevision + ",\"isLeader\":" + isLeader + "}");
        subscription.emitter.send(initEvent);
    }

    /**
     * 发送历史事件（单轨实现：仅依赖 EventHistory）
     *
     * 设计原则：
     * - EventHistory 是唯一的历史事件来源，由 publishEvent 实时写入 + onLeaderStart 的 prefill 共同维护
     * - 节点成为 Leader 时，prefillFromMvcc 会将 MVCCStore 的历史反推填充到 EventHistory，
     *   避免运行时并发查询 MVCCStore 导致的竞态（已写入未发布的版本被误读）
     * - 如果请求的 revision 比 EventHistory 的 oldestRevision 更老，则发送 COMPACT_REVISION 错误
     */
    public void sendHistoricalEvents(WatchSubscription subscription) {
        if (subscription.startRevision <= 0) {
            // startRevision <= 0 表示只订阅实时事件，不发送历史
            return;
        }

        // 仅从 EventHistory 获取历史事件（单一来源，无并发竞态）
        List<WatchEvent> events = eventHistory.getEventsFrom(subscription.startRevision);

        if (events.isEmpty()) {
            // EventHistory 为空或请求 revision 已被 compaction
            long oldestRevision = eventHistory.getOldestRevision();
            if (oldestRevision > 0 && subscription.startRevision < oldestRevision) {
                // revision 已被 compaction，通知客户端
                sendErrorEvent(subscription, "COMPACT_REVISION",
                        "Requested revision " + subscription.startRevision + " has been compacted, oldest available is " + oldestRevision,
                        oldestRevision);
            }
            // 否则：startRevision >= latestRevision，没有需要回放的历史事件，直接返回
            return;
        }

        for (WatchEvent event : events) {
            if (subscription.closed) {
                break;
            }

            if (subscription.prefix) {
                if (!event.getKey().startsWith(subscription.key)) {
                    continue;
                }
            } else {
                if (!event.getKey().equals(subscription.key)) {
                    continue;
                }
            }

            // 使用 safeSendEvent 保证版本号单调递增，防止重复发送
            safeSendEvent(subscription, event);
        }
    }

    /**
     * 从 MVCCStore 反推历史事件并预填充 EventHistory
     *
     * 调用时机：
     * - 节点成为 Leader 时（KVStoreStateMachine.onLeaderStart）
     * - onLeaderStart 与 onApply 在 Raft 内部串行调度，保证 prefill 与 publishEvent 不会并发穿插
     *
     * 设计要点：
     * 1. 直接扫描 MVCCStore.keyIndex 反推 WatchEvent
     * 2. 取最近 maxEvents 条 revision（按 revision 降序 top-N，再按升序插入）
     * 3. 通过 EventHistory.prefillOlderEvents（synchronized）插入，自动去重与容量裁剪
     *
     * @param maxEvents 最多预填充的事件数量（受限于 EventHistory 容量）
     */
    public void prefillFromMvcc(int maxEvents) {
        if (maxEvents <= 0) {
            return;
        }
        try {
            // 扫描所有 key 的历史版本，反推 WatchEvent
            List<WatchEvent> events = new ArrayList<>();
            for (String key : mvccStore.getAllKeys()) {
                java.util.NavigableMap<MVCCStore.Revision, MVCCStore.KeyValue> history = mvccStore.getHistory(key);
                for (java.util.Map.Entry<MVCCStore.Revision, MVCCStore.KeyValue> entry : history.entrySet()) {
                    long mainRev = entry.getKey().getMainRev();
                    MVCCStore.KeyValue kv = entry.getValue();
                    WatchEvent event = (kv.getValue() == null)
                            ? WatchEvent.delete(key, mainRev, kv.getCreateRevision().getMainRev(), kv.getVersion())
                            : WatchEvent.put(key, kv.getValue(), mainRev, kv.getCreateRevision().getMainRev(), kv.getVersion());
                    events.add(event);
                }
            }

            if (events.isEmpty()) {
                LOG.info("prefillFromMvcc: MVCCStore has no events to prefill");
                return;
            }

            events.sort(Comparator.comparingLong(WatchEvent::getRevision));

            // 只取最近 maxEvents 条
            List<WatchEvent> recent;
            if (events.size() > maxEvents) {
                recent = events.subList(events.size() - maxEvents, events.size());
            } else {
                recent = events;
            }

            eventHistory.prefillOlderEvents(recent);
            LOG.info("prefillFromMvcc completed: scanned={}, prefilled={}", events.size(), recent.size());
        } catch (Exception e) {
            LOG.error("prefillFromMvcc failed: {}", e.getMessage(), e);
        }
    }

    /**
     * 发送错误事件到指定订阅
     */
    private void sendErrorEvent(WatchSubscription sub, String code, String message, long oldestRevision) {
        if (sub.closed || sub.completed) {
            return;
        }
        try {
            SseEmitter.SseEventBuilder errorEvent = SseEmitter.event()
                    .name("error")
                    .data("{\"code\":\"" + code + "\",\"message\":\"" + message + "\",\"oldestRevision\":" + oldestRevision + "}");
            sub.emitter.send(errorEvent);
            LOG.warn("Sent error event to watch {}: code={}, oldestRevision={}", sub.watchId, code, oldestRevision);
        } catch (Exception e) {
            LOG.warn("Failed to send error event to watch {}, closing: {}", sub.watchId, e.getMessage());
            closeWatch(sub.watchId, sub);
        }
    }

    /**
     * 发布事件到所有匹配的订阅
     */
    public void publishEvent(WatchEvent event) {
        if (event == null) {
            return;
        }

        String key = event.getKey();

        // 1. 先写入历史记录（保证新连接的客户端能获取）
        eventHistory.addEvent(event);

        // 2. 再分发给当前的活跃订阅者（使用 safeSendEvent 保证幂等性）
        List<WatchSubscription> exactSubs = exactMatchIndex.get(key);
        if (exactSubs != null) {
            for (WatchSubscription sub : exactSubs) {
                if (!sub.closed) {
                    safeSendEvent(sub, event);
                }
            }
        }

        for (WatchSubscription sub : prefixWatches) {
            if (!sub.closed && key.startsWith(sub.key)) {
                safeSendEvent(sub, event);
            }
        }
    }

    /**
     * 安全发送事件（保证版本号单调递增，防止重复发送）
     */
    private void safeSendEvent(WatchSubscription sub, WatchEvent event) {
        // 只发送 revision >= startRevision 的事件
        if (event.getRevision() < sub.startRevision) {
            return;
        }

        // 利用 CAS 保证版本号单调递增，去重并防乱序
        while (true) {
            long currentLast = sub.lastSentRevision.get();
            if (event.getRevision() <= currentLast) {
                // 已经发送过更新或相同的版本，直接跳过
                return;
            }
            if (sub.lastSentRevision.compareAndSet(currentLast, event.getRevision())) {
                break;
            }
            // 其他线程修改了 lastSentRevision，重试
        }

        try {
            SseEmitter.SseEventBuilder eventBuilder = SseEmitter.event()
                    .name(event.getType().name().toLowerCase())
                    .data(event);
            sub.emitter.send(eventBuilder);
            LOG.debug("Sent event to watch {}: revision={}, key={}",
                    sub.watchId, event.getRevision(), event.getKey());
        } catch (Exception e) {
            LOG.warn("Failed to send event to watch {}, closing: {}", sub.watchId, e.getMessage());
            closeWatch(sub.watchId, sub);
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
        WatchSubscription sub = activeWatches.get(watchId);
        return sub != null && !sub.closed;
    }
}
