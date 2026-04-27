package com.raftkv.service;

import com.raftkv.config.RaftProperties;
import com.raftkv.entity.WatchEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 事件历史存储 - 用于 Watch 的历史事件回放
 *
 * 设计目标：
 * 1. 存储最近 N 个事件（可配置）
 * 2. 支持按版本号范围查询
 * 3. 线程安全（多生产者单消费者模型）
 * 4. 内存高效（自动淘汰旧事件）
 *
 * 实现方式：
 * - 使用 ConcurrentLinkedQueue 作为环形缓冲区
 * - 当事件数量超过容量时，移除最旧的事件
 *
 * 使用场景：
 * 1. 客户端断线重连：从上次接收的版本号继续接收
 * 2. 新 Watch 订阅：发送历史事件追赶当前状态
 *
 * 注意：
 * - 这是"尽力而为"的历史存储，不保证所有历史事件都可用
 * - 如果请求的 revision 太旧，可能已经被淘汰
 * - 客户端需要处理 "revision 已过期" 的情况（重新全量获取）
 */
@Component
public class EventHistory {

    private static final Logger LOG = LoggerFactory.getLogger(EventHistory.class);

    /**
     * 默认历史事件容量
     *
     * 存储最近 10000 个事件，约占用 1-2MB 内存
     * 可根据实际需求调整
     */
    public static final int DEFAULT_CAPACITY = 10000;

    /**
     * 事件队列（环形缓冲区实现）
     *
     * 使用 ConcurrentLinkedQueue 保证线程安全
     * 所有对队列的操作都是 O(1)
     */
    private final ConcurrentLinkedQueue<WatchEvent> eventQueue = new ConcurrentLinkedQueue<>();

    /**
     * 当前容量
     */
    private final int capacity;

    /**
     * 当前存储的事件数量
     */
    private final AtomicLong eventCount = new AtomicLong(0);

    /**
     * 从配置注入容量创建 EventHistory（Spring 使用）
     */
    @Autowired
    public EventHistory(RaftProperties raftProperties) {
        this.capacity = raftProperties != null && raftProperties.getWatchHistoryCapacity() > 0
                ? raftProperties.getWatchHistoryCapacity()
                : DEFAULT_CAPACITY;
        LOG.info("EventHistory initialized with capacity: {}", this.capacity);
    }

    /**
     * 使用指定容量创建 EventHistory（测试使用）
     *
     * @param capacity 最大事件数量
     */
    public EventHistory(int capacity) {
        this.capacity = capacity;
        LOG.info("EventHistory initialized with capacity: {}", capacity);
    }

    /**
     * 添加事件到历史记录
     *
     * <p>使用 {@code synchronized} 保证与读操作的原子性，
     * 防止 {@code sendHistoricalEvents} 在获取 {@code oldestRevision} 和
     * 查询 EventHistory 之间被 poll 插进来导致 revision 丢失。</p>
     *
     * @param event 要添加的事件
     */
    public synchronized void addEvent(WatchEvent event) {
        if (event == null) {
            return;
        }

        if (!eventQueue.offer(event)) {
            // offer 失败（极其罕见），不更新计数
            LOG.warn("Failed to add event to history queue: revision={}", event.getRevision());
            return;
        }

        long count = eventCount.incrementAndGet();

        // 如果超过容量，移除最旧的事件
        if (count > capacity) {
            eventQueue.poll();  // 移除最旧的事件
            eventCount.decrementAndGet();
            // oldestRevision 不再维护，每次通过 peek() 获取
            LOG.debug("Evicted oldest event, queue size now: {}", eventQueue.size());
        }

        LOG.debug("Added event to history: revision={}, type={}, key={}",
                event.getRevision(), event.getType(), event.getKey());
    }

    /**
     * 获取从指定版本开始的所有事件
     *
     * <p>使用 {@code synchronized} 保证与 {@code addEvent} 的互斥，
     * 避免查询期间队列头部被 poll 导致 oldestRevision 漂移。</p>
     *
     * @param startRevision 起始版本号（包含）
     * @return 从该版本开始的事件列表（已复制，线程安全），如果版本已过期返回空列表
     */
    public synchronized List<WatchEvent> getEventsFrom(long startRevision) {
        //如果startRevision <= 0，表示客户端没指定startRevision，表示只接收最新的消息不接收历史消息
        if (startRevision <= 0) {
            return Collections.emptyList();
        }

        // 快速检查：如果请求的 revision 比最旧的还旧，说明已过期
        long oldestAvailable = getOldestRevision();
        if (oldestAvailable > 0 && startRevision < oldestAvailable) {
            LOG.warn("Requested revision {} is too old, oldest available is {}",
                    startRevision, oldestAvailable);
            return Collections.emptyList();
        }

        // 过滤出 >= startRevision 的事件，并复制到新的列表（线程安全）
        return eventQueue.stream()
                .filter(e -> e.getRevision() >= startRevision)
                .collect(Collectors.toList());
    }

    /**
     * 获取指定版本范围内的事件
     *
     * @param startRevision 起始版本号（包含）
     * @param endRevision 结束版本号（包含）
     * @return 该范围内的事件列表
     */
    public synchronized List<WatchEvent> getEventsBetween(long startRevision, long endRevision) {
        if (startRevision <= 0 || endRevision < startRevision) {
            return Collections.emptyList();
        }

        return eventQueue.stream()
                .filter(e -> e.getRevision() >= startRevision && e.getRevision() <= endRevision)
                .collect(Collectors.toList());
    }

    /**
     * 获取最新的事件
     *
     * @param count 要获取的事件数量
     * @return 最新的 N 个事件
     */
    public synchronized List<WatchEvent> getLatestEvents(int count) {
        if (count <= 0) {
            return Collections.emptyList();
        }

        List<WatchEvent> allEvents = new ArrayList<>(eventQueue);
        int start = Math.max(0, allEvents.size() - count);
        return allEvents.subList(start, allEvents.size());
    }

    /**
     * 获取当前存储的事件数量
     */
    public synchronized long getEventCount() {
        return eventCount.get();
    }

    /**
     * 获取最早可用的版本号（队列中现存最老的 revision）
     *
     * @return 最早版本号，如果没有事件返回 0
     */
    public synchronized long getOldestRevision() {
        WatchEvent first = eventQueue.peek();
        return first != null ? first.getRevision() : 0;
    }

    /**
     * 获取最新事件的版本号
     *
     * @return 最新版本号，如果没有事件返回 0
     */
    public synchronized long getLatestRevision() {
        WatchEvent last = null;
        for (WatchEvent event : eventQueue) {
            last = event;
        }
        return last != null ? last.getRevision() : 0;
    }

    /**
     * 检查指定版本号是否可用
     *
     * @param revision 要检查的版本号
     * @return true 如果该版本号的事件还在历史记录中
     */
    public synchronized boolean isRevisionAvailable(long revision) {
        if (revision <= 0) {
            return false;
        }
        // 如果 revision >= oldestRevision，说明可用
        return revision >= getOldestRevision();
    }

    /**
     * 清空历史记录
     *
     * 用于测试或重置
     */
    public synchronized void clear() {
        eventQueue.clear();
        eventCount.set(0);
        LOG.info("EventHistory cleared");
    }

    /**
     * 预填充历史事件（与 addEvent 互斥，保证并发安全）
     *
     * 使用场景：
     * 节点成为 Leader 时，从 MVCCStore 反推历史事件填充到 EventHistory，
     * 使得 Watch 客户端即使请求比 EventHistory 原有 oldestRevision 更老的 revision
     * 也能从内存中直接获取，不再需要运行时查询 MVCCStore（避免并发竞态）。
     *
     * 合并策略：
     * 1. 将 olderEvents 按 revision 升序与现有队列合并
     * 2. 合并后若超出容量，保留最新（revision 最大）的 capacity 条
     * 3. 若 olderEvents 与现有队列 revision 重叠，去重保留一份
     *
     * 典型时序：
     * - Leader 刚上任，EventHistory 可能为空或有少量事件
     * - onLeaderStart 调用 prefillFromMvcc → 从 MVCCStore 获取最近 N 条历史
     * - prefill 与 publishEvent 通过 EventHistory 的 synchronized 互斥
     *
     * @param olderEvents 要补入的历史事件列表（允许乱序，方法内部会排序）
     */
    public synchronized void prefillOlderEvents(List<WatchEvent> olderEvents) {
        if (olderEvents == null || olderEvents.isEmpty()) {
            return;
        }

        // 1. 收集当前队列已有的 revision（用于去重）
        java.util.Set<Long> existingRevisions = new java.util.HashSet<>();
        for (WatchEvent e : eventQueue) {
            existingRevisions.add(e.getRevision());
        }

        // 2. 过滤重复 revision（保留队列已有的，丢弃 prefill 中重复的）
        List<WatchEvent> uniqueOlder = olderEvents.stream()
                .filter(e -> !existingRevisions.contains(e.getRevision()))
                .collect(Collectors.toList());

        if (uniqueOlder.isEmpty()) {
            LOG.info("EventHistory prefill skipped: all {} events already present", olderEvents.size());
            return;
        }

        // 3. 合并：按 revision 升序重建队列
        List<WatchEvent> merged = new ArrayList<>(uniqueOlder);
        merged.addAll(eventQueue);
        merged.sort(Comparator.comparingLong(WatchEvent::getRevision));

        // 4. 裁剪到容量上限（保留最新的 capacity 条）
        int startIndex = Math.max(0, merged.size() - capacity);
        List<WatchEvent> trimmed = merged.subList(startIndex, merged.size());

        // 5. 替换队列内容
        eventQueue.clear();
        eventQueue.addAll(trimmed);
        eventCount.set(trimmed.size());

        LOG.info("EventHistory prefilled: added {} older events, total size={}, oldestRevision={}, latestRevision={}",
                uniqueOlder.size(), trimmed.size(),
                trimmed.isEmpty() ? 0 : trimmed.get(0).getRevision(),
                trimmed.isEmpty() ? 0 : trimmed.get(trimmed.size() - 1).getRevision());
    }
}
