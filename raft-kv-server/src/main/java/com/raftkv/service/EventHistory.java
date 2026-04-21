package com.raftkv.service;

import com.raftkv.entity.WatchEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
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
     * 最早可用的版本号
     * 
     * 用于快速判断请求的 revision 是否可用
     */
    private volatile long oldestRevision = 0;

    /**
     * 使用默认容量创建 EventHistory
     */
    public EventHistory() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * 使用指定容量创建 EventHistory
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
     * 线程安全，可被多个生产者并发调用
     * 
     * @param event 要添加的事件
     */
    public void addEvent(WatchEvent event) {
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
            WatchEvent removed = eventQueue.poll();
            if (removed != null) {
                eventCount.decrementAndGet();
                oldestRevision = removed.getRevision();
                LOG.debug("Evicted old event with revision: {}", removed.getRevision());
            } else {
                // poll 返回 null 但 count > capacity，说明计数漂移，修正
                eventCount.decrementAndGet();
            }
        }

        LOG.debug("Added event to history: revision={}, type={}, key={}",
                event.getRevision(), event.getType(), event.getKey());
    }

    /**
     * 获取从指定版本开始的所有事件
     * 
     * 用于客户端断线重连或历史回放
     * 
     * @param startRevision 起始版本号（包含）
     * @return 从该版本开始的事件列表（已复制，线程安全），如果版本已过期返回空列表
     */
    public List<WatchEvent> getEventsFrom(long startRevision) {
        if (startRevision <= 0) {
            return Collections.emptyList();
        }

        // 快速检查：如果请求的 revision 比最旧的还旧，说明已过期
        if (oldestRevision > 0 && startRevision < oldestRevision) {
            LOG.warn("Requested revision {} is too old, oldest available is {}",
                    startRevision, oldestRevision);
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
    public List<WatchEvent> getEventsBetween(long startRevision, long endRevision) {
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
    public List<WatchEvent> getLatestEvents(int count) {
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
    public long getEventCount() {
        return eventCount.get();
    }

    /**
     * 获取最早可用的版本号
     * 
     * @return 最早版本号，如果没有事件返回 0
     */
    public long getOldestRevision() {
        if (oldestRevision > 0) {
            return oldestRevision;
        }
        // 如果没有淘汰过事件，返回队列中第一个事件的版本号
        WatchEvent first = eventQueue.peek();
        return first != null ? first.getRevision() : 0;
    }

    /**
     * 获取最新事件的版本号
     * 
     * @return 最新版本号，如果没有事件返回 0
     */
    public long getLatestRevision() {
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
    public boolean isRevisionAvailable(long revision) {
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
    public void clear() {
        eventQueue.clear();
        eventCount.set(0);
        oldestRevision = 0;
        LOG.info("EventHistory cleared");
    }
}
