package com.raftkv.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 全局版本号管理器
 * 
 * Revision（版本号）是 etcd 风格 Watch 机制的核心概念：
 * - 全局单调递增的 64 位整数
 * - 每次写操作（PUT/DELETE）都会生成一个新的 revision
 * - 用于事件的排序和去重
 * - 用于断线重连时的历史事件回放
 * 
 * 实现要点：
 * 1. 使用 AtomicLong 保证线程安全
 * 2. 需要持久化到 Raft 状态机（通过快照）
 * 3. 节点重启后从快照恢复
 * 
 * 与 Raft Log Index 的区别：
 * - Raft Log Index: 物理日志位置，包含所有 Raft 操作（包括心跳、配置变更）
 * - Revision: 逻辑版本号，只包含业务写操作（PUT/DELETE）
 */
@Component
public class RevisionManager {

    private static final Logger LOG = LoggerFactory.getLogger(RevisionManager.class);

    /**
     * 当前全局版本号
     * 
     * 从 1 开始递增，0 表示未初始化
     */
    private final AtomicLong currentRevision = new AtomicLong(0);

    /**
     * 获取当前版本号
     * 
     * @return 当前版本号（如果未初始化返回 0）
     */
    public long getCurrentRevision() {
        return currentRevision.get();
    }

    /**
     * 获取下一个版本号
     * 
     * 原子性地递增并返回新的版本号
     * 每次写操作都应该调用此方法获取新的 revision
     * 
     * @return 新的版本号
     */
    public long nextRevision() {
        long revision = currentRevision.incrementAndGet();
        LOG.debug("Generated new revision: {}", revision);
        return revision;
    }

    /**
     * 批量获取版本号
     * 
     * 用于事务操作（未来支持）
     * 
     * @param count 需要的版本号数量
     * @return 起始版本号（包含 count 个连续版本号）
     */
    public long nextRevisions(int count) {
        long startRevision = currentRevision.addAndGet(count) - count + 1;
        LOG.debug("Generated {} revisions starting from: {}", count, startRevision);
        return startRevision;
    }

    /**
     * 从快照恢复版本号
     * 
     * 节点重启时，从快照中恢复 revision
     * 确保版本号不会回退
     * 
     * @param revision 快照中保存的版本号
     */
    public void restoreFromSnapshot(long revision) {
        long current = currentRevision.get();
        if (revision > current) {
            currentRevision.set(revision);
            LOG.info("Restored revision from snapshot: {} (was: {})", revision, current);
        } else {
            LOG.warn("Ignoring stale revision from snapshot: {} (current: {})", revision, current);
        }
    }

    /**
     * 初始化版本号
     * 
     * 首次启动时使用
     * 
     * @param initialRevision 初始版本号
     */
    public void initialize(long initialRevision) {
        if (currentRevision.compareAndSet(0, initialRevision)) {
            LOG.info("Initialized revision to: {}", initialRevision);
        }
    }

    /**
     * 检查版本号是否有效
     * 
     * @param revision 要检查的版本号
     * @return true 如果版本号在有效范围内
     */
    public boolean isValidRevision(long revision) {
        return revision > 0 && revision <= currentRevision.get();
    }
}
