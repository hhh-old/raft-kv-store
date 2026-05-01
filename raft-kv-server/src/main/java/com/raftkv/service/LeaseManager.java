package com.raftkv.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lease 管理器 - 管理内存中的 Lease 过期时间
 *
 * 职责：
 * 1. 维护每个活跃 Lease 的过期时间（内存中，不走 Raft）
 * 2. 提供 KeepAlive 续约接口
 * 3. 提供过期 Lease 检测
 * 4. Leader 切换时从 MVCCStore 恢复 Lease 状态
 *
 * 与 MVCCStore 的分工：
 * - MVCCStore：持久化 Lease 元数据（id, ttl）和 key-lease 绑定关系到快照
 * - LeaseManager：管理内存中的 expiryTime，KeepAlive 只更新内存
 *
 * 对齐 etcd 设计：
 * - KeepAlive 不走 Raft，只更新 Leader 内存
 * - Leader 切换后，新 Leader 给所有 Lease 一个宽限期（currentTime + ttl）
 * - 客户端重连后发送 KeepAlive，新 Leader 刷新 expiryTime
 */
@Slf4j
@Component
public class LeaseManager {

    @Autowired
    private MVCCStore mvccStore;

    // leaseId -> 过期时间（毫秒时间戳）
    private final Map<Long, Long> leaseExpiryTimes = new ConcurrentHashMap<>();

    /**
     * Lease 被授予时调用（Raft apply 时）
     * 使用 Raft 日志中的授予时间计算过期时间
     */
    public void onLeaseGranted(long leaseId, int ttl, long grantedTime) {
        leaseExpiryTimes.put(leaseId, grantedTime + ttl * 1000L);
    }

    /**
     * Lease 被撤销时调用（Raft apply 时）
     */
    public void onLeaseRevoked(long leaseId) {
        leaseExpiryTimes.remove(leaseId);
    }

    /**
     * KeepAlive 续约（HTTP 请求时调用，不走 Raft）
     *
     * @param leaseId 租约 ID
     * @return true 如果租约存在并成功续约
     */
    public boolean keepAlive(long leaseId) {
        MVCCStore.LeaseMeta meta = mvccStore.getLeaseMeta(leaseId);
        if (meta == null) {
            return false;
        }
        leaseExpiryTimes.put(leaseId, System.currentTimeMillis() + meta.ttl * 1000L);
        return true;
    }

    /**
     * 获取已过期的 Lease 列表
     */
    public List<Long> getExpiredLeases() {
        long now = System.currentTimeMillis();
        List<Long> expired = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : leaseExpiryTimes.entrySet()) {
            if (entry.getValue() <= now) {
                expired.add(entry.getKey());
            }
        }
        return expired;
    }

    /**
     * 获取 Lease 的剩余 TTL（秒）
     *
     * @return 剩余秒数，-1 表示租约不存在
     */
    public long getRemainingTtl(long leaseId) {
        Long expiry = leaseExpiryTimes.get(leaseId);
        if (expiry == null) {
            return -1;
        }
        long remaining = (expiry - System.currentTimeMillis()) / 1000;
        return Math.max(remaining, 0);
    }

    /**
     * 获取所有活跃 Lease ID
     */
    public List<Long> getAllLeaseIds() {
        return new ArrayList<>(leaseExpiryTimes.keySet());
    }

    /**
     * 从 MVCCStore 重新加载 Lease 状态（Leader 切换时调用）
     *
     * 关键逻辑：从 MVCCStore 获取 grantedTime（原始授予时间），计算真实的剩余时间
     * 然后设置到当前时间点。
     *
     * <b>宽限期策略</b>：
     * - 未过期 lease（remainingMs > 0）：使用真实剩余时间，客户端可继续 KeepAlive 续约
     * - 已过期 lease（remainingMs <= 0）：设为立即过期（expiryTime = now），
     *   由过期检测线程在首次检测时（500ms 内）触发 leaseRevoke 清理
     *
     * 为什么已过期 lease 不给予宽限期：
     * - 如果 lease 确实已过期，绑定的 key 应该被自动删除
     * - 给予完整 TTL 宽限期会导致已过期 key 多活一个 TTL 周期
     * - 极端场景（集群完全重启）：所有已过期 lease 会全部复活，引发锁冲突/死锁
     *
     * 例如：
     * - Lease 于 T=0 授予，TTL=15秒
     * - T=5 时 Leader 切换，新 Leader reload → 剩余 10s，正常恢复
     * - T=20 时 Leader 切换（lease 已过期），新 Leader reload → 立即过期，快速清理
     */
    public void reloadFromStore() {
        java.util.Collection<MVCCStore.LeaseMeta> metas = mvccStore.getAllLeaseMetas();
        log.info("reloadFromStore: found {} leases in MVCCStore", metas.size());
        leaseExpiryTimes.clear();
        long now = System.currentTimeMillis();
        for (MVCCStore.LeaseMeta meta : metas) {
            // 从 MVCCStore 获取原始授予时间
            long grantedTime = meta.grantedTime;
            // 计算剩余 TTL（毫秒）
            long elapsed = now - grantedTime;
            long remainingMs = meta.ttl * 1000L - elapsed;
            if (remainingMs <= 0) {
                // 已过期：设为立即过期，让过期检测线程快速清理
                log.info("reloadFromStore: lease {} already expired (remainingMs={}), setting immediate expiration",
                        meta.id, remainingMs);
                leaseExpiryTimes.put(meta.id, now);
            } else {
                // 未过期：使用真实的剩余时间
                leaseExpiryTimes.put(meta.id, now + remainingMs);
                log.info("reloadFromStore: loaded lease id={}, ttl={}, grantedTime={}, remainingMs={}",
                        meta.id, meta.ttl, grantedTime, remainingMs);
            }
        }
    }

    /**
     * 清空所有内存中的 Lease 状态
     */
    public void clear() {
        leaseExpiryTimes.clear();
    }
}
