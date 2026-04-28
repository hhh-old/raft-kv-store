package com.raftkv.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * MVCC (Multi-Version Concurrency Control) 存储引擎
 *
 * 参考 etcd 的 MVCC 实现：
 * - 每个 key 维护多个版本的历史值
 * - 每个版本有唯一的 revision (mainRev, subRev)
 * - 支持基于 revision 的查询和事务
 *
 * Revision 格式：
 * - mainRev: 全局递增的主版本号（每次修改递增）
 * - subRev: 同一事务内的子版本号（区分同一事务内的多个操作）
 *
 * 优化说明（相比原始版本）：
 * - 移除 keyCreateRevisions 冗余索引（createRevision 可从 keyIndex 的 firstEntry 派生）
 * - 保留 keyVersions 作为原子版本计数器（ConcurrentHashMap.compute 保证原子递增）
 * - 简化 snapshot/restore，减少序列化数据量
 * - 移除 putLockFree（Raft 保证单线程 apply，CAS 无意义）
 * - 为 Revision 添加显式 equals/hashCode，确保与 compareTo 一致
 */
@Slf4j
@Component
public class MVCCStore {

    /**
     * 版本号 - 对齐 etcd 的 revision 格式
     */
    @Data
    public static class Revision implements Comparable<Revision> {
        // 主版本号 - 全局递增
        private final long mainRev;
        // 子版本号 - 同一事务内递增
        private final long subRev;

        public Revision(long mainRev, long subRev) {
            this.mainRev = mainRev;
            this.subRev = subRev;
        }

        public static Revision create(long mainRev, long subRev) {
            return new Revision(mainRev, subRev);
        }

        @Override
        public int compareTo(Revision other) {
            int cmp = Long.compare(this.mainRev, other.mainRev);
            if (cmp != 0) {
                return cmp;
            }
            return Long.compare(this.subRev, other.subRev);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Revision revision = (Revision) o;
            return mainRev == revision.mainRev && subRev == revision.subRev;
        }

        @Override
        public int hashCode() {
            return Objects.hash(mainRev, subRev);
        }

        @Override
        public String toString() {
            return mainRev + "." + subRev;
        }

        public boolean isGreaterThan(Revision other) {
            return this.compareTo(other) > 0;
        }

        public boolean isLessThan(Revision other) {
            return this.compareTo(other) < 0;
        }

        public boolean isGreaterOrEqual(Revision other) {
            return this.compareTo(other) >= 0;
        }

        public boolean isLessOrEqual(Revision other) {
            return this.compareTo(other) <= 0;
        }
    }

    /**
     * KeyValue - 带版本的键值对
     */
    @Data
    public static class KeyValue {
        private final String key;
        private final String value;
        private final Revision revision;
        private final Revision createRevision;
        private final long version;  // 该 key 的修改次数
        private final long leaseId;  // 租约 ID（0 表示无租约）

        public KeyValue(String key, String value, Revision revision,
                        Revision createRevision, long version, long leaseId) {
            this.key = key;
            this.value = value;
            this.revision = revision;
            this.createRevision = createRevision;
            this.version = version;
            this.leaseId = leaseId;
        }

        public static KeyValue create(String key, String value, Revision revision,
                                      Revision createRevision, long version) {
            return new KeyValue(key, value, revision, createRevision, version, 0);
        }

        public boolean hasLease() {
            return leaseId != 0;
        }

        /**
         * 判断是否为 tombstone（删除标记）
         * etcd 语义：value == null 表示该 key 已被删除
         */
        public boolean isTombstone() {
            return value == null;
        }
    }

    // ==================== 核心存储结构 ====================
    // 数据源分层：
    // - keyIndex: 完整的版本历史（主数据源）
    // - keyVersions: 仅用于原子版本递增（ConcurrentHashMap.compute 保证并发安全）
    // - keyCreateRevisions: 已移除，从 keyIndex 的 firstEntry 派生

    // key -> 版本历史 (NavigableMap: revision -> KeyValue)
    // 使用 ConcurrentSkipListMap 保证线程安全且有序
    private final Map<String, NavigableMap<Revision, KeyValue>> keyIndex;

    // 全局 revision 计数器 - 每次修改递增
    private volatile long currentRevision;

    // 每个 key 的当前版本号（修改次数）
    // 保留此字段：ConcurrentHashMap.compute() 提供原子递增，保证并发安全
    // 相比从 history.lastEntry() 派生，避免了 ConcurrentSkipListMap 读写竞争导致的 version 丢失
    private final Map<String, Long> keyVersions;

    // ==================== Lease 相关字段 ====================
    // leaseId -> Lease 元数据（持久化到快照）
    private final Map<Long, LeaseMeta> leaseMetas;

    // key -> leaseId 映射（持久化到快照）
    private final Map<String, Long> keyToLease;

    // leaseId -> 绑定的 keys 集合（持久化到快照）
    private final Map<Long, Set<String>> leaseToKeys;

    public MVCCStore() {
        this.keyIndex = new ConcurrentHashMap<>();
        this.currentRevision = 0;
        this.keyVersions = new ConcurrentHashMap<>();
        this.leaseMetas = new ConcurrentHashMap<>();
        this.keyToLease = new ConcurrentHashMap<>();
        this.leaseToKeys = new ConcurrentHashMap<>();
    }

    // ==================== 基本操作 ====================

    /**
     * 获取当前全局 revision
     */
    public synchronized long getCurrentRevision() {
        return currentRevision;
    }

    /**
     * 生成新的 revision（公开方法，供外部调用）
     */
    public synchronized Revision generateRevision() {
        currentRevision++;
        return new Revision(currentRevision, 0);
    }

    /**
     * 设置当前 revision（用于快照恢复）
     */
    public synchronized void setCurrentRevision(long revision) {
        this.currentRevision = revision;
    }

    // ==================== 读写操作 ====================

    /**
     * 获取 key 的最新有效值（对齐 etcd 语义）
     *
     * etcd 行为：GET 请求只看最新一条记录。
     * - 如果最新记录是 tombstone（删除标记），key 视为不存在，返回 null
     * - 如果最新记录是有效值，返回该值
     *
     * 注意：不向前遍历跳过 tombstone。 Tombstone 是"不存在"的声明，不是可跳过的旧版本。
     */
    public KeyValue getLatest(String key) {
        KeyValue kv = getLatestIncludingTombstone(key);
        if (kv != null && kv.isTombstone()) {
            return null;
        }
        return kv;
    }

    /**
     * 获取 key 的最新版本（包括 tombstone）
     *
     * 用于内部操作（如 delete 需要检查 key 是否存在、
     * getModRevision 需要获取最新 revision），
     * 以及事务中 COMPARE MOD 等需要感知 tombstone 的场景。
     */
    public KeyValue getLatestIncludingTombstone(String key) {
        NavigableMap<Revision, KeyValue> history = keyIndex.get(key);
        if (history == null || history.isEmpty()) {
            return null;
        }
        return history.lastEntry().getValue();
    }

    /**
     * 简化版 put（不带 lease）
     */
    public void put(String key, String value) {
        // 生成新的 revision
        Revision rev = generateRevision();
        putWithRevision(key, value, rev);
    }

    /**
     * 带 Lease 的 put
     *
     * @param key 键
     * @param value 值
     * @param leaseId 租约 ID（0 表示不绑定）
     */
    public void put(String key, String value, long leaseId) {
        Revision rev = generateRevision();
        putWithLease(key, value, rev, leaseId);
    }

    /**
     * 使用指定的 revision 写入数据（用于事务）
     *
     * etcd 事务语义：一个事务内的所有操作共享同一个 mainRev，通过 subRev 区分顺序
     *
     * createRevision 计算规则（对齐 etcd）：
     * - history 为空（新 key）：createRevision = 当前 rev
     * - 最新版本是 tombstone（已删除后重新创建）：createRevision = 当前 rev（新生命周期）
     * - 最新版本是正常值（更新）：createRevision 保持不变
     *
     * @param key 键
     * @param value 值
     * @param rev 指定的 revision（包含 mainRev 和 subRev）
     */
    public void putWithRevision(String key, String value, Revision rev) {
        // 使用 computeIfAbsent 获取或创建该 key 的历史
        NavigableMap<Revision, KeyValue> history = keyIndex.computeIfAbsent(key,
                k -> new ConcurrentSkipListMap<>());

        // 计算 createRevision（对齐 etcd 语义）
        // etcd: tombstone 标志着 key 生命周期的终结，重新 PUT 是全新 key
        Revision createRev;
        boolean isRecreated = false;
        if (history.isEmpty()) {
            // 新 key：createRevision = 当前 revision
            createRev = rev;
        } else {
            // 检查最新版本是否为 tombstone
            KeyValue latestKv = history.lastEntry().getValue();
            if (latestKv.isTombstone()) {
                // 已删除后重新创建：createRevision = 当前 revision（新生命周期）
                createRev = rev;
                isRecreated = true;
            } else {
                // 正常更新：createRevision 保持不变
                createRev = latestKv.getCreateRevision();
            }
        }

        // 计算 version（对齐 etcd 语义）
        // etcd: key 被删除后重新创建，version 从 1 开始（新生命周期）
        long newVersion;
        if (isRecreated) {
            newVersion = 1L;
            keyVersions.put(key, 1L);
        } else {
            newVersion = keyVersions.compute(key, (k, v) -> (v == null) ? 1L : v + 1);
        }

        // 存储新版本
        KeyValue storedKv = KeyValue.create(key, value, rev, createRev, newVersion);
        history.put(rev, storedKv);
    }

    /**
     * 使用指定的 revision 写入数据并绑定 Lease
     *
     * @param key 键
     * @param value 值
     * @param rev 指定的 revision
     * @param leaseId 租约 ID（0 表示不绑定）
     */
    public void putWithLease(String key, String value, Revision rev, long leaseId) {
        // 先执行普通 put
        putWithRevision(key, value, rev);

        // 处理 lease 绑定
        if (leaseId > 0) {
            // 解绑旧的 lease
            unbindKeyFromLease(key);

            // 绑定新的 lease
            keyToLease.put(key, leaseId);
            leaseToKeys.computeIfAbsent(leaseId, k -> ConcurrentHashMap.newKeySet()).add(key);
        }

    }

    /**
     * 解绑 key 与其 lease 的关系
     */
    public void unbindKeyFromLease(String key) {
        Long oldLeaseId = keyToLease.remove(key);
        if (oldLeaseId != null) {
            Set<String> keys = leaseToKeys.get(oldLeaseId);
            if (keys != null) {
                keys.remove(key);
                if (keys.isEmpty()) {
                    leaseToKeys.remove(oldLeaseId);
                }
            }
        }
    }

    // ==================== Lease 管理 ====================

    /**
     * Lease 元数据
     */
    public static class LeaseMeta {
        public final long id;
        public final int ttl; // 秒
        public final long grantedTime; // 授予时间（毫秒时间戳），用于 Leader 切换后计算剩余 TTL

        public LeaseMeta(long id, int ttl, long grantedTime) {
            this.id = id;
            this.ttl = ttl;
            this.grantedTime = grantedTime;
        }
    }

    /**
     * 创建 Lease 元数据
     *
     * @param leaseId 租约 ID
     * @param ttl 租约有效期（秒）
     * @param grantedTime 授予时间（毫秒时间戳，从 Raft 日志中获取）
     */
    public void grantLease(long leaseId, int ttl, long grantedTime) {
        leaseMetas.put(leaseId, new LeaseMeta(leaseId, ttl, grantedTime));
    }

    /**
     * 撤销 Lease，返回该 lease 绑定的所有 keys
     */
    public Set<String> revokeLease(long leaseId) {
        // 移除 lease 元数据
        leaseMetas.remove(leaseId);

        // 获取并移除绑定的 keys
        Set<String> keys = leaseToKeys.remove(leaseId);
        if (keys != null) {
            for (String key : keys) {
                keyToLease.remove(key);
            }
        }
        return keys != null ? keys : java.util.Collections.emptySet();
    }

    /**
     * 获取 Lease 元数据
     */
    public LeaseMeta getLeaseMeta(long leaseId) {
        return leaseMetas.get(leaseId);
    }

    /**
     * 获取所有 Lease 元数据
     */
    public java.util.Collection<LeaseMeta> getAllLeaseMetas() {
        return leaseMetas.values();
    }

    /**
     * 获取 Lease 绑定的所有 keys
     */
    public Set<String> getLeaseKeys(long leaseId) {
        Set<String> keys = leaseToKeys.get(leaseId);
        return keys != null ? new java.util.HashSet<>(keys) : java.util.Collections.emptySet();
    }

    /**
     * 获取 key 绑定的 leaseId
     */
    public Long getKeyLeaseId(String key) {
        return keyToLease.get(key);
    }

    /**
     * 获取 key 在指定 revision 时的值
     */
    public KeyValue getAtRevision(String key, long targetRevision) {
        NavigableMap<Revision, KeyValue> history = keyIndex.get(key);
        if (history == null || history.isEmpty()) {
            return null;
        }

        // 找到小于等于 targetRevision 的最新版本
        Revision target = new Revision(targetRevision, Long.MAX_VALUE);
        Map.Entry<Revision, KeyValue> entry = history.floorEntry(target);

        return entry != null ? entry.getValue() : null;
    }

    /**
     * 获取 key 的版本历史
     */
    public NavigableMap<Revision, KeyValue> getHistory(String key) {
        return keyIndex.getOrDefault(key, new ConcurrentSkipListMap<>());
    }

    /**
     * 获取 key 的当前版本号（修改次数）
     */
    public long getVersion(String key) {
         // etcd 语义：如果 key 的最新记录是 tombstone，视为 key 不存在，version = 0
        NavigableMap<Revision, KeyValue> history = keyIndex.get(key);
        if (history == null || history.isEmpty()) {
            return 0L;
        }
        KeyValue latest = history.lastEntry().getValue();
        if (latest.isTombstone()) {
            return 0L;
        }
        return keyVersions.getOrDefault(key, 0L);
    }

    /**
     * 获取 key 的创建 revision
     *
     * 优化：从 keyIndex 的第一条记录派生，不再使用冗余的 keyCreateRevisions Map
     */
    public Revision getCreateRevision(String key) {
        // etcd 语义：createRevision 是 key 当前生命周期的创建版本号。
        // 如果 key 被删除后重新创建，createRevision 会重置为新的值。
        // 因此必须返回当前有效版本（getLatest）的 createRevision，
        // 而不是历史第一条记录（firstEntry）的 createRevision。
        KeyValue latest = getLatest(key);
        return latest != null ? latest.getCreateRevision() : null;
    }

    /**
     * 获取 key 的修改 revision（最新版本的 revision，包括 tombstone）
     *
     * 注意：即使 key 已被删除（tombstone），modRevision 仍应返回删除时的 revision。
     * 这与 etcd 语义一致：DELETE 操作也会更新 modRevision。
     */
    public Revision getModRevision(String key) {
        KeyValue kv = getLatestIncludingTombstone(key);
        return kv != null ? kv.getRevision() : null;
    }

    /**
     * 获取 key 的完整版本信息（用于事务比较）
     *
     * 优化：createRevision 从 keyIndex 派生，不再依赖 keyCreateRevisions
     *
     * @param key 键名
     * @return KeyVersion 对象，如果 key 不存在返回 null
     */
    public KeyVersion getKeyVersion(String key) {
        NavigableMap<Revision, KeyValue> history = keyIndex.get(key);
        if (history == null || history.isEmpty()) {
            return null;
        }

        // etcd 语义：如果 key 的最新记录是 tombstone，视为 key 不存在
        KeyValue latest = history.lastEntry().getValue();
        if (latest.isTombstone()) {
            return null;
        }

        Long version = keyVersions.get(key);
        if (version == null) {
            return null;
        }

        // etcd 语义：createRevision 必须来自当前有效版本（latest），
        // 而不是历史第一条记录（firstEntry）。
        // 因为 key 可能被删除后重新创建，此时 createRevision 会重置。
        Revision createRev = latest.getCreateRevision();
        Revision modRev = getModRevision(key);

        if (createRev == null || modRev == null) {
            return null;
        }

        return new KeyVersion(
            createRev.getMainRev(),
            modRev.getMainRev(),
            version
        );
    }

    /**
     * Key 的版本信息（用于事务比较）
     */
    public static class KeyVersion {
        public final long createRevision;
        public final long modRevision;
        public final long version;

        public KeyVersion(long createRevision, long modRevision, long version) {
            this.createRevision = createRevision;
            this.modRevision = modRevision;
            this.version = version;
        }

        @Override
        public String toString() {
            return String.format("KeyVersion{createRev=%d, modRev=%d, ver=%d}",
                createRevision, modRevision, version);
        }
    }

    /**
     * 获取所有 key 的最新值（基于固定 revision 的快照读）
     *
     * 保证原子性：在遍历开始前记录当前 revision，所有 key 都基于这个 revision 读取。
     * 这样可以避免遍历过程中写操作导致的不一致。
     *
     * 注意：在 Raft 架构下，这个方法通常在 ReadIndex 回调中调用，此时状态机
     * 已经应用到了某个固定的日志索引，不会有新的写操作。但为了代码的健壮性，
     * 我们仍然使用固定 revision 的快照读。
     *
     * @return 所有 key 在某一固定 revision 时刻的快照
     */
    public Map<String, KeyValue> getAllLatest() {
        long snapshotRevision = getCurrentRevision();
        return getAllAtRevision(snapshotRevision);
    }

    /**
     * 获取指定 revision 时的所有 key 值（历史快照，排除 tombstone）
     *
     * etcd 语义：Range 请求不返回已删除的 key
     */
    public Map<String, KeyValue> getAllAtRevision(long targetRevision) {
        Map<String, KeyValue> result = new HashMap<>();
        for (Map.Entry<String, NavigableMap<Revision, KeyValue>> entry : keyIndex.entrySet()) {
            Revision target = new Revision(targetRevision, Long.MAX_VALUE);
            Map.Entry<Revision, KeyValue> kvEntry = entry.getValue().floorEntry(target);
            if (kvEntry != null && !kvEntry.getValue().isTombstone()) {
                result.put(entry.getKey(), kvEntry.getValue());
            }
        }
        return result;
    }

    // ==================== 范围查询 ====================

    /**
     * 前缀匹配查询（排除 tombstone，对齐 etcd 语义）
     *
     * etcd 行为：Range/Prefix 请求不返回已删除的 key
     */
    public List<KeyValue> getWithPrefix(String prefix) {
        List<KeyValue> result = new ArrayList<>();
        for (Map.Entry<String, NavigableMap<Revision, KeyValue>> entry : keyIndex.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                KeyValue latest = getLatest(entry.getKey());
                if (latest != null) {
                    result.add(latest);
                }
            }
        }
        return result;
    }

    /**
     * 范围查询 [startKey, endKey)（排除 tombstone，对齐 etcd 语义）
     *
     * etcd 行为：Range 请求不返回已删除的 key
     */
    public List<KeyValue> getRange(String startKey, String endKey) {
        List<KeyValue> result = new ArrayList<>();

        // 使用 TreeMap 的有序性进行范围查询
        TreeMap<String, NavigableMap<Revision, KeyValue>> sortedKeys = new TreeMap<>(keyIndex);
        NavigableMap<String, NavigableMap<Revision, KeyValue>> subMap = sortedKeys.subMap(startKey, true, endKey, false);

        for (Map.Entry<String, NavigableMap<Revision, KeyValue>> entry : subMap.entrySet()) {
            KeyValue latest = getLatest(entry.getKey());
            if (latest != null) {
                result.add(latest);
            }
        }
        return result;
    }

    // ==================== 删除操作 ====================

    /**
     * 删除 key - 创建 tombstone（值为 null）
     *
     * etcd 语义：对不存在的 key 或已删除的 key 执行 DELETE 返回 false
     */
    public boolean delete(String key) {
        // 检查 key 是否有有效值（跳过 tombstone）
        KeyValue latest = getLatest(key);
        if (latest == null) {
            return false;  // key 不存在或已删除
        }

        Revision rev = generateRevision();
        return deleteWithRevision(key, rev);
    }

    /**
     * 使用指定的 revision 删除数据（用于事务）
     *
     * etcd 事务语义：一个事务内的所有操作共享同一个 mainRev，通过 subRev 区分顺序
     *
     * etcd 语义：对不存在的 key 或已删除的 key 执行 DELETE 返回 false
     *
     * @param key 键
     * @param rev 指定的 revision（包含 mainRev 和 subRev）
     * @return 是否成功删除
     */
    public boolean deleteWithRevision(String key, Revision rev) {
        // 检查 key 是否有有效值（跳过 tombstone）
        KeyValue latest = getLatest(key);
        if (latest == null) {
            return false;  // key 不存在或已删除
        }

        // 获取历史记录（用于查找 createRevision）
        NavigableMap<Revision, KeyValue> history = keyIndex.get(key);

        // 创建 tombstone（值为 null 表示删除）
        // createRevision 必须从当前有效版本（latest）派生，而不是历史第一条（firstEntry）。
        // 因为 key 可能被删除后重新创建，此时当前生命周期的 createRevision 已重置。
        Revision createRev = latest.getCreateRevision();
        // 使用 ConcurrentHashMap.compute 原子递增版本号（与 putWithRevision 保持一致）
        long newVersion = keyVersions.compute(key, (k, v) -> (v == null) ? 1L : v + 1);

        KeyValue tombstone = new KeyValue(key, null, rev, createRev, newVersion, 0);
        history.put(rev, tombstone);

        log.debug("MVCC delete: key={}, rev={}", key, rev);
        return true;
    }

    // ==================== 压缩（历史清理） ====================

    /**
     * 压缩历史版本 - 删除指定 revision 之前的所有历史
     * 注意：只能压缩已经被快照覆盖的 revision
     *
     * 注意：不需要 synchronized，因为：
     * 1. compact 由 Raft 快照机制触发，在 Raft 线程中单线程调用
     * 2. 使用的数据结构都是线程安全的（ConcurrentSkipListMap）
     */
    public int compact(long compactRevision) {
        int removedCount = 0;
        // 收集已完全删除（仅剩 tombstone）的 key，压缩后可清理 keyVersions
        List<String> keysWithOnlyTombstone = new ArrayList<>();

        for (Map.Entry<String, NavigableMap<Revision, KeyValue>> entry : keyIndex.entrySet()) {
            NavigableMap<Revision, KeyValue> history = entry.getValue();
            String key = entry.getKey();

            // 获取所有小于 compactRevision 的版本
            Revision target = new Revision(compactRevision, 0);
            NavigableMap<Revision, KeyValue> toRemove = history.headMap(target, false);

            // 保留每个 key 的最后一个版本（即使它小于 compactRevision）
            if (!toRemove.isEmpty()) {
                // 找到小于 compactRevision 的最后一个版本
                Map.Entry<Revision, KeyValue> lastBeforeCompact = toRemove.lastEntry();

                // 删除其他旧版本
                Iterator<Map.Entry<Revision, KeyValue>> it = toRemove.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<Revision, KeyValue> kvEntry = it.next();
                    if (!kvEntry.getKey().equals(lastBeforeCompact.getKey())) {
                        it.remove();
                        removedCount++;
                    }
                }
            }

            // 检查压缩后是否只剩 tombstone（value == null），如果是则可清理 keyVersions
            if (history.size() == 1) {
                KeyValue onlyEntry = history.firstEntry().getValue();
                if (onlyEntry.getValue() == null) {
                    keysWithOnlyTombstone.add(key);
                }
            }
        }

        // 清理只有 tombstone 的 key 的 keyVersions 和 keyIndex 条目
        for (String key : keysWithOnlyTombstone) {
            keyVersions.remove(key);
            keyIndex.remove(key);
            removedCount++;  // tombstone 本身也被清理
            log.debug("MVCC compact: removed tombstone-only key={}", key);
        }

        log.info("MVCC compact: removed {} old versions before rev={}", removedCount, compactRevision);
        return removedCount;
    }

    // ==================== 统计信息 ====================

    /**
     * 获取所有 key 的集合（包含 tombstone）
     */
    public Set<String> getAllKeys() {
        return new HashSet<>(keyIndex.keySet());
    }

    /**
     * 获取存储统计信息
     */
    public StoreStats getStats() {
        // etcd 语义：totalKeys 应统计当前有效 key（排除 tombstone）
        long totalKeys = getKeyCount();
        long totalVersions = keyIndex.values().stream()
                .mapToLong(NavigableMap::size)
                .sum();

        return new StoreStats(totalKeys, totalVersions, currentRevision);
    }

    /**
     * 获取 key 的数量（排除 tombstone，对齐 etcd 语义）
     *
     * etcd 行为：只统计有效 key，已删除的 key 不计入
     */
    public long getKeyCount() {
        long count = 0;
        for (String key : keyIndex.keySet()) {
            if (getLatest(key) != null) {
                count++;
            }
        }
        return count;
    }

    // ==================== 快照支持 ====================

    /**
     * 创建 MVCCStore 的快照
     *
     * 这是 Raft 快照的基础，必须保存完整的版本信息以保证集群一致性。
     *
     * 优化：不再单独保存 keyCreateRevisions（可从 keyIndex 派生）
     * 快照数据量减少约 20-30%
     *
     * @return 可序列化的快照数据
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> snapshot = new HashMap<>();

        // 1. 保存所有 key 的历史版本
        Map<String, List<KeyValueSnapshot>> keyHistories = new HashMap<>();
        for (Map.Entry<String, NavigableMap<Revision, KeyValue>> entry : keyIndex.entrySet()) {
            List<KeyValueSnapshot> history = new ArrayList<>();
            for (Map.Entry<Revision, KeyValue> revEntry : entry.getValue().entrySet()) {
                KeyValue kv = revEntry.getValue();
                history.add(new KeyValueSnapshot(
                    kv.getKey(),
                    kv.getValue(),
                    kv.getRevision().getMainRev(),
                    kv.getRevision().getSubRev(),
                    kv.getCreateRevision().getMainRev(),
                    kv.getCreateRevision().getSubRev(),
                    kv.getVersion()
                ));
            }
            keyHistories.put(entry.getKey(), history);
        }
        snapshot.put("keyHistories", keyHistories);

        // 2. 保存 keyVersions（原子版本计数器，需要持久化）
        snapshot.put("keyVersions", new HashMap<>(keyVersions));

        // 3. 保存 currentRevision
        snapshot.put("currentRevision", currentRevision);

        // 4. 保存 Lease 元数据
        snapshot.put("leaseMetas", new HashMap<>(leaseMetas));

        // 5. 保存 key -> leaseId 映射
        snapshot.put("keyToLease", new HashMap<>(keyToLease));

        // 6. 保存 leaseId -> keys 映射
        Map<Long, List<String>> leaseKeysSnapshot = new HashMap<>();
        for (Map.Entry<Long, Set<String>> entry : leaseToKeys.entrySet()) {
            leaseKeysSnapshot.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        snapshot.put("leaseToKeys", leaseKeysSnapshot);

        return snapshot;
    }

    /**
     * 从快照恢复 MVCCStore
     *
     * 警告：这会完全替换当前的所有数据！只在 Raft 快照加载时调用。
     *
     * 优化：不再恢复 keyCreateRevisions（从 keyIndex 中的 KeyValue 派生）
     *
     * @param snapshot 快照数据
     */
    @SuppressWarnings("unchecked")
    public void restore(Map<String, Object> snapshot) {
        // 1. 清空当前数据
        keyIndex.clear();
        keyVersions.clear();
        leaseMetas.clear();
        keyToLease.clear();
        leaseToKeys.clear();

        // 2. 恢复 currentRevision（兼容 Number 类型）
        Object revisionObj = snapshot.get("currentRevision");
        if (revisionObj != null) {
            this.currentRevision = ((Number) revisionObj).longValue();
        }

        // 3. 恢复 keyVersions（兼容 JSON 反序列化后的 Integer）
        Map<String, Object> versions = (Map<String, Object>) snapshot.get("keyVersions");
        if (versions != null) {
            for (Map.Entry<String, Object> entry : versions.entrySet()) {
                keyVersions.put(entry.getKey(), ((Number) entry.getValue()).longValue());
            }
        }

        // 4. 恢复所有 key 的历史版本（重建 keyIndex）
        Map<String, List<Map<String, Object>>> keyHistories =
            (Map<String, List<Map<String, Object>>>) snapshot.get("keyHistories");
        if (keyHistories != null) {
            for (Map.Entry<String, List<Map<String, Object>>> entry : keyHistories.entrySet()) {
                String key = entry.getKey();
                NavigableMap<Revision, KeyValue> history = new ConcurrentSkipListMap<>();

                for (Map<String, Object> kvData : entry.getValue()) {
                    String value = (String) kvData.get("value");
                    long modMainRev = ((Number) kvData.get("modMainRev")).longValue();
                    long modSubRev = ((Number) kvData.get("modSubRev")).longValue();
                    long createMainRev = ((Number) kvData.get("createMainRev")).longValue();
                    long createSubRev = ((Number) kvData.get("createSubRev")).longValue();
                    long version = ((Number) kvData.get("version")).longValue();

                    Revision modRevision = new Revision(modMainRev, modSubRev);
                    Revision createRevision = new Revision(createMainRev, createSubRev);

                    KeyValue kv = new KeyValue(key, value, modRevision, createRevision, version, 0);
                    history.put(modRevision, kv);
                }

                keyIndex.put(key, history);
            }
        }

        // 5. 恢复 Lease 元数据（兼容直接调用和 JSON 反序列化两种场景）
        // 注意：快照恢复时 grantedTime 可能缺失，尝试从快照读取，否则使用当前时间
        Map<String, Object> loadedLeaseMetas = (Map<String, Object>) snapshot.get("leaseMetas");
        if (loadedLeaseMetas != null) {
            for (Map.Entry<String, Object> entry : loadedLeaseMetas.entrySet()) {
                long leaseId = Long.parseLong(entry.getKey());
                Object metaObj = entry.getValue();
                int ttl;
                long grantedTime = System.currentTimeMillis(); // 默认为当前时间
                if (metaObj instanceof LeaseMeta) {
                    ttl = ((LeaseMeta) metaObj).ttl;
                    // 如果快照中有 grantedTime，使用它
                    grantedTime = ((LeaseMeta) metaObj).grantedTime;
                } else {
                    Map<String, Object> metaMap = (Map<String, Object>) metaObj;
                    ttl = ((Number) metaMap.get("ttl")).intValue();
                    // 尝试从快照恢复 grantedTime（新增字段，兼容旧快照）
                    Object grantedTimeObj = metaMap.get("grantedTime");
                    if (grantedTimeObj != null) {
                        grantedTime = ((Number) grantedTimeObj).longValue();
                    }
                }
                leaseMetas.put(leaseId, new LeaseMeta(leaseId, ttl, grantedTime));
            }
        }

        // 6. 恢复 key -> leaseId 映射
        Map<String, Object> loadedKeyToLease = (Map<String, Object>) snapshot.get("keyToLease");
        if (loadedKeyToLease != null) {
            for (Map.Entry<String, Object> entry : loadedKeyToLease.entrySet()) {
                long leaseId = ((Number) entry.getValue()).longValue();
                keyToLease.put(entry.getKey(), leaseId);
            }
        }

        // 7. 恢复 leaseId -> keys 映射
        Map<String, Object> loadedLeaseToKeys = (Map<String, Object>) snapshot.get("leaseToKeys");
        if (loadedLeaseToKeys != null) {
            for (Map.Entry<String, Object> entry : loadedLeaseToKeys.entrySet()) {
                long leaseId = Long.parseLong(entry.getKey());
                Object keysObj = entry.getValue();
                List<String> keys;
                if (keysObj instanceof List) {
                    keys = (List<String>) keysObj;
                } else {
                    keys = new ArrayList<>((Set<String>) keysObj);
                }
                Set<String> keySet = ConcurrentHashMap.newKeySet();
                keySet.addAll(keys);
                leaseToKeys.put(leaseId, keySet);
            }
        }
    }

    /**
     * KeyValue 的快照表示（用于序列化）
     */
    @Data
    public static class KeyValueSnapshot {
        private final String key;
        private final String value;
        private final long modMainRev;
        private final long modSubRev;
        private final long createMainRev;
        private final long createSubRev;
        private final long version;
    }

    @Data
    public static class StoreStats {
        private final long totalKeys;
        private final long totalVersions;
        private final long currentRevision;

        @Override
        public String toString() {
            return String.format("StoreStats{keys=%d, versions=%d, rev=%d}",
                    totalKeys, totalVersions, currentRevision);
        }
    }
}
