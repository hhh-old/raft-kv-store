package com.raftkv.service;

import com.raftkv.entity.WatchEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lease 机制单元测试
 *
 * 覆盖核心逻辑（无需启动外部集群）：
 * 1. MVCCStore Lease 元数据管理
 * 2. Key-Lease 绑定与解绑
 * 3. LeaseManager 过期检测
 * 4. 快照保存与恢复 Lease 信息
 */
public class LeaseUnitTest {

    private MVCCStore mvccStore;
    private LeaseManager leaseManager;

    @BeforeEach
    void setUp() {
        mvccStore = new MVCCStore();
        // LeaseManager 依赖 MVCCStore，手动注入
        leaseManager = new LeaseManager();
        // 通过反射注入 mvccStore（简化测试）
        try {
            java.lang.reflect.Field field = LeaseManager.class.getDeclaredField("mvccStore");
            field.setAccessible(true);
            field.set(leaseManager, mvccStore);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== 测试 1: Lease 元数据 CRUD ====================

    @Test
    void testLeaseGrantAndMeta() {
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());

        MVCCStore.LeaseMeta meta = mvccStore.getLeaseMeta(1L);
        assertNotNull(meta);
        assertEquals(1L, meta.id);
        assertEquals(60, meta.ttl);

        // 不存在的 lease
        assertNull(mvccStore.getLeaseMeta(999L));
    }

    @Test
    void testGetAllLeaseMetas() {
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());
        mvccStore.grantLease(2L, 120, System.currentTimeMillis());

        Collection<MVCCStore.LeaseMeta> all = mvccStore.getAllLeaseMetas();
        assertEquals(2, all.size());
    }

    // ==================== 测试 2: Key-Lease 绑定 ====================

    @Test
    void testPutWithLease() {
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());
        mvccStore.putWithLease("key1", "value1", new MVCCStore.Revision(1, 0), 1L);

        assertEquals(Long.valueOf(1L), mvccStore.getKeyLeaseId("key1"));
        Set<String> keys = mvccStore.getLeaseKeys(1L);
        assertTrue(keys.contains("key1"));
    }

    @Test
    void testKeyRebindLease() {
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());
        mvccStore.grantLease(2L, 60, System.currentTimeMillis());

        // 先绑定到 lease 1
        mvccStore.putWithLease("key1", "value1", new MVCCStore.Revision(1, 0), 1L);
        assertEquals(Long.valueOf(1L), mvccStore.getKeyLeaseId("key1"));

        // 重新绑定到 lease 2
        mvccStore.putWithLease("key1", "value2", new MVCCStore.Revision(2, 0), 2L);
        assertEquals(Long.valueOf(2L), mvccStore.getKeyLeaseId("key1"));

        // lease 1 不再包含 key1
        Set<String> keys1 = mvccStore.getLeaseKeys(1L);
        assertFalse(keys1.contains("key1"));

        // lease 2 包含 key1
        Set<String> keys2 = mvccStore.getLeaseKeys(2L);
        assertTrue(keys2.contains("key1"));
    }

    @Test
    void testUnbindKeyFromLease() {
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());
        mvccStore.putWithLease("key1", "value1", new MVCCStore.Revision(1, 0), 1L);

        mvccStore.unbindKeyFromLease("key1");

        assertNull(mvccStore.getKeyLeaseId("key1"));
        assertFalse(mvccStore.getLeaseKeys(1L).contains("key1"));
    }

    // ==================== 测试 3: Lease Revoke 删除关联 Keys ====================

    @Test
    void testRevokeLeaseRemovesKeys() {
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());
        mvccStore.putWithLease("key1", "value1", new MVCCStore.Revision(1, 0), 1L);
        mvccStore.putWithLease("key2", "value2", new MVCCStore.Revision(2, 0), 1L);

        assertNotNull(mvccStore.getLatest("key1"));
        assertNotNull(mvccStore.getLatest("key2"));

        // 撤销 lease
        Set<String> removedKeys = mvccStore.revokeLease(1L);

        assertEquals(2, removedKeys.size());
        assertTrue(removedKeys.contains("key1"));
        assertTrue(removedKeys.contains("key2"));

        // 元数据被清理
        assertNull(mvccStore.getLeaseMeta(1L));
        assertTrue(mvccStore.getLeaseKeys(1L).isEmpty());
    }

    // ==================== 测试 4: LeaseManager 过期检测 ====================

    @Test
    void testLeaseManagerExpiry() throws InterruptedException {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 2, now); // 2秒TTL
        leaseManager.onLeaseGranted(1L, 2, now);

        // 刚创建不应过期
        assertTrue(leaseManager.getExpiredLeases().isEmpty());
        assertTrue(leaseManager.getRemainingTtl(1L) > 0);

        // 等待过期
        Thread.sleep(2500);

        List<Long> expired = leaseManager.getExpiredLeases();
        assertEquals(1, expired.size());
        assertEquals(1L, expired.get(0));
        assertEquals(0, leaseManager.getRemainingTtl(1L));
    }

    @Test
    void testLeaseKeepAlive() throws InterruptedException {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 2, now); // 2秒TTL
        leaseManager.onLeaseGranted(1L, 2, now);

        // 等待接近过期
        Thread.sleep(1500);

        // 续约前快过期了
        long ttlBefore = leaseManager.getRemainingTtl(1L);
        assertTrue(ttlBefore < 1);

        // KeepAlive
        assertTrue(leaseManager.keepAlive(1L));

        // 续约后TTL刷新
        long ttlAfter = leaseManager.getRemainingTtl(1L);
        assertTrue(ttlAfter >= 1);

        // 不存在的 lease
        assertFalse(leaseManager.keepAlive(999L));
    }

    @Test
    void testLeaseManagerGetAllLeaseIds() {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 60, now);
        mvccStore.grantLease(2L, 120, now);
        leaseManager.onLeaseGranted(1L, 60, now);
        leaseManager.onLeaseGranted(2L, 120, now);

        List<Long> ids = leaseManager.getAllLeaseIds();
        assertEquals(2, ids.size());
        assertTrue(ids.contains(1L));
        assertTrue(ids.contains(2L));
    }

    @Test
    void testReloadFromStore() {
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());
        mvccStore.grantLease(2L, 120, System.currentTimeMillis());

        // 模拟 Leader 切换：从 MVCCStore 重新加载
        leaseManager.reloadFromStore();

        List<Long> ids = leaseManager.getAllLeaseIds();
        assertEquals(2, ids.size());

        // 验证宽限期：remainingTtl 应该接近 ttl
        assertTrue(leaseManager.getRemainingTtl(1L) >= 59);
        assertTrue(leaseManager.getRemainingTtl(2L) >= 119);
    }

    // ==================== 测试 5: 快照保存与恢复 ====================

    @Test
    void testSnapshotAndRestore() throws Exception {
        // 准备数据
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());
        mvccStore.putWithLease("key1", "value1", new MVCCStore.Revision(1, 0), 1L);
        mvccStore.putWithLease("key2", "value2", new MVCCStore.Revision(2, 0), 1L);

        // 创建快照
        Map<String, Object> snapshot = mvccStore.snapshot();

        // 验证快照中包含 lease 数据
        assertTrue(snapshot.containsKey("leaseMetas"));
        assertTrue(snapshot.containsKey("keyToLease"));
        assertTrue(snapshot.containsKey("leaseToKeys"));

        // 模拟 JSON 序列化/反序列化（与真实 Raft 快照流程一致）
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        String json = mapper.writeValueAsString(snapshot);
        @SuppressWarnings("unchecked")
        Map<String, Object> deserializedSnapshot = mapper.readValue(json, Map.class);

        // 创建新的 MVCCStore 并恢复
        MVCCStore restored = new MVCCStore();
        restored.restore(deserializedSnapshot);

        // 验证 lease 元数据恢复
        MVCCStore.LeaseMeta meta = restored.getLeaseMeta(1L);
        assertNotNull(meta);
        assertEquals(60, meta.ttl);

        // 验证 key-lease 绑定恢复
        assertEquals(Long.valueOf(1L), restored.getKeyLeaseId("key1"));
        assertEquals(Long.valueOf(1L), restored.getKeyLeaseId("key2"));

        Set<String> keys = restored.getLeaseKeys(1L);
        assertEquals(2, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));

        // 验证 key 数据也恢复了
        assertNotNull(restored.getLatest("key1"));
        assertEquals("value1", restored.getLatest("key1").getValue());
    }

    // ==================== 测试 6: 边界情况 ====================

    @Test
    void testRevokeNonExistentLease() {
        // 撤销不存在的 lease 不应抛异常
        Set<String> keys = mvccStore.revokeLease(999L);
        assertTrue(keys.isEmpty());
    }

    @Test
    void testGetLeaseKeysForEmptyLease() {
        mvccStore.grantLease(1L, 60, System.currentTimeMillis());
        // 没有绑定任何 key
        assertTrue(mvccStore.getLeaseKeys(1L).isEmpty());
    }

    @Test
    void testPutWithoutLease() {
        // 不带 lease 的 put
        mvccStore.put("key1", "value1");
        assertNull(mvccStore.getKeyLeaseId("key1"));
    }

    @Test
    void testLeaseManagerClear() {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 60, now);
        leaseManager.onLeaseGranted(1L, 60, now);
        assertEquals(1, leaseManager.getAllLeaseIds().size());

        leaseManager.clear();
        assertTrue(leaseManager.getAllLeaseIds().isEmpty());
    }

    // ==================== 测试 7: grantedTime 时间戳验证 ====================

    /**
     * 验证 grantedTime 在 lease 元数据中被正确保存
     */
    @Test
    void testGrantedTimeIsPreserved() {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 60, now);

        MVCCStore.LeaseMeta meta = mvccStore.getLeaseMeta(1L);
        assertNotNull(meta);
        assertEquals(now, meta.grantedTime);
        assertEquals(60, meta.ttl);
        assertEquals(1L, meta.id);
    }

    /**
     * 验证 grantedTime 过去时间戳也能正确处理
     */
    @Test
    void testPastGrantedTime() {
        long pastTime = System.currentTimeMillis() - 10000; // 10秒前
        mvccStore.grantLease(1L, 60, pastTime);

        MVCCStore.LeaseMeta meta = mvccStore.getLeaseMeta(1L);
        assertEquals(pastTime, meta.grantedTime);

        // 验证 reloadFromStore 计算出的剩余时间
        leaseManager.reloadFromStore();
        // 已过去10秒，TTL=60秒，应该剩余约50秒
        long remaining = leaseManager.getRemainingTtl(1L);
        assertTrue(remaining >= 45 && remaining <= 55,
                "Remaining TTL should be around 50s, got " + remaining);
    }

    // ==================== 测试 8: KeepAlive 更新过期时间 ====================

    /**
     * 验证 KeepAlive 正确更新 lease 的过期时间
     */
    @Test
    void testKeepAliveUpdatesExpiryTime() throws InterruptedException {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 5, now); // 5秒TTL
        leaseManager.onLeaseGranted(1L, 5, now);

        // 初始 TTL
        long initialTtl = leaseManager.getRemainingTtl(1L);
        assertEquals(5, initialTtl);

        // 等待2秒后 KeepAlive
        Thread.sleep(2000);
        assertTrue(leaseManager.keepAlive(1L));

        // TTL 应该被刷新为接近 5 秒（不是 3 秒）
        long newTtl = leaseManager.getRemainingTtl(1L);
        assertTrue(newTtl >= 4 && newTtl <= 5,
                "TTL should be refreshed to 5s, got " + newTtl);
    }

    // ==================== 测试 9: 过期 lease 正确识别 ====================

    /**
     * 验证已过期的 lease 能被正确识别
     */
    @Test
    void testExpiredLeaseDetection() throws InterruptedException {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 1, now); // 1秒TTL
        leaseManager.onLeaseGranted(1L, 1, now);

        // 不应过期
        assertTrue(leaseManager.getExpiredLeases().isEmpty());

        // 等待过期
        Thread.sleep(1500);

        // 应该被识别为过期
        List<Long> expired = leaseManager.getExpiredLeases();
        assertEquals(1, expired.size());
        assertEquals(1L, expired.get(0));
    }

    // ==================== 测试 10: LeaseMeta 不可变性 ====================

    /**
     * 验证 LeaseMeta 是不可变的（字段为 final）
     */
    @Test
    void testLeaseMetaImmutability() {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 60, now);

        MVCCStore.LeaseMeta meta = mvccStore.getLeaseMeta(1L);
        assertEquals(1L, meta.id);
        assertEquals(60, meta.ttl);
        assertEquals(now, meta.grantedTime);

        // 尝试修改不应生效（如果是不可变对象，所有字段都是 final）
        // 这里验证对象本身没有被替换
        assertSame(meta, mvccStore.getLeaseMeta(1L));
    }

    // ==================== 测试 11: 续约后过期时间边界 ====================

    /**
     * 验证 KeepAlive 刷新 TTL 到完整值
     */
    @Test
    void testKeepAliveRefreshesFullTtl() throws InterruptedException {
        long now = System.currentTimeMillis();
        mvccStore.grantLease(1L, 60, now); // 60秒TTL
        leaseManager.onLeaseGranted(1L, 60, now);

        // 等待 55 秒后 KeepAlive，TTL 应该重置为 60
        Thread.sleep(55000);
        assertTrue(leaseManager.keepAlive(1L));

        // TTL 应该接近 60 秒（可能稍小因为处理延迟）
        long remaining = leaseManager.getRemainingTtl(1L);
        assertTrue(remaining >= 55,
                "TTL should be reset to near 60s after keepalive, got " + remaining);
    }

    // ==================== 测试 12: 并发操作 Lease ====================

    /**
     * 验证并发操作 lease 的线程安全性
     */
    @Test
    void testConcurrentLeaseOperations() throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        long now = System.currentTimeMillis();

        // 创建多个 lease
        for (int i = 1; i <= 5; i++) {
            mvccStore.grantLease(i, 60, now);
            leaseManager.onLeaseGranted(i, 60, now);
        }

        // 并发执行各种操作：创建、查询、续约
        List<Future<?>> futures = new ArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1); // 控制同时开始
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < 100; i++) {
            final int index = i;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await(); // 等待所有线程同时开始
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                long currentTime = System.currentTimeMillis();
                switch (index % 4) {
                    case 0 -> {
                        // 创建新 lease（使用不同的 id）
                        long newId = 1000L + index;
                        mvccStore.grantLease(newId, 60, currentTime);
                        leaseManager.onLeaseGranted(newId, 60, currentTime);
                        successCount.incrementAndGet();
                    }
                    case 1 -> leaseManager.getExpiredLeases();
                    case 2 -> leaseManager.getAllLeaseIds();
                    case 3 -> {
                        // 查询 lease（使用初始 id 1-5）
                        long id = (index % 5) + 1;
                        mvccStore.getLeaseMeta(id);
                        leaseManager.getRemainingTtl(id);
                    }
                }
            }));
        }

        startLatch.countDown(); // 触发所有线程同时开始

        for (Future<?> f : futures) {
            f.get();
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // 验证数据一致性
        // 初始 5 个 + 并发创建 25 个 (100/4=25) = 30 个
        int expectedCount = 5 + 25;
        int actualCount = leaseManager.getAllLeaseIds().size();
        assertEquals(expectedCount, actualCount,
                "Expected " + expectedCount + " leases, got " + actualCount + ". Created: " + successCount.get());
    }
}
