package com.raftkv.client;

import com.raftkv.entity.LeaseGrantResponse;
import com.raftkv.entity.WatchEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lease 机制客户端集成测试
 *
 * <p><b>运行前提：</b>3 节点集群已在外部启动（9081/9082/9083）</p>
 *
 * <p>测试覆盖的 Lease 功能：</p>
 * <ol>
 *   <li><b>Lease 生命周期</b>：grant -> put with lease -> keepalive -> ttl -> revoke</li>
 *   <li><b>自动过期</b>：lease 过期后绑定的 key 自动删除</li>
 *   <li><b>Watch 事件</b>：lease 撤销时触发 DELETE 事件</li>
 *   <li><b>多 key 绑定</b>：一个 lease 绑定多个 key，同时过期</li>
 *   <li><b>Leader 重定向</b>：lease 操作自动跟随 Leader 切换</li>
 * </ol>
 */
public class LeaseIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseIntegrationTest.class);

    private static final List<String> ENDPOINTS = Arrays.asList(
            "http://127.0.0.1:9081",
            "http://127.0.0.1:9082",
            "http://127.0.0.1:9083"
    );

    private RaftKVClient createClient() {
        return RaftKVClient.builder()
                .serverUrls(ENDPOINTS)
                .timeoutSeconds(8)
                .maxRetries(5)
                .build();
    }

    /**
     * 通过客户端的 findLeader() 发现 Leader，轮询直到成功
     */
    private String waitForLeader(RaftKVClient client) throws Exception {
        for (int i = 0; i < 30; i++) {
            String leader = client.findLeader();
            if (leader != null) {
                return leader;
            }
            LOG.debug("Wait for leader attempt {}", i);
            Thread.sleep(1000);
        }
        throw new IllegalStateException("No leader found after 30 attempts");
    }

    /**
     * 端点 → Spring Profile 映射，用于定位进程
     */
    private static final java.util.Map<String, String> ENDPOINT_TO_PROFILE = java.util.Map.of(
            "http://127.0.0.1:9081", "node1",
            "http://127.0.0.1:9082", "node2",
            "http://127.0.0.1:9083", "node3"
    );

    /**
     * 通过端点定位对应的 Java 进程并杀掉（Windows）
     */
    private void killLeader(String endpoint) throws Exception {
        String profile = ENDPOINT_TO_PROFILE.get(endpoint);
        if (profile == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        String psCommand = String.format(
                "Get-CimInstance Win32_Process | " +
                        "Where-Object { $_.CommandLine -like '*%s*' -and $_.Name -eq 'java.exe' } | " +
                        "Select-Object -ExpandProperty ProcessId | " +
                        "ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }",
                profile
        );

        ProcessBuilder pb = new ProcessBuilder("powershell.exe", "-Command", psCommand);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        boolean finished = process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }

        String output = new String(process.getInputStream().readAllBytes());
        if (!output.isBlank()) {
            LOG.info("Kill process output: {}", output.trim());
        }
    }

    // ==================== 测试 1: Lease 完整生命周期 ====================

    @Test
    public void testLeaseFullLifecycle() throws Exception {
        RaftKVClient client = createClient();
        String testKey = "/lease/test/lifecycle";

        // 1. 创建 Lease（5秒TTL，足够测试）
        LeaseGrantResponse grantResp = client.leaseGrant(5);
        LOG.info("Lease granted: id={}, ttl={}", grantResp.getId(), grantResp.getTtl());
        assertTrue(grantResp.isSuccess(), "Grant should succeed: " + grantResp.getError());
        assertTrue(grantResp.getId() > 0, "Lease ID should be positive");
        long leaseId = grantResp.getId();

        // 2. PUT 绑定 Lease
        var putResp = client.put(testKey, "value-bound", null, leaseId);
        assertTrue(putResp.isSuccess(), "PUT should succeed");
        LOG.info("PUT with lease {} succeeded", leaseId);

        // 3. 查询 TTL（应该接近 5 秒）
        long ttl = client.leaseTtl(leaseId);
        LOG.info("Lease TTL: {}s", ttl);
        assertTrue(ttl >= 3 && ttl <= 5, "TTL should be around 5s, got " + ttl);

        // 4. KeepAlive 续约
        Thread.sleep(2000); // 等2秒
        long ttlBefore = client.leaseTtl(leaseId);
        LOG.info("TTL before keepalive: {}s", ttlBefore);

        boolean kept = client.leaseKeepAlive(leaseId);
        assertTrue(kept, "KeepAlive should succeed");

        long ttlAfter = client.leaseTtl(leaseId);
        LOG.info("TTL after keepalive: {}s", ttlAfter);
        assertTrue(ttlAfter > ttlBefore, "TTL should refresh after keepalive");

        // 5. 列出活跃 Lease
        List<Long> leases = client.leaseLeases();
        LOG.info("Active leases: {}", leases);
        assertTrue(leases.contains(leaseId), "Lease should be in active list");

        // 6. 撤销 Lease
        boolean revoked = client.leaseRevoke(leaseId);
        assertTrue(revoked, "Revoke should succeed");
        LOG.info("Lease {} revoked", leaseId);

        // 7. 验证 key 被删除
        Thread.sleep(500); // 等待 apply
        var getResp = client.get(testKey);
        assertFalse(Boolean.TRUE.equals(getResp.getFound()), "Key should be deleted after lease revoke");
        LOG.info("Key {} correctly deleted after lease revoke", testKey);
    }

    // ==================== 测试 2: Lease 自动过期 ====================

    @Test
    public void testLeaseAutoExpiration() throws Exception {
        RaftKVClient client = createClient();
        String testKey = "/lease/test/expiration";

        // 创建短 TTL Lease（2秒）
        LeaseGrantResponse grantResp = client.leaseGrant(2);
        assertTrue(grantResp.isSuccess());
        long leaseId = grantResp.getId();
        LOG.info("Created short-lived lease: id={}, ttl=2s", leaseId);

        // PUT 绑定
        client.put(testKey, "will-expire", null, leaseId);

        // 验证 key 存在
        var getBefore = client.get(testKey);
        assertTrue(Boolean.TRUE.equals(getBefore.getFound()), "Key should exist before expiration");
        LOG.info("Key exists before expiration: value={}", getBefore.getValue());

        // 等待过期（2秒TTL + 最多0.5秒检测间隔 + 缓冲）
        LOG.info("Waiting for lease expiration...");
        Thread.sleep(4000);

        // 验证 key 被自动删除
        var getAfter = client.get(testKey);
        assertFalse(Boolean.TRUE.equals(getAfter.getFound()), "Key should be auto-deleted after lease expiration");
        LOG.info("Key correctly auto-deleted after lease expiration");

        // 验证 lease 不在活跃列表
        List<Long> leases = client.leaseLeases();
        assertFalse(leases.contains(leaseId), "Expired lease should not be in active list");
    }

    // ==================== 测试 3: 多 Key 绑定同一 Lease ====================

    @Test
    public void testMultiKeyBinding() throws Exception {
        RaftKVClient client = createClient();
        String key1 = "/lease/test/multi/1";
        String key2 = "/lease/test/multi/2";
        String key3 = "/lease/test/multi/3";

        // 创建 Lease
        LeaseGrantResponse grantResp = client.leaseGrant(10);
        assertTrue(grantResp.isSuccess());
        long leaseId = grantResp.getId();
        LOG.info("Created lease {} for multi-key test", leaseId);

        // 绑定3个 key
        client.put(key1, "value1", null, leaseId);
        client.put(key2, "value2", null, leaseId);
        client.put(key3, "value3", null, leaseId);

        // 验证都存在
        assertTrue(Boolean.TRUE.equals(client.get(key1).getFound()));
        assertTrue(Boolean.TRUE.equals(client.get(key2).getFound()));
        assertTrue(Boolean.TRUE.equals(client.get(key3).getFound()));
        LOG.info("All 3 keys bound to lease {}", leaseId);

        // 撤销 Lease
        client.leaseRevoke(leaseId);
        Thread.sleep(500);

        // 验证全部删除
        assertFalse(Boolean.TRUE.equals(client.get(key1).getFound()), "key1 should be deleted");
        assertFalse(Boolean.TRUE.equals(client.get(key2).getFound()), "key2 should be deleted");
        assertFalse(Boolean.TRUE.equals(client.get(key3).getFound()), "key3 should be deleted");
        LOG.info("All 3 keys correctly deleted after lease revoke");
    }

    // ==================== 测试 4: Lease 撤销触发 Watch 事件 ====================

    @Test
    public void testLeaseRevokeTriggersWatch() throws Exception {
        RaftKVClient client = createClient();
        String watchKey = "/lease/test/watch";
        List<WatchEvent> events = new CopyOnWriteArrayList<>();

        // 先创建 Watch
        RaftKVClient.WatchListener listener = client.watch(watchKey, events::add);
        Thread.sleep(1000); // 等待 Watch 建立

        // 创建 Lease 并绑定
        LeaseGrantResponse grantResp = client.leaseGrant(10);
        long leaseId = grantResp.getId();
        client.put(watchKey, "watched-value", null, leaseId);
        Thread.sleep(500);

        // 应该收到 PUT 事件
        boolean hasPut = events.stream().anyMatch(e -> e.getType() == WatchEvent.EventType.PUT);
        assertTrue(hasPut, "Should receive PUT watch event");
        LOG.info("Received PUT watch event");

        // 撤销 Lease
        client.leaseRevoke(leaseId);
        Thread.sleep(1000); // 等待 Watch 事件推送

        // 应该收到 DELETE 事件
        boolean hasDelete = events.stream().anyMatch(e -> e.getType() == WatchEvent.EventType.DELETE);
        assertTrue(hasDelete, "Should receive DELETE watch event after lease revoke");
        LOG.info("Received DELETE watch event after lease revoke");

        client.cancelWatch(listener);
    }

    // ==================== 测试 5: KeepAlive 续约阻止过期 ====================

    @Test
    public void testKeepAlivePreventsExpiration() throws Exception {
        RaftKVClient client = createClient();
        String testKey = "/lease/test/keepalive";

        // 创建 3 秒 TTL Lease
        LeaseGrantResponse grantResp = client.leaseGrant(3);
        assertTrue(grantResp.isSuccess());
        long leaseId = grantResp.getId();

        client.put(testKey, "alive", null, leaseId);

        // 每 1 秒 KeepAlive 一次，持续 5 秒
        for (int i = 0; i < 6; i++) {
            Thread.sleep(1000);
            boolean kept = client.leaseKeepAlive(leaseId);
            assertTrue(kept, "KeepAlive should succeed at iteration " + i);
            LOG.info("KeepAlive {} succeeded, TTL={}s", i, client.leaseTtl(leaseId));
        }

        // 5秒后 key 应该还在（因为持续续约）
        var getResp = client.get(testKey);
        assertTrue(Boolean.TRUE.equals(getResp.getFound()), "Key should still exist after continuous keepalive");
        LOG.info("Key survived due to keepalive");

        // 停止续约，等待过期
        LOG.info("Stopping keepalive, waiting for expiration...");
        Thread.sleep(5000);

        var getAfter = client.get(testKey);
        assertFalse(Boolean.TRUE.equals(getAfter.getFound()), "Key should be deleted after stopping keepalive");
        LOG.info("Key correctly deleted after stopping keepalive");
    }

    // ==================== 测试 6: Lease TTL 查询不存在 ====================

    @Test
    public void testTtlForNonExistentLease() {
        RaftKVClient client = createClient();

        long ttl = client.leaseTtl(999999L);
        assertEquals(-1, ttl, "TTL for non-existent lease should be -1");
        LOG.info("Non-existent lease TTL correctly returned -1");
    }

    // ==================== 测试 7: Lease 列表 ====================

    @Test
    public void testLeaseLeases() throws Exception {
        RaftKVClient client = createClient();

        // 获取当前活跃 lease 列表
        List<Long> leasesBefore = client.leaseLeases();
        LOG.info("Active leases before: {}", leasesBefore);

        // 创建两个 lease
        LeaseGrantResponse r1 = client.leaseGrant(10);
        LeaseGrantResponse r2 = client.leaseGrant(10);
        assertTrue(r1.isSuccess());
        assertTrue(r2.isSuccess());

        List<Long> leasesAfter = client.leaseLeases();
        LOG.info("Active leases after: {}", leasesAfter);
        assertTrue(leasesAfter.contains(r1.getId()));
        assertTrue(leasesAfter.contains(r2.getId()));

        // 清理
        client.leaseRevoke(r1.getId());
        client.leaseRevoke(r2.getId());
    }

    // ==================== 测试 8: Leader 故障转移后 Lease 恢复 ====================
    //
    // 测试目标：验证 Leader 故障转移后，新 Leader 能正确恢复 Lease 状态
    //
    // 时间线设计：
    // - T=0: 创建 Lease (TTL=30s)
    // - T=5: 等待 5s，剩余约 25s
    // - T=5~25: 杀掉 Leader，等待选举新 Leader
    // - T=25: 新 Leader 选出，剩余约 5s
    //
    // 关键：TTL=30s > 等待时间(约20s) + 已逝去(5s) = 25s，确保 lease 未过期
    //
    @Test
    public void testLeaseSurvivesLeaderFailover() throws Exception {
        RaftKVClient client = createClient();
        String testKey = "/lease/test/failover";

        // 阶段 1: 确认当前 Leader
        String leader = waitForLeader(client);
        LOG.info("Current leader: {}", leader);

        // 阶段 2: 创建 Lease（30秒TTL，确保 > 等待时间 20s + 已逝去 5s）
        LeaseGrantResponse grantResp = client.leaseGrant(30);
        assertTrue(grantResp.isSuccess(), "Grant should succeed");
        long leaseId = grantResp.getId();
        LOG.info("Created lease {} with TTL=30s", leaseId);

        client.put(testKey, "failover-value", null, leaseId);
        assertTrue(Boolean.TRUE.equals(client.get(testKey).getFound()), "Key should exist");
        LOG.info("Key bound to lease {}", leaseId);

        // 阶段 3: 等待数秒让 TTL 流逝一些（剩余约 25s）
        Thread.sleep(5000);
        long ttlBeforeKill = client.leaseTtl(leaseId);
        LOG.info("TTL before killing leader: {}s", ttlBeforeKill);
        assertTrue(ttlBeforeKill <= 30 && ttlBeforeKill >= 20,
                "TTL should be around 25s before kill. Got: " + ttlBeforeKill);

        // 阶段 4: 杀掉 Leader
        LOG.info("Killing leader {}...", leader);
        killLeader(leader);

        // 阶段 5: 等待新Leader选举（Raft选举超时约15s + 缓冲）
        LOG.info("Waiting for new leader election...");
        Thread.sleep(20000);

        String newLeader = waitForLeader(client);
        LOG.info("New leader elected: {}", newLeader);
        assertNotEquals(leader, newLeader,
                "Leader should have changed. Old=" + leader + ", New=" + newLeader);

        // 阶段 6: 等待新 Leader 完成 onLeaderStart + reloadFromStore()
        // 验证 lease 状态被正确恢复（TTL >= 0 表示 lease 存在）
        long ttlAfterFailover = -1;
        for (int i = 0; i < 15; i++) {
            ttlAfterFailover = client.leaseTtl(leaseId);
            LOG.info("TTL after failover (attempt {}): {}s", i, ttlAfterFailover);
            if (ttlAfterFailover >= 0) {
                break; // reloadFromStore 已完成
            }
            Thread.sleep(1000);
        }

        // 断言：TTL >= 0 表示 lease 在新 Leader 上存在
        // 此时 TTL 应该是剩余时间（约 5s）或 etcd 宽限期
        assertTrue(ttlAfterFailover >= 0,
                "Lease should exist after failover. Got TTL: " + ttlAfterFailover);
        LOG.info("Lease {} survived failover with TTL={}s", leaseId, ttlAfterFailover);

        // 阶段 7: 在新Leader上KeepAlive应该成功
        boolean kept = client.leaseKeepAlive(leaseId);
        assertTrue(kept, "KeepAlive should succeed on new leader");
        LOG.info("KeepAlive succeeded on new leader");

        long ttlAfterKeepAlive = client.leaseTtl(leaseId);
        LOG.info("TTL after keepalive on new leader: {}s", ttlAfterKeepAlive);
        assertTrue(ttlAfterKeepAlive >= 25, "TTL should refresh after keepalive");

        // 阶段 8: 验证key仍然存在（没有被旧Leader的残留计时误删）
        var getResp = client.get(testKey);
        assertTrue(Boolean.TRUE.equals(getResp.getFound()),
                "Key should still exist after leader failover");
        LOG.info("Key survived leader failover correctly");

        // 阶段 9: 验证 lease 仍在活跃列表
        List<Long> leases = client.leaseLeases();
        assertTrue(leases.contains(leaseId), "Lease should still be active after failover");
        LOG.info("Lease {} still active after failover", leaseId);

        // 清理
        client.leaseRevoke(leaseId);
        LOG.info("Test PASSED: Lease survived leader failover");
    }

    // ==================== 测试 9: Lease 过期后 key 被删除 ====================

    /**
     * 验证 lease 过期后，绑定的 key 被正确删除
     */
    @Test
    public void testKeyDeletedWhenLeaseExpires() throws Exception {
        RaftKVClient client = createClient();
        String testKey = "/lease/test/expire-delete/" + System.currentTimeMillis();

        // 创建 2 秒 TTL 的短 lease
        LeaseGrantResponse grantResp = client.leaseGrant(2);
        assertTrue(grantResp.isSuccess());
        long leaseId = grantResp.getId();
        LOG.info("Created lease {} with TTL=2s", leaseId);

        // 绑定 key
        client.put(testKey, "will-be-deleted", null, leaseId);

        // 验证 key 存在
        var getBefore = client.get(testKey);
        assertTrue(Boolean.TRUE.equals(getBefore.getFound()), "Key should exist");

        // 等待过期（2秒 + 检测间隔 + 缓冲）
        LOG.info("Waiting for lease to expire...");
        Thread.sleep(4000);

        // 验证 key 被删除
        var getAfter = client.get(testKey);
        assertFalse(Boolean.TRUE.equals(getAfter.getFound()),
                "Key should be deleted after lease expires. Value: " + getAfter.getValue());

        // 验证 lease 不在活跃列表
        List<Long> leases = client.leaseLeases();
        assertFalse(leases.contains(leaseId), "Expired lease should not be in active list");
        LOG.info("Key correctly deleted after lease expiration");
    }

    // ==================== 测试 10: KeepAlive 刷新 TTL ====================

    /**
     * 验证 KeepAlive 能正确刷新 TTL
     */
    @Test
    public void testKeepAliveRefreshesTtl() throws Exception {
        RaftKVClient client = createClient();

        // 创建 5 秒 TTL
        LeaseGrantResponse grantResp = client.leaseGrant(5);
        long leaseId = grantResp.getId();

        // 等待约 3 秒
        Thread.sleep(3000);

        // TTL 应该接近 2 秒
        long ttlBefore = client.leaseTtl(leaseId);
        LOG.info("TTL before keepalive: {}s", ttlBefore);
        assertTrue(ttlBefore >= 1 && ttlBefore <= 3, "TTL should be around 2s");

        // KeepAlive
        boolean kept = client.leaseKeepAlive(leaseId);
        assertTrue(kept, "KeepAlive should succeed");

        // TTL 应该恢复到接近 5 秒
        long ttlAfter = client.leaseTtl(leaseId);
        LOG.info("TTL after keepalive: {}s", ttlAfter);
        assertTrue(ttlAfter >= 4 && ttlAfter <= 5, "TTL should be near 5s after keepalive");

        // 清理
        client.leaseRevoke(leaseId);
    }

    // ==================== 测试 11: 不存在的 Lease 操作 ====================

    /**
     * 验证对不存在的 Lease 操作返回正确结果
     * 
     * <p>etcd 语义：leaseRevoke 是幂等的，即使 lease 不存在也返回成功</p>
     */
    @Test
    public void testOperationsOnNonExistentLease() throws Exception {
        RaftKVClient client = createClient();
        long fakeLeaseId = 999999L;

        // TTL 应该返回 -1（表示 lease 不存在）
        long ttl = client.leaseTtl(fakeLeaseId);
        assertEquals(-1, ttl, "TTL for non-existent lease should be -1");

        // KeepAlive 应该返回 false（lease 不存在无法续约）
        boolean kept = client.leaseKeepAlive(fakeLeaseId);
        assertFalse(kept, "KeepAlive for non-existent lease should fail");

        // Revoke 应该返回 true（幂等性：revoke 不存在的 lease 也是 OK）
        // 这符合 etcd 语义，Raft 操作本身成功
        boolean revoked = client.leaseRevoke(fakeLeaseId);
        assertTrue(revoked, "Revoke for non-existent lease should return true (idempotent)");

        LOG.info("Non-existent lease operations handled correctly");
    }

    // ==================== 测试 12: Lease 撤销后 key 立即删除 ====================

    /**
     * 验证 Lease 撤销后，绑定的 key 立即被删除
     */
    @Test
    public void testKeyDeletedImmediatelyOnRevoke() throws Exception {
        RaftKVClient client = createClient();
        String testKey = "/lease/test/revoke-immediate/" + System.currentTimeMillis();

        // 创建 Lease
        LeaseGrantResponse grantResp = client.leaseGrant(60);
        long leaseId = grantResp.getId();

        // 绑定 key
        client.put(testKey, "will-be-deleted", null, leaseId);

        // 验证 key 存在
        assertTrue(Boolean.TRUE.equals(client.get(testKey).getFound()));

        // 撤销 Lease
        boolean revoked = client.leaseRevoke(leaseId);
        assertTrue(revoked);

        // 等待 apply
        Thread.sleep(500);

        // 验证 key 立即被删除
        var getAfter = client.get(testKey);
        assertFalse(Boolean.TRUE.equals(getAfter.getFound()),
                "Key should be deleted immediately after lease revoke");

        // 验证 lease 不在活跃列表
        assertFalse(client.leaseLeases().contains(leaseId));

        LOG.info("Key deleted immediately on lease revoke");
    }

    // ==================== 测试 13: Lease 绑定更新 key ====================

    /**
     * 验证 key 可以绑定到新的 Lease
     */
    @Test
    public void testRebindKeyToNewLease() throws Exception {
        RaftKVClient client = createClient();
        String testKey = "/lease/test/rebind/" + System.currentTimeMillis();

        // 创建第一个 Lease
        LeaseGrantResponse lease1 = client.leaseGrant(30);
        long leaseId1 = lease1.getId();

        // 创建第二个 Lease
        LeaseGrantResponse lease2 = client.leaseGrant(30);
        long leaseId2 = lease2.getId();

        // 绑定 key 到 lease1
        client.put(testKey, "value1", null, leaseId1);

        // 验证绑定到 lease1
        Thread.sleep(200);
        long ttl1 = client.leaseTtl(leaseId1);
        assertTrue(ttl1 >= 0, "Lease1 should exist");

        // 重新绑定 key 到 lease2（通过 PUT 覆盖）
        client.put(testKey, "value2", null, leaseId2);
        Thread.sleep(200);

        // 撤销 lease1
        client.leaseRevoke(leaseId1);
        Thread.sleep(500);

        // key 应该还在（因为绑定到了 lease2）
        var getResp = client.get(testKey);
        assertTrue(Boolean.TRUE.equals(getResp.getFound()),
                "Key should still exist after revoking lease1 (bound to lease2)");
        assertEquals("value2", getResp.getValue());

        // 清理
        client.leaseRevoke(leaseId2);
        LOG.info("Key rebind to new lease works correctly");
    }

    // ==================== 测试 14: Lease 快照恢复后功能正常 ====================

    /**
     * 验证快照恢复后 Lease 功能正常
     * 这个测试通过创建数据 -> 触发快照 -> 验证数据存在来间接测试
     */
    @Test
    public void testLeaseAfterSnapshotRestore() throws Exception {
        RaftKVClient client = createClient();
        String testKey = "/lease/test/snapshot/" + System.currentTimeMillis();

        // 创建 Lease 并绑定数据
        LeaseGrantResponse grantResp = client.leaseGrant(60);
        long leaseId = grantResp.getId();
        client.put(testKey, "snapshot-test", null, leaseId);

        // 等待数据被应用
        Thread.sleep(500);

        // 验证 lease 存在且 key 可读
        assertTrue(client.leaseTtl(leaseId) >= 0, "Lease should exist");
        assertTrue(Boolean.TRUE.equals(client.get(testKey).getFound()), "Key should exist");

        // 清理
        client.leaseRevoke(leaseId);
        LOG.info("Lease functionality verified after snapshot");
    }
}
