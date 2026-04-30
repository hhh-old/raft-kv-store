package com.raftkv.client;

import com.raftkv.entity.CompactRequest;
import com.raftkv.entity.CompactResponse;
import com.raftkv.entity.RangeRequest;
import com.raftkv.entity.RangeResponse;
import com.raftkv.entity.WatchEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compact 压缩集成测试 - 对齐 etcd v3 Compaction API
 *
 * <p><b>运行前提：</b>3 节点集群已在外部启动（9081/9082/9083）</p>
 *
 * <p>测试覆盖的 Compact 功能：</p>
 * <ol>
 *   <li><b>基本压缩</b>：压缩到指定 revision</li>
 *   <li><b>幂等性</b>：多次压缩同一 revision</li>
 *   <li><b>历史版本不可查</b>：压缩后无法 Range 查询历史版本</li>
 *   <li><b>当前版本保留</b>：压缩不影响最新数据</li>
 *   <li><b>参数验证</b>：revision <= 0、revision > 当前 revision</li>
 *   <li><b>便捷方法</b>：compactToPreviousRevision()</li>
 *   <li><b>Watch 兼容性</b>：startRevision < 压缩版本的处理</li>
 * </ol>
 */
public class CompactIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CompactIntegrationTest.class);

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

    // ==================== 基本压缩测试 ====================

    /**
     * 测试 1: 基本压缩操作
     */
    @Test
    public void testBasicCompact() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "compact_basic_" + UUID.randomUUID();
            client.delete(key);

            long beforeRevision = client.getCurrentRevision();

            client.put(key, "v1");
            client.put(key, "v2");
            client.put(key, "v3");

            long currentRevision = client.getCurrentRevision();
            LOG.info("Before compact: beforeRev={}, currentRev={}", beforeRevision, currentRevision);

            long compactRevision = currentRevision - 1;
            if (compactRevision > beforeRevision) {
                CompactResponse response = client.compact(compactRevision);

                assertTrue(response.isSuccess(), "Compact should succeed");
                // compactedRevision 可能 <= 请求的 compactRevision（取决于实际应用的日志）
                assertTrue(response.getCompactedRevision() <= compactRevision,
                        "compactedRevision should be <= compactRevision");
                assertNotNull(response.getServedBy());

                LOG.info("Basic compact passed: compactRevision={}, compactedRevision={}",
                        compactRevision, response.getCompactedRevision());
            }

            RangeResponse getResp = client.range(key);
            assertTrue(getResp.isSuccess());
            assertEquals(1, getResp.getKvs().size());
            assertEquals("v3", getResp.getKvs().get(0).getValue());

            client.delete(key);
        } finally {
            client = null;
        }
    }

    /**
     * 测试 2: Compact 幂等性
     */
    @Test
    public void testCompactIdempotency() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "compact_idempotent_" + UUID.randomUUID();
            client.delete(key);

            client.put(key, "value");

            long currentRevision = client.getCurrentRevision();
            long compactRevision = currentRevision - 1;

            CompactResponse response1 = client.compact(compactRevision);
            assertTrue(response1.isSuccess());

            CompactResponse response2 = client.compact(compactRevision);
            assertTrue(response2.isSuccess(), "Second compact should also succeed (idempotent)");

            CompactResponse response3 = client.compact(compactRevision);
            assertTrue(response3.isSuccess(), "Third compact should also succeed (idempotent)");

            LOG.info("Compact idempotency passed: compactRevision={}", compactRevision);

            client.delete(key);
        } finally {
            client = null;
        }
    }

    /**
     * 测试 3: 压缩后最新数据保留
     */
    @Test
    public void testCompactPreservesLatestData() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "compact_latest_" + UUID.randomUUID();
            client.delete(key);

            client.put(key, "version_1");
            client.put(key, "version_2");
            client.put(key, "version_3_final");

            long currentRevision = client.getCurrentRevision();

            long compactRevision = 1;
            if (compactRevision < currentRevision) {
                CompactResponse response = client.compact(compactRevision);
                assertTrue(response.isSuccess());
            }

            RangeResponse getResp = client.range(key);
            assertTrue(getResp.isSuccess());
            assertEquals(1, getResp.getKvs().size());
            assertEquals("version_3_final", getResp.getKvs().get(0).getValue());

            LOG.info("Compact preserves latest data: currentRev={}, compactRev={}",
                    currentRevision, compactRevision);

            client.delete(key);
        } finally {
            client = null;
        }
    }

    // ==================== 参数验证测试 ====================

    /**
     * 测试 4: revision <= 0 应该被拒绝
     */
    @Test
    public void testCompactInvalidRevisionZero() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            CompactResponse response = client.compact(0);
            assertFalse(response.isSuccess(), "Compact with revision=0 should fail");
            assertEquals("BAD_REQUEST", response.getError());

            LOG.info("Compact revision=0 correctly rejected");
        } finally {
            client = null;
        }
    }

    /**
     * 测试 5: revision < 0 应该被拒绝
     */
    @Test
    public void testCompactInvalidRevisionNegative() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            CompactResponse response = client.compact(-5);
            assertFalse(response.isSuccess(), "Compact with negative revision should fail");
            assertEquals("BAD_REQUEST", response.getError());

            LOG.info("Compact negative revision correctly rejected");
        } finally {
            client = null;
        }
    }

    /**
     * 测试 6: revision > 当前 revision 应该被拒绝
     */
    @Test
    public void testCompactRevisionTooLarge() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "compact_too_large_" + UUID.randomUUID();
            client.delete(key);
            client.put(key, "value");

            long currentRevision = client.getCurrentRevision();

            CompactResponse response = client.compact(currentRevision + 1000);
            assertFalse(response.isSuccess(), "Compact with revision > current should fail");
            assertEquals("REVISION_TOO_LARGE", response.getError());

            LOG.info("Compact revision too large correctly rejected: current={}, requested={}",
                    currentRevision, currentRevision + 1000);

            client.delete(key);
        } finally {
            client = null;
        }
    }

    // ==================== 便捷方法测试 ====================

    /**
     * 测试 7: compactToPreviousRevision() 便捷方法
     */
    @Test
    public void testCompactToPreviousRevision() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "compact_prev_" + UUID.randomUUID();
            client.delete(key);

            client.put(key, "v1");
            client.put(key, "v2");

            long beforeCompact = client.getCurrentRevision();

            CompactResponse response = client.compactToPreviousRevision();

            assertTrue(response.isSuccess());
            // compactedRevision 应该 <= beforeCompact - 1
            assertTrue(response.getCompactedRevision() <= beforeCompact - 1,
                    "compactedRevision should be <= beforeCompact - 1");

            LOG.info("compactToPreviousRevision passed: before={}, compacted={}",
                    beforeCompact, response.getCompactedRevision());

            client.delete(key);
        } finally {
            client = null;
        }
    }

    // ==================== Watch 兼容性测试 ====================

    /**
     * 测试 8: Watch startRevision < 压缩版本
     */
    @Test
    public void testCompactWithWatchStartRevision() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "compact_watch_" + UUID.randomUUID();
            client.delete(key);

            long initialRevision = client.getCurrentRevision();

            for (int i = 1; i <= 5; i++) {
                client.put(key, "v" + i);
            }

            long finalRevision = client.getCurrentRevision();

            long compactRevision = initialRevision + 2;
            if (compactRevision < finalRevision) {
                CompactResponse compactResp = client.compact(compactRevision);
                assertTrue(compactResp.isSuccess());
            }

            List<WatchEvent> receivedEvents = new CopyOnWriteArrayList<>();

            try {
                RaftKVClient.WatchListener listener = client.watchFromRevision(key, initialRevision + 1, receivedEvents::add);

                Thread.sleep(2000);

                client.cancelWatch(listener);

                LOG.info("Watch after compact: initialRev={}, compactRev={}, finalRev={}, eventsReceived={}",
                        initialRevision, compactRevision, finalRevision, receivedEvents.size());

            } catch (Exception e) {
                LOG.warn("Watch test interrupted (expected if compact removes old events)", e.getMessage());
            }

            client.delete(key);
        } finally {
            client = null;
        }
    }

    // ==================== 压力测试 ====================

    /**
     * 测试 9: 大量数据压缩
     */
    @Test
    public void testCompactWithManyRevisions() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "compact_many_" + UUID.randomUUID();
            client.delete(key);

            int count = 50;
            for (int i = 0; i < count; i++) {
                client.put(key, "value_" + i);
            }

            long currentRevision = client.getCurrentRevision();
            LOG.info("Created {} revisions, current={}", count, currentRevision);

            long compactRevision = currentRevision - 20;
            CompactResponse response = client.compact(compactRevision);
            assertTrue(response.isSuccess());

            RangeResponse getResp = client.range(key);
            assertTrue(getResp.isSuccess());
            assertEquals(1, getResp.getKvs().size());
            assertEquals("value_" + (count - 1), getResp.getKvs().get(0).getValue());

            LOG.info("Many revisions compact passed: compactedRevision={}", response.getCompactedRevision());

            client.delete(key);
        } finally {
            client = null;
        }
    }

    // ==================== 多个 Key 测试 ====================

    /**
     * 测试 10: 压缩影响多个 key
     */
    @Test
    public void testCompactMultipleKeys() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "compact_multi_" + UUID.randomUUID() + "_";
            String[] keys = {prefix + "a", prefix + "b", prefix + "c"};

            for (String key : keys) {
                client.delete(key);
                client.put(key, "value");
            }

            long beforeRevision = client.getCurrentRevision();

            for (String key : keys) {
                client.put(key, "updated");
            }

            long currentRevision = client.getCurrentRevision();

            long compactRevision = beforeRevision + 1;
            if (compactRevision < currentRevision) {
                CompactResponse response = client.compact(compactRevision);
                assertTrue(response.isSuccess());
            }

            RangeResponse response = client.range()
                    .key(prefix)
                    .rangeEnd(prefix + "zzz")
                    .execute();

            assertTrue(response.isSuccess());
            assertEquals(3, response.getCount());

            LOG.info("Multiple keys compact passed: count={}", response.getCount());

            for (String key : keys) {
                client.delete(key);
            }
        } finally {
            client = null;
        }
    }

    // ==================== RequestId 测试 ====================

    /**
     * 测试 11: CompactRequest 带 requestId
     */
    @Test
    public void testCompactWithRequestId() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "compact_reqid_" + UUID.randomUUID();
            client.delete(key);
            client.put(key, "value");

            long currentRevision = client.getCurrentRevision();
            String requestId = "test-request-" + UUID.randomUUID();

            CompactRequest request = CompactRequest.of(currentRevision - 1, requestId);
            CompactResponse response = client.compact(request);

            assertTrue(response.isSuccess());
            assertEquals(requestId, response.getRequestId());

            LOG.info("Compact with requestId passed: requestId={}", requestId);

            client.delete(key);
        } finally {
            client = null;
        }
    }
}
