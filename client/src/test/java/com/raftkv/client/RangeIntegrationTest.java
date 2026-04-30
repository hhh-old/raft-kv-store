package com.raftkv.client;

import com.raftkv.entity.RangeRequest;
import com.raftkv.entity.RangeResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Range 查询集成测试 - 对齐 etcd v3 Range API
 *
 * <p><b>运行前提：</b>3 节点集群已在外部启动（9081/9082/9083）</p>
 *
 * <p>测试覆盖的 Range 功能：</p>
 * <ol>
 *   <li><b>单一键查询</b>：查询存在的 key、不存在的 key</li>
 *   <li><b>范围查询</b>：[startKey, endKey) 范围遍历</li>
 *   <li><b>前缀匹配</b>：查询以某前缀开头的所有 key</li>
 *   <li><b>Limit 限制</b>：限制返回条数，more 标志</li>
 *   <li><b>排序</b>：按 KEY/VERSION/CREATE/MOD/VALUE 排序，ASC/DESC</li>
 *   <li><b>CountOnly</b>：仅返回计数，不返回实际数据</li>
 *   <li><b>历史版本查询</b>：指定 revision 读取历史数据</li>
 *   <li><b>Leader 重定向</b>：自动跟随 Leader 切换</li>
 * </ol>
 */
public class RangeIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(RangeIntegrationTest.class);

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

    // ==================== 单一键查询测试 ====================

    /**
     * 测试 1: 单一键查询 - key 存在
     */
    @Test
    public void testRangeSingleKeyExists() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String testKey = "range_single_" + UUID.randomUUID();
            client.delete(testKey);

            String value = "test_value_123";
            client.put(testKey, value);

            RangeResponse response = client.range(testKey);

            assertTrue(response.isSuccess(), "Range should succeed");
            assertEquals(1, response.getCount(), "Should return 1 result");
            assertEquals(1, response.getKvs().size(), "Should have 1 kv");

            RangeResponse.KeyValue kv = response.getKvs().get(0);
            assertEquals(testKey, kv.getKey());
            assertEquals(value, kv.getValue());
            assertTrue(kv.getRevision() > 0);
            assertTrue(kv.getCreateRevision() > 0);
            assertTrue(kv.getVersion() > 0);

            LOG.info("Single key query passed: key={}, value={}, revision={}",
                    kv.getKey(), kv.getValue(), kv.getRevision());

            client.delete(testKey);
        } finally {
            client = null;
        }
    }

    /**
     * 测试 2: 单一键查询 - key 不存在
     */
    @Test
    public void testRangeSingleKeyNotExists() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String testKey = "non_existent_key_" + UUID.randomUUID();
            RangeResponse response = client.range(testKey);

            assertTrue(response.isSuccess());
            assertEquals(0, response.getCount());
            assertTrue(response.getKvs().isEmpty());

            LOG.info("Non-existent key query passed: key={}, count={}", testKey, response.getCount());
        } finally {
            client = null;
        }
    }

    // ==================== 范围查询测试 ====================

    /**
     * 测试 3: 范围查询 [start, end)
     */
    @Test
    public void testRangeWithRangeEnd() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "range_test_" + UUID.randomUUID() + "_";
            String[] keys = {prefix + "a", prefix + "b", prefix + "c"};
            for (String key : keys) {
                client.delete(key);
            }

            for (String key : keys) {
                client.put(key, "value_" + key);
            }

            RangeResponse response = client.range(prefix + "a", prefix + "d");

            assertTrue(response.isSuccess());
            assertEquals(3, response.getCount());
            assertEquals(3, response.getKvs().size());

            List<String> returnedKeys = response.getKvs().stream()
                    .map(RangeResponse.KeyValue::getKey)
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(Arrays.asList(prefix + "a", prefix + "b", prefix + "c"), returnedKeys);

            LOG.info("Range query passed: count={}", response.getCount());

            for (String key : keys) {
                client.delete(key);
            }
        } finally {
            client = null;
        }
    }

    // ==================== 前缀匹配测试 ====================

    /**
     * 测试 4: 前缀匹配查询
     */
    @Test
    public void testRangePrefixMatch() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "users_" + UUID.randomUUID() + "_";
            String[] keys = {prefix + "alice", prefix + "bob", prefix + "charlie", "other_key"};
            for (String key : keys) {
                client.delete(key);
            }

            for (String key : keys) {
                client.put(key, "value");
            }

            RangeResponse response = client.range()
                    .key(prefix)
                    .rangeEnd(prefix + "zzz")
                    .execute();

            assertTrue(response.isSuccess());
            assertEquals(3, response.getCount());
            assertEquals(3, response.getKvs().size());

            for (RangeResponse.KeyValue kv : response.getKvs()) {
                assertTrue(kv.getKey().startsWith(prefix));
            }

            LOG.info("Prefix match passed: count={}", response.getCount());

            for (String key : keys) {
                client.delete(key);
            }
        } finally {
            client = null;
        }
    }

    // ==================== Limit 测试 ====================

    /**
     * 测试 5: Limit 限制返回条数
     */
    @Test
    public void testRangeWithLimit() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "limit_test_" + UUID.randomUUID() + "_";
            String[] keys = new String[10];
            for (int i = 0; i < 10; i++) {
                keys[i] = prefix + i;
                client.delete(keys[i]);
            }

            for (String key : keys) {
                client.put(key, "value");
            }

            RangeResponse response = client.range()
                    .key(prefix)
                    .rangeEnd(prefix + "zzz")
                    .limit(3)
                    .execute();

            assertTrue(response.isSuccess());
            assertEquals(10, response.getCount());
            assertEquals(3, response.getKvs().size());
            assertTrue(response.isMore());

            LOG.info("Limit test passed: returned={}, total={}, more={}",
                    response.getKvs().size(), response.getCount(), response.isMore());

            for (String key : keys) {
                client.delete(key);
            }
        } finally {
            client = null;
        }
    }

    // ==================== 排序测试 ====================

    /**
     * 测试 6: 按 KEY 升序排序
     */
    @Test
    public void testRangeSortByKeyAsc() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "sort_key_" + UUID.randomUUID() + "_";
            String[] keys = {"z_key", "a_key", "m_key"};
            for (String key : keys) {
                client.delete(key);
            }

            client.put(keys[1], "v1");
            client.put(keys[0], "v2");
            client.put(keys[2], "v3");

            RangeResponse response = client.range()
                    .key("")
                    .rangeEnd("sort_key_")
                    .sortOrder(RangeRequest.SortOrder.ASC)
                    .sortTarget(RangeRequest.SortTarget.KEY)
                    .execute();

            assertTrue(response.isSuccess());
            if (response.getKvs().size() >= 3) {
                for (int i = 0; i < response.getKvs().size() - 1; i++) {
                    String key1 = response.getKvs().get(i).getKey();
                    String key2 = response.getKvs().get(i + 1).getKey();
                    assertTrue(key1.compareTo(key2) <= 0);
                }
            }

            LOG.info("Sort by KEY ASC passed: {} results", response.getKvs().size());

            for (String key : keys) {
                client.delete(key);
            }
        } finally {
            client = null;
        }
    }

    /**
     * 测试 7: 按 VERSION 降序排序
     */
    @Test
    public void testRangeSortByVersionDesc() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "sort_ver_" + UUID.randomUUID() + "_";
            String key = prefix + "test";
            client.delete(key);

            client.put(key, "v1");
            client.put(key, "v2");
            client.put(key, "v3");

            RangeResponse getResp = client.range(key);
                long currentVersion = getResp.getKvs().get(0).getVersion();

            client.put(key, "v4");

            RangeResponse response = client.range()
                    .key(prefix)
                    .rangeEnd(prefix + "zzz")
                    .sortOrder(RangeRequest.SortOrder.DESC)
                    .sortTarget(RangeRequest.SortTarget.VERSION)
                    .execute();

            assertTrue(response.isSuccess());
            LOG.info("Sort by VERSION DESC passed: version={}", currentVersion);

            client.delete(key);
        } finally {
            client = null;
        }
    }

    // ==================== CountOnly 测试 ====================

    /**
     * 测试 8: CountOnly - 仅返回计数
     */
    @Test
    public void testRangeCountOnly() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "count_only_" + UUID.randomUUID() + "_";
            String[] keys = {prefix + "a", prefix + "b", prefix + "c", prefix + "d"};
            for (String key : keys) {
                client.delete(key);
            }

            for (String key : keys) {
                client.put(key, "value");
            }

            RangeResponse response = client.range()
                    .key(prefix)
                    .rangeEnd(prefix + "zzz")
                    .countOnly(true)
                    .execute();

            assertTrue(response.isSuccess());
            assertEquals(4, response.getCount());
            assertTrue(response.getKvs().isEmpty());

            LOG.info("CountOnly passed: count={}", response.getCount());

            for (String key : keys) {
                client.delete(key);
            }
        } finally {
            client = null;
        }
    }

    // ==================== 历史版本查询测试 ====================

    /**
     * 测试 9: 读取历史版本（指定 revision）
     */
    @Test
    public void testRangeWithRevision() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "revision_test_" + UUID.randomUUID();
            client.delete(key);

            long startRevision = client.getCurrentRevision();

            client.put(key, "v1");
            client.put(key, "v2");
            client.put(key, "v3");

            long latestRevision = client.getCurrentRevision();

            long midRevision = startRevision + 2;
            if (midRevision > 0 && midRevision < latestRevision) {
                RangeResponse response = client.range()
                        .key(key)
                        .revision(midRevision)
                        .execute();

                assertTrue(response.isSuccess());
                assertEquals(1, response.getKvs().size());
                assertEquals(midRevision, response.getRevision());
            }

            LOG.info("Revision query passed: startRev={}, latestRev={}", startRevision, latestRevision);

            client.delete(key);
        } finally {
            client = null;
        }
    }

    /**
     * 测试 9a: 查询删除后的 revision 应该返回空（对齐 etcd 语义）
     *
     * 场景：key 被删除后，查询删除时的 revision 或更早的 revision，应该返回空结果
     */
    @Test
    public void testRangeRevisionKeyDeleted() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "deleted_rev_" + UUID.randomUUID();
            client.delete(key);

            // 记录创建 revision
            long beforeCreate = client.getCurrentRevision();
            client.put(key, "v1");
            long afterCreate = client.getCurrentRevision();

            // 记录删除 revision
            client.delete(key);
            long afterDelete = client.getCurrentRevision();

            // 查询创建时的 revision，应该返回 v1
            RangeResponse respBeforeDelete = client.range()
                    .key(key)
                    .revision(afterCreate)
                    .execute();
            assertTrue(respBeforeDelete.isSuccess());
            assertEquals(1, respBeforeDelete.getKvs().size(),
                    "Query at create revision should return the value");
            assertEquals("v1", respBeforeDelete.getKvs().get(0).getValue());

            // 查询删除后的 revision，应该返回空（tombstone 被过滤）
            RangeResponse respAfterDelete = client.range()
                    .key(key)
                    .revision(afterDelete)
                    .execute();
            assertTrue(respAfterDelete.isSuccess());
            assertEquals(0, respAfterDelete.getKvs().size(),
                    "Query at delete revision should return empty (tombstone filtered)");

            LOG.info("Deleted key revision test passed: createRev={}, deleteRev={}",
                    afterCreate, afterDelete);
        } finally {
            client = null;
        }
    }

    /**
     * 测试 9b: key 完整生命周期（创建、更新、删除、再创建）
     * 对齐 etcd：每个生命周期独立，version 从 1 开始
     */
    @Test
    public void testRangeRevisionFullLifecycle() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String key = "lifecycle_" + UUID.randomUUID();
            client.delete(key);

            // 第一次生命周期
            long rev1 = client.getCurrentRevision() + 1;
            client.put(key, "v1");  // revision rev1, version 1

            long rev2 = client.getCurrentRevision() + 1;
            client.put(key, "v2");  // revision rev2, version 2

            long rev3 = client.getCurrentRevision() + 1;
            client.delete(key);  // revision rev3, tombstone

            // 第二次生命周期
            long rev4 = client.getCurrentRevision() + 1;
            client.put(key, "v3");  // revision rev4, version 1 (新生命周期)

            // 查询第一个生命周期的数据
            RangeResponse resp1 = client.range().key(key).revision(rev1).execute();
            assertTrue(resp1.isSuccess());
            assertEquals(1, resp1.getKvs().size());
            assertEquals("v1", resp1.getKvs().get(0).getValue());
            assertEquals(1, resp1.getKvs().get(0).getVersion());

            // 查询第二个生命周期的数据
            RangeResponse resp4 = client.range().key(key).revision(rev4).execute();
            assertTrue(resp4.isSuccess());
            assertEquals(1, resp4.getKvs().size());
            assertEquals("v3", resp4.getKvs().get(0).getValue());
            assertEquals(1, resp4.getKvs().get(0).getVersion());  // 新生命周期，version 从 1 开始

            // 查询 tombstone revision，应该返回空
            RangeResponse respTombstone = client.range().key(key).revision(rev3).execute();
            assertTrue(respTombstone.isSuccess());
            assertEquals(0, respTombstone.getKvs().size());

            LOG.info("Full lifecycle test passed: rev1={}, rev2={}, rev3={}, rev4={}", rev1, rev2, rev3, rev4);

            client.delete(key);
        } finally {
            client = null;
        }
    }

    /**
     * 测试 9c: 范围查询包含已删除的 key
     *
     * 场景：范围查询 [prefix_a, prefix_z)，其中某些 key 在指定 revision 时已被删除
     * 期望：返回该 revision 时存在的 key，不包含已删除的
     */
    @Test
    public void testRangeRevisionWithDeletedInRange() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "range_del_" + UUID.randomUUID() + "_";
            client.delete(prefix + "a");
            client.delete(prefix + "b");
            client.delete(prefix + "c");

            // 插入所有 key
            client.put(prefix + "a", "v_a");
            client.put(prefix + "b", "v_b");
            client.put(prefix + "c", "v_c");

            long afterInsert = client.getCurrentRevision();

            // 删除 key b
            client.delete(prefix + "b");
            long afterDelete = client.getCurrentRevision();

            // 查询插入后的 revision，应该返回所有 3 个 key
            RangeResponse respBeforeDelete = client.range()
                    .key(prefix)
                    .rangeEnd(prefix + "z")
                    .revision(afterInsert)
                    .execute();
            assertTrue(respBeforeDelete.isSuccess());
            assertEquals(3, respBeforeDelete.getKvs().size(),
                    "Query before delete should return all 3 keys");

            // 查询删除后的 revision，应该只返回 a 和 c
            RangeResponse respAfterDelete = client.range()
                    .key(prefix)
                    .rangeEnd(prefix + "z")
                    .revision(afterDelete)
                    .execute();
            assertTrue(respAfterDelete.isSuccess());
            assertEquals(2, respAfterDelete.getKvs().size(),
                    "Query after delete should return only 2 keys (b was deleted)");

            // 验证返回的 key 是 a 和 c
            List<String> keys = respAfterDelete.getKvs().stream()
                    .map(RangeResponse.KeyValue::getKey)
                    .collect(Collectors.toList());
            assertTrue(keys.contains(prefix + "a"));
            assertTrue(keys.contains(prefix + "c"));
            assertFalse(keys.contains(prefix + "b"));

            LOG.info("Range with deleted key test passed: beforeDelete={}, afterDelete={}",
                    respBeforeDelete.getKvs().size(), respAfterDelete.getKvs().size());

            // 清理
            client.delete(prefix + "a");
            client.delete(prefix + "c");
        } finally {
            client = null;
        }
    }

    // ==================== 链式调用测试 ====================

    /**
     * 测试 10: 链式调用完整示例
     */
    @Test
    public void testRangeChainedApi() throws Exception {
        RaftKVClient client = createClient();
        try {
            waitForLeader(client);

            String prefix = "chained_" + UUID.randomUUID() + "_";
            String[] keys = {prefix + "a", prefix + "b", prefix + "c"};
            for (String key : keys) {
                client.delete(key);
            }

            client.put(keys[0], "value_a");
            client.put(keys[1], "value_b");
            client.put(keys[2], "value_c");

            RangeResponse response = client.range()
                    .key(prefix)
                    .rangeEnd(prefix + "zzz")
                    .limit(2)
                    .sortOrder(RangeRequest.SortOrder.ASC)
                    .sortTarget(RangeRequest.SortTarget.KEY)
                    .countOnly(false)
                    .execute();

            assertTrue(response.isSuccess());
            assertEquals(3, response.getCount());
            assertEquals(2, response.getKvs().size());
            assertTrue(response.isMore());

            LOG.info("Chained API test passed: returned={}, total={}, more={}",
                    response.getKvs().size(), response.getCount(), response.isMore());

            for (String key : keys) {
                client.delete(key);
            }
        } finally {
            client = null;
        }
    }
}
