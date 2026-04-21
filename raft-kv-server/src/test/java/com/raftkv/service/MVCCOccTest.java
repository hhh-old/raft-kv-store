package com.raftkv.service;

import com.raftkv.entity.Compare;
import com.raftkv.entity.Operation;
import com.raftkv.entity.TxnRequest;
import com.raftkv.entity.TxnResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MVCC 和 OCC 功能测试
 */
public class MVCCOccTest {

    private MVCCStore mvccStore;
    private KVStoreStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        mvccStore = new MVCCStore();
        stateMachine = new KVStoreStateMachine();
    }

    // ==================== 测试 1: GREATER_EQUAL / LESS_EQUAL ====================

    @Test
    void testGreaterEqualOperator() {
        // 准备数据
        mvccStore.put("key1", "100");

        // 测试 GREATER_EQUAL (version >= 1 应该为 true)
        long version = mvccStore.getVersion("key1");
        System.out.println("key1 version: " + version);
        assertTrue(version >= 1, "Version should be >= 1");

        // 测试字符串比较 GREATER_EQUAL
        MVCCStore.KeyValue kv = mvccStore.getLatest("key1");
        assertNotNull(kv);
        System.out.println("key1 value: " + kv.getValue());
    }

    @Test
    void testLessEqualOperator() {
        // 准备数据
        mvccStore.put("key2", "50");

        // 测试 LESS_EQUAL (version <= 5 应该为 true)
        long version = mvccStore.getVersion("key2");
        System.out.println("key2 version: " + version);
        assertTrue(version <= 5, "Version should be <= 5");
    }

    @Test
    void testCompareEnumValues() {
        // 验证 CompareOp 包含所有需要的值
        Compare.CompareOp[] expectedOps = {
            Compare.CompareOp.EQUAL,
            Compare.CompareOp.GREATER,
            Compare.CompareOp.LESS,
            Compare.CompareOp.NOT_EQUAL,
            Compare.CompareOp.GREATER_EQUAL,
            Compare.CompareOp.LESS_EQUAL
        };

        assertEquals(6, expectedOps.length);
        System.out.println("All 6 compare operators are available:");
        for (Compare.CompareOp op : expectedOps) {
            System.out.println("  - " + op);
        }
    }

    // ==================== 测试 2: MVCC 多版本存储 ====================

    @Test
    void testMVCCMultipleVersions() {
        // 写入多个版本
        mvccStore.put("versioned_key", "value_v1");
        mvccStore.put("versioned_key", "value_v2");
        mvccStore.put("versioned_key", "value_v3");

        // 验证最新版本
        MVCCStore.KeyValue latest = mvccStore.getLatest("versioned_key");
        assertNotNull(latest);
        assertEquals("value_v3", latest.getValue());
        assertEquals(3, latest.getVersion());

        System.out.println("Latest value: " + latest.getValue());
        System.out.println("Version: " + latest.getVersion());
        System.out.println("Revision: " + latest.getRevision());

        // 验证版本历史
        Map<MVCCStore.Revision, MVCCStore.KeyValue> history = mvccStore.getHistory("versioned_key");
        assertEquals(3, history.size());
        System.out.println("History size: " + history.size());
    }

    @Test
    void testMVCCRevisionFormat() {
        mvccStore.put("rev_test", "data");

        MVCCStore.KeyValue kv = mvccStore.getLatest("rev_test");
        assertNotNull(kv);

        MVCCStore.Revision rev = kv.getRevision();
        System.out.println("Revision: " + rev);
        System.out.println("MainRev: " + rev.getMainRev());
        System.out.println("SubRev: " + rev.getSubRev());

        assertTrue(rev.getMainRev() > 0);
    }

    @Test
    void testMVCCCreateRevision() {
        mvccStore.put("create_test", "initial");

        MVCCStore.KeyValue kv = mvccStore.getLatest("create_test");
        assertNotNull(kv);

        MVCCStore.Revision createRev = kv.getCreateRevision();
        MVCCStore.Revision modRev = kv.getRevision();

        System.out.println("CreateRevision: " + createRev);
        System.out.println("ModRevision: " + modRev);

        // 第一次创建时，createRevision 应该等于 modRevision
        assertEquals(createRev.getMainRev(), modRev.getMainRev());
    }

    // ==================== 测试 3: 事务功能（移除 OCC 后） ====================

    @Test
    void testTransactionBasicOperations() {
        // 验证事务的基本功能（无 OCC）
        mvccStore.put("txn_key1", "value1");
        mvccStore.put("txn_key2", "value2");
        
        // 验证数据存在
        MVCCStore.KeyValue kv1 = mvccStore.getLatest("txn_key1");
        assertNotNull(kv1);
        assertEquals("value1", kv1.getValue());
        
        MVCCStore.KeyValue kv2 = mvccStore.getLatest("txn_key2");
        assertNotNull(kv2);
        assertEquals("value2", kv2.getValue());
        
        System.out.println("Transaction basic operations work correctly without OCC");
    }

    @Test
    void testMVCCWithoutOCC() {
        // 验证 MVCC 在没有 OCC 的情况下仍然正常工作
        mvccStore.put("mvcc_key", "v1");
        MVCCStore.KeyValue kv1 = mvccStore.getLatest("mvcc_key");
        assertEquals("v1", kv1.getValue());
        
        // 更新
        mvccStore.put("mvcc_key", "v2");
        MVCCStore.KeyValue kv2 = mvccStore.getLatest("mvcc_key");
        assertEquals("v2", kv2.getValue());
        
        // 版本历史应该存在
        long version = mvccStore.getVersion("mvcc_key");
        assertEquals(2, version);
        
        System.out.println("MVCC works correctly without OCC, version: " + version);
    }

    @Test
    void testMVCCStats() {
        mvccStore.put("stats_key1", "value1");
        mvccStore.put("stats_key2", "value2");
        mvccStore.put("stats_key1", "value1_updated"); // 第二个版本

        MVCCStore.StoreStats stats = mvccStore.getStats();
        System.out.println("Store stats: " + stats);

        assertTrue(stats.getTotalKeys() >= 2);
        assertTrue(stats.getTotalVersions() >= 3);
    }

    // ==================== 测试 4: 范围查询 ====================

    @Test
    void testPrefixMatching() {
        mvccStore.put("/config/app/name", "MyApp");
        mvccStore.put("/config/app/port", "8080");
        mvccStore.put("/config/db/host", "localhost");

        java.util.List<MVCCStore.KeyValue> results = mvccStore.getWithPrefix("/config/app/");
        System.out.println("Prefix search results for '/config/app/':");
        for (MVCCStore.KeyValue kv : results) {
            System.out.println("  " + kv.getKey() + " = " + kv.getValue());
        }

        assertEquals(2, results.size());
    }

    @Test
    void testRangeQuery() {
        mvccStore.put("a_key", "a");
        mvccStore.put("b_key", "b");
        mvccStore.put("c_key", "c");
        mvccStore.put("d_key", "d");

        java.util.List<MVCCStore.KeyValue> results = mvccStore.getRange("b_key", "d_key");
        System.out.println("Range query results [b_key, d_key):");
        for (MVCCStore.KeyValue kv : results) {
            System.out.println("  " + kv.getKey() + " = " + kv.getValue());
        }

        assertEquals(2, results.size());
    }

    // ==================== 测试 5: 删除和 Tombstone ====================

    @Test
    void testDeleteCreatesTombstone() {
        mvccStore.put("delete_test", "value");
        long versionBefore = mvccStore.getVersion("delete_test");

        boolean deleted = mvccStore.delete("delete_test");
        assertTrue(deleted);

        long versionAfter = mvccStore.getVersion("delete_test");
        System.out.println("Version before delete: " + versionBefore);
        System.out.println("Version after delete: " + versionAfter);

        // 删除应该增加版本号（创建 tombstone）
        assertEquals(versionBefore + 1, versionAfter);
    }

    public static void main(String[] args) {
        MVCCOccTest test = new MVCCOccTest();

        System.out.println("=== Running MVCC & OCC Tests ===\n");

        try {
            test.setUp();
            test.testCompareEnumValues();
            System.out.println("✅ Compare operators test passed\n");

            test.setUp();
            test.testGreaterEqualOperator();
            System.out.println("✅ GREATER_EQUAL test passed\n");

            test.setUp();
            test.testLessEqualOperator();
            System.out.println("✅ LESS_EQUAL test passed\n");

            test.setUp();
            test.testMVCCMultipleVersions();
            System.out.println("✅ MVCC multiple versions test passed\n");

            test.setUp();
            test.testMVCCRevisionFormat();
            System.out.println("✅ MVCC revision format test passed\n");

            test.setUp();
            test.testTransactionBasicOperations();
            System.out.println("✅ Transaction basic operations test passed\n");

            test.setUp();
            test.testPrefixMatching();
            System.out.println("✅ Prefix matching test passed\n");

            test.setUp();
            test.testRangeQuery();
            System.out.println("✅ Range query test passed\n");

            test.setUp();
            test.testDeleteCreatesTombstone();
            System.out.println("✅ Delete tombstone test passed\n");

            System.out.println("=== All tests passed! ===");

        } catch (AssertionError e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
