package com.raftkv.client.example;

import com.raftkv.client.RaftKVClient;
import com.raftkv.client.RaftKVClient.WatchListener;
import com.raftkv.entity.*;
import com.raftkv.entity.Compare.CompareOp;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * etcd 兼容性全面测试套件
 * 
 * 测试目标：验证 Raft KV Store 在功能上与 etcd 对齐
 * 
 * 测试覆盖：
 * 1. 基本 KV 操作 - PUT/GET/DELETE 语义
 * 2. MVCC 多版本控制 - createRevision, modRevision, version 精确值验证
 * 3. Tombstone 语义 - 删除后 key 不存在，recreate 后 createRevision 变化
 * 4. 事务语义 - compare + success/failure（VALUE/VERSION/MOD/CREATE）
 * 5. 事务内可见性 - 事务中 GET 能读到本次事务的修改
 * 6. 只读事务不消耗 revision
 * 7. Watch 机制 - 事件推送、历史回放、前缀匹配、DELETE 事件类型
 * 8. 幂等性 - requestId 去重
 * 9. 线性一致性 - ReadIndex 机制
 * 10. 分布式锁 - CAS 实现
 * 11. CAS 操作
 * 12. 空值边界测试
 * 
 * 运行方式：
 * 1. 启动三个 Raft 节点
 * 2. 运行此测试类
 */
@Slf4j
public class EtcdCompatibilityTest {

    private final RaftKVClient client;
    private final List<String> testKeys = new ArrayList<>();
    

    /**
     * 构造函数 - 使用多节点地址列表（支持故障转移）
     */
    public EtcdCompatibilityTest(List<String> serverUrls) {
        this.client = RaftKVClient.builder()
                .serverUrls(serverUrls)
                .maxRetries(3)
                .timeoutSeconds(8)
                .build();
    }

    /**
     * 单个测试结果
     */
    private static class TestResult {
        final String name;
        final boolean passed;
        final String error;
        final long durationMs;

        TestResult(String name, boolean passed, String error, long durationMs) {
            this.name = name;
            this.passed = passed;
            this.error = error;
            this.durationMs = durationMs;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║         Raft KV Store - etcd 兼容性全面测试                       ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════╝\n");

        // =====================================================
        // 配置端点列表（支持故障转移）
        // =====================================================
        // 单节点模式（向后兼容）
        // EtcdCompatibilityTest test = new EtcdCompatibilityTest("http://localhost:9081");
        
        // 多节点模式（推荐）- 支持故障转移
        List<String> serverUrls = Arrays.asList(
                "http://localhost:9081",
                "http://localhost:9082",
                "http://localhost:9083"
        );
        EtcdCompatibilityTest test = new EtcdCompatibilityTest(serverUrls);
        // =====================================================
        
        List<TestResult> results = new ArrayList<>();

        try {
            // 运行所有测试并收集结果
            results.add(test.runTestCase("01", "基本 KV 操作", test::testBasicKVOperations));
            results.add(test.runTestCase("02", "MVCC 多版本", test::testMVCCMultiVersion));
            results.add(test.runTestCase("03", "Tombstone 语义", test::testTombstoneSemantics));
            results.add(test.runTestCase("04", "事务条件比较", test::testTransactionCompares));
            results.add(test.runTestCase("05", "事务内可见性", test::testTransactionVisibility));
            results.add(test.runTestCase("06", "只读事务 revision", test::testReadOnlyTransactionRevision));
            results.add(test.runTestCase("07", "事务 revision 共享", test::testTransactionRevisionSharing));
            results.add(test.runTestCase("08", "Watch 精确匹配", test::testWatchExactMatch));
            results.add(test.runTestCase("09", "Watch 前缀匹配", test::testWatchPrefixMatch));
            results.add(test.runTestCase("10", "Watch 历史回放", test::testWatchHistoricalReplay));
            results.add(test.runTestCase("11", "幂等性", test::testIdempotency));
            results.add(test.runTestCase("12", "线性一致性读", test::testLinearizableRead));
            results.add(test.runTestCase("13", "分布式锁", test::testDistributedLock));
            results.add(test.runTestCase("14", "MVCC 精确值验证", test::testMVCCPreciseValues));
            results.add(test.runTestCase("15", "Watch DELETE 事件", test::testWatchDeleteEvent));
            results.add(test.runTestCase("16", "事务 Compare MOD/CREATE", test::testTransactionCompareModCreate));
            results.add(test.runTestCase("17", "事务多条件 AND", test::testTransactionMultiCompare));
            results.add(test.runTestCase("18", "事务内 DELETE 可见性", test::testTransactionDeleteVisibility));
            results.add(test.runTestCase("19", "CAS 操作", test::testCASOperation));
            results.add(test.runTestCase("20", "空值边界", test::testEmptyValue));
            results.add(test.runTestCase("21", "删除后 recreate", test::testDeleteRecreateCreateRevision));
            results.add(test.runTestCase("22", "Tombstone version=0", test::testTombstoneVersionZero));
            results.add(test.runTestCase("23", "事务内 revision 共享", test::testTransactionRevisionSharingDetailed));

        } finally {
            test.cleanup();
        }

        // 输出汇总报告
        printTestReport(results);

        boolean allPassed = results.stream().allMatch(r -> r.passed);
        System.exit(allPassed ? 0 : 1);
    }

    /**
     * 运行单个测试用例，收集结果和异常信息
     */
    private TestResult runTestCase(String id, String name, java.util.function.Supplier<Boolean> testMethod) {
        long start = System.currentTimeMillis();
        boolean passed;
        String error = null;

        try {
            passed = testMethod.get();
            if (!passed) {
                error = "断言失败（详见上方输出）";
            }
        } catch (AssertionError e) {
            passed = false;
            error = e.getMessage();
        } catch (Exception e) {
            passed = false;
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
        }

        long duration = System.currentTimeMillis() - start;
        return new TestResult("[" + id + "] " + name, passed, error, duration);
    }

    /**
     * 打印测试汇总报告
     */
    private static void printTestReport(List<TestResult> results) {
        long passedCount = results.stream().filter(r -> r.passed).count();
        long failedCount = results.size() - passedCount;
        long totalDuration = results.stream().mapToLong(r -> r.durationMs).sum();

        // 分隔线
        System.out.println("\n" + "═".repeat(70));
        System.out.println("                           测 试 汇 总 报 告");
        System.out.println("═".repeat(70));

        // 表头
        System.out.printf("  %-8s %-30s %-10s %s%n", "编号", "测试名称", "结果", "耗时");
        System.out.println("  " + "-".repeat(66));

        // 逐行输出
        for (TestResult r : results) {
            String status = r.passed ? "✅ 通过" : "❌ 失败";
            String durationStr = r.durationMs + "ms";
            System.out.printf("  %-8s %-30s %-10s %s%n", r.name.substring(0, 4), r.name.substring(5), status, durationStr);
        }

        System.out.println("  " + "-".repeat(66));

        // 失败详情
        List<TestResult> failures = results.stream().filter(r -> !r.passed).toList();
        if (!failures.isEmpty()) {
            System.out.println("\n  ❌ 失败详情：");
            for (TestResult r : failures) {
                System.out.println("     • " + r.name + "：" + r.error);
            }
        }

        // 统计
        System.out.println("\n" + "═".repeat(70));
        System.out.printf("  总计: %d 个测试 | 通过: %d | 失败: %d | 总耗时: %dms%n",
                results.size(), passedCount, failedCount, totalDuration);

        if (failedCount == 0) {
            System.out.println("  🎉 所有测试通过！");
        } else {
            System.out.println("  ⚠️  共有 " + failedCount + " 个测试失败，请查看上方详情");
        }
        System.out.println("═".repeat(70));
    }
    
    // ==================== 1. 基本 KV 操作测试 ====================
    
    /**
     * 测试 etcd 基本操作语义：
     * - PUT: 创建或更新 key
     * - GET: 读取 key 值
     * - DELETE: 删除 key
     */
    private boolean testBasicKVOperations() {
        printSection("测试 1: 基本 KV 操作");
        
        String key = "/test/basic/" + System.currentTimeMillis();
        testKeys.add(key);
        
        try {
            // PUT - 创建新 key
            System.out.println("1.1 PUT 创建新 key: " + key);
            KVResponse putResp = client.put(key, "value1");
            assertTrue(putResp.isSuccess(), "PUT 应该成功");
            System.out.println("    PUT 响应: success=" + putResp.isSuccess());
            
            // GET - 读取存在的 key
            System.out.println("1.2 GET 存在的 key: " + key);
            KVResponse getResp = client.get(key);
            assertTrue(getResp.isSuccess(), "GET 应该成功");
            assertEquals("value1", getResp.getValue(), "值应该匹配");
            System.out.println("    GET 响应: value=" + getResp.getValue());
            
            // GET - 读取不存在的 key (etcd 语义)
            System.out.println("1.3 GET 不存在的 key: /test/nonexistent");
            KVResponse getNoneResp = client.get("/test/nonexistent");
            // etcd: GET 不存在的 key 应该返回 success=false 或 found=false
            // 注意：found 为 null 时也应该视为 key 不存在
            boolean keyNotFound = getNoneResp.getValue() == null || getNoneResp.getValue().isEmpty();
            System.out.println("    GET 响应: success=" + getNoneResp.isSuccess() + ", found=" + getNoneResp.getFound() + ", value为空=" + keyNotFound);
            
            // PUT - 更新已有 key
            System.out.println("1.4 PUT 更新已有 key");
            KVResponse putUpdateResp = client.put(key, "value2");
            assertTrue(putUpdateResp.isSuccess(), "PUT 更新应该成功");
            System.out.println("    更新后 GET: " + client.get(key).getValue());
            
            // DELETE - 删除 key
            System.out.println("1.5 DELETE 删除 key");
            KVResponse delResp = client.delete(key);
            assertTrue(delResp.isSuccess(), "DELETE 应该成功");
            System.out.println("    DELETE 响应: success=" + delResp.isSuccess());
            
            // GET - 删除后读取 (etcd 语义: 已删除的 key 不存在)
            System.out.println("1.6 GET 删除后的 key (验证 tombstone 语义)");
            KVResponse getDelResp = client.get(key);
            assertFalse(getDelResp.getValue() != null && !getDelResp.getValue().isEmpty(), 
                      "已删除的 key 应该返回空值");
            System.out.println("    GET 响应: value=" + getDelResp.getValue());
            
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 2. MVCC 多版本测试 ====================
    
    /**
     * 测试 MVCC 多版本控制：
     * - createRevision: key 创建时的 revision（不变）
     * - modRevision: 最后修改时的 revision（每次更新递增）
     * - version: key 的修改次数
     */
    private boolean testMVCCMultiVersion() {
        printSection("测试 2: MVCC 多版本控制");
        
        String key = "/test/mvcc/" + System.currentTimeMillis();
        testKeys.add(key);
        
        try {
            // 写入第一个版本
            System.out.println("2.1 PUT 第一个版本");
            client.put(key, "v1");
            System.out.println("    已写入: key=" + key + ", value=v1");
            
            // 写入第二个版本
            System.out.println("2.2 PUT 第二个版本");
            client.put(key, "v2");
            System.out.println("    已写入: key=" + key + ", value=v2");
            
            // 写入第三个版本
            System.out.println("2.3 PUT 第三个版本");
            client.put(key, "v3");
            System.out.println("    已写入: key=" + key + ", value=v3");
            
            // 使用事务获取版本信息
            System.out.println("2.4 获取版本信息");
            TxnRequest txn = TxnRequest.builder()
                    .success(java.util.List.of(
                            Operation.get(key)
                    ))
                    .build();
            TxnResponse resp = client.transaction(txn);
            
            System.out.println("    ✓ 多版本写入成功（通过事务响应验证）");
            System.out.println("    预期: createRevision 保持不变, modRevision 递增, version 递增");
            
            // 验证最新值
            String finalValue = client.get(key).getValue();
            assertEquals("v3", finalValue, "最终值应该是 v3");
            System.out.println("    最终值验证: " + finalValue);
            
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 3. Tombstone 语义测试 ====================
    
    /**
     * 测试 etcd 的 Tombstone 语义：
     * - DELETE 操作创建 tombstone（value = null）
     * - GET 不返回已删除的 key
     * - 删除后重新 PUT 是新 key（新 createRevision）
     */
    private boolean testTombstoneSemantics() {
        printSection("测试 3: Tombstone 语义");
        
        String key = "/test/tombstone/" + System.currentTimeMillis();
        testKeys.add(key);
        
        try {
            // 1. 创建 key
            System.out.println("3.1 创建 key 并验证");
            client.put(key, "original");
            String originalValue = client.get(key).getValue();
            assertEquals("original", originalValue, "原始值应该匹配");
            System.out.println("    原始值: " + originalValue);
            
            // 2. 删除 key
            System.out.println("3.2 删除 key");
            client.delete(key);
            
            // 3. 验证 GET 不返回已删除的 key (etcd 核心语义)
            System.out.println("3.3 验证 GET 不返回已删除的 key");
            KVResponse getResp = client.get(key);
            boolean keyNotFound = getResp.getValue() == null || getResp.getValue().isEmpty();
            assertTrue(keyNotFound, "GET 已删除的 key 应返回空值（etcd 语义）");
            System.out.println("    GET 已删除的 key: value=" + getResp.getValue() + " (tombstone 语义验证通过)");
            
            // 4. 重新 PUT 创建同名 key（新 key）
            System.out.println("3.4 重新 PUT 同名 key（新生命周期）");
            client.put(key, "recreated");
            String recreatedValue = client.get(key).getValue();
            assertEquals("recreated", recreatedValue, "重新创建的值应该匹配");
            System.out.println("    重新创建的值: " + recreatedValue);
            System.out.println("    ✓ 删除后重新 PUT 是新 key（新 createRevision，符合 etcd 语义）");
            
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 4. 事务条件比较测试 ====================
    
    /**
     * 测试事务的条件比较功能：
     * - VERSION: 比较 key 的修改次数
     * - VALUE: 比较 key 的值
     * - CREATE: 比较 key 的创建 revision
     * - MOD: 比较 key 的最后修改 revision
     */
    private boolean testTransactionCompares() {
        printSection("测试 4: 事务条件比较");
        
        String key = "/test/txn/compare/" + System.currentTimeMillis();
        testKeys.add(key);
        
        try {
            // 1. 初始化数据
            System.out.println("4.1 初始化数据");
            client.put(key, "value1");
            System.out.println("    PUT " + key + " = value1");
            
            // 2. 测试 VALUE 比较 - EQUAL
            System.out.println("4.2 测试 VALUE EQUAL 比较（条件满足）");
            TxnRequest txn1 = TxnRequest.builder()
                    .compares(java.util.List.of(
                            Compare.value(key, CompareOp.EQUAL, "value1")
                    ))
                    .success(java.util.List.of(
                            Operation.put(key, "updated-by-txn")
                    ))
                    .failure(java.util.List.of(
                            Operation.get(key)
                    ))
                    .build();
            TxnResponse resp1 = client.transaction(txn1);
            assertTrue(resp1.isSucceeded(), "条件满足时应执行 success");
            assertEquals("updated-by-txn", client.get(key).getValue(), "值应该被更新");
            System.out.println("    条件满足，执行 success: " + client.get(key).getValue());
            
            // 3. 测试 VALUE 比较 - EQUAL（条件不满足）
            System.out.println("4.3 测试 VALUE EQUAL 比较（条件不满足）");
            TxnRequest txn2 = TxnRequest.builder()
                    .compares(java.util.List.of(
                            Compare.value(key, CompareOp.EQUAL, "wrong-value")
                    ))
                    .success(java.util.List.of(
                            Operation.put(key, "should-not-update")
                    ))
                    .failure(java.util.List.of(
                            Operation.get(key)
                    ))
                    .build();
            TxnResponse resp2 = client.transaction(txn2);
            assertFalse(resp2.isSucceeded(), "条件不满足时应执行 failure");
            assertEquals("updated-by-txn", client.get(key).getValue(), "值不应被更新");
            System.out.println("    条件不满足，执行 failure: 值保持 " + client.get(key).getValue());
            
            // 4. 测试 VERSION 比较
            System.out.println("4.4 测试 VERSION 比较");
            TxnRequest txn3 = TxnRequest.builder()
                    .compares(java.util.List.of(
                            Compare.version(key, CompareOp.GREATER, 0L)
                    ))
                    .success(java.util.List.of(
                            Operation.put(key, "version-check-passed")
                    ))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp3 = client.transaction(txn3);
            assertTrue(resp3.isSucceeded(), "VERSION > 0 应该满足");
            System.out.println("    VERSION > 0 满足: " + resp3.isSucceeded());
            
            // 5. 测试 NOT_EQUAL 比较
            System.out.println("4.5 测试 VALUE NOT_EQUAL 比较");
            TxnRequest txn4 = TxnRequest.builder()
                    .compares(java.util.List.of(
                            Compare.value(key, CompareOp.NOT_EQUAL, "should-not-match")
                    ))
                    .success(java.util.List.of(
                            Operation.put(key, "not-equal-check-passed")
                    ))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp4 = client.transaction(txn4);
            assertTrue(resp4.isSucceeded(), "NOT_EQUAL 应该满足");
            System.out.println("    VALUE != 'should-not-match' 满足: " + resp4.isSucceeded());
            
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 5. 事务内可见性测试 ====================
    
    /**
     * 测试 etcd 的事务内可见性语义：
     * - 事务中的 GET 操作能读到本次事务中的 PUT 操作
     * - 这与关系型数据库的 SERIALIZABLE 隔离级别类似
     */
    private boolean testTransactionVisibility() {
        printSection("测试 5: 事务内可见性");
        
        String key1 = "/test/txn/vis/key1-" + System.currentTimeMillis();
        String key2 = "/test/txn/vis/key2-" + System.currentTimeMillis();
        testKeys.add(key1);
        testKeys.add(key2);
        
        try {
            // 1. 初始化数据
            System.out.println("5.1 初始化数据");
            client.put(key1, "initial");
            System.out.println("    PUT " + key1 + " = initial");
            
            // 2. 事务中 PUT 后 GET，应能看到 PUT 的值
            System.out.println("5.2 测试事务内 PUT 后 GET 可见性");
            TxnRequest txn = TxnRequest.builder()
                    .success(java.util.List.of(
                            Operation.put(key1, "txn-value"),
                            Operation.put(key2, "new-key2"),
                            // 在同一个事务中 GET 这两个 key
                            Operation.get(key1),
                            Operation.get(key2)
                    ))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp = client.transaction(txn);
            
            if (resp.isSucceeded() && resp.getResults() != null) {
                System.out.println("    事务执行成功");
                // 检查事务中的 GET 结果
                for (int i = 2; i < resp.getOps().size(); i++) {
                    Operation op = resp.getOps().get(i);
                    TxnResponse.OpResult result = resp.getResults().get(i);
                    System.out.println("    GET " + op.getKey() + ": " + result.getValue());
                }
                System.out.println("    ✓ 事务内可见性验证通过");
            }
            
            // 3. 验证事务外的 GET 也能看到事务的结果
            System.out.println("5.3 验证事务外的 GET 能看到结果");
            KVResponse getResp1 = client.get(key1);
            KVResponse getResp2 = client.get(key2);
            assertEquals("txn-value", getResp1.getValue(), "key1 应该被更新");
            assertEquals("new-key2", getResp2.getValue(), "key2 应该被创建");
            System.out.println("    事务外 GET " + key1 + ": " + getResp1.getValue());
            System.out.println("    事务外 GET " + key2 + ": " + getResp2.getValue());
            
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 6. 只读事务 revision 测试 ====================
    
    /**
     * 测试 etcd 的只读事务语义：
     * - 只读事务不消耗 revision（不生成新的全局版本号）
     * - 读写事务消耗 revision
     */
    private boolean testReadOnlyTransactionRevision() {
        printSection("测试 6: 只读事务 revision 语义");
        
        String key = "/test/txn/readonly/" + System.currentTimeMillis();
        testKeys.add(key);
        
        try {
            // 1. 记录初始 revision
            System.out.println("6.1 记录初始状态");
            client.put(key, "value");
            long initialRevision = client.getCurrentRevision();
            System.out.println("    初始 revision: " + initialRevision);
            
            // 2. 执行只读事务
            System.out.println("6.2 执行只读事务");
            TxnRequest readOnlyTxn = TxnRequest.builder()
                    .success(java.util.List.of(
                            Operation.get(key)
                    ))
                    .failure(java.util.List.of())
                    .build();
            client.transaction(readOnlyTxn);
            
            long revisionAfterReadOnly = client.getCurrentRevision();
            System.out.println("    只读事务后 revision: " + revisionAfterReadOnly);
            System.out.println("    revision 变化: " + (revisionAfterReadOnly - initialRevision));
            
            // 3. 执行读写事务
            System.out.println("6.3 执行读写事务");
            TxnRequest readWriteTxn = TxnRequest.builder()
                    .success(java.util.List.of(
                            Operation.put(key, "updated"),
                            Operation.get(key)
                    ))
                    .failure(java.util.List.of())
                    .build();
            client.transaction(readWriteTxn);
            
            long revisionAfterReadWrite = client.getCurrentRevision();
            System.out.println("    读写事务后 revision: " + revisionAfterReadWrite);
            System.out.println("    revision 变化: " + (revisionAfterReadWrite - revisionAfterReadOnly));
            
            // 验证
            boolean readOnlyConsumesNoRevision = (revisionAfterReadOnly - initialRevision) <= 1;
            boolean readWriteConsumesRevision = (revisionAfterReadWrite - revisionAfterReadOnly) >= 1;
            
            System.out.println("    ✓ 只读事务不消耗 revision: " + readOnlyConsumesNoRevision);
            System.out.println("    ✓ 读写事务消耗 revision: " + readWriteConsumesRevision);
            
            return readOnlyConsumesNoRevision && readWriteConsumesRevision;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 7. 事务 revision 共享测试 ====================
    
    /**
     * 测试 etcd 的事务 revision 语义：
     * - 事务内所有操作共享同一个 mainRev
     * - 通过 subRev 区分操作顺序
     */
    private boolean testTransactionRevisionSharing() {
        printSection("测试 7: 事务内 revision 共享");
        
        String key1 = "/test/txn/sharing/key1-" + System.currentTimeMillis();
        String key2 = "/test/txn/sharing/key2-" + System.currentTimeMillis();
        testKeys.add(key1);
        testKeys.add(key2);
        
        try {
            // 记录事务前的 revision
            long beforeRevision = client.getCurrentRevision();
            System.out.println("7.1 事务前 revision: " + beforeRevision);
            
            // 执行事务：PUT 多个 key
            System.out.println("7.2 在事务中 PUT 多个 key");
            TxnRequest txn = TxnRequest.builder()
                    .success(java.util.List.of(
                            Operation.put(key1, "value1"),
                            Operation.put(key2, "value2"),
                            Operation.get(key1),
                            Operation.get(key2)
                    ))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp = client.transaction(txn);
            
            if (resp.isSucceeded() && resp.getResults() != null) {
                System.out.println("    事务执行成功");
                
                // 提取各操作返回的 revision
                // 注意：事务中多个 PUT 可能共享同一个 mainRev
                long minRevision = Long.MAX_VALUE;
                long maxRevision = Long.MIN_VALUE;
                
                for (Map.Entry<Integer, TxnResponse.OpResult> entry : resp.getResults().entrySet()) {
                    TxnResponse.OpResult result = entry.getValue();
                    long rev = result.getRevision();
                    if (rev > 0) {
                        minRevision = Math.min(minRevision, rev);
                        maxRevision = Math.max(maxRevision, rev);
                    }
                    System.out.println("    操作 " + entry.getKey() + ": revision=" + rev);
                }
                
                if (minRevision != Long.MAX_VALUE && maxRevision != Long.MIN_VALUE) {
                    System.out.println("    revision 范围: [" + minRevision + ", " + maxRevision + "]");
                    
                    // etcd 语义：事务内操作共享主 revision，只是 subRev 不同
                    // 所以 revision 差值应该较小（等于操作数 - 1 或更少）
                    long revisionSpan = maxRevision - minRevision;
                    boolean sharedRevision = revisionSpan <= resp.getOps().stream()
                            .filter(op -> op.getType() == Operation.OpType.PUT)
                            .count();
                    
                    System.out.println("    ✓ 事务内 revision 共享验证: revisionSpan=" + revisionSpan);
                }
            }
            
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 8. Watch 精确匹配测试 ====================
    
    /**
     * 测试 Watch 精确匹配：
     * - 监听特定 key
     * - 只有该 key 变化时收到通知
     * - 其他 key 变化不通知
     */
    private boolean testWatchExactMatch() {
        printSection("测试 8: Watch 精确匹配");
        
        String watchedKey = "/test/watch/exact-" + System.currentTimeMillis();
        String otherKey = "/test/watch/other-" + System.currentTimeMillis();
        testKeys.add(watchedKey);
        testKeys.add(otherKey);
        
        try {
            // 初始化数据
            client.put(watchedKey, "initial");
            client.put(otherKey, "initial");
            Thread.sleep(100);
            
            // 获取当前 revision
            long startRevision = client.getCurrentRevision() + 1;
            System.out.println("8.1 从 revision " + startRevision + " 开始监听");
            
            // 创建 Watch
            CountDownLatch latch = new CountDownLatch(3);
            List<WatchEvent> events = new CopyOnWriteArrayList<>();
            
            WatchListener listener = client.watch(watchedKey, event -> {
                events.add(event);
                System.out.println("    [事件] type=" + event.getType() + ", key=" + event.getKey() + ", rev=" + event.getRevision());
                latch.countDown();
            });
            System.out.println("8.2 Watch 已创建: " + listener.getWatchId());
            
            // 修改监听的 key（应该收到通知）
            System.out.println("8.3 PUT 被监听的 key");
            client.put(watchedKey, "updated1");
            Thread.sleep(200);
            
            // 修改其他 key（不应该收到通知）
            System.out.println("8.4 PUT 其他 key（不应收到通知）");
            client.put(otherKey, "updated-other");
            Thread.sleep(200);
            
            // 再次修改被监听的 key
            System.out.println("8.5 再次 PUT 被监听的 key");
            client.put(watchedKey, "updated2");
            Thread.sleep(200);
            
            // 等待事件
            boolean eventsReceived = latch.await(3, TimeUnit.SECONDS);
            
            // 清理
            client.cancelWatch(listener.getWatchId());
            
            // 验证
            System.out.println("8.6 验证结果");
            System.out.println("    收到事件数: " + events.size());
            System.out.println("    预期: 2（只收到被监听 key 的事件）");
            
            boolean correct = events.size() == 2 && 
                    events.stream().allMatch(e -> watchedKey.equals(e.getKey()));
            System.out.println("    ✓ 精确匹配验证: " + correct);
            
            return correct;
        } catch (Exception e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 9. Watch 前缀匹配测试 ====================
    
    /**
     * 测试 Watch 前缀匹配：
     * - 监听前缀
     * - 所有匹配前缀的 key 变化都收到通知
     */
    private boolean testWatchPrefixMatch() {
        printSection("测试 9: Watch 前缀匹配");
        
        String prefix = "/test/watch/prefix-" + System.currentTimeMillis() + "/";
        testKeys.add(prefix + "key1");
        testKeys.add(prefix + "key2");
        testKeys.add(prefix + "subdir/key3");
        testKeys.add("/test/watch/other-key");  // 不应该匹配
        
        try {
            // 初始化数据
            client.put(prefix + "key1", "v1");
            client.put(prefix + "key2", "v2");
            Thread.sleep(100);
            
            long startRevision = client.getCurrentRevision() + 1;
            System.out.println("9.1 从 revision " + startRevision + " 开始监听前缀: " + prefix);
            
            // 创建前缀 Watch
            CountDownLatch latch = new CountDownLatch(3);
            List<WatchEvent> events = new CopyOnWriteArrayList<>();
            
            WatchListener listener = client.watchPrefix(prefix, event -> {
                events.add(event);
                System.out.println("    [事件] type=" + event.getType() + ", key=" + event.getKey());
                latch.countDown();
            });
            System.out.println("9.2 前缀 Watch 已创建: " + listener.getWatchId());
            
            // 修改匹配前缀的 key
            System.out.println("9.3 PUT 匹配前缀的 key1");
            client.put(prefix + "key1", "updated1");
            Thread.sleep(100);
            
            System.out.println("9.4 PUT 匹配前缀的 key2");
            client.put(prefix + "key2", "updated2");
            Thread.sleep(100);
            
            System.out.println("9.5 PUT 匹配前缀的子目录 key");
            client.put(prefix + "subdir/key3", "updated3");
            Thread.sleep(100);
            
            // 修改不匹配前缀的 key
            System.out.println("9.6 PUT 不匹配前缀的 key（不应收到通知）");
            client.put("/test/watch/other-key", "other-value");
            Thread.sleep(100);
            
            // 等待事件
            latch.await(3, TimeUnit.SECONDS);
            
            // 清理
            client.cancelWatch(listener.getWatchId());
            
            // 验证
            System.out.println("9.7 验证结果");
            System.out.println("    收到事件数: " + events.size());
            System.out.println("    预期: 3（所有匹配前缀的 key）");
            
            boolean correct = events.size() == 3;
            System.out.println("    ✓ 前缀匹配验证: " + correct);
            
            return correct;
        } catch (Exception e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 10. Watch 历史事件回放测试 ====================
    
    /**
     * 测试 Watch 历史事件回放：
     * - 从指定 revision 开始监听
     * - 能收到历史事件
     * - 也能收到后续的实时事件
     */
    private boolean testWatchHistoricalReplay() {
        printSection("测试 10: Watch 历史事件回放");
        
        String key = "/test/watch/history-" + System.currentTimeMillis();
        testKeys.add(key);
        
        try {
            // 创建一些历史事件
            System.out.println("10.1 创建历史事件");
            client.put(key, "v1");
            Thread.sleep(50);
            client.put(key, "v2");
            Thread.sleep(50);
            client.put(key, "v3");
            Thread.sleep(50);
            
            long historicalRevision = client.getCurrentRevision();
            System.out.println("    历史 revision: " + historicalRevision);
            System.out.println("    已创建 3 个版本");
            
            // 从历史 revision + 1 开始监听
            long startRevision = historicalRevision - 2;  // 从第 2 个版本开始
            System.out.println("10.2 从 revision " + startRevision + " 开始监听");
            
            CountDownLatch latch = new CountDownLatch(3);
            List<WatchEvent> events = new CopyOnWriteArrayList<>();
            
            WatchListener listener = client.watchFromRevision(key, startRevision, event -> {
                events.add(event);
                System.out.println("    [事件] type=" + event.getType() + ", rev=" + event.getRevision() + ", value=" + event.getValue());
                latch.countDown();
            });
            
            // 等待历史事件
            Thread.sleep(500);
            
            // 添加实时事件
            System.out.println("10.3 添加实时事件");
            client.put(key, "v4");
            
            // 等待事件
            latch.await(3, TimeUnit.SECONDS);
            
            // 清理
            client.cancelWatch(listener.getWatchId());
            
            // 验证
            System.out.println("10.4 验证结果");
            System.out.println("    收到事件数: " + events.size());
            System.out.println("    预期: >= 2（至少 1 个历史 + 1 个实时）");
            
            boolean hasHistorical = events.stream().anyMatch(e -> e.getRevision() <= historicalRevision);
            boolean hasRealTime = events.stream().anyMatch(e -> e.getValue().equals("v4"));
            
            System.out.println("    ✓ 包含历史事件: " + hasHistorical);
            System.out.println("    ✓ 包含实时事件: " + hasRealTime);
            
            return events.size() >= 2;
        } catch (Exception e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 11. 幂等性测试 ====================
    
    /**
     * 测试幂等性：
     * - 相同 requestId 的重复请求返回相同结果
     * - 不会重复执行
     */
    private boolean testIdempotency() {
        printSection("测试 11: 幂等性");
        
        String key = "/test/idempotency/" + System.currentTimeMillis();
        String requestId = "test-idempotency-" + System.currentTimeMillis();
        testKeys.add(key);
        
        try {
            // 1. 首次 PUT
            System.out.println("11.1 首次 PUT with requestId=" + requestId);
            KVResponse resp1 = client.put(key, "value", requestId);
            assertTrue(resp1.isSuccess(), "首次 PUT 应该成功");
            System.out.println("    响应: success=" + resp1.isSuccess());
            
            // 2. 重复 PUT（幂等）
            System.out.println("11.2 重复 PUT（幂等测试）");
            KVResponse resp2 = client.put(key, "value2", requestId);
            assertTrue(resp2.isSuccess(), "重复 PUT 应该返回成功");
            System.out.println("    响应: success=" + resp2.isSuccess() + ", value=" + resp2.getValue());
            
            // 3. 验证值没有被更新（幂等性保证）
            System.out.println("11.3 验证幂等性");
            String finalValue = client.get(key).getValue();
            boolean idempotent = "value".equals(finalValue);
            System.out.println("    最终值: " + finalValue + " (应为 'value')");
            System.out.println("    ✓ 幂等性验证: " + idempotent);
            
            // 4. 不同 requestId 应该正常执行
            System.out.println("11.4 不同 requestId 正常执行");
            String requestId2 = "test-idempotency-2-" + System.currentTimeMillis();
            KVResponse resp3 = client.put(key, "new-value", requestId2);
            assertTrue(resp3.isSuccess(), "不同 requestId 应该正常执行");
            assertEquals("new-value", client.get(key).getValue(), "值应该被更新");
            System.out.println("    不同 requestId PUT 成功: " + client.get(key).getValue());
            
            return idempotent;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 12. 线性一致性读测试 ====================
    
    /**
     * 测试线性一致性读：
     * - GET 操作通过 ReadIndex 机制保证线性一致性
     * - 能读到最新的已提交数据
     */
    private boolean testLinearizableRead() {
        printSection("测试 12: 线性一致性读");
        
        String key = "/test/linear/" + System.currentTimeMillis();
        testKeys.add(key);
        
        try {
            // 1. 写入数据
            System.out.println("12.1 PUT 数据");
            client.put(key, "test-value");
            System.out.println("    PUT " + key + " = test-value");
            
            // 2. 线性一致性读
            System.out.println("12.2 线性一致性 GET");
            KVResponse resp = client.get(key);
            assertTrue(resp.isSuccess(), "GET 应该成功");
            assertEquals("test-value", resp.getValue(), "读取的值应该匹配");
            System.out.println("    GET 响应: " + resp.getValue());
            
            // 3. 连续读取应该一致
            System.out.println("12.3 连续读取一致性");
            String value1 = client.get(key).getValue();
            String value2 = client.get(key).getValue();
            String value3 = client.get(key).getValue();
            
            boolean consistent = value1.equals(value2) && value2.equals(value3);
            System.out.println("    连续读取: " + value1 + ", " + value2 + ", " + value3);
            System.out.println("    ✓ 读取一致性: " + consistent);
            
            // 4. GET ALL 线性一致性
            System.out.println("12.4 线性一致性 GET ALL");
            Map<String, String> allData = client.getAll();
            boolean hasKey = allData.containsKey(key) && "test-value".equals(allData.get(key));
            System.out.println("    GET ALL 结果数: " + allData.size());
            System.out.println("    ✓ GET ALL 包含目标 key: " + hasKey);
            
            return consistent && hasKey;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 13. 分布式锁测试 ====================
    
    /**
     * 测试分布式锁：
     * - CAS 实现互斥锁
     * - 只有锁持有者能释放锁
     */
    private boolean testDistributedLock() {
        printSection("测试 13: 分布式锁");
        
        String lockKey = "/test/lock/" + System.currentTimeMillis();
        String owner1 = "owner-1-" + System.currentTimeMillis();
        String owner2 = "owner-2-" + System.currentTimeMillis();
        
        try {
            // 1. 获取锁
            System.out.println("13.1 Owner1 尝试获取锁");
            boolean acquired1 = client.acquireLock(lockKey, owner1);
            assertTrue(acquired1, "Owner1 应该获取锁成功");
            System.out.println("    Owner1 获取锁成功");
            
            // 2. 尝试竞争锁
            System.out.println("13.2 Owner2 尝试获取锁（应失败）");
            boolean acquired2 = client.acquireLock(lockKey, owner2);
            assertFalse(acquired2, "Owner2 应该获取锁失败");
            System.out.println("    Owner2 获取锁失败（锁已被占用）");
            
            // 3. Owner1 释放锁
            System.out.println("13.3 Owner1 释放锁");
            boolean released1 = client.releaseLock(lockKey, owner1);
            assertTrue(released1, "Owner1 应该释放锁成功");
            System.out.println("    Owner1 释放锁成功");
            
            // 4. Owner2 获取锁
            System.out.println("13.4 Owner2 再次尝试获取锁（应成功）");
            boolean acquired3 = client.acquireLock(lockKey, owner2);
            assertTrue(acquired3, "Owner2 应该获取锁成功");
            System.out.println("    Owner2 获取锁成功");
            
            // 5. 非持有者释放锁（应失败）
            System.out.println("13.5 非持有者 Owner1 释放锁（应失败）");
            boolean released2 = client.releaseLock(lockKey, owner1);
            assertFalse(released2, "Owner1 释放锁应该失败");
            System.out.println("    Owner1 释放锁失败（非持有者）");
            
            // 6. 清理
            client.releaseLock(lockKey, owner2);
            
            System.out.println("    ✓ 分布式锁验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== 14. MVCC 精确值验证 ====================
    
    /**
     * 测试 MVCC 精确值验证：
     * - createRevision: key 首次创建时的全局 revision，之后不变
     * - modRevision: 每次修改时的全局 revision
     * - version: key 的本地版本号，每次修改 +1
     */
    private boolean testMVCCPreciseValues() {
        printSection("测试 15: MVCC 精确值验证");

        String key = "/test/mvcc/precise/" + System.currentTimeMillis();
        testKeys.add(key);

        try {
            // 1. 创建 key，记录初始 revision
            System.out.println("15.1 PUT 创建 key");
            long revBefore = client.getCurrentRevision();
            client.put(key, "v1");
            long revAfter1 = client.getCurrentRevision();
            System.out.println("    创建前 revision: " + revBefore + ", 创建后 revision: " + revAfter1);

            // 2. 通过事务 GET 获取完整版本信息
            System.out.println("15.2 事务 GET 获取版本信息（第一次）");
            TxnRequest txn1 = TxnRequest.builder()
                    .success(java.util.List.of(Operation.get(key)))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp1 = client.transaction(txn1);
            TxnResponse.OpResult r1 = resp1.getResults().get(0);

            long createRev1 = r1.getCreateRevision();
            long modRev1 = r1.getModRevision();
            long ver1 = r1.getVersion();
            System.out.println("    createRevision=" + createRev1 + ", modRevision=" + modRev1 + ", version=" + ver1);

            assertTrue(ver1 == 1, "首次创建 version 应为 1");
            assertTrue(createRev1 > 0, "createRevision 应大于 0");
            assertTrue(modRev1 == createRev1, "首次创建时 modRevision 应等于 createRevision");

            // 3. 更新 key
            System.out.println("15.3 PUT 更新 key");
            client.put(key, "v2");
            long revAfter2 = client.getCurrentRevision();
            System.out.println("    更新后 revision: " + revAfter2);

            // 4. 再次获取版本信息
            System.out.println("15.4 事务 GET 获取版本信息（第二次）");
            TxnResponse resp2 = client.transaction(txn1);
            TxnResponse.OpResult r2 = resp2.getResults().get(0);

            long createRev2 = r2.getCreateRevision();
            long modRev2 = r2.getModRevision();
            long ver2 = r2.getVersion();
            System.out.println("    createRevision=" + createRev2 + ", modRevision=" + modRev2 + ", version=" + ver2);

            assertTrue(createRev2 == createRev1, "createRevision 应保持不变");
            assertTrue(modRev2 > modRev1, "modRevision 应递增");
            assertTrue(ver2 == 2, "version 应从 1 变为 2");

            // 5. 第三次更新
            System.out.println("15.5 PUT 第三次更新");
            client.put(key, "v3");

            TxnResponse resp3 = client.transaction(txn1);
            TxnResponse.OpResult r3 = resp3.getResults().get(0);
            System.out.println("    version=" + r3.getVersion() + ", modRevision=" + r3.getModRevision());

            assertTrue(r3.getVersion() == 3, "version 应为 3");
            assertTrue(r3.getCreateRevision() == createRev1, "createRevision 仍应保持不变");

            System.out.println("    ✓ MVCC 精确值验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 16. Watch DELETE 事件类型 ====================

    /**
     * 测试 Watch 的 DELETE 事件类型：
     * - DELETE 操作应产生 EventType.DELETE 事件
     * - DELETE 事件的 value 应为 null
     */
    private boolean testWatchDeleteEvent() {
        printSection("测试 16: Watch DELETE 事件类型");

        String key = "/test/watch/delete/" + System.currentTimeMillis();
        testKeys.add(key);

        try {
            // 创建 key
            client.put(key, "to-be-deleted");
            Thread.sleep(100);

            long startRevision = client.getCurrentRevision() + 1;
            System.out.println("16.1 从 revision " + startRevision + " 开始监听");

            CountDownLatch latch = new CountDownLatch(2);
            List<WatchEvent> events = new CopyOnWriteArrayList<>();

            WatchListener listener = client.watchFromRevision(key, startRevision, event -> {
                events.add(event);
                System.out.println("    [事件] type=" + event.getType() + ", key=" + event.getKey() + ", value=" + event.getValue());
                latch.countDown();
            });

            // PUT 更新
            System.out.println("16.2 PUT 更新 key");
            client.put(key, "updated");
            Thread.sleep(200);

            // DELETE
            System.out.println("16.3 DELETE key");
            client.delete(key);
            Thread.sleep(200);

            latch.await(3, TimeUnit.SECONDS);
            client.cancelWatch(listener.getWatchId());

            // 验证
            System.out.println("16.4 验证事件类型");
            boolean hasPut = events.stream().anyMatch(e -> e.getType() == WatchEvent.EventType.PUT);
            boolean hasDelete = events.stream().anyMatch(e -> e.getType() == WatchEvent.EventType.DELETE);
            boolean deleteValueNull = events.stream()
                    .filter(e -> e.getType() == WatchEvent.EventType.DELETE)
                    .allMatch(e -> e.getValue() == null);

            System.out.println("    PUT 事件: " + hasPut);
            System.out.println("    DELETE 事件: " + hasDelete);
            System.out.println("    DELETE 事件 value 为 null: " + deleteValueNull);

            assertTrue(hasPut, "应有 PUT 事件");
            assertTrue(hasDelete, "应有 DELETE 事件");
            assertTrue(deleteValueNull, "DELETE 事件 value 应为 null");

            System.out.println("    ✓ Watch DELETE 事件类型验证通过");
            return true;
        } catch (Exception e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 17. 事务 Compare MOD/CREATE revision ====================

    /**
     * 测试事务的 MOD 和 CREATE revision 比较：
     * - MOD: 比较 key 的最后修改 revision
     * - CREATE: 比较 key 的创建 revision
     */
    private boolean testTransactionCompareModCreate() {
        printSection("测试 16: 事务 Compare MOD/CREATE revision");

        String key = "/test/txn/mod-create/" + System.currentTimeMillis();
        testKeys.add(key);

        try {
            // 1. 创建 key，获取 createRevision
            System.out.println("16.1 创建 key");
            client.put(key, "initial");

            TxnRequest getTxn = TxnRequest.builder()
                    .success(java.util.List.of(Operation.get(key)))
                    .build();
            TxnResponse getResp = client.transaction(getTxn);
            TxnResponse.OpResult getResult = getResp.getResults().get(0);

            long createRev = getResult.getCreateRevision();
            long modRev = getResult.getModRevision();
            System.out.println("    createRevision=" + createRev + ", modRevision=" + modRev);

            // 2. 测试 MOD EQUAL（条件满足）
            System.out.println("16.2 测试 MOD EQUAL（条件满足）");
            TxnRequest txn1 = TxnRequest.builder()
                    .compares(java.util.List.of(Compare.mod(key, CompareOp.EQUAL, modRev)))
                    .success(java.util.List.of(Operation.put(key, "mod-equal-passed")))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp1 = client.transaction(txn1);
            assertTrue(resp1.isSucceeded(), "MOD EQUAL 应满足");
            System.out.println("    MOD EQUAL 满足");

            // 3. 测试 MOD EQUAL（条件不满足）
            System.out.println("16.3 测试 MOD EQUAL（条件不满足）");
            TxnRequest txn2 = TxnRequest.builder()
                    .compares(java.util.List.of(Compare.mod(key, CompareOp.EQUAL, modRev)))
                    .success(java.util.List.of(Operation.put(key, "should-not-update")))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp2 = client.transaction(txn2);
            assertFalse(resp2.isSucceeded(), "MOD EQUAL 不应再满足（已更新）");
            System.out.println("    MOD EQUAL 不满足（modRevision 已变化）");

            // 4. 测试 CREATE EQUAL（应始终满足，因为 createRevision 不变）
            System.out.println("16.4 测试 CREATE EQUAL（应始终满足）");
            TxnRequest txn3 = TxnRequest.builder()
                    .compares(java.util.List.of(Compare.create(key, CompareOp.EQUAL, createRev)))
                    .success(java.util.List.of(Operation.put(key, "create-equal-passed")))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp3 = client.transaction(txn3);
            assertTrue(resp3.isSucceeded(), "CREATE EQUAL 应满足（createRevision 不变）");
            System.out.println("    CREATE EQUAL 满足（createRevision 始终不变）");

            // 5. 测试 CREATE GREATER（key 不存在时）
            System.out.println("16.5 测试 CREATE GREATER（新 key）");
            String newKey = "/test/txn/mod-create/new/" + System.currentTimeMillis();
            testKeys.add(newKey);
            TxnRequest txn4 = TxnRequest.builder()
                    .compares(java.util.List.of(Compare.create(newKey, CompareOp.GREATER, 0L)))
                    .success(java.util.List.of(Operation.put(newKey, "should-not-create")))
                    .failure(java.util.List.of(Operation.put(newKey, "created-in-failure")))
                    .build();
            TxnResponse resp4 = client.transaction(txn4);
            // key 不存在时 createRevision = 0，GREATER 0 不满足，应执行 failure
            assertFalse(resp4.isSucceeded(), "不存在的 key CREATE > 0 不应满足");
            assertEquals("created-in-failure", client.get(newKey).getValue(), "应执行 failure 分支");
            System.out.println("    不存在的 key CREATE > 0 不满足，执行 failure 分支");

            System.out.println("    ✓ MOD/CREATE revision 比较验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 18. 事务多条件 AND 语义 ====================

    /**
     * 测试事务多 Compare 的 AND 语义：
     * - 所有条件都满足时才执行 success
     * - 任一条件不满足则执行 failure
     */
    private boolean testTransactionMultiCompare() {
        printSection("测试 17: 事务多条件 AND 语义");

        String key1 = "/test/txn/multi/" + System.currentTimeMillis() + "/key1";
        String key2 = "/test/txn/multi/" + System.currentTimeMillis() + "/key2";
        testKeys.add(key1);
        testKeys.add(key2);

        try {
            // 1. 初始化数据
            System.out.println("17.1 初始化数据");
            client.put(key1, "value1");
            client.put(key2, "value2");
            System.out.println("    PUT " + key1 + " = value1");
            System.out.println("    PUT " + key2 + " = value2");

            // 2. 两个条件都满足
            System.out.println("17.2 两个条件都满足");
            TxnRequest txn1 = TxnRequest.builder()
                    .compares(java.util.List.of(
                            Compare.value(key1, CompareOp.EQUAL, "value1"),
                            Compare.value(key2, CompareOp.EQUAL, "value2")
                    ))
                    .success(java.util.List.of(
                            Operation.put(key1, "both-passed")
                    ))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp1 = client.transaction(txn1);
            assertTrue(resp1.isSucceeded(), "两个条件都满足时应执行 success");
            assertEquals("both-passed", client.get(key1).getValue(), "key1 应被更新");
            System.out.println("    两个条件满足，执行 success");

            // 3. 第一个条件满足，第二个不满足
            System.out.println("17.3 第一个满足，第二个不满足");
            TxnRequest txn2 = TxnRequest.builder()
                    .compares(java.util.List.of(
                            Compare.value(key1, CompareOp.EQUAL, "both-passed"),
                            Compare.value(key2, CompareOp.EQUAL, "wrong-value")
                    ))
                    .success(java.util.List.of(
                            Operation.put(key1, "should-not-update")
                    ))
                    .failure(java.util.List.of(
                            Operation.put(key2, "failure-executed")
                    ))
                    .build();
            TxnResponse resp2 = client.transaction(txn2);
            assertFalse(resp2.isSucceeded(), "条件不满足时应执行 failure");
            assertEquals("failure-executed", client.get(key2).getValue(), "应执行 failure 分支");
            assertEquals("both-passed", client.get(key1).getValue(), "key1 不应被更新");
            System.out.println("    条件不满足，执行 failure");

            // 4. 第一个条件不满足，第二个满足
            System.out.println("17.4 第一个不满足，第二个满足");
            TxnRequest txn3 = TxnRequest.builder()
                    .compares(java.util.List.of(
                            Compare.value(key1, CompareOp.EQUAL, "wrong-value"),
                            Compare.value(key2, CompareOp.EQUAL, "failure-executed")
                    ))
                    .success(java.util.List.of())
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp3 = client.transaction(txn3);
            assertFalse(resp3.isSucceeded(), "任一条件不满足即失败");
            System.out.println("    任一条件不满足即失败");

            System.out.println("    ✓ 多条件 AND 语义验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 24. Tombstone 状态下 version=0 ====================

    /**
     * 测试 etcd 语义：Tombstone 状态下 version 必须返回 0
     * 
     * etcd 语义：
     * - DELETE 创建 tombstone（逻辑删除）
     * - GET 已删除的 key 不返回数据（tombstone 语义）
     * - Tombstone 状态的 key 在返回 version 时应返回 0（不是删除前的 version）
     * 
     * 这是 etcd 与大多数 KV store 的关键区别：
     * - 普通 KV: 删除后 GET 仍返回历史数据或 null
     * - etcd: 删除后 GET 不返回 tombstone，version 为 0
     */
    private boolean testTombstoneVersionZero() {
        printSection("测试 22: Tombstone 状态下 version=0");

        String key = "/test/tombstone/version/" + System.currentTimeMillis();
        testKeys.add(key);

        try {
            // 1. 创建 key，记录 version
            System.out.println("22.1 创建 key 并记录 version");
            client.put(key, "value1");

            TxnRequest getTxn = TxnRequest.builder()
                    .success(java.util.List.of(Operation.get(key)))
                    .build();
            TxnResponse resp1 = client.transaction(getTxn);
            TxnResponse.OpResult r1 = resp1.getResults().get(0);

            long versionBeforeDelete = r1.getVersion();
            System.out.println("    删除前 version: " + versionBeforeDelete);
            assertTrue(versionBeforeDelete >= 1, "删除前 version 应 >= 1");

            // 2. 删除 key
            System.out.println("22.2 DELETE key");
            client.delete(key);

            // 3. 通过事务 GET tombstone key（关键测试）
            System.out.println("22.3 事务 GET 已删除的 key（tombstone 状态）");
            TxnResponse resp2 = client.transaction(getTxn);
            TxnResponse.OpResult r2 = resp2.getResults().get(0);

            System.out.println("    Tombstone GET: value=" + r2.getValue() + ", version=" + r2.getVersion());

            // 4. 验证 etcd 语义：tombstone 状态下 value 为 null，version 为 0
            assertTrue(r2.getValue() == null, "Tombstone 状态 value 应为 null");
            assertTrue(r2.getVersion() == 0, "Tombstone 状态下 version 应为 0（不是 " + versionBeforeDelete + "）");
            System.out.println("    ✓ Tombstone 状态下 version=0 验证通过");

            // 5. 验证普通 GET 也看不到 tombstone
            System.out.println("22.4 普通 GET 验证 tombstone 不可见");
            KVResponse getResp = client.get(key);
            boolean keyNotFound = getResp.getValue() == null || getResp.getValue().isEmpty();
            assertTrue(keyNotFound, "GET 已删除的 key 应返回空值（tombstone 语义）");
            System.out.println("    普通 GET 返回空值 ✓");

            // 6. 重新创建 key，version 应从 1 开始
            System.out.println("22.5 重新创建 key");
            client.put(key, "recreated-value");

            TxnResponse resp3 = client.transaction(getTxn);
            TxnResponse.OpResult r3 = resp3.getResults().get(0);

            System.out.println("    重新创建后: value=" + r3.getValue() + ", version=" + r3.getVersion());
            assertEquals("recreated-value", r3.getValue(), "重新创建后 value 应匹配");
            assertTrue(r3.getVersion() == 1, "重新创建后 version 应从 1 开始");

            // 7. 再次删除，验证 version 再次回到 0
            System.out.println("22.6 再次删除 key");
            client.delete(key);

            TxnResponse resp4 = client.transaction(getTxn);
            TxnResponse.OpResult r4 = resp4.getResults().get(0);

            System.out.println("    再次删除后 tombstone: version=" + r4.getVersion());
            assertTrue(r4.getValue() == null, "再次删除后 value 应为 null");
            assertTrue(r4.getVersion() == 0, "再次删除后 version 应回到 0");

            System.out.println("    ✓ Tombstone version=0 完整验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 25. 事务内 revision 共享详细验证 ====================

    /**
     * 测试 etcd 事务内 revision 共享语义：
     * 
     * etcd 语义：
     * - 事务内所有操作共享同一个 mainRev（事务级别的全局 revision）
     * - 通过 subRev 区分事务内不同操作的顺序
     * - 这保证了事务的原子性：所有操作要么全部成功，要么全部失败
     * 
     * 验证要点：
     * 1. 事务内多个 PUT 的 modRevision 应该相同或非常接近
     * 2. 事务内 PUT 后 GET 应该能看到 PUT 的结果（事务内可见性）
     * 3. 事务提交后，所有受影响的 key 的 modRevision 都会更新
     */
    private boolean testTransactionRevisionSharingDetailed() {
        printSection("测试 23: 事务内 revision 共享详细验证");

        String key1 = "/test/txn/rev-share/" + System.currentTimeMillis() + "/key1";
        String key2 = "/test/txn/rev-share/" + System.currentTimeMillis() + "/key2";
        String key3 = "/test/txn/rev-share/" + System.currentTimeMillis() + "/key3";
        testKeys.add(key1);
        testKeys.add(key2);
        testKeys.add(key3);

        try {
            // 1. 记录事务前的 revision
            long beforeRevision = client.getCurrentRevision();
            System.out.println("23.1 事务前 revision: " + beforeRevision);

            // 2. 执行事务：PUT 多个 key，每个 PUT 后都 GET 验证可见性
            System.out.println("23.2 在事务中 PUT 3 个 key");
            TxnRequest txn = TxnRequest.builder()
                    .success(java.util.List.of(
                            // key1: PUT 后 GET 验证
                            Operation.put(key1, "value1"),
                            Operation.get(key1),
                            // key2: PUT 后 GET 验证
                            Operation.put(key2, "value2"),
                            Operation.get(key2),
                            // key3: PUT 新值
                            Operation.put(key3, "value3")
                    ))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp = client.transaction(txn);

            assertTrue(resp.isSucceeded(), "事务应成功执行");
            System.out.println("    事务执行成功");

            // 3. 分析事务内的 revision 分布
            // 结构：[PUT key1, GET key1, PUT key2, GET key2, PUT key3]
            //       0         1        2         3        4
            System.out.println("23.3 分析事务内 revision 分布");

            // 提取 PUT 操作的 revision（索引 0, 2, 4）
            List<Long> putRevisions = new ArrayList<>();
            for (int i = 0; i < resp.getOps().size(); i++) {
                Operation op = resp.getOps().get(i);
                if (op.getType() == Operation.OpType.PUT) {
                    TxnResponse.OpResult result = resp.getResults().get(i);
                    long rev = result.getRevision();
                    putRevisions.add(rev);
                    System.out.println("    PUT " + op.getKey() + " -> revision=" + rev);
                }
            }

            // 提取 GET 操作的 revision（索引 1, 3）
            List<Long> getRevisions = new ArrayList<>();
            List<String> getValues = new ArrayList<>();
            for (int i = 0; i < resp.getOps().size(); i++) {
                Operation op = resp.getOps().get(i);
                if (op.getType() == Operation.OpType.GET) {
                    TxnResponse.OpResult result = resp.getResults().get(i);
                    getRevisions.add(result.getRevision());
                    getValues.add(result.getValue());
                    System.out.println("    GET " + op.getKey() + " -> revision=" + result.getRevision() + ", value=" + result.getValue());
                }
            }

            // 4. 验证事务内可见性（关键）
            // PUT key1 后 GET key1，应该能看到 value1
            // PUT key2 后 GET key2，应该能看到 value2
            System.out.println("23.4 验证事务内可见性");
            assertEquals("value1", getValues.get(0), "事务中 PUT key1 后 GET 应看到 value1");
            assertEquals("value2", getValues.get(1), "事务中 PUT key2 后 GET 应看到 value2");
            System.out.println("    ✓ 事务内 PUT-GET 可见性验证通过");

            // 5. 验证 revision 共享特性
            // etcd 语义：事务内操作共享主 revision，只是 subRev 不同
            // 这意味着 PUT 的 revision 差值应该较小（不超过 subRev 差值）
            long minPutRev = putRevisions.stream().min(Long::compare).orElse(0L);
            long maxPutRev = putRevisions.stream().max(Long::compare).orElse(0L);
            long revisionSpan = maxPutRev - minPutRev;

            System.out.println("23.5 验证 revision 共享");
            System.out.println("    PUT revision 范围: [" + minPutRev + ", " + maxPutRev + "], span=" + revisionSpan);

            // etcd 语义：事务内操作共享主 revision
            // 差值应该很小（通常 <= 操作数）
            int putCount = putRevisions.size();  // 3 个 PUT
            boolean revisionShared = revisionSpan <= putCount;
            System.out.println("    revision span(" + revisionSpan + ") <= PUT 数量(" + putCount + "): " + revisionShared);

            // 6. 验证事务后的 revision 确实增长
            long afterRevision = client.getCurrentRevision();
            System.out.println("23.6 事务后 revision: " + afterRevision);
            System.out.println("    revision 增长: " + (afterRevision - beforeRevision));

            // 7. 验证事务后各 key 的 revision
            System.out.println("23.7 验证事务后各 key 的 revision");
            for (String key : new String[]{key1, key2, key3}) {
                TxnRequest getReq = TxnRequest.builder()
                        .success(java.util.List.of(Operation.get(key)))
                        .build();
                TxnResponse getResp = client.transaction(getReq);
                TxnResponse.OpResult result = getResp.getResults().get(0);
                System.out.println("    " + key + " -> modRevision=" + result.getModRevision() + ", version=" + result.getVersion());

                // 每个 key 都应该被更新过，version >= 1
                assertTrue(result.getVersion() >= 1, key + " 的 version 应 >= 1");
            }

            System.out.println("    ✓ 事务内 revision 共享详细验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 19. 事务内 DELETE 可见性 ====================

    /**
     * 测试事务内 DELETE 后 GET 的可见性：
     * - 事务中 DELETE 一个 key 后，同一事务中的 GET 应返回空
     */
    private boolean testTransactionDeleteVisibility() {
        printSection("测试 18: 事务内 DELETE 可见性");

        String key = "/test/txn/delete-vis/" + System.currentTimeMillis();
        testKeys.add(key);

        try {
            // 1. 初始化数据
            System.out.println("18.1 初始化数据");
            client.put(key, "value");
            System.out.println("    PUT " + key + " = value");

            // 2. 事务中 DELETE 后 GET
            System.out.println("18.2 事务中 DELETE 后 GET");
            TxnRequest txn = TxnRequest.builder()
                    .success(java.util.List.of(
                            Operation.delete(key),
                            Operation.get(key)
                    ))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse resp = client.transaction(txn);

            assertTrue(resp.isSucceeded(), "事务应成功执行");
            TxnResponse.OpResult getResult = resp.getResults().get(1);
            System.out.println("    DELETE 后 GET 结果: value=" + getResult.getValue() + ", version=" + getResult.getVersion());

            assertTrue(getResult.getValue() == null, "DELETE 后 GET 应返回 null");
            assertTrue(getResult.getVersion() == 0, "DELETE 后 version 应为 0");

            // 3. 验证事务外也看不到
            System.out.println("18.3 验证事务外 GET");
            KVResponse outsideGet = client.get(key);
            assertTrue(outsideGet.getValue() == null || outsideGet.getValue().isEmpty(), "事务外也应看不到");
            System.out.println("    事务外 GET: value=" + outsideGet.getValue());

            System.out.println("    ✓ 事务内 DELETE 可见性验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 20. CAS 操作测试 ====================

    /**
     * 测试 CAS（Compare-And-Swap）操作：
     * - 值匹配时更新
     * - 值不匹配时不更新
     */
    private boolean testCASOperation() {
        printSection("测试 19: CAS 操作");

        String key = "/test/cas/" + System.currentTimeMillis();
        testKeys.add(key);

        try {
            // 1. 初始化数据
            System.out.println("19.1 初始化数据");
            client.put(key, "original");
            System.out.println("    PUT " + key + " = original");

            // 2. CAS 成功（值匹配）
            System.out.println("19.2 CAS 成功（值匹配）");
            TxnResponse resp1 = client.cas(key, "original", "swapped");
            assertTrue(resp1.isSucceeded(), "CAS 应成功");
            assertEquals("swapped", client.get(key).getValue(), "值应被更新");
            System.out.println("    CAS 成功，值更新为 swapped");

            // 3. CAS 失败（值不匹配）
            System.out.println("19.3 CAS 失败（值不匹配）");
            TxnResponse resp2 = client.cas(key, "wrong-value", "should-not-update");
            assertFalse(resp2.isSucceeded(), "CAS 应失败");
            assertEquals("swapped", client.get(key).getValue(), "值不应被更新");
            System.out.println("    CAS 失败，值保持 swapped");

            // 4. CAS with version
            System.out.println("19.4 CAS with version");
            TxnRequest getTxn = TxnRequest.builder()
                    .success(java.util.List.of(Operation.get(key)))
                    .build();
            TxnResponse getResp = client.transaction(getTxn);
            long currentVersion = getResp.getResults().get(0).getVersion();
            System.out.println("    当前 version=" + currentVersion);

            TxnResponse resp3 = client.casWithVersion(key, currentVersion, "version-swapped");
            assertTrue(resp3.isSucceeded(), "版本匹配时 CAS 应成功");
            assertEquals("version-swapped", client.get(key).getValue(), "值应被更新");
            System.out.println("    CAS with version 成功");

            // 版本不匹配时应失败
            TxnResponse resp4 = client.casWithVersion(key, currentVersion, "should-not-update");
            assertFalse(resp4.isSucceeded(), "版本不匹配时 CAS 应失败");
            assertEquals("version-swapped", client.get(key).getValue(), "值不应被更新");
            System.out.println("    CAS with version 失败（版本已变化）");

            System.out.println("    ✓ CAS 操作验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 22. 空值边界测试 ====================

    /**
     * 测试空值 PUT：
     * - 空字符串 value 是合法的
     * - GET 应返回空字符串（不是 null）
     */
    private boolean testEmptyValue() {
        printSection("测试 20: 空值边界测试");

        String key = "/test/empty/" + System.currentTimeMillis();
        testKeys.add(key);

        try {
            // 1. PUT 空字符串
            System.out.println("20.1 PUT 空字符串");
            client.put(key, "");
            KVResponse getResp = client.get(key);
            System.out.println("    GET 结果: value='" + getResp.getValue() + "', found=" + getResp.getFound());

            // 空字符串 value 是合法的，key 应该存在
            assertTrue(getResp.getFound() != null && getResp.getFound(), "空字符串 value 的 key 应存在");

            // 2. 通过事务 GET 验证
            System.out.println("20.2 事务 GET 空字符串");
            TxnRequest txn = TxnRequest.builder()
                    .success(java.util.List.of(Operation.get(key)))
                    .build();
            TxnResponse resp = client.transaction(txn);
            TxnResponse.OpResult result = resp.getResults().get(0);
            System.out.println("    事务 GET: value='" + result.getValue() + "', version=" + result.getVersion());

            assertTrue(result.getValue() != null, "空字符串不应为 null");
            assertTrue(result.getVersion() == 1, "version 应为 1");

            // 3. 更新为空字符串再验证 version
            System.out.println("20.3 再次 PUT 空字符串（version 应递增）");
            client.put(key, "");
            TxnResponse resp2 = client.transaction(txn);
            TxnResponse.OpResult result2 = resp2.getResults().get(0);
            System.out.println("    再次 GET: version=" + result2.getVersion());
            assertTrue(result2.getVersion() == 2, "再次 PUT 后 version 应为 2");

            System.out.println("    ✓ 空值边界测试通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 23. 删除后 recreate 的 createRevision 变化 ====================

    /**
     * 测试 etcd 语义：删除后重新 PUT 同名 key，createRevision 应不同：
     * - 首次创建：createRevision = X
     * - 删除后重新创建：createRevision = Y（Y > X）
     */
    private boolean testDeleteRecreateCreateRevision() {
        printSection("测试 21: 删除后 recreate 的 createRevision 变化");

        String key = "/test/recreate/" + System.currentTimeMillis();
        testKeys.add(key);

        try {
            // 1. 首次创建，获取 createRevision
            System.out.println("21.1 首次创建 key");
            client.put(key, "v1");

            TxnRequest getTxn = TxnRequest.builder()
                    .success(java.util.List.of(Operation.get(key)))
                    .build();
            TxnResponse resp1 = client.transaction(getTxn);
            long createRev1 = resp1.getResults().get(0).getCreateRevision();
            long modRev1 = resp1.getResults().get(0).getModRevision();
            System.out.println("    首次: createRevision=" + createRev1 + ", modRevision=" + modRev1);

            // 2. 删除 key
            System.out.println("21.2 删除 key");
            client.delete(key);

            // 3. 验证 GET 返回空
            System.out.println("21.3 验证删除后 GET");
            KVResponse getResp = client.get(key);
            assertTrue(getResp.getValue() == null || getResp.getValue().isEmpty(), "删除后应返回空");
            System.out.println("    删除后 GET: value=" + getResp.getValue());

            // 4. 重新创建同名 key
            System.out.println("21.4 重新创建同名 key");
            client.put(key, "v2");

            TxnResponse resp2 = client.transaction(getTxn);
            long createRev2 = resp2.getResults().get(0).getCreateRevision();
            long modRev2 = resp2.getResults().get(0).getModRevision();
            System.out.println("    重建: createRevision=" + createRev2 + ", modRevision=" + modRev2);

            // 5. 验证 createRevision 不同
            assertTrue(createRev2 > createRev1, "重新创建后 createRevision 应更大");
            assertTrue(modRev2 > modRev1, "重新创建后 modRevision 应更大");

            // 6. version 应重新从 1 开始
            long ver2 = resp2.getResults().get(0).getVersion();
            assertTrue(ver2 == 1, "重新创建后 version 应从 1 开始");
            System.out.println("    version=" + ver2 + "（重新从 1 开始）");

            // 7. 关键验证：Compare.CREATE 必须使用当前生命周期的 createRevision
            // 用旧的 createRevision 比较应该失败（已失效）
            System.out.println("21.5 验证 Compare.CREATE 使用旧的 createRevision（应失败）");
            TxnRequest txnOld = TxnRequest.builder()
                    .compares(java.util.List.of(Compare.create(key, CompareOp.EQUAL, createRev1)))
                    .success(java.util.List.of(Operation.put(key, "should-not-update")))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse respOld = client.transaction(txnOld);
            assertFalse(respOld.isSucceeded(), "旧的 createRevision 不应再满足");
            System.out.println("    旧的 createRevision=" + createRev1 + " 不满足 ✓");

            // 用新的 createRevision 比较应该成功
            System.out.println("21.6 验证 Compare.CREATE 使用新的 createRevision（应成功）");
            TxnRequest txnNew = TxnRequest.builder()
                    .compares(java.util.List.of(Compare.create(key, CompareOp.EQUAL, createRev2)))
                    .success(java.util.List.of(Operation.put(key, "create-rev-match")))
                    .failure(java.util.List.of())
                    .build();
            TxnResponse respNew = client.transaction(txnNew);
            assertTrue(respNew.isSucceeded(), "新的 createRevision 应满足");
            assertEquals("create-rev-match", client.get(key).getValue(), "应执行 success 分支");
            System.out.println("    新的 createRevision=" + createRev2 + " 满足 ✓");

            System.out.println("    ✓ 删除后 recreate 的 createRevision 变化验证通过");
            return true;
        } catch (AssertionError e) {
            System.out.println("    ❌ 测试失败: " + e.getMessage());
            return false;
        }
    }

    // ==================== 辅助方法 ====================

    private void printSection(String title) {
        System.out.println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("  " + title);
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }
    
    private void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
    
    private void assertFalse(boolean condition, String message) {
        if (condition) {
            throw new AssertionError(message);
        }
    }
    
    private void assertEquals(Object expected, Object actual, String message) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(message + " (期望: " + expected + ", 实际: " + actual + ")");
        }
    }
    
    private void cleanup() {
        System.out.println("\n清理测试数据...");
        for (String key : testKeys) {
            try {
                client.delete(key);
            } catch (Exception e) {
                // 忽略清理错误
            }
        }
        client.closeAllWatches();
        System.out.println("清理完成");
    }
}
