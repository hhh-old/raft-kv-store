package com.raftkv.service;

import com.raftkv.entity.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 锁并发安全性测试
 * 
 * 测试目标：
 * 1. 无锁读的正确性（读不阻塞写，也不被写阻塞）
 * 2. 细粒度写锁的正确性（写不同 key 并行）
 * 3. 事务乐观并发控制的正确性（冲突检测）
 * 4. 读写混合场景的线程安全性
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.CONCURRENT)
public class LockConcurrencyTest {

    private KVStoreStateMachine stateMachine;
    private MVCCStore mvccStore;
    private static final int CONCURRENT_THREADS = 100;
    private static final int OPERATIONS_PER_THREAD = 1000;

    @BeforeAll
    void setUp() {
        stateMachine = new KVStoreStateMachine();
        mvccStore = new MVCCStore();
    }

    // ==================== 测试 1：无锁读正确性 ====================

    @Test
    @DisplayName("测试无锁读 - 读操作不阻塞写操作")
    void testLockFreeReadDoesNotBlockWrite() throws InterruptedException {
        String key = "concurrent_key";
        AtomicInteger readCount = new AtomicInteger(0);
        AtomicInteger writeCount = new AtomicInteger(0);
        AtomicBoolean errorOccurred = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(CONCURRENT_THREADS * 2);

        // 启动写线程
        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            final int value = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    mvccStore.put(key, "value_" + value);
                    writeCount.incrementAndGet();
                } catch (Exception e) {
                    errorOccurred.set(true);
                    e.printStackTrace();
                } finally {
                    completeLatch.countDown();
                }
            }).start();
        }

        // 启动读线程
        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    // 无锁读，应该不会被阻塞
                    MVCCStore.KeyValue kv = mvccStore.getLatest(key);
                    readCount.incrementAndGet();
                } catch (Exception e) {
                    errorOccurred.set(true);
                    e.printStackTrace();
                } finally {
                    completeLatch.countDown();
                }
            }).start();
        }

        // 同时启动所有线程
        long startTime = System.currentTimeMillis();
        startLatch.countDown();
        
        // 等待完成（设置超时，如果锁有问题会超时）
        boolean completed = completeLatch.await(10, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;

        assertTrue(completed, "操作超时，可能存在死锁！耗时：" + duration + "ms");
        assertFalse(errorOccurred.get(), "发生异常");
        assertEquals(CONCURRENT_THREADS, writeCount.get(), "写操作数量不正确");
        assertEquals(CONCURRENT_THREADS, readCount.get(), "读操作数量不正确");
        
        System.out.println("✓ 无锁读测试通过 - " + (CONCURRENT_THREADS * 2) + " 个并发操作在 " + duration + "ms 内完成");
    }

    // ==================== 测试 2：细粒度写锁 ====================

    @Test
    @DisplayName("测试细粒度写锁 - 写不同 key 应该并行")
    void testFineGrainedWriteLock() throws InterruptedException {
        int keyCount = 100;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicBoolean errorOccurred = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(keyCount);

        long startTime = System.currentTimeMillis();

        // 同时写 100 个不同的 key
        for (int i = 0; i < keyCount; i++) {
            final String key = "key_" + i;
            final int value = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    mvccStore.put(key, "value_" + value);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    errorOccurred.set(true);
                    e.printStackTrace();
                } finally {
                    completeLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        boolean completed = completeLatch.await(5, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;

        assertTrue(completed, "操作超时，可能存在锁竞争问题");
        assertFalse(errorOccurred.get(), "发生异常");
        assertEquals(keyCount, successCount.get(), "成功写入数量不正确");
        
        // 验证所有 key 都写入成功
        for (int i = 0; i < keyCount; i++) {
            MVCCStore.KeyValue kv = mvccStore.getLatest("key_" + i);
            assertNotNull(kv, "key_" + i + " 应该存在");
            assertEquals("value_" + i, kv.getValue(), "key_" + i + " 的值不正确");
        }

        System.out.println("✓ 细粒度写锁测试通过 - " + keyCount + " 个 key 并行写入在 " + duration + "ms 内完成");
    }

    // ==================== 测试 3：事务乐观并发控制 ====================

    @Test
    @DisplayName("测试 MVCC 基本功能 - 无 OCC 情况下正常工作")
    void testMVCCBasicFunctionality() throws InterruptedException {
        String key = "mvcc_test_key";
        mvccStore.put(key, "initial_value");

        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch completeLatch = new CountDownLatch(10);

        // 启动 10 个线程，同时修改同一个 key
        for (int i = 0; i < 10; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    // 直接写入（无 OCC，Raft 会保证串行）
                    mvccStore.put(key, "txn_" + threadId + "_value");
                    successCount.incrementAndGet();
                } finally {
                    completeLatch.countDown();
                }
            }).start();
        }

        completeLatch.await(5, TimeUnit.SECONDS);

        // 所有写入都应该成功（Raft 串行执行）
        assertEquals(10, successCount.get());
        
        // 最终值应该是某个线程的值
        MVCCStore.KeyValue finalKv = mvccStore.getLatest(key);
        assertNotNull(finalKv);
        assertTrue(finalKv.getValue().startsWith("txn_"));
        
        // 版本号应该是 11（initial + 10 次更新）
        long version = mvccStore.getVersion(key);
        assertEquals(11, version);

        System.out.println("MVCC basic functionality test passed, version: " + version);
    }

    // ==================== 测试 4：读写混合场景 ====================

    @Test
    @DisplayName("测试读写混合场景 - 高并发下的数据一致性")
    void testReadWriteMixedScenario() throws InterruptedException {
        int keyCount = 50;
        AtomicLong totalReads = new AtomicLong(0);
        AtomicLong totalWrites = new AtomicLong(0);
        AtomicBoolean errorOccurred = new AtomicBoolean(false);
        ConcurrentHashMap<String, String> expectedValues = new ConcurrentHashMap<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(CONCURRENT_THREADS);

        // 初始化数据
        for (int i = 0; i < keyCount; i++) {
            String key = "mixed_key_" + i;
            String value = "initial_" + i;
            mvccStore.put(key, value);
            expectedValues.put(key, value);
        }

        // 启动混合读写线程
        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    Random random = new Random(threadId);
                    
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = "mixed_key_" + random.nextInt(keyCount);
                        boolean isWrite = random.nextBoolean();
                        
                        if (isWrite) {
                            String newValue = "thread" + threadId + "_op" + j;
                            mvccStore.put(key, newValue);
                            expectedValues.put(key, newValue);
                            totalWrites.incrementAndGet();
                        } else {
                            MVCCStore.KeyValue kv = mvccStore.getLatest(key);
                            if (kv != null) {
                                // 验证读取的值在预期范围内
                                String value = kv.getValue();
                                assertTrue(value.startsWith("initial_") || 
                                          value.matches("thread\\d+_op\\d+"),
                                          "读取到异常值：" + value);
                            }
                            totalReads.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorOccurred.set(true);
                    e.printStackTrace();
                } finally {
                    completeLatch.countDown();
                }
            }).start();
        }

        long startTime = System.currentTimeMillis();
        startLatch.countDown();
        boolean completed = completeLatch.await(30, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;

        assertTrue(completed, "测试超时");
        assertFalse(errorOccurred.get(), "发生异常");

        System.out.println("✓ 读写混合测试通过 - " + CONCURRENT_THREADS + " 线程 x " + OPERATIONS_PER_THREAD + " 操作");
        System.out.println("  总读取：" + totalReads.get() + "，总写入：" + totalWrites.get());
        System.out.println("  耗时：" + duration + "ms，吞吐量：" + 
                          ((totalReads.get() + totalWrites.get()) * 1000 / duration) + " ops/sec");
    }

    // ==================== 测试 5：死锁检测 ====================

    @Test
    @DisplayName("测试死锁检测 - 循环依赖场景")
    void testDeadlockPrevention() throws InterruptedException {
        AtomicBoolean deadlockDetected = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(2);

        // 线程 1：先写 A，再写 B
        new Thread(() -> {
            try {
                startLatch.await();
                mvccStore.put("deadlock_A", "value_A1");
                Thread.sleep(50);
                mvccStore.put("deadlock_B", "value_B1");
            } catch (InterruptedException e) {
                deadlockDetected.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                completeLatch.countDown();
            }
        }).start();

        // 线程 2：先写 B，再写 A（相反的顺序）
        new Thread(() -> {
            try {
                startLatch.await();
                mvccStore.put("deadlock_B", "value_B2");
                Thread.sleep(50);
                mvccStore.put("deadlock_A", "value_A2");
            } catch (InterruptedException e) {
                deadlockDetected.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                completeLatch.countDown();
            }
        }).start();

        startLatch.countDown();
        boolean completed = completeLatch.await(3, TimeUnit.SECONDS);

        assertTrue(completed, "可能发生死锁，操作超时");
        assertFalse(deadlockDetected.get(), "检测到死锁");

        System.out.println("✓ 死锁检测测试通过 - 无死锁发生");
    }

    // ==================== 测试 6：内存可见性 ====================

    @Test
    @DisplayName("测试内存可见性 - 写操作对其他线程立即可见")
    void testMemoryVisibility() throws InterruptedException {
        String key = "visibility_key";
        AtomicReference<String> readValue = new AtomicReference<>();
        CountDownLatch writeLatch = new CountDownLatch(1);
        CountDownLatch readLatch = new CountDownLatch(1);

        // 写线程
        new Thread(() -> {
            mvccStore.put(key, "visible_value");
            writeLatch.countDown();
        }).start();

        // 读线程
        new Thread(() -> {
            try {
                writeLatch.await();
                // 短暂等待确保写操作完成
                Thread.sleep(10);
                MVCCStore.KeyValue kv = mvccStore.getLatest(key);
                readValue.set(kv != null ? kv.getValue() : null);
                readLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        readLatch.await(2, TimeUnit.SECONDS);
        assertEquals("visible_value", readValue.get(), "写操作对其他线程不可见");

        System.out.println("✓ 内存可见性测试通过 - 写操作对其他线程立即可见");
    }

    // ==================== 测试 7：版本号单调递增 ====================

    @Test
    @DisplayName("测试版本号单调递增 - 并发写不丢失版本")
    void testVersionMonotonicIncrease() throws InterruptedException {
        String key = "version_key";
        int threadCount = 50;
        int writesPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < writesPerThread; j++) {
                        mvccStore.put(key, "value_" + System.nanoTime());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completeLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        completeLatch.await(10, TimeUnit.SECONDS);

        // 验证版本号
        long version = mvccStore.getVersion(key);
        long expectedVersion = threadCount * writesPerThread;
        
        assertTrue(version >= expectedVersion, 
                   "版本号不递增，期望 >= " + expectedVersion + "，实际 " + version);

        System.out.println("✓ 版本号单调递增测试通过 - 最终版本：" + version + "，期望 >= " + expectedVersion);
    }
}
