package com.raftkv.client.example;

import com.raftkv.client.RaftKVClient;
import com.raftkv.entity.*;
import com.raftkv.entity.Compare.CompareOp;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分布式锁实战示例
 * 
 * 演示如何使用 RaftKVClient 实现各种分布式锁模式：
 * 1. 互斥锁（Mutex Lock）- 定时任务互斥
 * 2. 读写锁（ReadWrite Lock）- 缓存刷新
 * 3. 信号量（Semaphore）- 资源限流
 * 4. 领导选举（Leader Election）- 集群调度
 */
@Slf4j
public class DistributedLockExample {

    private final RaftKVClient client;
    private final String instanceId;

    public DistributedLockExample(RaftKVClient client) {
        this.client = client;
        String id;
        try {
            id = InetAddress.getLocalHost().getHostName() + "-" + Thread.currentThread().getId();
        } catch (Exception e) {
            id = "instance-" + System.currentTimeMillis();
        }
        this.instanceId = id;
    }

    // ==================== 1. 互斥锁（Mutex Lock）====================

    /**
     * 获取互斥锁
     * @param lockName 锁名称
     * @return 是否获取成功
     */
    public boolean acquireMutexLock(String lockName) {
        String lockKey = "/locks/mutex/" + lockName;
        
        List<Compare> compares = Arrays.asList(
            Compare.version(lockKey, CompareOp.EQUAL, 0L)
        );
        List<Operation> successOps = Arrays.asList(
            Operation.put(lockKey, instanceId)
        );
        List<Operation> failureOps = Arrays.asList(
            Operation.get(lockKey)
        );
        
        TxnRequest txn = TxnRequest.builder()
            .compares(compares)
            .success(successOps)
            .failure(failureOps)
            .build();
        
        TxnResponse response = client.transaction(txn);
        
        if (response.isSucceeded()) {
            log.info("[{}] 成功获取互斥锁: {}", instanceId, lockName);
            return true;
        } else {
            String currentOwner = response.getResults().get(0).getValue();
            log.info("[{}] 锁已被占用，持有者: {}", instanceId, currentOwner);
            return false;
        }
    }

    /**
     * 释放互斥锁
     */
    public boolean releaseMutexLock(String lockName) {
        String lockKey = "/locks/mutex/" + lockName;
        
        List<Compare> compares = Arrays.asList(
            Compare.value(lockKey, CompareOp.EQUAL, instanceId)
        );
        List<Operation> successOps = Arrays.asList(
            Operation.delete(lockKey)
        );
        List<Operation> failureOps = Arrays.asList(
            Operation.get(lockKey)
        );
        
        TxnRequest txn = TxnRequest.builder()
            .compares(compares)
            .success(successOps)
            .failure(failureOps)
            .build();
        
        TxnResponse response = client.transaction(txn);
        
        if (response.isSucceeded()) {
            log.info("[{}] 成功释放互斥锁: {}", instanceId, lockName);
            return true;
        } else {
            log.warn("[{}] 释放锁失败，不是锁的持有者", instanceId);
            return false;
        }
    }

    /**
     * 使用互斥锁执行定时任务
     */
    public void executeScheduledTask(String taskName, Runnable task) {
        if (acquireMutexLock(taskName)) {
            try {
                log.info("[{}] 开始执行定时任务: {}", instanceId, taskName);
                task.run();
                log.info("[{}] 定时任务执行完成: {}", instanceId, taskName);
            } finally {
                releaseMutexLock(taskName);
            }
        } else {
            log.info("[{}] 跳过定时任务，未能获取锁: {}", instanceId, taskName);
        }
    }

    // ==================== 2. 读写锁（ReadWrite Lock）====================

    /**
     * 获取读锁
     */
    public boolean acquireReadLock(String resourceName) {
        String readCountKey = "/locks/rw/" + resourceName + "/read-count";
        String writeLockKey = "/locks/rw/" + resourceName + "/write";
        String readerKey = "/locks/rw/" + resourceName + "/readers/" + instanceId;
        
        // 先检查是否有写锁
        KVResponse writeLockResp = client.get(writeLockKey);
        if (writeLockResp.isSuccess() && writeLockResp.getValue() != null) {
            log.info("[{}] 读锁获取失败，有写锁存在", instanceId);
            return false;
        }
        
        // 增加读计数
        KVResponse countResp = client.get(readCountKey);
        int currentCount = (countResp.isSuccess() && countResp.getValue() != null) 
            ? Integer.parseInt(countResp.getValue()) : 0;
        int newCount = currentCount + 1;
        
        List<Compare> compares = Arrays.asList(
            Compare.version(writeLockKey, CompareOp.EQUAL, 0L)
        );
        List<Operation> successOps = Arrays.asList(
            Operation.put(readCountKey, String.valueOf(newCount)),
            Operation.put(readerKey, "active")
        );
        
        TxnRequest txn = TxnRequest.builder()
            .compares(compares)
            .success(successOps)
            .build();
        
        TxnResponse response = client.transaction(txn);
        
        if (response.isSucceeded()) {
            log.info("[{}] 成功获取读锁，当前读者数: {}", instanceId, newCount);
            return true;
        }
        return false;
    }

    /**
     * 释放读锁
     */
    public void releaseReadLock(String resourceName) {
        String readCountKey = "/locks/rw/" + resourceName + "/read-count";
        String readerKey = "/locks/rw/" + resourceName + "/readers/" + instanceId;
        
        KVResponse countResp = client.get(readCountKey);
        int currentCount = (countResp.isSuccess() && countResp.getValue() != null) 
            ? Integer.parseInt(countResp.getValue()) : 0;
        
        if (currentCount > 0) {
            client.put(readCountKey, String.valueOf(currentCount - 1));
        }
        client.delete(readerKey);
        
        log.info("[{}] 释放读锁，当前读者数: {}", instanceId, currentCount - 1);
    }

    /**
     * 获取写锁
     */
    public boolean acquireWriteLock(String resourceName) {
        String readCountKey = "/locks/rw/" + resourceName + "/read-count";
        String writeLockKey = "/locks/rw/" + resourceName + "/write";
        
        List<Compare> compares = Arrays.asList(
            Compare.version(readCountKey, CompareOp.EQUAL, 0L),
            Compare.version(writeLockKey, CompareOp.EQUAL, 0L)
        );
        List<Operation> successOps = Arrays.asList(
            Operation.put(writeLockKey, instanceId)
        );
        List<Operation> failureOps = Arrays.asList(
            Operation.get(readCountKey)
        );
        
        TxnRequest txn = TxnRequest.builder()
            .compares(compares)
            .success(successOps)
            .failure(failureOps)
            .build();
        
        TxnResponse response = client.transaction(txn);
        
        if (response.isSucceeded()) {
            log.info("[{}] 成功获取写锁", instanceId);
            return true;
        } else {
            String readCount = response.getResults().get(0).getValue();
            log.info("[{}] 写锁获取失败，当前读者数: {}", instanceId, readCount);
            return false;
        }
    }

    /**
     * 释放写锁
     */
    public void releaseWriteLock(String resourceName) {
        String writeLockKey = "/locks/rw/" + resourceName + "/write";
        client.delete(writeLockKey);
        log.info("[{}] 释放写锁", instanceId);
    }

    // ==================== 3. 信号量（Semaphore）====================

    /**
     * 初始化信号量
     */
    public void initSemaphore(String name, int maxPermits) {
        String permitsKey = "/semaphore/" + name + "/permits";
        String maxKey = "/semaphore/" + name + "/max";
        
        client.put(maxKey, String.valueOf(maxPermits));
        client.put(permitsKey, String.valueOf(maxPermits));
        
        log.info("初始化信号量 {}，许可数: {}", name, maxPermits);
    }

    /**
     * 获取许可
     */
    public boolean acquirePermit(String name, long timeoutMs) {
        String permitsKey = "/semaphore/" + name + "/permits";
        String holderKey = "/semaphore/" + name + "/holders/" + instanceId;
        
        long deadline = System.currentTimeMillis() + timeoutMs;
        
        while (System.currentTimeMillis() < deadline) {
            KVResponse resp = client.get(permitsKey);
            int permits = (resp.isSuccess() && resp.getValue() != null) 
                ? Integer.parseInt(resp.getValue()) : 0;
            
            if (permits > 0) {
                // 尝试获取许可
                List<Compare> compares = Arrays.asList(
                    Compare.value(permitsKey, CompareOp.EQUAL, String.valueOf(permits))
                );
                List<Operation> successOps = Arrays.asList(
                    Operation.put(permitsKey, String.valueOf(permits - 1)),
                    Operation.put(holderKey, String.valueOf(System.currentTimeMillis()))
                );
                
                TxnRequest txn = TxnRequest.builder()
                    .compares(compares)
                    .success(successOps)
                    .build();
                
                TxnResponse response = client.transaction(txn);
                
                if (response.isSucceeded()) {
                    log.info("[{}] 获取许可成功，剩余: {}", instanceId, permits - 1);
                    return true;
                }
            }
            
            // 等待后重试
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        log.warn("[{}] 获取许可超时", instanceId);
        return false;
    }

    /**
     * 释放许可
     */
    public void releasePermit(String name) {
        String permitsKey = "/semaphore/" + name + "/permits";
        String holderKey = "/semaphore/" + name + "/holders/" + instanceId;
        
        KVResponse resp = client.get(permitsKey);
        int permits = (resp.isSuccess() && resp.getValue() != null) 
            ? Integer.parseInt(resp.getValue()) : 0;
        
        client.put(permitsKey, String.valueOf(permits + 1));
        client.delete(holderKey);
        
        log.info("[{}] 释放许可成功，当前: {}", instanceId, permits + 1);
    }

    // ==================== 4. 领导选举（Leader Election）====================

    private volatile boolean isLeader = false;
    private ScheduledExecutorService leaderRenewalExecutor;

    /**
     * 尝试成为 Leader
     */
    public boolean tryBecomeLeader(String electionName) {
        String leaderKey = "/leader/" + electionName;
        
        List<Compare> compares = Arrays.asList(
            Compare.version(leaderKey, CompareOp.EQUAL, 0L)
        );
        List<Operation> successOps = Arrays.asList(
            Operation.put(leaderKey, instanceId)
        );
        List<Operation> failureOps = Arrays.asList(
            Operation.get(leaderKey)
        );
        
        TxnRequest txn = TxnRequest.builder()
            .compares(compares)
            .success(successOps)
            .failure(failureOps)
            .build();
        
        TxnResponse response = client.transaction(txn);
        
        if (response.isSucceeded()) {
            isLeader = true;
            log.info("[{}] 成功当选 Leader！", instanceId);
            
            // 启动续约任务
            startLeaderRenewal(electionName);
            return true;
        } else {
            String currentLeader = response.getResults().get(0).getValue();
            log.info("[{}] Leader 选举失败，当前 Leader: {}", instanceId, currentLeader);
            return false;
        }
    }

    /**
     * 启动 Leader 续约任务
     */
    private void startLeaderRenewal(String electionName) {
        leaderRenewalExecutor = Executors.newSingleThreadScheduledExecutor();
        leaderRenewalExecutor.scheduleAtFixedRate(() -> {
            if (!isLeader) {
                leaderRenewalExecutor.shutdown();
                return;
            }
            
            String leaderKey = "/leader/" + electionName;
            KVResponse resp = client.get(leaderKey);
            
            if (resp.isSuccess() && instanceId.equals(resp.getValue())) {
                log.debug("[{}] Leader 续约成功", instanceId);
            } else {
                log.warn("[{}] 失去 Leadership", instanceId);
                isLeader = false;
                leaderRenewalExecutor.shutdown();
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * 放弃 Leadership
     */
    public void resign(String electionName) {
        String leaderKey = "/leader/" + electionName;
        
        List<Compare> compares = Arrays.asList(
            Compare.value(leaderKey, CompareOp.EQUAL, instanceId)
        );
        List<Operation> successOps = Arrays.asList(
            Operation.delete(leaderKey)
        );
        
        TxnRequest txn = TxnRequest.builder()
            .compares(compares)
            .success(successOps)
            .build();
        
        TxnResponse response = client.transaction(txn);
        
        if (response.isSucceeded()) {
            isLeader = false;
            if (leaderRenewalExecutor != null) {
                leaderRenewalExecutor.shutdown();
            }
            log.info("[{}] 已放弃 Leadership", instanceId);
        }
    }

    /**
     * 检查是否是 Leader
     */
    public boolean isLeader() {
        return isLeader;
    }

    // ==================== 主方法：演示各种锁的使用 ====================

    public static void main(String[] args) throws Exception {
        // 创建客户端
        RaftKVClient client = RaftKVClient.builder()
            .serverUrls(Arrays.asList(
                "http://localhost:9081",
                "http://localhost:9082",
                "http://localhost:9083"
            ))
            .maxRetries(3)
            .timeoutSeconds(5)
            .build();

        DistributedLockExample lockExample = new DistributedLockExample(client);

        System.out.println("========== 1. 互斥锁演示 ==========");
        demonstrateMutexLock(lockExample);

        System.out.println("\n========== 2. 读写锁演示 ==========");
        demonstrateReadWriteLock(lockExample);

        System.out.println("\n========== 3. 信号量演示 ==========");
        demonstrateSemaphore(lockExample);

        System.out.println("\n========== 4. 领导选举演示 ==========");
        demonstrateLeaderElection(lockExample);

        System.out.println("\n所有演示完成！");
    }

    private static void demonstrateMutexLock(DistributedLockExample lockExample) throws InterruptedException {
        String lockName = "demo-mutex-task";
        
        // 模拟两个实例竞争锁
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        for (int i = 0; i < 2; i++) {
            final int instanceNum = i;
            executor.submit(() -> {
                boolean acquired = lockExample.acquireMutexLock(lockName);
                if (acquired) {
                    try {
                        System.out.println("实例 " + instanceNum + " 获取锁成功，执行业务逻辑...");
                        Thread.sleep(1000);
                        System.out.println("实例 " + instanceNum + " 业务逻辑完成");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lockExample.releaseMutexLock(lockName);
                    }
                } else {
                    System.out.println("实例 " + instanceNum + " 获取锁失败");
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    private static void demonstrateReadWriteLock(DistributedLockExample lockExample) throws InterruptedException {
        String resourceName = "demo-cache";
        
        // 先获取读锁
        boolean readAcquired = lockExample.acquireReadLock(resourceName);
        if (readAcquired) {
            System.out.println("读锁获取成功，读取缓存数据...");
            Thread.sleep(500);
            lockExample.releaseReadLock(resourceName);
        }
        
        // 再获取写锁
        boolean writeAcquired = lockExample.acquireWriteLock(resourceName);
        if (writeAcquired) {
            System.out.println("写锁获取成功，刷新缓存...");
            Thread.sleep(500);
            lockExample.releaseWriteLock(resourceName);
        }
    }

    private static void demonstrateSemaphore(DistributedLockExample lockExample) throws InterruptedException {
        String semaphoreName = "demo-resource";
        
        // 初始化信号量（3个许可）
        lockExample.initSemaphore(semaphoreName, 3);
        
        // 模拟5个线程竞争3个许可
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(5);
        
        for (int i = 0; i < 5; i++) {
            final int threadNum = i;
            executor.submit(() -> {
                boolean acquired = lockExample.acquirePermit(semaphoreName, 5000);
                if (acquired) {
                    try {
                        System.out.println("线程 " + threadNum + " 获取许可，使用资源...");
                        Thread.sleep(1000);
                        System.out.println("线程 " + threadNum + " 释放资源");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lockExample.releasePermit(semaphoreName);
                    }
                } else {
                    System.out.println("线程 " + threadNum + " 获取许可超时");
                }
                latch.countDown();
            });
        }
        
        latch.await();
        executor.shutdown();
    }

    private static void demonstrateLeaderElection(DistributedLockExample lockExample) throws InterruptedException {
        String electionName = "demo-scheduler";
        
        // 尝试成为 Leader
        boolean becameLeader = lockExample.tryBecomeLeader(electionName);
        
        if (becameLeader) {
            System.out.println("成功成为 Leader，执行 Leader 任务...");
            Thread.sleep(2000);
            lockExample.resign(electionName);
        } else {
            System.out.println("未能成为 Leader，作为 Follower 运行");
        }
    }
}
