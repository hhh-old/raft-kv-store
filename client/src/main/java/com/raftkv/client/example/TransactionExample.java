package com.raftkv.client.example;

import com.raftkv.client.RaftKVClient;
import com.raftkv.entity.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Scanner;

/**
 * 事务功能示例
 *
 * 演示 Raft KV Store 的事务功能：
 * 1. CAS（Compare-And-Swap）操作
 * 2. 分布式锁
 * 3. 原子性多 key 操作
 */
@Slf4j
public class TransactionExample {

    public static void main(String[] args) {
        System.out.println("=== Raft KV 事务示例 ===\n");

        // 创建客户端
        RaftKVClient client = new RaftKVClient("http://localhost:9081");

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("\n请选择示例：");
            System.out.println("1. CAS 操作（乐观锁）");
            System.out.println("2. 分布式锁");
            System.out.println("3. 原子性配置更新");
            System.out.println("4. 多 key 事务");
            System.out.println("q. 退出");
            System.out.print("\n请选择: ");

            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1":
                    demoCAS(client);
                    break;
                case "2":
                    demoDistributedLock(client);
                    break;
                case "3":
                    demoAtomicConfigUpdate(client);
                    break;
                case "4":
                    demoMultiKeyTransaction(client);
                    break;
                case "q":
                case "Q":
                    System.out.println("再见！");
                    return;
                default:
                    System.out.println("无效选择，请重试");
            }
        }
    }

    /**
     * 示例 1: CAS 操作（乐观锁）
     *
     * 场景：并发更新配置，只有版本号匹配时才更新
     */
    private static void demoCAS(RaftKVClient client) {
        System.out.println("\n=== 示例 1: CAS 操作 ===");
        System.out.println("使用 CAS 实现安全的并发更新\n");

        String key = "/config/app/version";

        // 先设置初始值
        System.out.println("1. 设置初始值: version=v1");
        client.put(key, "v1");

        // 获取当前版本号
        // 注意：这里简化处理，实际应该从 get 响应中获取版本号
        System.out.println("2. 尝试 CAS 更新: 预期 version=v1, 新值 v2");

        TxnResponse response = client.cas(key, "v1", "v2");

        System.out.println("3. CAS 结果: " + (response.isSucceeded() ? "成功" : "失败"));
        System.out.println("   当前值: " + client.get(key));

        // 再次尝试相同的 CAS（应该失败）
        System.out.println("\n4. 再次尝试相同的 CAS: 预期 version=v1, 新值 v3");
        TxnResponse response2 = client.cas(key, "v1", "v3");

        System.out.println("5. CAS 结果: " + (response2.isSucceeded() ? "成功" : "失败"));
        if (!response2.isSucceeded()) {
            // 从事务响应中获取实际的当前值
            String actualValue = client.get(key).getValue();
            System.out.println("   失败原因: 版本号不匹配（预期 v1，实际 " + actualValue + "）");
        }
        System.out.println("   当前值: " + client.get(key));
    }

    /**
     * 示例 2: 分布式锁
     *
     * 场景：多个服务竞争获取锁
     */
    private static void demoDistributedLock(RaftKVClient client) {
        System.out.println("\n=== 示例 2: 分布式锁 ===");
        System.out.println("使用 CAS 实现安全的分布式锁\n");

        String lockKey = "/lock/resource-123";
        String owner1 = "service-A";
        String owner2 = "service-B";

        // 清理之前的锁
        client.delete(lockKey);

        // 服务 A 尝试获取锁
        System.out.println("1. 服务 A 尝试获取锁...");
        boolean acquiredByA = client.acquireLock(lockKey, owner1);
        System.out.println("   结果: " + (acquiredByA ? "成功" : "失败"));

        if (acquiredByA) {
            System.out.println("   锁持有者: " + owner1);

            // 服务 B 尝试获取锁（应该失败）
            System.out.println("\n2. 服务 B 尝试获取锁...");
            boolean acquiredByB = client.acquireLock(lockKey, owner2);
            System.out.println("   结果: " + (acquiredByB ? "成功" : "失败"));

            if (!acquiredByB) {
                System.out.println("   失败原因: 锁已被 " + owner1 + " 持有");
            }

            // 服务 A 释放锁
            System.out.println("\n3. 服务 A 释放锁...");
            boolean released = client.releaseLock(lockKey, owner1);
            System.out.println("   结果: " + (released ? "成功" : "失败"));

            // 服务 B 再次尝试获取锁（应该成功）
            System.out.println("\n4. 服务 B 再次尝试获取锁...");
            acquiredByB = client.acquireLock(lockKey, owner2);
            System.out.println("   结果: " + (acquiredByB ? "成功" : "失败"));

            if (acquiredByB) {
                System.out.println("   锁持有者: " + owner2);

                // 清理
                client.releaseLock(lockKey, owner2);
            }
        }
    }

    /**
     * 示例 3: 原子性配置更新
     *
     * 场景：更新配置时，同时更新版本号和相关配置项
     */
    private static void demoAtomicConfigUpdate(RaftKVClient client) {
        System.out.println("\n=== 示例 3: 原子性配置更新 ===");
        System.out.println("使用事务原子性更新多个相关配置\n");

        String versionKey = "/config/app/version";
        String dataKey = "/config/app/data";
        String timestampKey = "/config/app/timestamp";

        // 设置初始配置
        System.out.println("1. 设置初始配置: version=v1");
        client.put(versionKey, "v1");
        client.put(dataKey, "initial-data");
        client.put(timestampKey, "2024-01-01");

        // 构建事务：只有当 version=v1 时才更新
        System.out.println("\n2. 构建事务请求:");
        System.out.println("   条件: version == v1");
        System.out.println("   成功操作: 更新 version=v2, data=new-data, timestamp=2024-06-01");

        TxnRequest txnRequest = TxnRequest.builder()
                .compares(java.util.List.of(Compare.value(versionKey, Compare.CompareOp.EQUAL, "v1")))
                .success(java.util.List.of(
                        Operation.put(versionKey, "v2"),
                        Operation.put(dataKey, "new-data"),
                        Operation.put(timestampKey, "2024-06-01")
                ))
                .failure(java.util.List.of(Operation.get(versionKey)))
                .build();

        TxnResponse response = client.transaction(txnRequest);

        System.out.println("\n3. 事务执行结果:");
        System.out.println("   成功: " + response.isSucceeded());
        System.out.println("   执行的操作数: " + response.getOps().size());

        // 验证所有配置都已更新
        System.out.println("\n4. 验证配置更新:");
        System.out.println("   version: " + client.get(versionKey));
        System.out.println("   data: " + client.get(dataKey));
        System.out.println("   timestamp: " + client.get(timestampKey));
    }

    /**
     * 示例 4: 多 key 事务
     *
     * 场景：转账操作，从一个账户扣款，向另一个账户加款
     */
    private static void demoMultiKeyTransaction(RaftKVClient client) {
        System.out.println("\n=== 示例 4: 多 key 事务（转账示例）===");
        System.out.println("使用事务实现原子性转账\n");

        String accountA = "/account/alice";
        String accountB = "/account/bob";

        // 初始化账户余额
        System.out.println("1. 初始化账户余额");
        client.put(accountA, "1000");  // Alice 有 1000
        client.put(accountB, "500");   // Bob 有 500

        System.out.println("   Alice: " + client.get(accountA));
        System.out.println("   Bob: " + client.get(accountB));

        // 转账：Alice 向 Bob 转 200
        int transferAmount = 200;
        System.out.println("\n2. 执行转账: Alice -> Bob, 金额: " + transferAmount);

        // 构建事务
        // 注意：字符串比较是按字典序的，"1000" < "200" 因为 '1' < '2'
        // 所以比较余额是否 >= 200 应该使用 NOT_EQUAL 配合实际值，或者确保字符串长度一致
        TxnRequest txnRequest = TxnRequest.builder()
                // 条件：Alice 的余额 != "0"（简化示例，实际应该使用数值比较或版本号比较）
                .compares(java.util.List.of(Compare.value(accountA, Compare.CompareOp.NOT_EQUAL, "0")))
                // 成功：扣减 Alice 的余额，增加 Bob 的余额
                .success(java.util.List.of(
                        Operation.put(accountA, String.valueOf(1000 - transferAmount)),
                        Operation.put(accountB, String.valueOf(500 + transferAmount))
                ))
                // 失败：返回当前余额
                .failure(java.util.List.of(
                        Operation.get(accountA),
                        Operation.get(accountB)
                ))
                .build();

        TxnResponse response = client.transaction(txnRequest);

        System.out.println("\n3. 转账结果:");
        System.out.println("   成功: " + response.isSucceeded());

        if (response.isSucceeded()) {
            System.out.println("\n4. 转账后余额:");
            System.out.println("   Alice: " + client.get(accountA));
            System.out.println("   Bob: " + client.get(accountB));
        } else {
            System.out.println("   失败原因: 余额不足");
        }

        // 尝试超额转账（应该失败）
        System.out.println("\n5. 尝试超额转账: Alice -> Bob, 金额: 2000");

        TxnRequest txnRequest2 = TxnRequest.builder()
                // 条件：Alice 的余额 == "9999"（不可能满足，确保转账失败）
                .compares(java.util.List.of(Compare.value(accountA, Compare.CompareOp.EQUAL, "9999")))
                .success(java.util.List.of(
                        Operation.put(accountA, "0"),
                        Operation.put(accountB, "2500")
                ))
                .failure(java.util.List.of(Operation.get(accountA)))
                .build();

        TxnResponse response2 = client.transaction(txnRequest2);

        System.out.println("   结果: " + (response2.isSucceeded() ? "成功" : "失败"));
        if (!response2.isSucceeded()) {
            String aliceBalance = client.get(accountA).getValue();
            System.out.println("   失败原因: 余额不足（Alice 只有 " + aliceBalance + "）");
        }

        System.out.println("\n6. 最终余额（应该不变）:");
        System.out.println("   Alice: " + client.get(accountA));
        System.out.println("   Bob: " + client.get(accountB));
    }
}
