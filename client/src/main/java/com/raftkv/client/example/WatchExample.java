package com.raftkv.client.example;

import com.raftkv.client.RaftKVClient;
import com.raftkv.entity.WatchEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Watch 机制使用示例
 * 
 * 演示如何使用 RaftKVClient 的 Watch 功能：
 * 1. 监听单个 Key 的变化
 * 2. 监听前缀（目录）的变化
 * 3. 从历史版本恢复监听（断线重连）
 * 4. 取消监听
 * 
 * 运行方式：
 * 1. 先启动三个 Raft 节点
 * 2. 运行此示例程序
 * 3. 使用 curl 或其他客户端修改 Key，观察事件推送
 * 
 * 示例命令：
 * curl -X PUT http://localhost:9081/kv/config/db -H "Content-Type: application/json" -d '{"value":"localhost:3306"}'
 * curl -X PUT http://localhost:9081/kv/config/cache -H "Content-Type: application/json" -d '{"value":"redis:6379"}'
 * curl -X DELETE http://localhost:9081/kv/config/db
 */
@Slf4j
public class WatchExample {

    public static void main(String[] args) throws Exception {
        // 创建客户端
        RaftKVClient client = RaftKVClient.builder()
                .serverUrls(Arrays.asList(
                        "http://localhost:9081",
                        "http://localhost:9082",
                        "http://localhost:9083"
                ))
                .maxRetries(3)
                .timeoutSeconds(8)
                .build();

        System.out.println("=== Raft KV Watch 示例 ===\n");
        System.out.println("1. 监听单个 Key");
        System.out.println("2. 监听前缀（目录）");
        System.out.println("3. 断线重连演示");
        System.out.println("4. 综合演示");
        System.out.println("q. 退出");
        System.out.print("\n请选择: ");

        Scanner scanner = new Scanner(System.in);
        String choice = scanner.nextLine().trim();

        switch (choice) {
            case "1":
                demoSingleKeyWatch(client);
                break;
            case "2":
                demoPrefixWatch(client);
                break;
            case "3":
                demoReconnect(client);
                break;
            case "4":
                demoComprehensive(client);
                break;
            case "q":
                System.out.println("退出");
                return;
            default:
                System.out.println("无效选择");
        }

        // 等待用户按回车退出
        System.out.println("\n按回车键退出...");
        scanner.nextLine();
        
        // 关闭所有 Watch
        client.closeAllWatches();
        System.out.println("已关闭所有 Watch");
    }

    /**
     * 示例 1: 监听单个 Key
     * 
     * 演示如何监听一个具体的 Key，当该 Key 被修改或删除时收到通知
     */
    private static void demoSingleKeyWatch(RaftKVClient client) throws Exception {
        System.out.println("\n=== 示例 1: 监听单个 Key ===");
        String key = "/config/database/url";
        System.out.println("正在监听 Key: " + key);
        System.out.println("提示: 使用 curl 修改这个 key，观察事件推送\n");
        System.out.println("curl -X PUT http://localhost:9081/kv/config/database/url \\\n");
        System.out.println("  -H \"Content-Type: application/json\" \\\n");
        System.out.println("  -d '{\"value\":\"mysql://localhost:3306\"}'\n");

        // 创建 Watch
        RaftKVClient.WatchListener listener = client.watch(key, event -> {
            System.out.println("\n[收到事件]");
            System.out.println("  类型: " + event.getType());
            System.out.println("  Key: " + event.getKey());
            System.out.println("  值: " + event.getValue());
            System.out.println("  全局版本: " + event.getRevision());
            System.out.println("  Key版本: " + event.getVersion());
            
            if (event.isCreate()) {
                System.out.println("  [这是新创建的 Key]");
            } else if (event.isModify()) {
                System.out.println("  [这是修改操作]");
            }
        });

        System.out.println("Watch 创建成功，ID: " + listener.getWatchId());
        System.out.println("等待事件...（按回车取消监听）\n");
        
        new Scanner(System.in).nextLine();
        client.cancelWatch(listener);
        System.out.println("已取消监听");
    }

    /**
     * 示例 2: 监听前缀（目录）
     * 
     * 演示如何监听一个前缀下的所有 Key 变化，类似于监听一个目录
     */
    private static void demoPrefixWatch(RaftKVClient client) throws Exception {
        System.out.println("\n=== 示例 2: 监听前缀（目录） ===");
        String prefix = "/config/";
        System.out.println("正在监听前缀: " + prefix);
        System.out.println("所有以 '" + prefix + "' 开头的 Key 变化都会收到通知\n");
        System.out.println("提示: 尝试创建以下 key:");
        System.out.println("  /config/database/url");
        System.out.println("  /config/redis/host");
        System.out.println("  /config/app/name\n");

        // 创建前缀 Watch
        RaftKVClient.WatchListener listener = client.watchPrefix(prefix, event -> {
            System.out.println("\n[前缀监听事件]");
            System.out.println("  类型: " + event.getType());
            System.out.println("  Key: " + event.getKey());
            System.out.println("  值: " + event.getValue());
            System.out.println("  全局版本: " + event.getRevision());
        });

        System.out.println("前缀 Watch 创建成功，ID: " + listener.getWatchId());
        System.out.println("等待事件...（按回车取消监听）\n");
        
        new Scanner(System.in).nextLine();
        client.cancelWatch(listener);
        System.out.println("已取消监听");
    }

    /**
     * 示例 3: 断线重连演示
     * 
     * 演示如何在断线后从历史版本恢复监听，确保不丢失事件
     */
    private static void demoReconnect(RaftKVClient client) throws Exception {
        System.out.println("\n=== 示例 3: 断线重连演示 ===");
        String key = "/config/app/title";
        
        // 获取当前版本号
        long currentRevision = client.getCurrentRevision();
        System.out.println("当前全局版本号: " + currentRevision);
        
        // 先创建一些数据
        System.out.println("\n先创建一些测试数据...");
        client.put(key, "My App v1");
        TimeUnit.MILLISECONDS.sleep(100);
        client.put(key, "My App v2");
        TimeUnit.MILLISECONDS.sleep(100);
        client.put(key, "My App v3");
        
        System.out.println("已创建 3 个版本");
        
        // 模拟断线重连：从版本 1 开始监听（应该能收到所有历史事件）
        long startRevision = currentRevision + 1;
        System.out.println("\n模拟断线重连，从历史版本 " + startRevision + " 开始监听...");
        System.out.println("应该能收到 3 个历史事件\n");

        RaftKVClient.WatchListener listener = client.watchFromRevision(key, startRevision, event -> {
            System.out.println("\n[历史/实时事件]");
            System.out.println("  类型: " + event.getType());
            System.out.println("  Key: " + event.getKey());
            System.out.println("  值: " + event.getValue());
            System.out.println("  全局版本: " + event.getRevision());
        });

        System.out.println("Watch 创建成功，ID: " + listener.getWatchId());
        System.out.println("等待事件...（按回车取消监听）\n");
        
        new Scanner(System.in).nextLine();
        client.cancelWatch(listener);
        System.out.println("已取消监听");
        
        // 清理
        client.delete(key);
    }

    /**
     * 示例 4: 综合演示
     * 
     * 同时创建多个 Watch，展示系统的高并发处理能力
     */
    private static void demoComprehensive(RaftKVClient client) throws Exception {
        System.out.println("\n=== 示例 4: 综合演示 ===");
        System.out.println("同时创建多个 Watch，观察事件分发\n");

        // Watch 1: 监听 /config/database/url
        RaftKVClient.WatchListener watch1 = client.watch("/config/database/url", event -> {
            System.out.println("[Watch-1 数据库配置] " + event.getKey() + " = " + event.getValue());
        });

        // Watch 2: 监听 /config/redis/host
        RaftKVClient.WatchListener watch2 = client.watch("/config/redis/host", event -> {
            System.out.println("[Watch-2 Redis配置] " + event.getKey() + " = " + event.getValue());
        });

        // Watch 3: 监听前缀 /config/（会收到所有配置变化）
        RaftKVClient.WatchListener watch3 = client.watchPrefix("/config/", event -> {
            System.out.println("[Watch-3 所有配置] " + event.getType() + " " + event.getKey());
        });

        System.out.println("已创建 3 个 Watch:");
        System.out.println("  1. " + watch1.getWatchId() + " - /config/database/url");
        System.out.println("  2. " + watch2.getWatchId() + " - /config/redis/host");
        System.out.println("  3. " + watch3.getWatchId() + " - /config/* (前缀)");
        
        System.out.println("\n提示: 尝试修改以下 key，观察多个 Watch 同时收到事件:");
        System.out.println("  /config/database/url");
        System.out.println("  /config/redis/host");
        System.out.println("  /config/app/name");
        System.out.println("\n注意: /config/app/name 只会触发 Watch-3（前缀匹配）\n");
        
        System.out.println("等待事件...（按回车取消所有监听）\n");
        new Scanner(System.in).nextLine();
        
        client.cancelWatch(watch1);
        client.cancelWatch(watch2);
        client.cancelWatch(watch3);
        System.out.println("已取消所有监听");
    }
}
