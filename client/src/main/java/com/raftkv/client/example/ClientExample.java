package com.raftkv.client.example;

import com.raftkv.client.RaftKVClient;
import com.raftkv.client.entity.KVResponse;

import java.util.Arrays;
import java.util.Map;

/**
 * Raft KV Client 使用示例
 */
public class ClientExample {

    public static void main(String[] args) {
        // 1. 创建客户端
        // 注意：客户端超时时间必须大于服务端 write-timeout（5000ms）
        // 设置为 8 秒，给网络传输和 Raft 日志复制留出足够余量
        RaftKVClient client = RaftKVClient.builder()
                .serverUrls(Arrays.asList(
                        "http://127.0.0.1:9081",
                        "http://127.0.0.1:9082",
                        "http://127.0.0.1:9083"
                ))
                .maxRetries(3)
                .timeoutSeconds(8)
                .build();

        System.out.println("=== Raft KV Client 示例 ===\n");

        try {
            // 2. 健康检查
            System.out.println("[1] 健康检查");
            boolean healthy = client.healthCheck();
            System.out.println("服务状态: " + (healthy ? "健康 ✅" : "不健康 ❌"));
            System.out.println();

            // 3. PUT 操作（自动生成 requestId）
            System.out.println("[2] PUT 操作（自动生成 requestId）");
            KVResponse putResponse1 = client.put("name", "Alice");
            System.out.println("响应: " + putResponse1);
            System.out.println();

            // 4. PUT 操作（指定 requestId，支持幂等）
            System.out.println("[3] PUT 操作（指定 requestId）");
            String requestId = "req-user-001";
            KVResponse putResponse2 = client.put("age", "25", requestId);
            System.out.println("响应: " + putResponse2);
            System.out.println();

            // 5. 重复 PUT（幂等性测试）
            System.out.println("[4] 重复 PUT（幂等性测试）");
            KVResponse putResponse3 = client.put("age", "25", requestId);
            System.out.println("响应: " + putResponse3);
            System.out.println("RequestId 相同: " + requestId.equals(putResponse3.getRequestId()));
            System.out.println();

            // 6. GET 操作
            System.out.println("[5] GET 操作");
            KVResponse getResponse = client.get("name");
            System.out.println("name = " + getResponse.getValue());
            System.out.println();

            // 7. GET 操作（不存在的 key）
            System.out.println("[6] GET 操作（不存在的 key）");
            KVResponse getResponse2 = client.get("email");
            System.out.println("email = " + getResponse2.getValue());
            System.out.println();

            // 8. DELETE 操作
            System.out.println("[7] DELETE 操作");
            KVResponse deleteResponse = client.delete("name");
            System.out.println("响应: " + deleteResponse);
            System.out.println();

            // 9. 验证删除
            System.out.println("[8] 验证删除");
            KVResponse getResponse3 = client.get("name");
            System.out.println("name = " + getResponse3.getValue());
            System.out.println();

            // 10. 批量操作
            System.out.println("[9] 批量操作");
            String[] keys = {"key1", "key2", "key3"};
            for (String key : keys) {
                client.put(key, "value-" + key);
                System.out.println("PUT " + key + " = value-" + key);
            }
            System.out.println();

            // 11. 读取批量数据
            System.out.println("[10] 读取批量数据");
            for (String key : keys) {
                KVResponse response = client.get(key);
                System.out.println(key + " = " + response.getValue());
            }
            System.out.println();

            // 12. GET ALL 操作（获取所有键值对）
            System.out.println("[11] GET ALL 操作（获取所有键值对）");
            System.out.println("说明：getAll 使用 ReadIndex 保证线性一致性");
            System.out.println("      只有 Leader 能处理此请求，Follower 会自动重定向到 Leader");
            Map<String, String> allData = client.getAll();
            System.out.println("所有数据:");
            if (allData != null && !allData.isEmpty()) {
                allData.forEach((k, v) -> System.out.println("  " + k + " = " + v));
            } else {
                System.out.println("  (空)");
            }
            System.out.println("总计: " + (allData != null ? allData.size() : 0) + " 条记录");
            System.out.println();

            // 13. 获取集群统计信息
            System.out.println("[12] 集群统计信息");
            String stats = client.getStats();
            System.out.println(stats);
            System.out.println();

            System.out.println("=== 示例完成 ===");

        } catch (Exception e) {
            System.err.println("错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
