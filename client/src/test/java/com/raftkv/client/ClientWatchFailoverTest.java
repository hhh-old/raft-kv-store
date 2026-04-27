package com.raftkv.client;

import com.raftkv.entity.WatchEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 客户端 Watch 自动重连与 Leader 故障转移集成测试
 *
 * <p>测试覆盖的客户端能力：</p>
 * <ol>
 *   <li><b>自动重连</b>：SSE 连接断开后，客户端进入指数退避重试循环</li>
 *   <li><b>断线续传</b>：重连时使用 {@code startRevision = lastReceivedRevision + 1}，
 *       确保不重复、不遗漏断线期间的事件</li>
 *   <li><b>跨节点故障转移</b>：连接失败时自动尝试其他可用节点，
 *       包括收到 NOT_LEADER 时自动更新 Leader 地址</li>
 *   <li><b>实时推送恢复</b>：重连成功后能继续接收新事件</li>
 * </ol>
 *
 * <p><b>运行前提：</b>3 节点集群已在外部启动（9081/9082/9083）</p>
 */
public class ClientWatchFailoverTest {

    private static final Logger LOG = LoggerFactory.getLogger(ClientWatchFailoverTest.class);

    private static final List<String> ENDPOINTS = Arrays.asList(
            "http://127.0.0.1:9081",
            "http://127.0.0.1:9082",
            "http://127.0.0.1:9083"
    );

    /**
     * 端点 → Spring Profile 映射，用于定位进程
     */
    private static final java.util.Map<String, String> ENDPOINT_TO_PROFILE = java.util.Map.of(
            "http://127.0.0.1:9081", "node1",
            "http://127.0.0.1:9082", "node2",
            "http://127.0.0.1:9083", "node3"
    );

    @Test
    public void testClientWatchAutoReconnectPreservesHistory() throws Exception {
        // ========== 阶段 1: 创建客户端 ==========
        RaftKVClient client = RaftKVClient.builder()
                .serverUrls(ENDPOINTS)
                .timeoutSeconds(5)
                .maxRetries(5)
                .build();
        LOG.info("Client created with endpoints: {}", ENDPOINTS);

        // ========== 阶段 2: 确认 Leader ==========
        String leader = waitForLeader(client);
        LOG.info("Current leader: {}", leader);

        String testKey = "client-failover-" + System.currentTimeMillis();
        LOG.info("Test key: {}", testKey);

        // ========== 阶段 3: PUT 初始数据（revision 1~3） ==========
        client.put(testKey, "value1");
        client.put(testKey, "value2");
        client.put(testKey, "value3");
        LOG.info("PUT 3 values completed");

        // ========== 阶段 4: 客户端建立 Watch（startRevision=1）==========
        List<WatchEvent> allEvents = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger eventCounter = new AtomicInteger(0);

        RaftKVClient.WatchListener listener = client.watchFromRevision(testKey, 1, event -> {
            LOG.info("[EVENT] type={}, key={}, revision={}, value={}",
                    event.getType(), event.getKey(), event.getRevision(), event.getValue());
            allEvents.add(event);
            eventCounter.incrementAndGet();
        });

        // 等待客户端收到初始历史事件（value1~value3）
        Thread.sleep(3000);
        int eventsBeforeKill = eventCounter.get();
        LOG.info("Events received before killing leader: {}", eventsBeforeKill);
        assertTrue(eventsBeforeKill >= 3,
                "Client should receive at least 3 historical events before failover. Got: " + eventsBeforeKill);

        // ========== 阶段 5: 杀掉 Leader ==========
        LOG.info("Killing leader {}...", leader);
        killLeader(leader);

        // ========== 阶段 6: 等待客户端自动重连 + 新 Leader 选举 ==========
        // 15s Raft 选举超时 + 客户端指数退避（1s → 2s → 4s...）
        LOG.info("Waiting for new leader election and client auto-reconnect...");
        Thread.sleep(20000);

        String newLeader = waitForLeader(client);
        LOG.info("New leader elected: {}", newLeader);
        assertNotEquals(leader, newLeader,
                "Leader should have changed. Old=" + leader + ", New=" + newLeader);

        // ========== 阶段 7: 在新 Leader 上 PUT 新数据 ==========
        LOG.info("PUT value4 and value5 on new leader...");
        client.put(testKey, "value4");
        Thread.sleep(500);
        client.put(testKey, "value5");
        Thread.sleep(2000);

        // ========== 阶段 8: 验证 ==========
        int totalEvents = eventCounter.get();
        LOG.info("Total events received by client: {}", totalEvents);

        // 打印收到的事件序列，便于调试
        synchronized (allEvents) {
            for (int i = 0; i < allEvents.size(); i++) {
                WatchEvent e = allEvents.get(i);
                LOG.info("Event[{}]: revision={}, value={}", i, e.getRevision(), e.getValue());
            }
        }

        // 核心断言：
        // - 初始收到了 value1~value3（>=3 个事件）
        // - 重连后应该收到 value4 和 value5（断线续传 + 实时推送）
        // - 因此总数应 >= 5
        //
        // 如果总数 < 5，说明断线续传机制有问题（可能丢失了 value4）
        assertTrue(totalEvents >= 5,
                "Client auto-reconnect should preserve all events through failover. " +
                        "Expected >=5 (value1~value5), got " + totalEvents);

        // 精确验证：确保 value5 被收到（证明重连后的实时推送正常）
        boolean hasValue5 = allEvents.stream()
                .anyMatch(e -> "value5".equals(e.getValue()));
        assertTrue(hasValue5, "Client should receive 'value5' after auto-reconnect");

        // 验证事件按 revision 单调递增（无乱序、无重复）
        long lastRevision = 0;
        for (WatchEvent e : allEvents) {
            assertTrue(e.getRevision() >= lastRevision,
                    "Events should be ordered by revision. Found " + e.getRevision() + " after " + lastRevision);
            lastRevision = e.getRevision();
        }

        // ========== 阶段 9: 清理 ==========
        client.cancelWatch(listener);
        LOG.info("Test PASSED: Client auto-reconnect preserved {} events through leader failover", totalEvents);
    }

    // ==================== 辅助方法 ====================

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
        boolean finished = process.waitFor(5, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }

        String output = new String(process.getInputStream().readAllBytes());
        if (!output.isBlank()) {
            LOG.info("Kill process output: {}", output.trim());
        }
    }
}
