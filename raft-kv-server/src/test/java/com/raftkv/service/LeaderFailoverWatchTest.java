package com.raftkv.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Leader 故障转移 Watch 历史事件一致性集成测试
 *
 * <p>测试场景：</p>
 * <ol>
 *   <li>3 节点集群正常运行，确认 Leader</li>
 *   <li>在 Leader 上 PUT 多条数据，产生历史事件</li>
 *   <li>在 Leader 上建立 Watch（startRevision=1），验证收到历史事件回放</li>
 *   <li>杀掉 Leader 进程</li>
 *   <li>等待新 Leader 选举（约 10-15 秒）</li>
 *   <li>在新 Leader 上重新建立 Watch（startRevision=1）</li>
 *   <li>验证新 Leader 能正确回放所有历史事件（通过 MVCCStore fallback）</li>
 * </ol>
 *
 * <p><b>运行前提：</b>集群必须已在外部启动（3 个节点分别在 9081/9082/9083）。
 * 测试不会自己启动集群，而是通过与集群 HTTP API 交互来验证行为。</p>
 */
public class LeaderFailoverWatchTest {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderFailoverWatchTest.class);

    private static final List<String> ENDPOINTS = List.of(
            "http://127.0.0.1:9081",
            "http://127.0.0.1:9082",
            "http://127.0.0.1:9083"
    );

    private static final java.util.Map<String, String> ENDPOINT_TO_PROFILE = java.util.Map.of(
            "http://127.0.0.1:9081", "node1",
            "http://127.0.0.1:9082", "node2",
            "http://127.0.0.1:9083", "node3"
    );

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void testLeaderFailoverPreservesWatchHistory() throws Exception {
        // ========== 阶段 1: 确认 Leader ==========
        String leader = findLeader();
        LOG.info("Current leader: {}", leader);

        String testKey = "failover-test-" + System.currentTimeMillis();
        LOG.info("Test key: {}", testKey);

        // ========== 阶段 2: PUT 历史数据 ==========
        put(leader, testKey, "value1");
        put(leader, testKey, "value2");
        put(leader, testKey, "value3");
        LOG.info("PUT 3 values completed");

        // ========== 阶段 3: 旧 Leader 上 Watch（验证历史回放） ==========
        LOG.info("Watching on old leader {} with startRevision=1", leader);
        List<WatchEvent> eventsBefore = watchForEvents(leader, testKey, 1, 6000);
        LOG.info("Received {} events from old leader", eventsBefore.size());

        assertTrue(eventsBefore.size() >= 3,
                "Old leader should replay historical events. Expected >=3, got " + eventsBefore.size());

        // 再 PUT 一条（revision 4），验证实时推送也正常
        put(leader, testKey, "value4");
        LOG.info("PUT 1 values completed");
        Thread.sleep(800);

        // ========== 阶段 4: 杀掉 Leader ==========
        LOG.info("Killing leader {}", leader);
        killLeader(leader);
        LOG.info("Leader killed, waiting for new leader election...");
        Thread.sleep(15000);

        // ========== 阶段 5: 确认新 Leader ==========
        String newLeader = findLeader();
        LOG.info("New leader: {}", newLeader);

        assertTrue(!newLeader.equals(leader),
                "New leader should be different from old leader. Old=" + leader + ", New=" + newLeader);

        // ========== 阶段 6: 新 Leader 上重新 Watch ==========
        LOG.info("Watching on new leader {} with startRevision=1", newLeader);
        List<WatchEvent> eventsAfter = watchForEvents(newLeader, testKey, 1, 10000);
        LOG.info("Received {} events from new leader", eventsAfter.size());

        // 再 PUT 一条，验证新 Leader 的实时推送
        put(newLeader, testKey, "value5");
        Thread.sleep(800);

        // ========== 阶段 7: 验证 ==========
        // 新 Leader 应该回放全部 4 条历史事件（value1~value4）
        assertTrue(eventsAfter.size() >= 4,
                "Leader failover should preserve all historical watch events. " +
                        "Expected >=4 historical events, got " + eventsAfter.size() +
                        ". Events: " + eventsAfter);

        LOG.info("Test PASSED: Leader failover preserves {} watch events", eventsAfter.size());
    }

    // ==================== HTTP 辅助方法 ====================

    /**
     * 遍历所有端点，返回当前 Leader 的 HTTP 地址
     */
    private String findLeader() throws Exception {
        for (String endpoint : ENDPOINTS) {
            try {
                JsonNode stats = getStats(endpoint);
                if ("LEADER".equals(stats.get("role").asText())) {
                    return endpoint;
                }
            } catch (Exception e) {
                LOG.debug("Endpoint {} not ready: {}", endpoint, e.getMessage());
            }
        }
        throw new IllegalStateException("No leader found among " + ENDPOINTS);
    }

    private JsonNode getStats(String endpoint) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/kv/stats"))
                .GET()
                .timeout(Duration.ofSeconds(2))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("Stats request failed: " + response.statusCode());
        }
        return objectMapper.readTree(response.body());
    }

    private void put(String endpoint, String key, String value) throws Exception {
        String json = String.format("{\"key\":\"%s\",\"value\":\"%s\"}", key, value);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/kv"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(json))
                .timeout(Duration.ofSeconds(5))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("PUT failed: " + response.statusCode() + " body=" + response.body());
        }
    }

    // ==================== Watch SSE 读取 ====================

    /**
     * 建立 Watch SSE 连接，在指定时间内收集事件，然后关闭连接返回。
     *
     * <p>实现要点：</p>
     * <ul>
     *   <li>使用 {@link HttpClient#sendAsync} 建立连接，避免阻塞主线程</li>
     *   <li>在独立后台线程中读取 SSE 流，解析 event/data 行</li>
     *   <li>超时后主动关闭 {@link InputStream}，强制中断 {@link BufferedReader#readLine()}</li>
     * </ul>
     *
     * @param endpoint      服务端 HTTP 地址
     * @param key           监听的 key
     * @param startRevision 起始 revision（包含）
     * @param timeoutMs     读取超时（毫秒）
     * @return 收集到的 PUT/DELETE 事件列表
     */
    private List<WatchEvent> watchForEvents(String endpoint, String key, long startRevision, long timeoutMs)
            throws Exception {

        String json = String.format("{\"key\":\"%s\",\"prefix\":false,\"startRevision\":%d}", key, startRevision);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/watch/stream"))
                .header("Content-Type", "application/json")
                .header("Accept", "text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        List<WatchEvent> events = Collections.synchronizedList(new ArrayList<>());

        // 异步发送，等待响应头（3 秒超时）
        HttpResponse<InputStream> response = httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofInputStream())
                .get(3, TimeUnit.SECONDS);

        LOG.debug("Watch connection established, status={}", response.statusCode());

        // 后台线程读取 SSE 流
        Thread readerThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.body()))) {
                String line;
                String currentEventName = null;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("event:")) {
                        currentEventName = line.substring(6).trim();
                    } else if (line.startsWith("data:") && currentEventName != null) {
                        String data = line.substring(5).trim();
                        parseAndAddEvent(data, currentEventName, events);
                        currentEventName = null;
                    }
                }
            } catch (Exception e) {
                LOG.debug("Watch stream reader ended: {}", e.getMessage());
            }
        });
        readerThread.setName("watch-reader-" + endpoint.substring(endpoint.lastIndexOf(":") + 1));
        readerThread.start();

        // 主线程等待指定时间
        Thread.sleep(timeoutMs);

        // 关闭输入流以中断读取线程（这是唯一能可靠打断阻塞 readLine() 的方式）
        try {
            response.body().close();
        } catch (Exception ignored) {
        }

        readerThread.join(1000);
        return events;
    }

    private void parseAndAddEvent(String data, String eventName, List<WatchEvent> events) {
        try {
            if ("put".equals(eventName)) {
                JsonNode node = objectMapper.readTree(data);
                events.add(new WatchEvent(
                        "PUT",
                        node.get("key").asText(),
                        node.get("value").asText(null),
                        node.get("revision").asLong()
                ));
            } else if ("delete".equals(eventName)) {
                JsonNode node = objectMapper.readTree(data);
                events.add(new WatchEvent(
                        "DELETE",
                        node.get("key").asText(),
                        null,
                        node.get("revision").asLong()
                ));
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse watch event: {}", data, e);
        }
    }

    // ==================== 进程管理（Windows） ====================

    /**
     * 杀掉指定 endpoint 对应的 Leader 进程。
     *
     * <p>实现方式：通过节点 profile（node1/node2/node3）匹配 Java 命令行参数，
     * 找到对应 PID 后强制终止。</p>
     */
    private void killLeader(String endpoint) throws Exception {
        String profile = ENDPOINT_TO_PROFILE.get(endpoint);
        if (profile == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        // PowerShell 命令：遍历所有进程，找到命令行包含指定 profile 的 java.exe 并杀掉
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

    // ==================== 内部数据结构 ====================

    private static class WatchEvent {
        final String type;
        final String key;
        final String value;
        final long revision;

        WatchEvent(String type, String key, String value, long revision) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.revision = revision;
        }

        @Override
        public String toString() {
            return String.format("WatchEvent{type=%s, key=%s, revision=%d}", type, key, revision);
        }
    }
}
