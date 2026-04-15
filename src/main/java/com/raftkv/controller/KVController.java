package com.raftkv.controller;

import com.raftkv.entity.ClusterStats;
import com.raftkv.entity.KVRequest;
import com.raftkv.entity.KVResponse;
import com.raftkv.service.RaftKVService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST Controller for KV Store operations
 *
 * All write operations (PUT, DELETE) are forwarded to the Raft leader
 * and replicated through the cluster.
 *
 * Read operations are served directly by any node with linearizability.
 */
@Slf4j
@RestController
@RequestMapping("/kv")
public class KVController {

    @Autowired
    private RaftKVService raftKVService;
    
    /**
     * 获取 Leader 的 HTTP URL
     * 用于非 Leader 节点重定向请求
     */
    private String getLeaderHttpUrl() {
        String leaderEndpoint = raftKVService.getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return null;
        }
        
        // 解析 leader endpoint (格式: ip:port:index 或 ip:port)
        // 需要转换为 HTTP URL (ip:httpPort)
        int lastColon = leaderEndpoint.lastIndexOf(':');
        if (lastColon > 0) {
            String leaderIp = leaderEndpoint.substring(0, lastColon);
            try {
                int leaderRaftPort = Integer.parseInt(leaderEndpoint.substring(lastColon + 1));
                // HTTP port = raft port + offset (假设 offset 为 1000，根据配置调整)
                int leaderHttpPort = leaderRaftPort + 1000;
                return leaderIp + ":" + leaderHttpPort;
            } catch (NumberFormatException e) {
                log.warn("Failed to parse leader endpoint: {}", leaderEndpoint);
            }
        }
        return leaderEndpoint;
    }

    /**
     * PUT a key-value pair（支持幂等）
     *
     * @param key   The key
     * @param body  The request body (must contain "value", optional "requestId")
     * @return Response indicating success or failure
     */
    @PutMapping("/{key}")
    public ResponseEntity<KVResponse> put(
            @PathVariable String key,
            @RequestBody Map<String, Object> body) {

        String value = (String) body.get("value");
        if (value == null) {
            return ResponseEntity.badRequest()
                    .body(KVResponse.failure("Missing 'value' in request body", null));
        }

        // 客户端可以提供 requestId（用于幂等）
        // 如果不提供，服务端会自动生成
        String requestId = (String) body.get("requestId");

        log.info("PUT request: key={}, value={}, requestId={}", key, value, requestId);
        KVResponse response = raftKVService.put(key, value, requestId);

        if (!response.isSuccess() && "NOT_LEADER".equals(response.getError())) {
            // Redirect to leader
            return ResponseEntity.status(301)
                    .header("Location", "http://" + response.getLeaderEndpoint() + "/kv/" + key)
                    .body(response);
        }

        return ResponseEntity.ok(response);
    }

    /**
     * GET a value by key
     *
     * @param key The key
     * @return The value if found
     */
    @GetMapping("/{key}")
    public ResponseEntity<KVResponse> get(@PathVariable String key) {
        log.debug("GET request: key={}", key);
        KVResponse response = raftKVService.get(key);

        if (!response.isSuccess() && "NOT_LEADER".equals(response.getError())) {
            // Redirect to leader
            return ResponseEntity.status(301)
                    .header("Location", "http://" + response.getLeaderEndpoint() + "/kv/" + key)
                    .body(response);
        }

        return ResponseEntity.ok(response);
    }

    /**
     * DELETE a key（支持幂等）
     *
     * @param key       The key to delete
     * @param requestId Optional request ID for idempotency
     * @return Response indicating success or failure
     */
    @DeleteMapping("/{key}")
    public ResponseEntity<KVResponse> delete(
            @PathVariable String key,
            @RequestParam(required = false) String requestId) {
        
        log.info("DELETE request: key={}, requestId={}", key, requestId);
        KVResponse response = raftKVService.delete(key, requestId);

        if (!response.isSuccess() && "NOT_LEADER".equals(response.getError())) {
            // Redirect to leader
            return ResponseEntity.status(301)
                    .header("Location", "http://" + response.getLeaderEndpoint() + "/kv/" + key)
                    .body(response);
        }

        return ResponseEntity.ok(response);
    }

    /**
     * GET all key-value pairs (admin endpoint)
     *
     * 使用线性一致性读：只有 Leader 能处理此请求
     * 非 Leader 节点会返回 301 重定向到 Leader
     *
     * @return All key-value pairs
     */
    @GetMapping("/all")
    public ResponseEntity<?> getAll() {
        Map<String, String> result = raftKVService.getAll();
        
        // 如果返回 null，说明当前不是 Leader，需要重定向
        if (result == null) {
            String leader = getLeaderHttpUrl();
            if (leader != null) {
                return ResponseEntity.status(301)
                        .header("Location", "http://" + leader + "/kv/all")
                        .body("{\"error\":\"NOT_LEADER\",\"leaderEndpoint\":\"" + leader + "\"}");
            }
            return ResponseEntity.status(503)
                    .body("{\"error\":\"NO_LEADER_AVAILABLE\"}");
        }
        
        return ResponseEntity.ok(result);
    }

    /**
     * GET cluster statistics
     *
     * @return Cluster status information
     */
    @GetMapping("/stats")
    public ResponseEntity<ClusterStats> getStats() {
        return ResponseEntity.ok(raftKVService.getClusterStats());
    }

    /**
     * Health check endpoint
     *
     * @return OK if the service is healthy
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        if (raftKVService.isReady()) {
            return ResponseEntity.ok("OK");
        } else {
            return ResponseEntity.status(503).body("NOT_READY");
        }
    }

    /**
     * Get leader information
     *
     * @return Leader endpoint if available
     */
    @GetMapping("/leader")
    public ResponseEntity<KVResponse> getLeader() {
        String leaderEndpoint = raftKVService.getLeaderEndpoint();
        String currentEndpoint = raftKVService.getCurrentEndpoint();
        boolean isLeader = raftKVService.isLeader();

        KVResponse response = KVResponse.builder()
                .success(true)
                .leaderEndpoint(leaderEndpoint)
                .servedBy(currentEndpoint)
                .error(isLeader ? "I_AM_LEADER" : "I_AM_FOLLOWER")
                .build();

        return ResponseEntity.ok(response);
    }
}
