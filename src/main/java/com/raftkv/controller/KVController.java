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
     * 
     * 通过 raft.peer-http-endpoints 配置查找 Leader 的 HTTP 地址
     * 支持每个节点有不同的 httpPort
     */
    private String getLeaderHttpUrl() {
        String leaderEndpoint = raftKVService.getLeaderEndpoint();
        if (leaderEndpoint == null) {
            return null;
        }
        
        // 直接通过映射查找 HTTP endpoint
        String httpEndpoint = raftKVService.getHttpEndpointByRaftEndpoint(leaderEndpoint);
        if (httpEndpoint != null) {
            return httpEndpoint;
        }
        
        // 如果找不到映射，返回 null（由调用方处理）
        log.error("Cannot find HTTP endpoint mapping for leader: {}. " +
                  "Please check raft.peer-http-endpoints configuration.", leaderEndpoint);
        return null;
    }

    /**
     * PUT a key-value pair（支持幂等，支持带斜杠的key）
     *
     * @param key   The key (路径变量，支持简单key)
     * @param body  The request body (must contain "value", optional "requestId", optional "key")
     * @return Response indicating success or failure
     */
    @PutMapping({"", "/", "/{key:.+}"})
    public ResponseEntity<KVResponse> put(
            @PathVariable(required = false) String key,
            @RequestBody Map<String, Object> body) {

        // 优先从 body 获取 key（支持带斜杠的key）
        String actualKey = (String) body.get("key");
        if (actualKey == null || actualKey.isEmpty()) {
            actualKey = key;
        }

        if (actualKey == null || actualKey.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(KVResponse.failure("Missing 'key' in request", null));
        }

        String value = (String) body.get("value");
        if (value == null) {
            return ResponseEntity.badRequest()
                    .body(KVResponse.failure("Missing 'value' in request body", null));
        }

        // 客户端可以提供 requestId（用于幂等）
        String requestId = (String) body.get("requestId");

        log.info("PUT request: key={}, value={}, requestId={}", actualKey, value, requestId);
        KVResponse response = raftKVService.put(actualKey, value, requestId);

        if (!response.isSuccess() && "NOT_LEADER".equals(response.getError())) {
            // Redirect to leader
            return ResponseEntity.status(301)
                    .header("Location", "http://" + response.getLeaderEndpoint() + "/kv/" + actualKey)
                    .body(response);
        }

        return ResponseEntity.ok(response);
    }

    /**
     * GET a value by key（支持带斜杠的key）
     *
     * @param key The key (路径变量，支持简单key)
     * @param keyParam The key (查询参数，支持带斜杠的key)
     * @return The value if found
     */
    @GetMapping({"", "/", "/{key:.+}"})
    public ResponseEntity<KVResponse> get(
            @PathVariable(required = false) String key,
            @RequestParam(required = false) String keyParam) {

        // 优先使用查询参数（支持带斜杠的key）
        String actualKey = (keyParam != null && !keyParam.isEmpty()) ? keyParam : key;

        if (actualKey == null || actualKey.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(KVResponse.failure("Missing 'key' parameter", null));
        }

        log.debug("GET request: key={}", actualKey);
        KVResponse response = raftKVService.get(actualKey);

        if (!response.isSuccess() && "NOT_LEADER".equals(response.getError())) {
            // Redirect to leader
            return ResponseEntity.status(301)
                    .header("Location", "http://" + response.getLeaderEndpoint() + "/kv/" + actualKey)
                    .body(response);
        }

        return ResponseEntity.ok(response);
    }

    /**
     * DELETE a key（支持幂等，支持带斜杠的key）
     *
     * @param key       The key to delete (路径变量)
     * @param keyParam  The key to delete (查询参数，支持带斜杠的key)
     * @param requestId Optional request ID for idempotency
     * @return Response indicating success or failure
     */
    @DeleteMapping({"", "/", "/{key:.+}"})
    public ResponseEntity<KVResponse> delete(
            @PathVariable(required = false) String key,
            @RequestParam(required = false) String keyParam,
            @RequestParam(required = false) String requestId) {

        // 优先使用查询参数（支持带斜杠的key）
        String actualKey = (keyParam != null && !keyParam.isEmpty()) ? keyParam : key;

        if (actualKey == null || actualKey.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(KVResponse.failure("Missing 'key' parameter", null));
        }

        log.info("DELETE request: key={}, requestId={}", actualKey, requestId);
        KVResponse response = raftKVService.delete(actualKey, requestId);

        if (!response.isSuccess() && "NOT_LEADER".equals(response.getError())) {
            // Redirect to leader
            return ResponseEntity.status(301)
                    .header("Location", "http://" + response.getLeaderEndpoint() + "/kv/" + actualKey)
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
