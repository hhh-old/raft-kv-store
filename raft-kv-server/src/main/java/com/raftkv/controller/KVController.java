package com.raftkv.controller;

import com.raftkv.entity.ClusterStats;
import com.raftkv.entity.CompactRequest;
import com.raftkv.entity.CompactResponse;
import com.raftkv.entity.KVRequest;
import com.raftkv.entity.KVResponse;
import com.raftkv.entity.RangeRequest;
import com.raftkv.entity.RangeResponse;
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
     * 构建重定向响应（如果不是 Leader）
     * 
     * @param response 原始响应
     * @param basePath 基础路径（如 "/kv/key"）
     * @return 如果是 NOT_LEADER 则返回重定向响应，否则返回 null
     */
    private ResponseEntity<KVResponse> redirectIfNotLeader(KVResponse response, String basePath) {
        if (!response.isSuccess() && "NOT_LEADER".equals(response.getError())) {
            String leaderHttpUrl = raftKVService.getLeaderHttpUrl();
            if (leaderHttpUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderHttpUrl + basePath)
                        .body(response);
            }
            return ResponseEntity.status(503)
                    .body(KVResponse.failure("No leader available", response.getRequestId()));
        }
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

        // 客户端可以提供 leaseId 绑定租约
        Number leaseIdNum = (Number) body.get("leaseId");
        Long leaseId = leaseIdNum != null ? leaseIdNum.longValue() : null;

        log.info("PUT request: key={}, value={}, requestId={}, leaseId={}", actualKey, value, requestId, leaseId);
        KVResponse response = raftKVService.put(actualKey, value, requestId, leaseId);

        // 检查是否需要重定向
        ResponseEntity<KVResponse> redirectResponse = redirectIfNotLeader(response, "/kv/" + actualKey);
        if (redirectResponse != null) {
            return redirectResponse;
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

        // 检查是否需要重定向
        ResponseEntity<KVResponse> redirectResponse = redirectIfNotLeader(response, "/kv/" + actualKey);
        if (redirectResponse != null) {
            return redirectResponse;
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

        // 检查是否需要重定向
        ResponseEntity<KVResponse> redirectResponse = redirectIfNotLeader(response, "/kv/" + actualKey);
        if (redirectResponse != null) {
            return redirectResponse;
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
            String leaderHttpUrl = raftKVService.getLeaderHttpUrl();
            if (leaderHttpUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderHttpUrl + "/kv/all")
                        .body("{\"error\":\"NOT_LEADER\",\"leaderEndpoint\":\"" + leaderHttpUrl + "\"}");
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

    // ==================== Range 查询（对齐 etcd v3 API） ====================

    /**
     * Range 查询 - 对齐 etcd v3 Range API
     *
     * 支持的参数：
     * - key: 起始键（必填）
     * - range_end: 结束键（可选，null 表示单一键查询）
     * - limit: 返回条数限制（0 表示无限制）
     * - revision: 读取指定版本（0 表示最新）
     * - sort_order: 排序顺序（NONE/ASC/DESC）
     * - sort_target: 排序字段（KEY/VERSION/CREATE/MOD/VALUE）
     * - count_only: 仅返回计数（true/false）
     *
     * 示例请求：
     * GET /kv/range?key=/users/
     * GET /kv/range?key=/users/&range_end=/users0&limit=10
     * GET /kv/range?key=a&sort_order=ASC&sort_target=KEY
     * GET /kv/range?key=a&count_only=true
     *
     * @param key 起始键
     * @param rangeEnd 结束键（可选）
     * @param limit 返回条数限制
     * @param revision 读取指定版本
     * @param sortOrder 排序顺序
     * @param sortTarget 排序字段
     * @param countOnly 仅返回计数
     * @return Range 响应
     */
    @GetMapping("/range")
    public ResponseEntity<RangeResponse> range(
            @RequestParam String key,
            @RequestParam(required = false) String rangeEnd,
            @RequestParam(required = false, defaultValue = "0") Long limit,
            @RequestParam(required = false, defaultValue = "0") Long revision,
            @RequestParam(required = false, defaultValue = "NONE") String sortOrder,
            @RequestParam(required = false, defaultValue = "KEY") String sortTarget,
            @RequestParam(required = false, defaultValue = "false") Boolean countOnly) {

        if (key == null || key.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(RangeResponse.failure("BAD_REQUEST", "Missing 'key' parameter", null));
        }

        // 解析排序顺序
        RangeRequest.SortOrder order;
        try {
            order = RangeRequest.SortOrder.valueOf(sortOrder.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(RangeResponse.failure("BAD_REQUEST",
                            "Invalid sort_order: " + sortOrder + ". Valid values: NONE, ASC, DESC", null));
        }

        // 解析排序字段
        RangeRequest.SortTarget target;
        try {
            target = RangeRequest.SortTarget.valueOf(sortTarget.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest()
                    .body(RangeResponse.failure("BAD_REQUEST",
                            "Invalid sort_target: " + sortTarget + ". Valid values: KEY, VERSION, CREATE, MOD, VALUE", null));
        }

        // 构建 Range 请求
        RangeRequest request = RangeRequest.builder()
                .key(key)
                .rangeEnd(rangeEnd)
                .limit(limit != null ? limit : 0L)
                .revision(revision != null ? revision : 0L)
                .sortOrder(order)
                .sortTarget(target)
                .countOnly(countOnly != null && countOnly)
                .build();

        log.debug("Range request: key={}, rangeEnd={}, limit={}, revision={}, sortOrder={}, sortTarget={}, countOnly={}",
                key, rangeEnd, limit, revision, order, target, countOnly);

        RangeResponse response = raftKVService.range(request);

        // 检查是否需要重定向
        if ("NOT_LEADER".equals(response.getError())) {
            String leaderUrl = response.getLeaderEndpoint();
            if (leaderUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderUrl + "/kv/range")
                        .body(response);
            }
            return ResponseEntity.status(503)
                    .body(RangeResponse.failure("NO_LEADER", "No leader available", response.getRequestId()));
        }

        return ResponseEntity.ok(response);
    }

    /**
     * Range 查询（POST 方式，支持复杂请求体）
     *
     * 请求体示例：
     * {
     *   "key": "/users/",
     *   "rangeEnd": "/users0",
     *   "limit": 10,
     *   "revision": 0,
     *   "sortOrder": "ASC",
     *   "sortTarget": "KEY",
     *   "countOnly": false
     * }
     *
     * @param request Range 请求体
     * @return Range 响应
     */
    @PostMapping("/range")
    public ResponseEntity<RangeResponse> rangePost(@RequestBody RangeRequest request) {

        // 允许空字符串 key（表示从最小开始扫描），但不允许 null
        if (request.getKey() == null) {
            return ResponseEntity.badRequest()
                    .body(RangeResponse.failure("BAD_REQUEST", "Missing 'key' in request body", request.getRequestId()));
        }

        log.debug("Range POST request: key={}, rangeEnd={}, limit={}, revision={}",
                request.getKey(), request.getRangeEnd(), request.getLimit(), request.getRevision());

        RangeResponse response = raftKVService.range(request);

        // 检查是否需要重定向
        if ("NOT_LEADER".equals(response.getError())) {
            String leaderUrl = response.getLeaderEndpoint();
            if (leaderUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderUrl + "/kv/range")
                        .body(response);
            }
            return ResponseEntity.status(503)
                    .body(RangeResponse.failure("NO_LEADER", "No leader available", response.getRequestId()));
        }

        return ResponseEntity.ok(response);
    }

    // ==================== Compact 压缩（对齐 etcd v3 API） ====================

    /**
     * 压缩历史版本 - 对齐 etcd v3 Compaction API
     *
     * 压缩流程：
     * 1. 验证 revision 参数（必须是正数，且 <= 当前 revision）
     * 2. 将压缩请求提交到 Raft 日志
     * 3. 等待日志被 committed 并应用到状态机
     * 4. 返回压缩结果
     *
     * 示例请求：
     * POST /kv/compact
     * {
     *   "revision": 10
     * }
     *
     * @param request 压缩请求
     * @return 压缩响应
     */
    @PostMapping("/compact")
    public ResponseEntity<CompactResponse> compact(@RequestBody CompactRequest request) {

        if (request == null) {
            return ResponseEntity.badRequest()
                    .body(CompactResponse.failure("BAD_REQUEST", "Request body is required", null));
        }

        if (request.getRevision() <= 0) {
            return ResponseEntity.badRequest()
                    .body(CompactResponse.failure("BAD_REQUEST",
                            "revision must be a positive integer", request.getRequestId()));
        }

        log.info("Compact request: revision={}, requestId={}", request.getRevision(), request.getRequestId());

        CompactResponse response = raftKVService.compact(request);

        // 检查是否需要重定向
        if ("NOT_LEADER".equals(response.getError())) {
            String leaderHttpUrl = raftKVService.getLeaderHttpUrl();
            if (leaderHttpUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderHttpUrl + "/kv/compact")
                        .body(response);
            }
            return ResponseEntity.status(503)
                    .body(CompactResponse.failure("NO_LEADER", "No leader available", response.getRequestId()));
        }

        if (!response.isSuccess()) {
            log.error("Compact failed: error={}, detail={}", response.getError(), response.getErrorDetail());
            return ResponseEntity.badRequest().body(response);
        }

        return ResponseEntity.ok(response);
    }
}
