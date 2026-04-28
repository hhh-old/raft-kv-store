package com.raftkv.controller;

import com.raftkv.entity.*;
import com.raftkv.service.RaftKVService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lease 控制器 - 提供 etcd 风格的 Lease API
 *
 * API 列表：
 * - POST /lease/grant      创建租约
 * - POST /lease/revoke     撤销租约
 * - POST /lease/keepalive  续约
 * - GET  /lease/ttl        查询剩余 TTL
 * - GET  /lease/leases     列出所有活跃租约
 */
@Slf4j
@RestController
@RequestMapping("/lease")
public class LeaseController {

    @Autowired
    private RaftKVService raftKVService;

    /**
     * 创建 Lease
     *
     * 请求体：{"ttl": 60}
     * 响应：{"success": true, "id": 1, "ttl": 60}
     */
    @PostMapping("/grant")
    public ResponseEntity<LeaseGrantResponse> grant(@RequestBody LeaseGrantRequest request) {
        log.info("Lease grant request: ttl={}", request.getTtl());
        LeaseGrantResponse response = raftKVService.leaseGrant(request.getTtl());

        if (!response.isSuccess() && "NOT_LEADER".equals(response.getError())) {
            String leaderUrl = raftKVService.getLeaderHttpUrl();
            if (leaderUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderUrl + "/lease/grant")
                        .body(response);
            }
            return ResponseEntity.status(503).body(response);
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 撤销 Lease
     *
     * 请求体：{"id": 1}
     */
    @PostMapping("/revoke")
    public ResponseEntity<Map<String, Object>> revoke(@RequestBody LeaseRevokeRequest request) {
        log.info("Lease revoke request: id={}", request.getId());
        boolean success = raftKVService.leaseRevoke(request.getId());

        Map<String, Object> response = new HashMap<>();
        if (!success && !raftKVService.isLeader()) {
            String leaderUrl = raftKVService.getLeaderHttpUrl();
            if (leaderUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderUrl + "/lease/revoke")
                        .body(response);
            }
            response.put("error", "No leader available");
            return ResponseEntity.status(503).body(response);
        }

        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    /**
     * Lease KeepAlive（续约）
     *
     * 请求体：{"id": 1}
     */
    @PostMapping("/keepalive")
    public ResponseEntity<Map<String, Object>> keepAlive(@RequestBody LeaseKeepAliveRequest request) {
        log.debug("Lease keepalive request: id={}", request.getId());
        boolean success = raftKVService.leaseKeepAlive(request.getId());

        Map<String, Object> response = new HashMap<>();
        if (!success && !raftKVService.isLeader()) {
            String leaderUrl = raftKVService.getLeaderHttpUrl();
            if (leaderUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderUrl + "/lease/keepalive")
                        .body(response);
            }
            response.put("error", "No leader available");
            return ResponseEntity.status(503).body(response);
        }

        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    /**
     * 查询 Lease 剩余 TTL
     *
     * @param id 租约 ID
     * @return 剩余秒数
     */
    @GetMapping("/ttl")
    public ResponseEntity<Map<String, Object>> ttl(@RequestParam long id) {
        log.debug("Lease TTL request: id={}", id);
        long ttl = raftKVService.leaseTtl(id);

        Map<String, Object> response = new HashMap<>();
        if (ttl < 0 && !raftKVService.isLeader()) {
            String leaderUrl = raftKVService.getLeaderHttpUrl();
            if (leaderUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderUrl + "/lease/ttl?id=" + id)
                        .body(response);
            }
            response.put("error", "No leader available");
            return ResponseEntity.status(503).body(response);
        }

        response.put("id", id);
        response.put("ttl", ttl);
        return ResponseEntity.ok(response);
    }

    /**
     * 获取所有活跃 Lease
     *
     * @return Lease 列表
     */
    @GetMapping("/leases")
    public ResponseEntity<?> leases() {
        log.debug("Lease leases request");
        List<Long> leaseIds = raftKVService.leaseLeases();

        if (leaseIds == null && !raftKVService.isLeader()) {
            String leaderUrl = raftKVService.getLeaderHttpUrl();
            if (leaderUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderUrl + "/lease/leases")
                        .body("{\"error\":\"NOT_LEADER\"}");
            }
            return ResponseEntity.status(503).body("{\"error\":\"NO_LEADER_AVAILABLE\"}");
        }

        Map<String, Object> response = new HashMap<>();
        response.put("leases", leaseIds != null ? leaseIds : java.util.Collections.emptyList());
        return ResponseEntity.ok(response);
    }
}
