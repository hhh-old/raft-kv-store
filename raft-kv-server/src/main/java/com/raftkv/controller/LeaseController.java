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

        // Service 层会抛出 NotLeaderException，由全局异常处理器处理重定向
        LeaseGrantResponse response = raftKVService.leaseGrant(request.getTtl());

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

        // Service 层会抛出 NotLeaderException，由全局异常处理器处理重定向
        boolean success = raftKVService.leaseRevoke(request.getId());

        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    /**
     * Lease KeepAlive（续约）
     *
     * 请求体：{"id": 1}
     *
     * 注意：Service 层会抛出 NotLeaderException，由全局异常处理器处理重定向
     */
    @PostMapping("/keepalive")
    public ResponseEntity<Map<String, Object>> keepAlive(@RequestBody LeaseKeepAliveRequest request) {
        log.debug("Lease keepalive request: id={}", request.getId());
        boolean success = raftKVService.leaseKeepAlive(request.getId());

        Map<String, Object> response = new HashMap<>();
        response.put("success", success);
        return ResponseEntity.ok(response);
    }

    /**
     * 查询 Lease 剩余 TTL
     *
     * @param id 租约 ID
     * @return 剩余秒数，-1 表示租约不存在
     *
     * 注意：Service 层会抛出 NotLeaderException，由全局异常处理器处理重定向
     */
    @GetMapping("/ttl")
    public ResponseEntity<Map<String, Object>> ttl(@RequestParam long id) {
        log.debug("Lease TTL request: id={}", id);
        long ttl = raftKVService.leaseTtl(id);

        Map<String, Object> response = new HashMap<>();
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
    public ResponseEntity<Map<String, Object>> leases() {
        log.debug("Lease leases request");

        // Service 层会抛出 NotLeaderException，由全局异常处理器处理重定向
        List<Long> leaseIds = raftKVService.leaseLeases();

        Map<String, Object> response = new HashMap<>();
        response.put("leases", leaseIds != null ? leaseIds : java.util.Collections.emptyList());
        return ResponseEntity.ok(response);
    }
}
