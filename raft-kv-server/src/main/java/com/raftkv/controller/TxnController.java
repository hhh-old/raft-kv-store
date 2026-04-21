package com.raftkv.controller;

import com.raftkv.entity.*;
import com.raftkv.service.RaftKVService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 事务控制器 - 提供 etcd 风格的事务 API
 *
 * 事务是原子性的多 key 操作，支持条件判断（CAS）。
 *
 * 使用示例：
 * <pre>
 * POST /txn
 * {
 *   "compare": [
 *     {"target": "VERSION", "key": "/lock/mylock", "op": "EQUAL", "value": 0}
 *   ],
 *   "success": [
 *     {"type": "PUT", "key": "/lock/mylock", "value": "owner-123"}
 *   ],
 *   "failure": [
 *     {"type": "GET", "key": "/lock/mylock"}
 *   ]
 * }
 * </pre>
 */
@Slf4j
@RestController
@RequestMapping("/txn")
public class TxnController {

    @Autowired
    private RaftKVService raftKVService;

    /**
     * 构建重定向响应（如果不是 Leader）
     * 
     * @param response 原始响应
     * @param basePath 基础路径（如 "/txn"）
     * @return 如果是 NOT_LEADER 则返回重定向响应，否则返回 null
     */
    private ResponseEntity<TxnResponse> redirectIfNotLeader(TxnResponse response, String basePath) {
        if (response.getError() != null && "NOT_LEADER".equals(response.getError())) {
            String leaderHttpUrl = raftKVService.getLeaderHttpUrl();
            if (leaderHttpUrl != null) {
                return ResponseEntity.status(301)
                        .header("Location", leaderHttpUrl + basePath)
                        .body(response);
            }
            return ResponseEntity.status(503)
                    .body(TxnResponse.error("No leader available"));
        }
        return null;
    }

    /**
     * 执行事务
     *
     * 请求体格式：
     * {
     *   "compare": [          // 条件列表（AND 关系，可选）
     *     {
     *       "target": "VERSION",  // 比较目标：VERSION, CREATE, MOD, VALUE
     *       "key": "/lock/mylock",
     *       "op": "EQUAL",        // 操作符：EQUAL, GREATER, LESS, NOT_EQUAL
     *       "value": 0            // 比较值
     *     }
     *   ],
     *   "success": [          // 条件满足时执行的操作
     *     {"type": "PUT", "key": "/lock/mylock", "value": "owner-123"},
     *     {"type": "GET", "key": "/config/version"}
     *   ],
     *   "failure": [          // 条件不满足时执行的操作
     *     {"type": "GET", "key": "/lock/mylock"}
     *   ],
     *   "requestId": "xxx"    // 可选，用于幂等性
     * }
     *
     * 响应体格式：
     * {
     *   "succeeded": true,    // 条件是否满足
     *   "ops": [...],         // 实际执行的操作列表
     *   "results": {          // 每个操作的执行结果
     *     "0": {"success": true, "type": "PUT", "key": "/lock/mylock", "version": 1, "revision": 100},
     *     "1": {"success": true, "type": "GET", "key": "/config/version", "value": "v1", "version": 5}
     *   }
     * }
     */
    @PostMapping
    public ResponseEntity<TxnResponse> executeTransaction(@RequestBody TxnRequest request) {
        log.info("Transaction request: compares={}, successOps={}, failureOps={}",
                request.getCompares() != null ? request.getCompares().size() : 0,
                request.getSuccess() != null ? request.getSuccess().size() : 0,
                request.getFailure() != null ? request.getFailure().size() : 0);

        TxnResponse response = raftKVService.executeTransaction(request);

        // 检查是否需要重定向
        ResponseEntity<TxnResponse> redirectResponse = redirectIfNotLeader(response, "/txn");
        if (redirectResponse != null) {
            return redirectResponse;
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 执行 CAS（Compare-And-Swap）操作
     *
     * 简化的事务 API，用于单个 key 的 CAS 操作。
     *
     * 请求体格式：
     * {
     *   "key": "/config/version",
     *   "expectedValue": "v1",      // 预期值
     *   "newValue": "v2"            // 新值
     * }
     *
     * 或使用版本号：
     * {
     *   "key": "/lock/mylock",
     *   "expectedVersion": 0,       // 预期版本（0 表示 key 不存在）
     *   "newValue": "owner-123"
     * }
     */
    @PostMapping("/cas")
    public ResponseEntity<TxnResponse> cas(@RequestBody Map<String, Object> request) {
        String key = (String) request.get("key");
        String expectedValue = (String) request.get("expectedValue");
        String newValue = (String) request.get("newValue");
        Number expectedVersion = (Number) request.get("expectedVersion");

        if (key == null) {
            return ResponseEntity.badRequest()
                    .body(TxnResponse.error("Missing 'key' in request"));
        }

        // 使用 Builder 模式创建 TxnRequest，确保列表字段正确初始化
        TxnRequest.TxnRequestBuilder txnBuilder = TxnRequest.builder();

        // 构建 compare 条件
        if (expectedVersion != null) {
            // 使用版本号比较
            txnBuilder.compares(java.util.List.of(Compare.version(key, Compare.CompareOp.EQUAL, expectedVersion.longValue())));
        } else if (expectedValue != null) {
            // 使用值比较
            txnBuilder.compares(java.util.List.of(Compare.value(key, Compare.CompareOp.EQUAL, expectedValue)));
        } else {
            return ResponseEntity.badRequest()
                    .body(TxnResponse.error("Missing 'expectedValue' or 'expectedVersion' in request"));
        }

        // 构建 success 操作
        if (newValue != null) {
            txnBuilder.success(java.util.List.of(Operation.put(key, newValue)));
        }

        // 构建 failure 操作（返回当前值）
        txnBuilder.failure(java.util.List.of(Operation.get(key)));

        TxnRequest txnRequest = txnBuilder.build();

        TxnResponse response = raftKVService.executeTransaction(txnRequest);

        // 检查是否需要重定向
        ResponseEntity<TxnResponse> redirectResponse = redirectIfNotLeader(response, "/txn/cas");
        if (redirectResponse != null) {
            return redirectResponse;
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 获取分布式锁
     *
     * 使用 CAS 实现安全的分布式锁获取。
     *
     * 请求体格式：
     * {
     *   "lockKey": "/lock/myresource",
     *   "owner": "service-123",
     *   "ttl": 30                    // 锁的 TTL（秒），可选
     * }
     */
    @PostMapping("/lock")
    public ResponseEntity<TxnResponse> acquireLock(@RequestBody Map<String, Object> request) {
        String lockKey = (String) request.get("lockKey");
        String owner = (String) request.get("owner");

        if (lockKey == null || owner == null) {
            return ResponseEntity.badRequest()
                    .body(TxnResponse.error("Missing 'lockKey' or 'owner' in request"));
        }

        // 使用 CAS：只有当 key 不存在时（version=0）才创建
        TxnRequest txnRequest = TxnRequest.builder()
                .compares(java.util.List.of(Compare.version(lockKey, Compare.CompareOp.EQUAL, 0)))
                .success(java.util.List.of(Operation.put(lockKey, owner)))
                .failure(java.util.List.of(Operation.get(lockKey)))
                .build();

        TxnResponse response = raftKVService.executeTransaction(txnRequest);

        // 检查是否需要重定向
        ResponseEntity<TxnResponse> redirectResponse = redirectIfNotLeader(response, "/txn/lock");
        if (redirectResponse != null) {
            return redirectResponse;
        }

        return ResponseEntity.ok(response);
    }

    /**
     * 释放分布式锁
     *
     * 请求体格式：
     * {
     *   "lockKey": "/lock/myresource",
     *   "owner": "service-123"      // 只有持有锁的 owner 才能释放
     * }
     */
    @PostMapping("/unlock")
    public ResponseEntity<TxnResponse> releaseLock(@RequestBody Map<String, Object> request) {
        String lockKey = (String) request.get("lockKey");
        String owner = (String) request.get("owner");

        if (lockKey == null || owner == null) {
            return ResponseEntity.badRequest()
                    .body(TxnResponse.error("Missing 'lockKey' or 'owner' in request"));
        }

        // 使用 CAS：只有当 key 存在且值等于 owner 时才删除
        TxnRequest txnRequest = TxnRequest.builder()
                .compares(java.util.List.of(Compare.value(lockKey, Compare.CompareOp.EQUAL, owner)))
                .success(java.util.List.of(Operation.delete(lockKey)))
                .failure(java.util.List.of(Operation.get(lockKey)))
                .build();

        TxnResponse response = raftKVService.executeTransaction(txnRequest);

        // 检查是否需要重定向
        ResponseEntity<TxnResponse> redirectResponse = redirectIfNotLeader(response, "/txn/unlock");
        if (redirectResponse != null) {
            return redirectResponse;
        }

        return ResponseEntity.ok(response);
    }
}
