package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Task to be applied to the Raft state machine.
 * This object is serialized and stored in the Raft log.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KVTask implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Operation type: PUT, DELETE
     */
    private String op;

    /**
     * Key for the operation
     */
    private String key;

    /**
     * Value for PUT operation
     */
    private String value;

    /**
     * Timestamp of the task
     */
    private long timestamp;

    /**
     * Request ID for tracking
     */
    private String requestId;

    public static final String OP_PUT = "PUT";
    public static final String OP_DELETE = "DELETE";
    public static final String OP_TXN = "TXN";
    public static final String OP_LEASE_GRANT = "LEASE_GRANT";
    public static final String OP_LEASE_REVOKE = "LEASE_REVOKE";
    public static final String OP_COMPACT = "COMPACT";

    /**
     * 租约 ID（ Lease 相关操作使用）
     */
    private Long leaseId;

    /**
     * 租约 TTL（Lease Grant 时使用，单位：秒）
     */
    private Integer leaseTtl;

    /**
     * 租约授予时间（毫秒时间戳，Leader 创建日志时记录）
     * 用于 Leader 切换后新 Leader 正确计算剩余 TTL
     */
    private Long grantedTime;

    /**
     * 压缩目标 revision（Compact 操作使用）
     */
    private Long compactRevision;

    /**
     * 事务请求（当 op 为 TXN 时使用）
     * 使用 transient 避免序列化问题，实际序列化时使用 JSON 字符串存储在 value 字段
     */
    private transient TxnRequest txnRequest;

    /**
     * 事务响应结果（仅在 onApply 后设置，用于回调获取结果）
     * 使用 transient 避免序列化
     */
    private transient TxnResponse txnResponse;

    public static KVTask put(String key, String value, String requestId) {
        KVTask task = new KVTask();
        task.setOp(OP_PUT);
        task.setKey(key);
        task.setValue(value);
        task.setTimestamp(System.currentTimeMillis());
        task.setRequestId(requestId);
        return task;
    }

    public static KVTask delete(String key, String requestId) {
        KVTask task = new KVTask();
        task.setOp(OP_DELETE);
        task.setKey(key);
        task.setValue(null);
        task.setTimestamp(System.currentTimeMillis());
        task.setRequestId(requestId);
        return task;
    }

    /**
     * 创建事务任务
     *
     * @param txnRequest 事务请求
     * @param requestId 请求 ID
     * @return KVTask
     */
    public static KVTask txn(TxnRequest txnRequest, String requestId) {
        KVTask task = new KVTask();
        task.setOp(OP_TXN);
        task.setKey("");
        task.setValue(null);
        task.setTimestamp(System.currentTimeMillis());
        task.setRequestId(requestId);
        task.setTxnRequest(txnRequest);
        return task;
    }

    /**
     * 创建 Lease Grant 任务
     *
     * @param leaseId 租约 ID
     * @param ttl 租约有效期（秒）
     * @param requestId 请求 ID
     * @return KVTask
     */
    public static KVTask leaseGrant(long leaseId, int ttl, String requestId) {
        KVTask task = new KVTask();
        task.setOp(OP_LEASE_GRANT);
        task.setKey("");
        task.setValue(null);
        task.setLeaseId(leaseId);
        task.setLeaseTtl(ttl);
        task.setGrantedTime(System.currentTimeMillis());  // 记录授予时间，用于 Leader 切换后正确计算 TTL
        task.setTimestamp(System.currentTimeMillis());
        task.setRequestId(requestId);
        return task;
    }

    /**
     * 创建 Lease Revoke 任务
     *
     * @param leaseId 租约 ID
     * @param requestId 请求 ID
     * @return KVTask
     */
    public static KVTask leaseRevoke(long leaseId, String requestId) {
        KVTask task = new KVTask();
        task.setOp(OP_LEASE_REVOKE);
        task.setKey("");
        task.setValue(null);
        task.setLeaseId(leaseId);
        task.setTimestamp(System.currentTimeMillis());
        task.setRequestId(requestId);
        return task;
    }

    /**
     * 创建 Compact 压缩任务
     *
     * @param compactRevision 压缩目标 revision
     * @param requestId 请求 ID
     * @return KVTask
     */
    public static KVTask compact(long compactRevision, String requestId) {
        KVTask task = new KVTask();
        task.setOp(OP_COMPACT);
        task.setKey("");
        task.setValue(null);
        task.setCompactRevision(compactRevision);
        task.setTimestamp(System.currentTimeMillis());
        task.setRequestId(requestId);
        return task;
    }
}
