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

    /**
     * 事务请求（当 op 为 TXN 时使用）
     * 使用 transient 避免序列化问题，实际序列化时使用 JSON 字符串存储在 value 字段
     */
    private transient TxnRequest txnRequest;

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
}
