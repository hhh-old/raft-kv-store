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

    public static KVTask put(String key, String value, String requestId) {
        return new KVTask(OP_PUT, key, value, System.currentTimeMillis(), requestId);
    }

    public static KVTask delete(String key, String requestId) {
        return new KVTask(OP_DELETE, key, null, System.currentTimeMillis(), requestId);
    }
}
