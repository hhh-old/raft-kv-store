package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request entity for KV operations
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KVRequest {

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
     * Request ID for tracking
     */
    private String requestId;

    public static final String OP_PUT = "PUT";
    public static final String OP_DELETE = "DELETE";
}
