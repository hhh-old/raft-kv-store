package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response entity for KV operations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KVResponse {

    /**
     * Whether the operation was successful
     */
    private boolean success;

    /**
     * The value (for GET operations)
     */
    private String value;

    /**
     * Error message if failed
     */
    private String error;

    /**
     * Request ID
     */
    private String requestId;

    /**
     * The node that served the request
     */
    private String servedBy;

    /**
     * Leader endpoint (for redirect)
     */
    private String leaderEndpoint;

    /**
     * The key
     */
    private String key;

    public static KVResponse success(String key, String value, String requestId, String servedBy) {
        KVResponse response = new KVResponse();
        response.setSuccess(true);
        response.setKey(key);
        response.setValue(value);
        response.setRequestId(requestId);
        response.setServedBy(servedBy);
        return response;
    }

    public static KVResponse failure(String error, String requestId) {
        KVResponse response = new KVResponse();
        response.setSuccess(false);
        response.setError(error);
        response.setRequestId(requestId);
        return response;
    }
}
