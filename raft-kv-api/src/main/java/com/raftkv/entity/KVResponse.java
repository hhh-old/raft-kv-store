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

    /**
     * key 是否存在（对齐 etcd 语义）
     *
     * etcd 用 count=0 和空 kvs 数组表示 key 不存在。
     * 这里用 found 字段明确标识：
     * - found=true: key 存在，value 为实际值（包括空字符串）
     * - found=false: key 不存在（从未创建或已被删除）
     *
     * 注意：value=null 有两种情况：
     * 1. found=false, value=null → key 不存在
     * 2. found=true, value=非null → key 存在
     * found 字段消除了二义性
     */
    private Boolean found;

    public static KVResponse success(String key, String value, String requestId, String servedBy) {
        KVResponse response = new KVResponse();
        response.setSuccess(true);
        response.setKey(key);
        response.setValue(value);
        response.setRequestId(requestId);
        response.setServedBy(servedBy);
        response.setFound(value != null);  // value 非 null 表示 key 存在
        return response;
    }

    /**
     * 创建 key 不存在的响应（对齐 etcd：空 kvs + count=0）
     */
    public static KVResponse notFound(String key, String requestId, String servedBy) {
        KVResponse response = new KVResponse();
        response.setSuccess(true);  // 操作本身成功，只是 key 不存在
        response.setKey(key);
        response.setValue(null);
        response.setRequestId(requestId);
        response.setServedBy(servedBy);
        response.setFound(false);
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
