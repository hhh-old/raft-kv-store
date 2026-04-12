package com.raftkv.client.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * KV 操作响应
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KVResponse {

    /**
     * 操作是否成功
     */
    private boolean success;

    /**
     * 值（GET 操作）
     */
    private String value;

    /**
     * 错误信息
     */
    private String error;

    /**
     * 请求 ID
     */
    private String requestId;

    /**
     * 处理请求的节点
     */
    private String servedBy;

    /**
     * Leader 节点地址（用于重定向）
     */
    private String leaderEndpoint;

    /**
     * 键
     */
    private String key;

    /**
     * 检查是否是需要重定向到 Leader
     */
    public boolean isNotLeader() {
        return !success && "NOT_LEADER".equals(error);
    }

    /**
     * 获取 Leader 地址（带协议前缀）
     */
    public String getLeaderUrl() {
        if (leaderEndpoint == null || leaderEndpoint.isEmpty()) {
            return null;
        }
        // 如果没有协议前缀，添加 http://
        if (!leaderEndpoint.startsWith("http://") && !leaderEndpoint.startsWith("https://")) {
            return "http://" + leaderEndpoint;
        }
        return leaderEndpoint;
    }
}
