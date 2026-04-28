package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Lease 创建响应 - 返回新创建的租约信息
 *
 * 对齐 etcd LeaseGrant 响应语义：
 * - id: 租约唯一标识符
 * - ttl: 服务端实际授予的 TTL（可能与请求不同）
 * - error: 错误信息（成功时为 null）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LeaseGrantResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否成功
     */
    private boolean success;

    /**
     * 租约 ID
     */
    private long id;

    /**
     * 实际授予的 TTL（秒）
     */
    private int ttl;

    /**
     * 错误信息
     */
    private String error;

    public static LeaseGrantResponse success(long id, int ttl) {
        return LeaseGrantResponse.builder()
                .success(true)
                .id(id)
                .ttl(ttl)
                .build();
    }

    public static LeaseGrantResponse failure(String error) {
        return LeaseGrantResponse.builder()
                .success(false)
                .error(error)
                .build();
    }
}
