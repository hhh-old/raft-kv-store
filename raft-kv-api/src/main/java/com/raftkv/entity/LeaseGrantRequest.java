package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Lease 创建请求 - 申请一个新的租约
 *
 * 使用示例：
 * <pre>
 * LeaseGrantRequest request = new LeaseGrantRequest();
 * request.setTtl(60); // 60秒有效期
 * </pre>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LeaseGrantRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 租约有效期（秒）
     * 如果为 0，服务端会使用默认 TTL
     */
    private int ttl;
}
