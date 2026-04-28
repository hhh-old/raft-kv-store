package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Lease 续约请求 - 刷新租约的过期时间
 *
 * 对齐 etcd KeepAlive 语义：
 * - 客户端定期发送 KeepAlive 请求维持租约
 * - 服务端刷新租约的过期时间
 * - 如果租约不存在，返回失败
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LeaseKeepAliveRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 要续约的租约 ID
     */
    private long id;
}
