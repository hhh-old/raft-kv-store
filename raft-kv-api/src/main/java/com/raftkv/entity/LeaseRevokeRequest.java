package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Lease 撤销请求 - 手动撤销一个租约
 *
 * 撤销后，该租约绑定的所有 key 会被立即删除。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LeaseRevokeRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 要撤销的租约 ID
     */
    private long id;
}
