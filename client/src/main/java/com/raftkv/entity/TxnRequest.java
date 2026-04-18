package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 事务请求 - 实现 etcd 风格的事务语义
 *
 * 事务执行逻辑：
 * 1. 评估所有 compare 条件（AND 关系）
 * 2. 如果全部条件为 true，执行 success 操作列表
 * 3. 如果有任何条件为 false，执行 failure 操作列表
 * 4. 整个事务是原子性的（要么全部成功，要么全部失败）
 *
 * 使用示例：
 * <pre>
 * TxnRequest txn = TxnRequest.builder()
 *     .compare(Compare.version("/lock/mylock", CompareOp.EQUAL, 0))  // key 不存在
 *     .success(Operation.put("/lock/mylock", "owner-123"))           // 获取锁
 *     .failure(Operation.get("/lock/mylock"))                        // 返回当前持有者
 *     .build();
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TxnRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 条件比较列表
     * 所有条件必须同时满足（AND 关系）
     * 如果为空列表，默认为 true
     */
    @Builder.Default
    private List<Compare> compares = new ArrayList<>();

    /**
     * 条件满足时执行的操作列表
     */
    @Builder.Default
    private List<Operation> success = new ArrayList<>();

    /**
     * 条件不满足时执行的操作列表
     */
    @Builder.Default
    private List<Operation> failure = new ArrayList<>();

    /**
     * 请求 ID（用于幂等性）
     */
    private String requestId;

    /**
     * 添加比较条件
     */
    public TxnRequest compare(Compare compare) {
        if (this.compares == null) {
            this.compares = new ArrayList<>();
        }
        this.compares.add(compare);
        return this;
    }

    /**
     * 添加成功操作
     */
    public TxnRequest success(Operation op) {
        if (this.success == null) {
            this.success = new ArrayList<>();
        }
        this.success.add(op);
        return this;
    }

    /**
     * 添加失败操作
     */
    public TxnRequest failure(Operation op) {
        if (this.failure == null) {
            this.failure = new ArrayList<>();
        }
        this.failure.add(op);
        return this;
    }
}
