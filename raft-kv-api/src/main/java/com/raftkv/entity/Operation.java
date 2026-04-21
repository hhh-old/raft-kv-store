package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 事务操作 - 在事务中执行的具体操作
 *
 * 支持的操作类型：
 * - PUT: 设置 key-value
 * - DELETE: 删除 key
 * - GET: 获取 key 的值（只读）
 * - TXN: 嵌套事务
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Operation implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 操作类型
     */
    private OpType type;

    /**
     * 操作的 key
     */
    private String key;

    /**
     * 操作的 value（PUT 时使用）
     */
    private String value;

    /**
     * 嵌套事务（type 为 TXN 时使用）
     */
    private TxnRequest nestedTxn;

    /**
     * 操作类型
     */
    public enum OpType {
        /**
         * 设置 key-value
         */
        PUT,

        /**
         * 删除 key
         */
        DELETE,

        /**
         * 获取 key 的值
         */
        GET,

        /**
         * 嵌套事务
         */
        TXN
    }

    // ========== 便捷构造方法 ==========

    /**
     * 创建 PUT 操作
     */
    public static Operation put(String key, String value) {
        return Operation.builder()
                .type(OpType.PUT)
                .key(key)
                .value(value)
                .build();
    }

    /**
     * 创建 DELETE 操作
     */
    public static Operation delete(String key) {
        return Operation.builder()
                .type(OpType.DELETE)
                .key(key)
                .build();
    }

    /**
     * 创建 GET 操作
     */
    public static Operation get(String key) {
        return Operation.builder()
                .type(OpType.GET)
                .key(key)
                .build();
    }

    /**
     * 创建嵌套事务操作
     */
    public static Operation txn(TxnRequest nestedTxn) {
        return Operation.builder()
                .type(OpType.TXN)
                .nestedTxn(nestedTxn)
                .build();
    }
}
