package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 事务比较条件 - 用于条件判断
 *
 * 支持比较的类型：
 * - VERSION: key 的当前版本号（每次修改递增）
 * - CREATE: key 的创建版本号
 * - MOD: key 的修改版本号
 * - VALUE: key 的值
 *
 * 支持比较的操作：
 * - EQUAL: 等于
 * - GREATER: 大于
 * - LESS: 小于
 * - NOT_EQUAL: 不等于
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Compare implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 比较目标类型
     */
    private CompareType target;

    /**
     * 比较的 key
     */
    private String key;

    /**
     * 比较操作符
     */
    private CompareOp op;

    /**
     * 比较的值（Long 类型用于版本号比较，String 类型用于值比较）
     */
    private Object value;

    /**
     * 比较目标类型
     */
    public enum CompareType {
        /**
         * key 的版本号（每次修改递增，从 1 开始）
         * 用于 CAS 操作：只有当版本号匹配时才更新
         */
        VERSION,

        /**
         * key 的创建版本号（创建时的全局 revision）
         */
        CREATE,

        /**
         * key 的修改版本号（最后一次修改的全局 revision）
         */
        MOD,

        /**
         * key 的值
         */
        VALUE
    }

    /**
     * 比较操作符
     */
    public enum CompareOp {
        EQUAL,
        GREATER,
        LESS,
        NOT_EQUAL
    }

    // ========== 便捷构造方法 ==========

    /**
     * 创建版本号比较条件
     *
     * @param key key
     * @param op 操作符
     * @param version 版本号（key 不存在时为 0）
     */
    public static Compare version(String key, CompareOp op, long version) {
        return Compare.builder()
                .target(CompareType.VERSION)
                .key(key)
                .op(op)
                .value(version)
                .build();
    }

    /**
     * 创建值比较条件
     *
     * @param key key
     * @param op 操作符
     * @param value 值
     */
    public static Compare value(String key, CompareOp op, String value) {
        return Compare.builder()
                .target(CompareType.VALUE)
                .key(key)
                .op(op)
                .value(value)
                .build();
    }

    /**
     * 创建创建版本号比较条件
     */
    public static Compare create(String key, CompareOp op, long createRevision) {
        return Compare.builder()
                .target(CompareType.CREATE)
                .key(key)
                .op(op)
                .value(createRevision)
                .build();
    }

    /**
     * 创建修改版本号比较条件
     */
    public static Compare mod(String key, CompareOp op, long modRevision) {
        return Compare.builder()
                .target(CompareType.MOD)
                .key(key)
                .op(op)
                .value(modRevision)
                .build();
    }
}
