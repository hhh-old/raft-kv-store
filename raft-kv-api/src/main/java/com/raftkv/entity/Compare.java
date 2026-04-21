package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 事务比较条件 - 用于条件判断
 * 这个 Compare 类是分布式键值存储（如 etcd）中事务控制的核心逻辑单元。它定义了在执行事务之前，需要对存储状态进行的“断言”或“检查”。
 * 简单来说，它回答了一个问题：“在执行操作前，我要检查某个 Key 的状态是否满足我的预期？”
 * 以下是该类的详细拆解：
 * 1. 核心组成部分
 * 该类通过四个关键维度描述一个条件：
 * Key (键)：你要检查哪一个数据（例如 /lock/resource_1）。
 * Target (比较目标类型)：你要检查这个 Key 的什么属性？
 *  VERSION: 该 Key 被修改过几次。
 *  VALUE: 该 Key 存储的具体内容。
 *  CREATE/MOD: 全局版本号（Revision），用于跟踪整个数据库的更新序列。
 * Op (操作符)：怎么比？（等于、大于、不等于等）。
 * Value (期望值)：你预期的值是多少？
 *
 * 2. 比较目标类型 (CompareType) 的深层含义
 * 这部分是对齐 etcd 语义设计的：
 * VERSION：
 * 特点：每个 Key 独有的计数器。Key 不存在时为 0，每次 Put 操作都会 +1。
 * 用途：实现 CAS (Compare And Swap)。比如：“只有当版本号是 5 时才允许修改”，防止在你读取和写入之间有其他人改了数据。
 * VALUE：
 * 用途：直接检查内容。比如：“只有当值为 'OPEN' 时才执行关门操作”。
 * CREATE / MOD：
 * 这是全局修订号（Revision）。在 etcd 等系统中，每次集群发生数据变动，全局修订号都会递增。这可以用来判断某个 Key 是在哪个时间点之后产生的。
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
     * 比较操作符 - 对齐 etcd 语义
     */
    public enum CompareOp {
        EQUAL,          // 等于
        GREATER,        // 大于
        LESS,           // 小于
        NOT_EQUAL,      // 不等于
        GREATER_EQUAL,  // 大于等于 (>=)
        LESS_EQUAL      // 小于等于 (<=)
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
