package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Range Request - 对齐 etcd v3 Range API
 *
 * 支持的参数：
 * - key: 起始键
 * - range_end: 结束键（null 表示单一键查询）
 * - limit: 返回条数限制（0 表示无限制）
 * - revision: 读取指定版本的快照数据（0 表示最新）
 * - sort_order: 排序顺序
 * - sort_target: 排序字段
 * - count_only: 仅返回计数
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RangeRequest {

    /**
     * 起始键（必填）
     */
    private String key;

    /**
     * 结束键（可选）
     * - 如果为 null 或空：执行单一键查询
     * - 如果非空：执行范围查询 [key, range_end)
     */
    private String rangeEnd;

    /**
     * 返回条数限制（0 表示无限制）
     * 对齐 etcd：limit=0 等同于无限制
     */
    @Builder.Default
    private long limit = 0;

    /**
     * 读取指定版本的快照数据
     * - revision=0：读取最新数据（默认）
     * - revision>0：读取该版本的历史数据
     *
     * 对齐 etcd：revision 是全局递增的 mainRev
     */
    @Builder.Default
    private long revision = 0;

    /**
     * 排序顺序
     * - NONE: 不排序（默认）
     * - ASC: 升序
     * - DESC: 降序
     */
    @Builder.Default
    private SortOrder sortOrder = SortOrder.NONE;

    /**
     * 排序字段
     * - KEY: 按 key 排序（默认）
     * - VERSION: 按 version 排序
     * - CREATE: 按 create_revision 排序
     * - MOD: 按 mod_revision 排序
     * - VALUE: 按 value 排序
     */
    @Builder.Default
    private SortTarget sortTarget = SortTarget.KEY;

    /**
     * 仅返回计数，不返回实际数据
     * 对齐 etcd：返回 count 字段，kvs 为空数组
     */
    @Builder.Default
    private boolean countOnly = false;

    /**
     * 客户端请求 ID（用于追踪和幂等）
     */
    private String requestId;

    /**
     * 排序顺序枚举
     */
    public enum SortOrder {
        /**
         * 不排序（按自然顺序返回）
         */
        NONE(0),
        /**
         * 升序
         */
        ASC(1),
        /**
         * 降序
         */
        DESC(2);

        private final int value;

        SortOrder(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static SortOrder fromValue(int value) {
            for (SortOrder order : values()) {
                if (order.value == value) {
                    return order;
                }
            }
            return NONE;
        }
    }

    /**
     * 排序字段枚举
     */
    public enum SortTarget {
        /**
         * 按 key 排序（默认）
         */
        KEY(0),
        /**
         * 按 version 排序
         */
        VERSION(1),
        /**
         * 按 create_revision 排序
         */
        CREATE(2),
        /**
         * 按 mod_revision 排序
         */
        MOD(3),
        /**
         * 按 value 排序
         */
        VALUE(4);

        private final int value;

        SortTarget(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static SortTarget fromValue(int value) {
            for (SortTarget target : values()) {
                if (target.value == value) {
                    return target;
                }
            }
            return KEY;
        }
    }
}
