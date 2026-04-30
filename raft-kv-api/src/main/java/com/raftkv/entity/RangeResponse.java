package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Range Response - 对齐 etcd v3 RangeResponse
 *
 * 包含：
 * - kvs: 键值对列表（排除已删除的）
 * - count: 匹配的 key 总数（可用于分页）
 * - revision: 读取时的全局版本号
 * - more: 是否有更多数据（limit 限制时）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RangeResponse {

    /**
     * 键值对列表
     * - countOnly=true 时为空列表
     * - 不包含已删除的 key（tombstone）
     */
    @Builder.Default
    private List<KeyValue> kvs = new ArrayList<>();

    /**
     * 匹配的 key 总数
     * - 用于分页场景，告知客户端还有多少条数据未返回
     * - 即使设置了 limit，返回的 count 是符合条件的总数
     */
    private long count;

    /**
     * 读取时的全局版本号
     * - 用于追踪本次读取基于的快照版本
     * - 客户端可用于判断数据是否更新
     */
    private long revision;

    /**
     * 是否有更多数据未返回
     * - true: 还有更多数据（因为 limit 限制）
     * - false: 已返回全部数据
     */
    private boolean more;

    /**
     * 操作是否成功
     */
    private boolean success;

    /**
     * 错误信息
     */
    private String error;

    /**
     * 错误描述
     */
    private String errorDetail;

    /**
     * 请求 ID
     */
    private String requestId;

    /**
     * 服务的节点
     */
    private String servedBy;

    /**
     * Leader endpoint（用于重定向）
     */
    private String leaderEndpoint;

    /**
     * 单个键值对（对齐 etcd mvccpb.KeyValue）
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KeyValue {
        /**
         * 键
         */
        private String key;

        /**
         * 值
         */
        private String value;

        /**
         * 该版本的 revision（mod_revision）
         */
        private long revision;

        /**
         * 创建时的 revision（create_revision）
         * - 标识 key 的当前生命周期的创建版本
         * - key 被删除后重新创建会重置
         */
        private long createRevision;

        /**
         * 修改次数（version）
         * - 每次修改递增
         * - key 被删除后重新创建会重置为 1
         */
        private long version;

        /**
         * 租约 ID（0 表示无租约）
         */
        @Builder.Default
        private long leaseId = 0;
    }

    /**
     * 创建成功的响应
     */
    public static RangeResponse success(List<KeyValue> kvs, long count, long revision,
                                         boolean more, String requestId, String servedBy) {
        return RangeResponse.builder()
                .success(true)
                .kvs(kvs)
                .count(count)
                .revision(revision)
                .more(more)
                .requestId(requestId)
                .servedBy(servedBy)
                .build();
    }

    /**
     * 创建 NOT_LEADER 响应
     */
    public static RangeResponse notLeader(String leaderEndpoint, String requestId) {
        return RangeResponse.builder()
                .success(false)
                .error("NOT_LEADER")
                .leaderEndpoint(leaderEndpoint)
                .requestId(requestId)
                .build();
    }

    /**
     * 创建失败响应
     */
    public static RangeResponse failure(String error, String errorDetail, String requestId) {
        return RangeResponse.builder()
                .success(false)
                .error(error)
                .errorDetail(errorDetail)
                .requestId(requestId)
                .build();
    }
}
