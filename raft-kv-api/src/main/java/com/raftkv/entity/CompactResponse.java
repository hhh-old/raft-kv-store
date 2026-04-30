package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Compaction Response - 对齐 etcd v3 CompactionResponse
 *
 * 压缩操作完成后返回。
 *
 * 对齐 etcd 语义：
 * - compaction 操作是幂等的，多次压缩同一 revision 都返回成功
 * - 压缩后，旧版本数据被删除，无法再通过 Range 查询历史版本
 * - 返回的 header 包含压缩发生时的 revision
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CompactResponse {

    /**
     * 操作是否成功
     */
    private boolean success;

    /**
     * 错误码
     */
    private String error;

    /**
     * 错误详情
     */
    private String errorDetail;

    /**
     * 压缩发生的 revision
     * - 这是压缩生效时的全局 revision
     * - 用于客户端确认压缩的版本号
     */
    private long compactedRevision;

    /**
     * 请求 ID
     */
    private String requestId;

    /**
     * 服务的节点
     */
    private String servedBy;

    /**
     * 创建成功响应
     */
    public static CompactResponse success(long compactedRevision, String requestId, String servedBy) {
        return CompactResponse.builder()
                .success(true)
                .compactedRevision(compactedRevision)
                .requestId(requestId)
                .servedBy(servedBy)
                .build();
    }

    /**
     * 创建失败响应
     */
    public static CompactResponse failure(String error, String errorDetail, String requestId) {
        return CompactResponse.builder()
                .success(false)
                .error(error)
                .errorDetail(errorDetail)
                .requestId(requestId)
                .build();
    }
}