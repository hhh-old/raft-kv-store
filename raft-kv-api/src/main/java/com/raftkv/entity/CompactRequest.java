package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Compaction Request - 对齐 etcd v3 Compaction API
 *
 * 用于压缩历史版本，回收存储空间。
 *
 * etcd 行为：
 * - compaction 是异步操作，执行后 revision 之前的旧数据会被标记为可回收
 * - 实际回收可能需要调用 Defragment API 才能完全释放磁盘空间
 * - compaction 操作本身是幂等的，多次压缩同一 revision 是安全的
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CompactRequest {

    /**
     * 要压缩到的 revision
     * - 执行后，revision < revision 的所有历史版本都会被标记为可回收
     * - 必须是正整数
     * - 通常传入当前 revision - 1，以保留最新版本
     */
    private long revision;

    /**
     * 物理压缩标志
     * - true: 物理删除已压缩的数据（更彻底但更慢）
     * - false: 仅标记为可回收（默认，更快）
     *
     * 注意：本实现暂时忽略此字段，总是执行物理删除
     */
    @Builder.Default
    private boolean physical = false;

    /**
     * 客户端请求 ID（用于追踪）
     */
    private String requestId;

    /**
     * 创建请求
     */
    public static CompactRequest of(long revision) {
        return CompactRequest.builder().revision(revision).build();
    }

    /**
     * 创建请求（带 requestId）
     */
    public static CompactRequest of(long revision, String requestId) {
        return CompactRequest.builder().revision(revision).requestId(requestId).build();
    }
}