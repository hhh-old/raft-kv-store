package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 事务响应 - 返回事务执行结果
 *
 * 包含：
 * - 事务是否成功（条件是否满足）
 * - 执行的操作列表（success 或 failure）
 * - 每个操作的执行结果
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TxnResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 事务是否成功
     * true: 条件满足，执行了 success 操作
     * false: 条件不满足，执行了 failure 操作
     */
    private boolean succeeded;

    /**
     * 事务执行的操作列表（success 或 failure）
     */
    @Builder.Default
    private List<Operation> ops = new ArrayList<>();

    /**
     * 每个操作的执行结果
     * key: 操作的索引（在 ops 列表中的位置）
     * value: 操作结果
     */
    @Builder.Default
    private Map<Integer, OpResult> results = new HashMap<>();

    /**
     * 错误信息（如果事务执行失败）
     */
    private String error;

    /**
     * 当前 leader 地址（如果不是 leader 返回）
     */
    private String leaderEndpoint;

    /**
     * 请求 ID（用于幂等性）
     */
    private String requestId;

    /**
     * 操作结果
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OpResult implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * 操作是否成功
         */
        private boolean success;

        /**
         * 操作类型
         */
        private Operation.OpType type;

        /**
         * 操作的 key
         */
        private String key;

        /**
         * GET 操作返回的值
         */
        private String value;

        /**
         * key 的版本号（PUT/DELETE 后）
         */
        private long version;

        /**
         * 全局 revision（PUT/DELETE 后）
         */
        private long revision;

        /**
         * key 的创建版本号（createRevision）
         */
        private long createRevision;

        /**
         * key 的修改版本号（modRevision）
         */
        private long modRevision;

        /**
         * 错误信息
         */
        private String error;

        // ========== 便捷构造方法 ==========

        public static OpResult success(Operation.OpType type, String key, long version, long revision) {
            return OpResult.builder()
                    .success(true)
                    .type(type)
                    .key(key)
                    .version(version)
                    .revision(revision)
                    .build();
        }

        public static OpResult success(Operation.OpType type, String key, long version, long revision,
                                         long createRevision, long modRevision) {
            return OpResult.builder()
                    .success(true)
                    .type(type)
                    .key(key)
                    .version(version)
                    .revision(revision)
                    .createRevision(createRevision)
                    .modRevision(modRevision)
                    .build();
        }

        public static OpResult getSuccess(String key, String value, long version, long revision) {
            return OpResult.builder()
                    .success(true)
                    .type(Operation.OpType.GET)
                    .key(key)
                    .value(value)
                    .version(version)
                    .revision(revision)
                    .build();
        }

        public static OpResult getSuccess(String key, String value, long version, long revision,
                                           long createRevision, long modRevision) {
            return OpResult.builder()
                    .success(true)
                    .type(Operation.OpType.GET)
                    .key(key)
                    .value(value)
                    .version(version)
                    .revision(revision)
                    .createRevision(createRevision)
                    .modRevision(modRevision)
                    .build();
        }

        public static OpResult failure(Operation.OpType type, String key, String error) {
            return OpResult.builder()
                    .success(false)
                    .type(type)
                    .key(key)
                    .error(error)
                    .build();
        }
    }

    // ========== 便捷构造方法 ==========

    public static TxnResponse success(List<Operation> ops, Map<Integer, OpResult> results) {
        return TxnResponse.builder()
                .succeeded(true)
                .ops(ops)
                .results(results)
                .build();
    }

    public static TxnResponse failure(List<Operation> ops, Map<Integer, OpResult> results) {
        return TxnResponse.builder()
                .succeeded(false)
                .ops(ops)
                .results(results)
                .build();
    }

    public static TxnResponse error(String error) {
        return TxnResponse.builder()
                .succeeded(false)
                .error(error)
                .build();
    }

    public static TxnResponse notLeader(String leaderEndpoint) {
        return TxnResponse.builder()
                .succeeded(false)
                .error("NOT_LEADER")
                .leaderEndpoint(leaderEndpoint)
                .build();
    }
}
