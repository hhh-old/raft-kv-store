package com.raftkv.entity;

/**
 * 所有 Raft KV Response 的基类接口
 *
 * 定义统一的响应行为，支持：
 * 1. 错误状态检查
 * 2. Leader 重定向处理
 * 3. 统一的 JSON 反序列化
 *
 * 设计原则：
 * - 所有实现类必须提供 isSuccess() 和 getError() 方法
 * - getLeaderEndpoint() 提供默认实现（返回 null），不需要重定向的实现类无需覆盖
 * - isNotLeader() 提供工具方法
 */
public interface RaftBaseResponse {

    /**
     * 判断操作是否成功
     */
    boolean isSuccess();

    /**
     * 获取错误信息
     */
    String getError();

    /**
     * 获取 Leader 端点（用于 NOT_LEADER 重定向）
     *
     * @return Leader 地址，如果不需要重定向或没有 Leader 信息则返回 null
     */
    default String getLeaderEndpoint() {
        return null;
    }

    /**
     * 判断是否为 NOT_LEADER 错误（需要重定向）
     */
    default boolean isNotLeader() {
        return "NOT_LEADER".equals(getError());
    }
}
