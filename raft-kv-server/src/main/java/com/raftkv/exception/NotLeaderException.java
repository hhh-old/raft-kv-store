package com.raftkv.exception;

import lombok.Getter;

/**
 * 非 Leader 节点异常
 *
 * 高性能设计：
 * 1. fillInStackTrace() 返回 this: 禁用异常栈抓取，避免性能开销
 * 2. 仅携带 leaderUrl: 重定向 URL 由全局异常处理器从 HttpServletRequest 构建
 *
 * 职责分离：
 * - Service 层: 纯业务逻辑，不关心 HTTP 细节
 * - GlobalRaftExceptionHandler: 唯一处理 HTTP 请求上下文的地方，负责构建重定向 URL
 *
 * 使用场景：
 * - Service 层检测到当前节点不是 Leader 时抛出
 * - GlobalRaftExceptionHandler 捕获并从 HttpServletRequest 构建重定向响应
 */
@Getter
public class NotLeaderException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Leader 的 HTTP URL (如: http://127.0.0.1:8080) */
    private final String leaderUrl;

    /**
     * 构造函数
     *
     * @param leaderUrl Leader 的 HTTP URL
     */
    public NotLeaderException(String leaderUrl) {
        super("Not leader, redirect to: " + leaderUrl);
        this.leaderUrl = leaderUrl;
    }

    /**
     * 禁用异常栈抓取，提升性能
     *
     * 这是关键的性能优化点。
     * NotLeaderException 是控制业务流转的异常（非错误），发生频率可能较高。
     * 通过禁用 StackTrace 抓取，可以将抛出性能提升数百倍。
     *
     * @return 返回 this，不进行栈轨迹填充
     */
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    @Override
    public String toString() {
        return "NotLeaderException{" +
                "leaderUrl='" + leaderUrl + '\'' +
                '}';
    }
}
