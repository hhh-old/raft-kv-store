package com.raftkv.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * Raft 全局异常处理器
 *
 * 职责：
 * 1. 捕获 NotLeaderException
 * 2. 根据请求类型返回合适的响应（HTTP 重定向 或 SSE 错误事件）
 * 3. 处理 leaderUrl 为 null 的边界情况（返回 503）
 *
 * 设计原则：
 * - 统一处理，避免每个 Controller 重复编写重定向逻辑
 * - 完善的日志记录便于问题排查
 * - 对外保持兼容：响应格式不变
 */
@Slf4j
@RestControllerAdvice
public class GlobalRaftExceptionHandler {

    /**
     * 处理 NotLeaderException
     *
     * 策略：
     * 1. 检测 Accept 头或 Content-Type 是否为 text/event-stream
     * 2. 普通请求 → 返回 HTTP 301 重定向（从 HttpServletRequest 构建重定向 URL）
     * 3. leaderUrl 为 null → 返回 HTTP 503 Service Unavailable
     *
     * 职责分离：
     * - Service 层只抛异常（携带 leaderUrl）
     * - Handler 层负责从 HTTP 请求上下文构建完整的重定向 URL
     *
     * 注意：WatchController 的 SSE 场景不在此处处理，保持 Controller 原逻辑
     */
    @ExceptionHandler(NotLeaderException.class)
    public Object handleNotLeaderException(NotLeaderException ex,
                                           HttpServletRequest request) {
        log.warn("Not leader exception: leaderUrl={}, requestMethod={}, requestUri={}",
                ex.getLeaderUrl(), request.getMethod(), request.getRequestURI());

        // 边界情况：leaderUrl 为 null 或 "null"
        if (isInvalidLeaderUrl(ex.getLeaderUrl())) {
            log.error("No leader available for redirect: requestUri={}", request.getRequestURI());
            return build503Response("No leader available");
        }

        return buildRedirectResponse(ex, request);
    }

    /**
     * 检查 leaderUrl 是否无效
     */
    private boolean isInvalidLeaderUrl(String leaderUrl) {
        return leaderUrl == null
                || "null".equals(leaderUrl)
                || leaderUrl.isEmpty();
    }

    /**
     * 构建 HTTP 301 重定向响应
     *
     * 从 HttpServletRequest 构建重定向 URL，确保与原始请求完全一致：
     * - 请求路径：request.getRequestURI()（如 /kv/range）
     * - 查询参数：request.getQueryString()（如 key=/app&limit=10）
     */
    private ResponseEntity<?> buildRedirectResponse(NotLeaderException ex, HttpServletRequest request) {
        String redirectUrl = buildRedirectUrl(ex.getLeaderUrl(), request);

        log.info("Redirecting to leader: {}", redirectUrl);

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.add("Location", redirectUrl);

        // 返回带有 NOT_LEADER 信息的响应体
        Map<String, Object> body = new HashMap<>();
        body.put("success", false);
        body.put("error", "NOT_LEADER");
        body.put("leaderEndpoint", ex.getLeaderUrl());
        body.put("message", "Redirect to leader: " + ex.getLeaderUrl());

        return ResponseEntity
                .status(HttpStatus.MOVED_PERMANENTLY)  // 301
                .headers(responseHeaders)
                .body(body);
    }

    /**
     * 从 Leader URL 和 HttpServletRequest 构建完整的重定向 URL
     *
     * @param leaderUrl Leader 的 HTTP 地址
     * @param request   当前 HTTP 请求
     * @return 完整的重定向 URL（如 http://127.0.0.1:8080/kv/range?key=/app）
     */
    private String buildRedirectUrl(String leaderUrl, HttpServletRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append(leaderUrl);
        sb.append(request.getRequestURI());
        String queryString = request.getQueryString();
        if (queryString != null && !queryString.isEmpty()) {
            sb.append("?").append(queryString);
        }
        return sb.toString();
    }

    /**
     * 构建 503 响应（无 Leader 可用）
     */
    private ResponseEntity<?> build503Response(String message) {
        Map<String, Object> body = new HashMap<>();
        body.put("success", false);
        body.put("error", "NO_LEADER_AVAILABLE");
        body.put("message", message);

        return ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)  // 503
                .body(body);
    }

    /**
     * 处理其他未预期的异常
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleGenericException(Exception ex) {
        log.error("Unexpected exception in controller", ex);

        Map<String, Object> body = new HashMap<>();
        body.put("success", false);
        body.put("error", "INTERNAL_ERROR");
        body.put("message", ex.getMessage());

        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(body);
    }
}
