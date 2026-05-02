package com.raftkv.client.transport;

import com.raftkv.client.config.ClientConfig;
import com.raftkv.entity.RaftBaseResponse;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Set;

/**
 * 统一重试引擎
 *
 * 职责：
 * 1. 重试机制（指数退避 + 随机抖动）
 * 2. Leader 重定向处理（HTTP 301 + NOT_LEADER）
 * 3. 端点健康检测与故障转移
 * 4. 首次超时立即重试
 *
 * 合并了原 RaftKVClient 中 executeApi 和 executeMapApi 的重复重试逻辑。
 */
@Slf4j
public class RetryEngine {

    private final EndpointManager endpointManager;
    private final ClientConfig config;

    public RetryEngine(EndpointManager endpointManager, ClientConfig config) {
        this.endpointManager = endpointManager;
        this.config = config;
    }

    /**
     * API 执行器接口
     */
    @FunctionalInterface
    public interface ApiExecutor<T> {
        HttpResponse<String> execute(String endpoint) throws Exception;
    }

    /**
     * Response 解析器接口
     */
    @FunctionalInterface
    public interface ResponseParser<T> {
        T parse(HttpResponse<String> httpResponse) throws Exception;
    }

    /**
     * 执行 API 请求（支持 NOT_LEADER 重定向）
     *
     * @param operation 操作名称（用于日志）
     * @param executor  HTTP 请求执行器
     * @param parser    响应解析器
     * @param <T>       Response 类型，必须继承 RaftBaseResponse
     * @return 解析后的 Response 对象
     */
    public <T extends RaftBaseResponse> T executeApi(
            String operation,
            ApiExecutor<T> executor,
            ResponseParser<T> parser) {

        RetryContext ctx = new RetryContext(operation, config.getMaxRetries());

        while (true) {
            ctx.incrementAttempt();

            try {
                String currentEndpoint = endpointManager.getCurrentLeader();
                log.debug("{} attempt {} using endpoint: {}", operation, ctx.getAttempt(), currentEndpoint);

                HttpResponse<String> httpResponse = executor.execute(currentEndpoint);

                // 处理 HTTP 301 重定向
                if (httpResponse.statusCode() == 301) {
                    String location = httpResponse.headers().firstValue("Location").orElse(null);
                    if (location != null) {
                        handleHttpRedirect(location);
                    }
                    continue;
                }

                // 尝试解析错误响应（允许返回业务错误，如 HTTP 400）
                if (httpResponse.statusCode() != 200) {
                    try {
                        T errorResponse = parser.parse(httpResponse);
                        endpointManager.markEndpointHealthy(currentEndpoint);
                        log.warn("{} failed: error={}", operation, errorResponse.getError());
                        return errorResponse;
                    } catch (Exception parseEx) {
                        throw new RuntimeException(operation + " failed: HTTP " + httpResponse.statusCode()
                                + ", body: " + httpResponse.body());
                    }
                }

                // 解析成功响应
                T response = parser.parse(httpResponse);
                endpointManager.markEndpointHealthy(currentEndpoint);

                // 处理 NOT_LEADER 重定向循环
                response = handleLeaderRedirect(operation, executor, parser, response);

                if (response.isSuccess()) {
                    log.debug("{} successful", operation);
                } else {
                    log.warn("{} failed: error={}", operation, response.getError());
                }

                return response;

            } catch (Exception e) {
                ctx.setLastException(e);
                log.warn("{} attempt {} failed: {}", operation, ctx.getAttempt(), e.getMessage());

                if (isConnectionFailure(e)) {
                    handleConnectionFailure(ctx);
                    if (ctx.hasMoreAttempts()) {
                        continue;
                    }
                }

                if (ctx.hasMoreAttempts()) {
                    if (ctx.isFirstAttempt() && e instanceof java.net.http.HttpTimeoutException) {
                        log.debug("Timeout on first attempt, retrying immediately...");
                        continue;
                    }
                    sleepQuietly(calculateBackoff(ctx.getAttempt()));
                    continue;
                }

                break;
            }
        }

        throw new RuntimeException(operation + " failed after " + config.getMaxRetries() + " attempts",
                ctx.getLastException());
    }

    /**
     * 执行返回 Map 的 API 请求（不支持 NOT_LEADER 重定向）
     *
     * @param operation 操作名称（用于日志）
     * @param executor  HTTP 请求执行器
     * @return 解析后的 Map 对象
     */
    public Map<String, Object> executeMapApi(String operation, ApiExecutor<Map<String, Object>> executor) {
        RetryContext ctx = new RetryContext(operation, config.getMaxRetries());

        while (true) {
            ctx.incrementAttempt();

            try {
                String currentEndpoint = endpointManager.getCurrentLeader();
                log.debug("{} attempt {} using endpoint: {}", operation, ctx.getAttempt(), currentEndpoint);

                HttpResponse<String> httpResponse = executor.execute(currentEndpoint);

                // 处理 HTTP 301 重定向
                if (httpResponse.statusCode() == 301) {
                    String location = httpResponse.headers().firstValue("Location").orElse(null);
                    if (location != null) {
                        handleHttpRedirect(location);
                    }
                    continue;
                }

                if (httpResponse.statusCode() != 200) {
                    throw new RuntimeException(operation + " failed: HTTP " + httpResponse.statusCode());
                }

                endpointManager.markEndpointHealthy(currentEndpoint);
                return parseMapResponse(httpResponse);

            } catch (Exception e) {
                ctx.setLastException(e);
                log.warn("{} attempt {} failed: {}", operation, ctx.getAttempt(), e.getMessage());

                if (isConnectionFailure(e)) {
                    handleConnectionFailure(ctx);
                    if (ctx.hasMoreAttempts()) {
                        continue;
                    }
                }

                if (ctx.hasMoreAttempts()) {
                    if (ctx.isFirstAttempt() && e instanceof java.net.http.HttpTimeoutException) {
                        continue;
                    }
                    sleepQuietly(calculateBackoff(ctx.getAttempt()));
                    continue;
                }

                break;
            }
        }

        throw new RuntimeException(operation + " failed after " + config.getMaxRetries() + " attempts",
                ctx.getLastException());
    }

    // ==================== 内部方法 ====================

    /**
     * 处理 HTTP 301 重定向
     */
    private void handleHttpRedirect(String location) {
        try {
            URI redirectUri = new URI(location);
            String newLeader = redirectUri.getScheme() + "://" + redirectUri.getHost();
            if (redirectUri.getPort() > 0) {
                newLeader += ":" + redirectUri.getPort();
            }
            endpointManager.updateLeader(newLeader);
        } catch (Exception e) {
            log.warn("Failed to parse redirect location: {}, using regex fallback", location);
            String newLeader = extractBaseUrl(location);
            if (newLeader != null) {
                endpointManager.updateLeader(newLeader);
            } else {
                log.error("Cannot extract leader from redirect location: {}", location);
            }
        }
    }

    /**
     * 从任意字符串中提取 base URL（scheme://host:port）
     */
    private String extractBaseUrl(String url) {
        if (url == null || url.isEmpty()) return null;
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(https?://[^/]+)");
        java.util.regex.Matcher matcher = pattern.matcher(url);
        return matcher.find() ? matcher.group(1) : null;
    }

    /**
     * 处理 Leader 重定向循环（NOT_LEADER）
     */
    private <T extends RaftBaseResponse> T handleLeaderRedirect(
            String operation,
            ApiExecutor<T> executor,
            ResponseParser<T> parser,
            T response) throws Exception {

        int redirectCount = 0;
        final int MAX_REDIRECTS = 3;

        while (response.isNotLeader() && response.getLeaderEndpoint() != null) {
            if (++redirectCount > MAX_REDIRECTS) {
                throw new RuntimeException(operation + " failed: too many leader redirects ("
                        + redirectCount + "), possible redirect loop");
            }

            log.info("Redirecting to leader ({}): {}", redirectCount, response.getLeaderEndpoint());
            endpointManager.updateLeader(response.getLeaderEndpoint());
            endpointManager.markEndpointHealthy(endpointManager.getCurrentLeader());

            HttpResponse<String> httpResponse = executor.execute(endpointManager.getCurrentLeader());

            if (httpResponse.statusCode() != 200) {
                throw new RuntimeException(operation + " failed after redirect: HTTP "
                        + httpResponse.statusCode());
            }

            response = parser.parse(httpResponse);
        }

        return response;
    }

    /**
     * 处理连接失败（故障转移）
     */
    private void handleConnectionFailure(RetryContext ctx) {
        String currentEndpoint = endpointManager.getCurrentLeader();
        endpointManager.markEndpointUnhealthy(currentEndpoint);
        ctx.addTriedEndpoint(currentEndpoint);

        String available = endpointManager.findAvailableEndpoint(ctx.getTriedEndpoints());
        if (available != null) {
            log.info("Connection failure, switching to: {}", available);
            endpointManager.updateLeader(available);
        } else {
            log.warn("No available endpoints, all endpoints have been tried");
        }
    }

    /**
     * 计算指数退避时间（带随机抖动）
     */
    private long calculateBackoff(int attempt) {
        long baseDelay = 100;
        long maxDelay = 3000;
        long delay = Math.min(baseDelay * (long) Math.pow(2, attempt - 1), maxDelay);
        long jitter = (long) (delay * 0.25 * Math.random());
        return delay + jitter;
    }

    /**
     * 将 HTTP 响应解析为 Map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseMapResponse(HttpResponse<String> httpResponse) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper().readValue(httpResponse.body(), Map.class);
    }

    /**
     * 判断是否为连接失败异常（需要故障转移）
     */
    private boolean isConnectionFailure(Throwable e) {
        if (e == null) return false;
        String msg = e.getMessage();
        return e instanceof java.net.ConnectException
                || e instanceof java.net.http.HttpTimeoutException
                || e instanceof java.io.IOException
                || (msg != null && (
                        msg.contains("Connection refused")
                                || msg.contains("Connection reset")
                                || msg.contains("Connect timed out")
                                || msg.contains("connection closed")
                                || msg.contains("Unreachable")
                                || msg.contains("Network is unreachable")
                ));
    }

    /**
     * 安全休眠（忽略中断）
     */
    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 重试上下文
     */
    private static class RetryContext {
        private final String operation;
        private final int maxRetries;
        private final Set<String> triedEndpoints = new java.util.HashSet<>();
        private Exception lastException;
        private int attempt;

        RetryContext(String operation, int maxRetries) {
            this.operation = operation;
            this.maxRetries = maxRetries;
        }

        String getOperation() { return operation; }
        int getMaxRetries() { return maxRetries; }
        int getAttempt() { return attempt; }
        Exception getLastException() { return lastException; }
        Set<String> getTriedEndpoints() { return triedEndpoints; }

        void incrementAttempt() { this.attempt++; }
        void setLastException(Exception e) { this.lastException = e; }
        void addTriedEndpoint(String endpoint) { this.triedEndpoints.add(endpoint); }

        boolean hasMoreAttempts() { return attempt < maxRetries; }
        boolean isFirstAttempt() { return attempt == 1; }
    }
}