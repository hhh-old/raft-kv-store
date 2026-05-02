package com.raftkv.client.transport;

import com.raftkv.client.config.ClientConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * HTTP 传输层
 *
 * 职责：
 * 1. 管理 HttpClient 实例和连接超时配置
 * 2. 提供便捷的 HTTP 请求构建和发送方法
 * 3. 支持同步请求（String 响应体和 InputStream 响应体）
 */
public class HttpTransport {

    private final HttpClient httpClient;
    private final int timeoutSeconds;

    public HttpTransport(ClientConfig config) {
        this.timeoutSeconds = config.getTimeoutSeconds();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                .build();
    }

    /**
     * 发送 GET 请求，返回 String 响应体
     */
    public HttpResponse<String> sendGet(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 发送 POST 请求（JSON 请求体），返回 String 响应体
     */
    public HttpResponse<String> sendPost(String url, String jsonBody) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 发送 PUT 请求（JSON 请求体），返回 String 响应体
     */
    public HttpResponse<String> sendPut(String url, String jsonBody) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 发送 DELETE 请求，返回 String 响应体
     */
    public HttpResponse<String> sendDelete(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .DELETE()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 发送 SSE 流式 POST 请求，返回 InputStream 响应体
     */
    public HttpResponse<java.io.InputStream> sendSsePost(String url, String jsonBody) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Accept", "text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
    }

    /**
     * 发送自定义 HttpRequest，返回 String 响应体
     */
    public HttpResponse<String> send(HttpRequest request) throws Exception {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * 发送自定义 HttpRequest，返回 InputStream 响应体
     */
    public HttpResponse<java.io.InputStream> sendStream(HttpRequest request) throws Exception {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
    }

    /**
     * 创建 JSON HTTP 请求 Builder（不发送）
     *
     * @param url    请求 URL
     * @param method HTTP 方法 ("GET", "POST", "PUT", "DELETE")
     * @param body   请求体（GET 请求传 null）
     * @return HttpRequest 实例
     */
    public HttpRequest createJsonRequest(String url, String method, Object body) throws Exception {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .header("Content-Type", "application/json");

        if ("PUT".equals(method)) {
            requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(bodyToJson(body)));
        } else if ("POST".equals(method)) {
            requestBuilder.POST(HttpRequest.BodyPublishers.ofString(bodyToJson(body)));
        } else if ("DELETE".equals(method)) {
            requestBuilder.DELETE();
        } else {
            requestBuilder.GET();
        }

        return requestBuilder.build();
    }

    private String bodyToJson(Object body) throws Exception {
        if (body == null) return "";
        if (body instanceof String) return (String) body;
        return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(body);
    }
}
