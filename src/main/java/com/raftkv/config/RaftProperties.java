package com.raftkv.config;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Raft configuration properties
 * 
 * 简化后的配置，只需要指定最核心的参数，其他自动推导
 */
@Slf4j
@Data
@Configuration
@ConfigurationProperties(prefix = "raft")
public class RaftProperties {

    /**
     * Raft group ID
     * 默认: kv-store-group
     */
    private String groupId = "kv-store-group";

    /**
     * This node's ID (format: ip:port)
     * 示例: 127.0.0.1:8081
     * 
     * 从这个字段自动推导:
     * - endpoint: 与 nodeId 相同
     * - port: port 部分
     */
    private String nodeId;

    /**
     * All peers in the cluster (comma-separated list of ip:port)
     * 示例: 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
     */
    private String peers;

    /**
     * HTTP endpoints for all peers (comma-separated list of ip:httpPort)
     * 示例: 127.0.0.1:9081,127.0.0.1:9082,127.0.0.1:9083
     * 
     * 用于 Leader 重定向时查找其他节点的 HTTP 地址
     */
    private String peerHttpEndpoints;

    /**
     * HTTP port for client API (REST)
     * 从 server.port 读取，不需要在 raft 配置中显式指定
     * 
     * 这样设计的好处：
     * - 符合 Spring Boot 习惯，直接配置 server.port
     * - raft.http-port 自动同步，保持配置一致性
     */
    private int httpPort;

    /**
     * Data directory for storing Raft logs and snapshots
     */
    private String dataDir = "./raft-data";

    /**
     * Whether this node is an initializer (first node that creates the group)
     */
    private boolean initializer = false;

    /**
     * Read timeout in milliseconds
     */
    private int readTimeout = 5000;

    /**
     * Write timeout in milliseconds
     */
    private int writeTimeout = 5000;

    /**
     * Election timeout in milliseconds
     */
    private int electionTimeoutMs = 1000;

    // === 以下字段由配置自动推导，不需要在配置文件中指定 ===
    
    /**
     * This node's endpoint (format: ip:port)
     * 从 nodeId 推导: 127.0.0.1:8081 -> 127.0.0.1:8081
     */
    private String endpoint;

    /**
     * RPC port for Raft communication
     * 从 nodeId 推导
     */
    private int port;

    /**
     * 配置初始化，自动推导字段
     */
    @PostConstruct
    public void init() {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("raft.node-id must be configured");
        }
        if (peers == null || peers.isEmpty()) {
            throw new IllegalArgumentException("raft.peers must be configured");
        }
        if (peerHttpEndpoints == null || peerHttpEndpoints.isEmpty()) {
            throw new IllegalArgumentException("raft.peer-http-endpoints must be configured");
        }

        // 从 nodeId 推导 endpoint 和 port
        // nodeId 格式: ip:port
        String[] parts = nodeId.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid node-id format: " + nodeId + ", expected: ip:port");
        }
        
        this.endpoint = nodeId;
        try {
            this.port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid port in node-id: " + nodeId);
        }

        // httpPort 从 server.port 读取（通过 @Value 注入）
        // 如果未设置，使用默认值 8080
        if (httpPort == 0) {
            httpPort = 8080;
        }

        log.info("RaftProperties initialized: nodeId={}, endpoint={}, port={}, httpPort={}", 
                nodeId, endpoint, port, httpPort);
    }
}
