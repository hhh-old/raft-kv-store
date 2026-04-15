package com.raftkv.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Raft configuration properties
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "raft")
public class RaftProperties {

    /**
     * Raft group ID
     */
    private String groupId = "kv-store-group";

    /**
     * This node's ID (format: ip:port:index)
     */
    private String nodeId;

    /**
     * This node's endpoint (format: ip:port)
     */
    private String endpoint;

    /**
     * All peers in the cluster (comma-separated list of ip:port)
     */
    private String peers;

    /**
     * Data directory for storing Raft logs and snapshots
     */
    private String dataDir = "./raft-data";

    /**
     * Whether this node is an initializer (first node that creates the group)
     */
    private boolean initializer = false;

    /**
     * RPC port for Raft communication
     */
    private int port;

    /**
     * HTTP port for client API (REST)
     */
    private int httpPort;

    /**
     * Read timeout in milliseconds
     */
    private int readTimeout = 3000;

    /**
     * Write timeout in milliseconds
     */
    private int writeTimeout = 3000;
}
