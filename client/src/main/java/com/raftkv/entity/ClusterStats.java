package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Cluster statistics and status information
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterStats {

    /**
     * Current node's role (LEADER, FOLLOWER, CANDIDATE)
     */
    private String role;

    /**
     * Current node's endpoint
     */
    private String endpoint;

    /**
     * Current term
     */
    private long currentTerm;

    /**
     * Current commit index
     */
    private long commitIndex;

    /**
     * Last applied index
     */
    private long lastApplied;

    /**
     * Leader endpoint (null if this node is leader)
     */
    private String leaderEndpoint;

    /**
     * List of all peers
     */
    private List<String> peers;

    /**
     * Total number of keys in the store
     */
    private int keyCount;

    /**
     * Cluster group ID
     */
    private String groupId;

    /**
     * Node ID
     */
    private String nodeId;
}
