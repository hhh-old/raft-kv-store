package com.raftkv.client.config;

import java.util.List;

/**
 * RaftKVClient 配置类
 *
 * 集中管理客户端的所有配置项，便于维护和扩展。
 */
public class ClientConfig {

    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final int DEFAULT_TIMEOUT_SECONDS = 8;
    public static final int DEFAULT_MAX_CONSECUTIVE_FAILURES = 3;

    private final List<String> serverUrls;
    private final int maxRetries;
    private final int timeoutSeconds;
    private final int maxConsecutiveFailures;

    private ClientConfig(Builder builder) {
        this.serverUrls = builder.serverUrls;
        this.maxRetries = builder.maxRetries;
        this.timeoutSeconds = builder.timeoutSeconds;
        this.maxConsecutiveFailures = builder.maxConsecutiveFailures;
    }

    public List<String> getServerUrls() {
        return serverUrls;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public int getMaxConsecutiveFailures() {
        return maxConsecutiveFailures;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> serverUrls;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private int timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
        private int maxConsecutiveFailures = DEFAULT_MAX_CONSECUTIVE_FAILURES;

        public Builder serverUrls(List<String> serverUrls) {
            this.serverUrls = serverUrls;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder timeoutSeconds(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
            return this;
        }

        public Builder maxConsecutiveFailures(int maxConsecutiveFailures) {
            this.maxConsecutiveFailures = maxConsecutiveFailures;
            return this;
        }

        public ClientConfig build() {
            if (serverUrls == null || serverUrls.isEmpty()) {
                throw new IllegalArgumentException("serverUrls cannot be null or empty");
            }
            return new ClientConfig(this);
        }
    }
}
