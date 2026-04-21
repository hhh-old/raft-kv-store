package com.raftkv;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Raft KV Store Application
 *
 * A distributed key-value store built on SOFAJRaft with Spring Boot.
 */
@SpringBootApplication
public class RaftKVApplication {

    public static void main(String[] args) {
        SpringApplication.run(RaftKVApplication.class, args);
    }
}
