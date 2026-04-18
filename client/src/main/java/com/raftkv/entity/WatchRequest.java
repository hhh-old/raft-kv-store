package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Watch 订阅请求 - 客户端用于创建 Watch 订阅
 * 
 * 客户端通过发送此请求来订阅 Key 的变化：
 * - 支持精确匹配（prefix=false）
 * - 支持前缀匹配（prefix=true）
 * - 支持从历史版本开始监听（startRevision > 0）
 * 
 * 示例：
 * 1. 监听单个 Key：key="/config/app", prefix=false
 * 2. 监听配置目录：key="/config/", prefix=true
 * 3. 从历史版本恢复：key="/config/app", startRevision=100
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WatchRequest {

    /**
     * 要监听的 Key 或 Key 前缀
     * 
     * 当 prefix=true 时，表示前缀匹配模式
     * 例如：key="/config/" 会匹配 "/config/app", "/config/db" 等
     */
    private String key;

    /**
     * 是否为前缀匹配模式
     * 
     * false（默认）- 精确匹配，只监听指定的 key
     * true - 前缀匹配，监听所有以 key 开头的 key
     */
    @Builder.Default
    private boolean prefix = false;

    /**
     * 起始版本号
     * 
     * 0（默认）- 从当前时间开始监听，只接收新事件
     * >0 - 从指定版本开始，服务器会先发送历史事件
     * 
     * 用于断线重连场景：客户端记录上次接收的 revision，
     * 重连时从该 revision+1 开始请求，确保不丢失事件
     */
    @Builder.Default
    private long startRevision = 0;

    /**
     * 客户端提供的 Watch ID（可选）
     * 
     * 如果不提供，服务器会自动生成
     * 用于客户端识别和管理多个 Watch
     */
    private String watchId;

    /**
     * 创建精确匹配的 Watch 请求
     */
    public static WatchRequest exact(String key) {
        return WatchRequest.builder()
                .key(key)
                .prefix(false)
                .startRevision(0)
                .build();
    }

    /**
     * 创建前缀匹配的 Watch 请求
     */
    public static WatchRequest prefix(String prefix) {
        return WatchRequest.builder()
                .key(prefix)
                .prefix(true)
                .startRevision(0)
                .build();
    }

    /**
     * 创建从历史版本开始的 Watch 请求
     */
    public static WatchRequest fromRevision(String key, long revision) {
        return WatchRequest.builder()
                .key(key)
                .prefix(false)
                .startRevision(revision)
                .build();
    }
}
