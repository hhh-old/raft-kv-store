package com.raftkv.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Watch 事件 - 表示一个 Key 的变化
 * 
 * 设计参考 etcd 的 Event 结构，包含完整的版本信息：
 * - type: 事件类型（PUT/DELETE）
 * - key/value: 变化的键值
 * - revision: 全局单调递增的版本号
 * - createRevision: Key 首次创建时的版本
 * - modRevision: Key 本次修改的版本
 * - version: Key 的本地版本（每次修改递增）
 * 
 * 通过这些版本信息，客户端可以：
 * 1. 判断事件顺序（通过 revision）
 * 2. 判断 Key 是否是新创建的（modRevision == createRevision）
 * 3. 追踪 Key 的修改次数（通过 version）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WatchEvent {

    /**
     * 事件类型
     */
    public enum EventType {
        /**
         * Key 被创建或修改
         */
        PUT,
        
        /**
         * Key 被删除
         */
        DELETE
    }

    /**
     * 事件类型
     */
    private EventType type;

    /**
     * 发生变化的 Key
     */
    private String key;

    /**
     * Key 的值（DELETE 事件为 null）
     */
    private String value;

    /**
     * 全局版本号（单调递增，每次写操作 +1）
     * 
     * 这是整个集群的逻辑时钟，所有事件按此排序
     */
    private long revision;

    /**
     * Key 首次创建时的版本号
     * 
     * 用于判断 Key 是否是新创建的：
     * if (modRevision == createRevision) -> 新创建的 Key
     */
    private long createRevision;

    /**
     * Key 本次修改的版本号
     * 
     * 等于本次修改对应的全局 revision
     */
    private long modRevision;

    /**
     * Key 的本地版本（从 1 开始，每次修改 +1）
     * 
     * 用于追踪单个 Key 的修改次数
     */
    private long version;

    /**
     * 创建 PUT 事件的便捷方法
     */
    public static WatchEvent put(String key, String value, long revision, 
                                  long createRevision, long version) {
        return WatchEvent.builder()
                .type(EventType.PUT)
                .key(key)
                .value(value)
                .revision(revision)
                .createRevision(createRevision)
                .modRevision(revision)
                .version(version)
                .build();
    }

    /**
     * 创建 DELETE 事件的便捷方法
     */
    public static WatchEvent delete(String key, long revision) {
        return WatchEvent.builder()
                .type(EventType.DELETE)
                .key(key)
                .value(null)
                .revision(revision)
                .createRevision(0)
                .modRevision(revision)
                .version(0)
                .build();
    }

    /**
     * 检查是否是创建事件（新 Key）
     */
    public boolean isCreate() {
        return type == EventType.PUT && modRevision == createRevision;
    }

    /**
     * 检查是否是修改事件（已存在的 Key）
     */
    public boolean isModify() {
        return type == EventType.PUT && modRevision != createRevision;
    }
}
