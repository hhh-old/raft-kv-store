package com.raftkv.client.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Watch 事件 - 客户端版本
 * 
 * 与服务端的 WatchEvent 结构相同，用于客户端接收事件
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
        PUT,
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
     * 全局版本号
     */
    private long revision;

    /**
     * Key 首次创建时的版本号
     */
    private long createRevision;

    /**
     * Key 本次修改的版本号
     */
    private long modRevision;

    /**
     * Key 的本地版本
     */
    private long version;

    /**
     * 判断是否是新创建的 Key
     */
    public boolean isCreate() {
        return type == EventType.PUT && modRevision == createRevision;
    }

    /**
     * 判断是否是修改操作
     */
    public boolean isModify() {
        return type == EventType.PUT && modRevision > createRevision;
    }

    @Override
    public String toString() {
        return String.format("WatchEvent{type=%s, key='%s', revision=%d, version=%d}",
                type, key, revision, version);
    }
}
