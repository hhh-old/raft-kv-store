package com.raftkv.client.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON 序列化/反序列化工具
 *
 * 封装 ObjectMapper 实例，提供统一的 JSON 编解码能力。
 * 配置忽略未知字段，兼容服务端返回的额外字段。
 */
public class JsonCodec {

    private final ObjectMapper objectMapper;

    public JsonCodec() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public String writeValueAsString(Object value) throws Exception {
        return objectMapper.writeValueAsString(value);
    }

    public <T> T readValue(String content, Class<T> valueType) throws Exception {
        return objectMapper.readValue(content, valueType);
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
