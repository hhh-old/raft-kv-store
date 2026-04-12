# Raft KV Store Client

一个功能完善的 Java 客户端，用于访问 Raft KV Store 分布式键值存储系统。

## ✨ 核心特性

- **幂等性支持**：基于 RequestId 的幂等去重机制，支持安全重试
- **自动重试**：内置指数退避重试策略
- **Leader 自动重定向**：自动跟随 Leader 切换，对应用层透明
- **线程安全**：可在多线程环境中安全使用
- **健康检查**：检测服务可用性
- **零外部 HTTP 依赖**：使用 Java 11+ 内置 HttpClient

## 📦 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>com.raftkv</groupId>
    <artifactId>raft-kv-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. 创建客户端

```java
RaftKVClient client = RaftKVClient.builder()
        .serverUrls(Arrays.asList(
                "http://localhost:9081",
                "http://localhost:9082",
                "http://localhost:9083"
        ))
        .maxRetries(3)
        .timeoutSeconds(3)
        .build();
```

### 3. 基本操作

```java
// PUT
KVResponse putResponse = client.put("name", "Alice");

// GET
KVResponse getResponse = client.get("name");
System.out.println("name = " + getResponse.getValue());

// DELETE
KVResponse deleteResponse = client.delete("name");
```

## 🔧 构建

```bash
cd client
mvn clean compile
```

## 📖 使用示例

运行完整示例：

```bash
mvn exec:java -Dexec.mainClass="com.raftkv.client.example.ClientExample"
```

## 📚 文档

- [客户端使用指南](客户端使用指南.md) - 详细的使用文档
- [幂等性实现总结](../幂等性实现总结.md) - 服务端幂等性实现
- [SOFAJRaft学习指南](../SOFAJRaft学习指南.md) - Raft 协议详解

## 🎯 高级功能

### 幂等性支持

```java
// 指定 requestId（支持幂等重试）
String requestId = "order-123-payment";
KVResponse response = client.put("order:123:status", "paid", requestId);

// 如果超时，安全重试（使用相同 requestId）
KVResponse retryResponse = client.put("order:123:status", "paid", requestId);
```

### 健康检查

```java
boolean healthy = client.healthCheck();
```

### 集群统计

```java
String stats = client.getStats();
System.out.println(stats);
```

## ⚙️ 配置选项

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| serverUrls | List<String> | 必填 | 服务器地址列表 |
| maxRetries | int | 3 | 最大重试次数 |
| timeoutSeconds | int | 3 | 请求超时时间（秒） |

## 🏗️ 项目结构

```
client/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/raftkv/client/
│   │   │       ├── RaftKVClient.java           # 核心客户端类
│   │   │       ├── entity/
│   │   │       │   └── KVResponse.java         # 响应实体
│   │   │       └── example/
│   │   │           └── ClientExample.java      # 使用示例
│   │   └── resources/
│   │       └── logback.xml                     # 日志配置
│   └── test/
│       └── java/
└── 客户端使用指南.md
```

## 🔍 工作原理

### 请求流程

```
客户端              服务端（Follower）      服务端（Leader）
  |                        |                      |
  |--- PUT request ------->|                      |
  |                        |-- NOT_LEADER -------->|
  |<-- 重定向响应 ----------|                      |
  |                                               |
  |------------------- PUT request -------------->|
  |                                               |-- Raft 日志复制
  |                                               |-- 应用到状态机
  |<-- 成功响应 ----------------------------------|
```

### 重试流程

```
第 1 次请求失败
  ↓
等待 100ms（指数退避）
  ↓
第 2 次请求失败
  ↓
等待 200ms
  ↓
第 3 次请求失败
  ↓
等待 400ms
  ↓
第 4 次请求
  ↓
成功 / 抛出异常
```

## 💡 最佳实践

### 1. RequestId 生成

```java
// ✅ 推荐：基于业务键
String requestId = "order-" + orderId + "-payment";

// ❌ 不推荐：每次生成不同的 requestId
String requestId = UUID.randomUUID().toString();
```

### 2. 客户端复用

```java
// ✅ 推荐：单例或依赖注入
@Component
public class KVService {
    private final RaftKVClient client;
    
    public KVService() {
        this.client = RaftKVClient.builder()
                .serverUrls(Arrays.asList("http://localhost:9081"))
                .build();
    }
}

// ❌ 不推荐：每次请求都创建新客户端
```

### 3. 错误处理

```java
try {
    KVResponse response = client.put("key", "value");
    if (response.isSuccess()) {
        // 成功
    } else {
        // 业务错误
        log.error("Operation failed: {}", response.getError());
    }
} catch (RuntimeException e) {
    // 网络错误或超过重试次数
    log.error("Request failed", e);
}
```

## 🐛 故障排查

### 连接失败

```bash
# 检查服务是否运行
curl http://localhost:9081/kv/health
```

### 超时错误

```java
// 增加超时时间
RaftKVClient client = RaftKVClient.builder()
        .timeoutSeconds(10)
        .build();
```

### 查看日志

客户端日志会输出详细的请求和重试信息：

```
2026-04-11 18:50:00.123 [main] DEBUG com.raftkv.client.RaftKVClient - PUT request: key=name, value=Alice, requestId=req-001
2026-04-11 18:50:00.456 [main] INFO  com.raftkv.client.RaftKVClient - Redirecting to Leader: http://localhost:9082
```

## 📝 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📧 联系方式

如有问题，请提交 Issue 或联系开发团队。
