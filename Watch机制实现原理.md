# Raft KV Store Watch 机制实现原理

## 概述

Watch 机制实现了 etcd 风格的键值监听功能，允许客户端订阅特定 Key 或前缀的变化事件。当监听的 Key 被修改、删除或新增时，服务器会实时推送事件通知给客户端。

## 核心组件

### 1. WatchManager - Watch 管理器

**职责**：
- 管理所有活跃的 Watch 订阅（注册、取消、查询）
- 将事件路由到匹配的订阅者
- 支持精确匹配和前缀匹配
- 处理客户端连接断开

**数据结构**：
```java
// 活跃订阅表：watchId -> WatchSubscription
private final Map<String, WatchSubscription> activeWatches = new ConcurrentHashMap<>();

// 精确匹配索引：key -> List<watchId>
private final Map<String, List<String>> exactMatchIndex = new ConcurrentHashMap<>();

// 前缀匹配列表：存储所有前缀订阅
private final List<String> prefixWatchIds = new CopyOnWriteArrayList<>();
```

**线程安全设计**：
- `ConcurrentHashMap`：保证并发读写安全
- `CopyOnWriteArrayList`：读多写少场景，读操作无锁
- 异步事件发送：不阻塞事件生产者

### 2. EventHistory - 事件历史存储

**职责**：
- 存储最近 N 个事件（默认 10000 个）
- 支持按版本号范围查询
- 用于断线重连时的历史事件回放

**实现方式**：
```java
// 环形缓冲区：使用 ConcurrentLinkedQueue
private final ConcurrentLinkedQueue<WatchEvent> eventQueue = new ConcurrentLinkedQueue<>();

// 自动淘汰：当超过容量时，移除最旧事件
private void addEvent(WatchEvent event) {
    eventQueue.offer(event);
    if (eventCount.incrementAndGet() > capacity) {
        WatchEvent removed = eventQueue.poll();
        if (removed != null) {
            oldestRevision = removed.getRevision();
            eventCount.decrementAndGet();
        }
    }
}
```

### 3. RevisionManager - 全局版本号管理器

**职责**：
- 生成单调递增的 revision
- 保证事件顺序性

```java
private final AtomicLong revision = new AtomicLong(0);

public long nextRevision() {
    return revision.incrementAndGet();
}
```

### 4. WatchController - REST API 控制器

**接口设计**：

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /watch | 创建 Watch 订阅 |
| GET | /watch/{watchId} | 获取事件流（SSE） |
| DELETE | /watch/{watchId} | 取消 Watch |
| GET | /watch/revision | 获取当前 revision |

**创建 Watch 流程**：
```
1. 客户端 POST /watch
   Body: {"key": "/config/app", "prefix": false, "startRevision": 0}

2. 服务端创建 WatchSubscription
   - 生成唯一 watchId
   - 创建 SseEmitter（长连接）
   - 添加到索引结构

3. 返回响应
   {"watchId": "xxx", "currentRevision": 42, "streamUrl": "/watch/xxx"}

4. 客户端 GET /watch/{watchId}（SSE 长连接）
   - 接收实时事件推送
```

### 5. RaftKVClient - 客户端实现

**核心方法**：
```java
// 监听单个 Key
public WatchListener watchKey(String key, Consumer<WatchEvent> callback)

// 监听前缀
public WatchListener watchPrefix(String prefix, Consumer<WatchEvent> callback)

// 从历史版本开始监听
public WatchListener watchFromRevision(String key, long revision, Consumer<WatchEvent> callback)
```

**事件流处理**：
```java
private void startWatchStream(WatchListener listener) {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Accept", "text/event-stream")
        .GET()
        .build();
    
    // 长连接，持续读取 SSE 事件
    HttpResponse<InputStream> response = httpClient.send(request, ...);
    try (BufferedReader reader = new BufferedReader(...)) {
        while (!listener.isCancelled()) {
            String line = reader.readLine();
            // 解析 SSE 格式：event: xxx\ndata: xxx\n\n
            processEvent(eventType, eventData);
        }
    }
}
```

## 事件流转流程

### 完整数据流

```
┌─────────────┐    PUT/DELETE    ┌─────────────────┐
│   Client    │ ───────────────> │  KVController   │
│  (Writer)   │                  │  (REST API)     │
└─────────────┘                  └────────┬────────┘
                                          │
                                          v
                              ┌─────────────────────┐
                              │   RaftKVService     │
                              │   (apply operation) │
                              └────────┬────────────┘
                                       │
                                       v
                              ┌─────────────────────┐
                              │ KVStoreStateMachine │
                              │     (onApply)       │
                              └────────┬────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    v                  v                  v
           ┌─────────────┐   ┌─────────────────┐   ┌─────────────┐
           │  Update KV  │   │  EventHistory   │   │ WatchManager│
           │    Store    │   │  (save event)   │   │ (notifyAll) │
           └─────────────┘   └─────────────────┘   └──────┬──────┘
                                                          │
                              ┌───────────────────────────┼───────────┐
                              │                           │           │
                              v                           v           v
                        ┌──────────┐              ┌──────────┐  ┌──────────┐
                        │ Watch-1  │              │ Watch-2  │  │ Watch-3  │
                        │(exact)   │              │(exact)   │  │(prefix)  │
                        └────┬─────┘              └────┬─────┘  └────┬─────┘
                             │                         │            │
                             v                         v            v
                        ┌──────────┐              ┌──────────┐  ┌──────────┐
                        │SseEmitter│              │SseEmitter│  │SseEmitter│
                        │(Client-1)│              │(Client-2)│  │(Client-3)│
                        └──────────┘              └──────────┘  └──────────┘
```

### 事件匹配逻辑

```java
public void publishEvent(WatchEvent event) {
    String key = event.getKey();
    
    // 1. 精确匹配
    List<String> exactMatches = exactMatchIndex.get(key);
    if (exactMatches != null) {
        for (String watchId : exactMatches) {
            sendEvent(watchId, event);
        }
    }
    
    // 2. 前缀匹配
    for (String watchId : prefixWatchIds) {
        WatchSubscription sub = activeWatches.get(watchId);
        if (sub != null && key.startsWith(sub.getKey())) {
            sendEvent(watchId, event);
        }
    }
}
```

## 关键特性

### 1. 精确匹配 vs 前缀匹配

```java
// 精确匹配：只监听 /config/app
watchKey("/config/app", callback);

// 前缀匹配：监听 /config/ 下的所有 key
watchPrefix("/config/", callback);
// 包括：/config/app, /config/db/host, /config/redis/port 等
```

### 2. 历史事件回放

**场景**：客户端断线重连

```java
// 客户端记录最后接收的 revision
long lastRevision = 100;

// 重连时，从下一个版本开始监听
watchFromRevision("/config/app", lastRevision + 1, callback);

// 服务端先发送历史事件（101, 102, ...），再推送实时事件
```

**实现逻辑**：
```java
private void replayHistoryEvents(WatchSubscription sub, long startRevision) {
    List<WatchEvent> history = eventHistory.getEventsSince(startRevision);
    
    for (WatchEvent event : history) {
        if (matches(sub, event)) {  // 检查 key 是否匹配
            sendEvent(sub.getWatchId(), event);
        }
    }
}
```

### 3. 断线重连机制

**客户端策略**：
```java
public class WatchListener {
    private volatile boolean cancelled = false;
    private volatile long lastReceivedRevision = 0;
    
    public void onError(Exception e) {
        if (!cancelled) {
            // 自动重连
            scheduleReconnect(() -> {
                client.watchFromRevision(key, lastReceivedRevision + 1, callback);
            }, 1000);  // 1秒后重试
        }
    }
}
```

### 4. SSE 长连接

**协议格式**：
```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

event: init
data: {"watchId":"xxx","currentRevision":42}

event: put
data: {"type":"PUT","key":"/config/app","value":"v1","revision":43}

event: delete
data: {"type":"DELETE","key":"/config/app","revision":44}
```

## 性能优化

### 1. 索引优化

- **精确匹配**：O(1) 哈希表查找
- **前缀匹配**：O(n) 遍历，n = 前缀订阅数量
- **建议**：前缀订阅数量控制在合理范围（< 1000）

### 2. 异步发送

```java
// 事件发送不阻塞状态机
executorService.submit(() -> {
    try {
        emitter.send(event);
    } catch (IOException e) {
        // 发送失败，清理订阅
        closeWatch(watchId);
    }
});
```

### 3. 内存控制

- **EventHistory**：固定容量，自动淘汰旧事件
- **SseEmitter**：客户端断开时立即清理
- **WatchSubscription**：取消订阅时立即移除

## 使用示例

### 服务端（已集成）

```java
@RestController
public class WatchController {
    @Autowired
    private WatchManager watchManager;
    
    @PostMapping("/watch")
    public ResponseEntity<?> createWatch(@RequestBody WatchRequest request) {
        String watchId = watchManager.createWatch(request);
        return ResponseEntity.ok(Map.of(
            "watchId", watchId,
            "currentRevision", revisionManager.getCurrentRevision()
        ));
    }
}
```

### 客户端

```java
// 创建客户端
RaftKVClient client = new RaftKVClient("http://localhost:9081");

// 监听配置变化
WatchListener listener = client.watchKey("/config/database/url", event -> {
    System.out.println("Config changed: " + event.getKey() + " = " + event.getValue());
});

// 稍后取消监听
listener.cancel();
```

## 注意事项

1. **Revision 0**：表示从当前开始监听，不接收历史事件
2. **Revision 过期**：如果请求的版本号已被淘汰，需要全量重新获取
3. **网络分区**：Raft 保证一致性，但 Watch 连接可能中断，需要客户端重连
4. **内存限制**：EventHistory 容量有限，根据业务需求调整
