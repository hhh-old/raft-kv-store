package com.raftkv.controller;

import com.raftkv.entity.WatchRequest;
import com.raftkv.service.EventHistory;
import com.raftkv.service.RaftKVService;
import com.raftkv.service.WatchManager;
import com.raftkv.service.WatchManager.WatchSubscription;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Watch 控制器 - 提供 etcd 风格的一步式 Watch API
 *
 * API：POST /watch/stream → 直接返回 SSE 流
 * 客户端在请求体中携带 key、prefix、startRevision，服务端立即建立 SSE 连接并推送事件。
 * 流中的第一条事件为 init，包含 watchId 和 currentRevision。
 *
 * 事件流格式（SSE）：
 * event: init\n
 * data: {"watchId":"xxx","currentRevision":42}\n\n
 *
 * event: put\n
 * data: {"type":"PUT","key":"/config/app","value":"v1","revision":43,...}\n\n
 *
 * event: delete\n
 * data: {"type":"DELETE","key":"/config/app","revision":44,...}\n\n
 */
@Slf4j
@RestController
@RequestMapping("/watch")
public class WatchController {

    @Autowired
    private WatchManager watchManager;

    @Autowired
    private RaftKVService raftKVService;

    @Autowired
    private EventHistory eventHistory;

    private static final long DEFAULT_SSE_TIMEOUT = 0L; // 0 = 永不超时，由心跳保活

    /**
     * 创建 Watch 并订阅事件流（etcd 风格一步式）
     *
     * 客户端在请求体中直接携带监听条件，服务端立即返回 SSE 流。
     *
     * 请求体（JSON）：
     * {
     *   "key": "/config/app",
     *   "prefix": false,
     *   "startRevision": 0,
     *   "watchId": "optional-id"
     * }
     *
     * 流中的第一条事件为 init，包含 watchId 和 currentRevision。
     */
    @PostMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseBodyEmitter watchStream(@RequestBody WatchRequest request) {
        log.info("Watch stream: key={}, prefix={}, startRevision={}",
                request.getKey(), request.isPrefix(), request.getStartRevision());

        // Leader 校验：非 Leader 节点拒绝建立 Watch 连接，防止客户端吸附在僵尸连接上
        if (!raftKVService.isLeader()) {
            String leaderUrl = raftKVService.getLeaderHttpUrl();
            log.warn("Rejecting watch request on non-leader node. Current leader: {}", leaderUrl);
            SseEmitter errorEmitter = new SseEmitter(0L);
            CompletableFuture.runAsync(() -> {
                try {
                    errorEmitter.send(SseEmitter.event()
                            .name("error")
                            .data("{\"code\":\"NOT_LEADER\",\"leaderUrl\":\"" + leaderUrl + "\"}"));
                    errorEmitter.complete();
                } catch (Exception e) {
                    errorEmitter.completeWithError(e);
                }
            });
            return errorEmitter;
        }

        String watchId = request.getWatchId() != null ?
                request.getWatchId() : java.util.UUID.randomUUID().toString();
        return buildWatchEmitter(watchId, request.getKey(), request.isPrefix(), request.getStartRevision());
    }

    /**
     * 构建 Watch SSE 连接的通用逻辑
     */
    private ResponseBodyEmitter buildWatchEmitter(String watchId, String key, boolean prefix, long startRevision) {
        SseEmitter emitter = new SseEmitter(DEFAULT_SSE_TIMEOUT);

        WatchSubscription subscription = new WatchSubscription(
                watchId, key, prefix, startRevision, emitter);

        // 注册生命周期回调（在返回前先设置，确保连接断开后能立即清理）
        emitter.onCompletion(() -> {
            // 触发条件：
            // 1. 客户端主动关闭连接（如关闭浏览器、断开 SSE）
            // 2. emitter.complete() 被调用且发送完成
            log.debug("SSE connection completed for watch: {}", watchId);
            watchManager.closeWatch(watchId, subscription);
        });
        emitter.onTimeout(() -> {
            // 触发条件：
            // 1. 达到设定的超时时间
            // 2. 当前设置为 0L（永不超时），所以这个回调通常不会触发
            // 3. 只有在超时时间 > 0 时才会触发
            log.warn("SSE connection timeout for watch: {}", watchId);
            watchManager.closeWatch(watchId, subscription);
        });
        emitter.onError(e -> {
            // 触发条件：
            // 1. 网络中断（客户端网络断开）
            // 2. 连接被重置（RST）
            // 3. 发送数据时 IOException
            // 4. 客户端提前关闭但服务器还在发送
            log.error("SSE connection error for watch: {}", watchId, e);
            watchManager.closeWatch(watchId, subscription);
        });

        // 异步完成初始化：先返回 emitter 让 Spring 立即发送响应头，
        // 再在后台线程发送 init、历史事件并注册连接，避免阻塞 HTTP 处理线程
        CompletableFuture.runAsync(() -> {
            try {
                // 1. 发送 init 事件宣告连接成功和当前 revision
                watchManager.sendInitialEvent(subscription);

                // 2. 发送历史事件（此时还未注册到索引，不会与实时事件竞争 lastSentRevision）
                watchManager.sendHistoricalEvents(subscription);

                // 3. 注册到 WatchManager，开始接收实时变化
                watchManager.registerConnection(subscription);

                // 4. Double-Check 补偿：再次发送历史事件
                // 填补步骤2~3之间可能产生的key写事件；safeSendEvent 的 lastSentRevision 保证不会重复发送历史事件
                watchManager.sendHistoricalEvents(subscription);

            } catch (Exception e) {
                log.warn("Failed to initialize watch stream {}: {}", watchId, e.getMessage());
                watchManager.closeWatch(watchId, subscription);
            }
        });

        return emitter;
    }

    /**
     * 取消 Watch 订阅
     */
    @DeleteMapping("/{watchId}")
    public ResponseEntity<Map<String, Object>> cancelWatch(@PathVariable String watchId) {
        log.info("Canceling watch: {}", watchId);
        WatchSubscription sub = watchManager.getSubscription(watchId);
        if (sub != null) {
            watchManager.closeWatch(watchId, sub);
        }
        Map<String, Object> response = new HashMap<>();
        response.put("watchId", watchId);
        response.put("message", "Watch canceled successfully");
        return ResponseEntity.ok(response);
    }

    /**
     * 获取当前全局版本号
     * 
     * 客户端断线重连时，可先调用此接口获取 currentRevision，
     * 然后用 startRevision = currentRevision + 1 重新创建 Watch，
     * 确保不漏掉断线期间发生的事件。
     */
    @GetMapping("/revision")
    public ResponseEntity<Map<String, Object>> getRevision() {
        Map<String, Object> response = new HashMap<>();
        response.put("revision", raftKVService.getCurrentRevision());
        response.put("activeWatches", watchManager.getActiveWatchCount());
        return ResponseEntity.ok(response);
    }

    /**
     * 获取当前最小可用 revision（管理接口）
     *
     * 客户端可据此判断断线后能否用当前 lastReceivedRevision 继续追赶历史事件。
     */
    @GetMapping("/compact-revision")
    public ResponseEntity<Map<String, Object>> getCompactRevision() {
        Map<String, Object> response = new HashMap<>();
        response.put("oldestRevision", eventHistory.getOldestRevision());
        response.put("latestRevision", eventHistory.getLatestRevision());
        response.put("eventCount", eventHistory.getEventCount());
        return ResponseEntity.ok(response);
    }

    /**
     * Watch 统计信息（管理接口）
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getWatchStats() {
        Map<String, Object> response = new HashMap<>();
        response.put("activeWatches", watchManager.getActiveWatchCount());
        return ResponseEntity.ok(response);
    }
}
